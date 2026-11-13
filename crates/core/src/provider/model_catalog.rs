//! Refreshable model-capability catalog.
//!
//! Capability facts — context windows, output caps, modality flags — change
//! faster than engine binaries ship. The catalog lets a deployment update them
//! without a release: a JSON document is cached on disk under the engine
//! config directory and optionally revalidated from a configured HTTPS source
//! (ETag conditional requests, fixed refresh interval). Every lookup overlays
//! the best-matching entry's fields onto the provider's BUILT-IN capabilities,
//! which stay the authoritative offline baseline — an absent catalog, an
//! unreachable source, or an unmatched model all resolve to exactly the
//! built-in answer.
//!
//! Wiring: wrap any provider with [`with_model_catalog`]; the decorator only
//! reshapes `capabilities()` and binds media policies on unbound direct requests.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use crate::runtime_values::{
    runtime_codec, runtime_json, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder,
    RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};
use crate::types::abort::AbortSignal;
use crate::types::errors::{ProviderError, ProviderErrorKind};
use crate::types::provider::{
    AuthHeaders, ImageLimits, ModelCapabilities, ModelProvider, ModelRequest, ModelStream,
    ModelStreamEvent, ProviderCapabilities, ProviderInfo, DEFAULT_MAX_MEDIA_BLOCKS,
};

/// Offline native-tool facts. Exact declarations override these compatibility
/// defaults; unlisted models remain on the ordinary function-tool protocol.
/// This lookup deliberately does not infer capabilities from version numbers.
pub(crate) fn native_deferred_tools_default(provider: &str, model: &RuntimeString) -> bool {
    let Ok(model) = model.try_to_utf8() else {
        return false;
    };
    let models: &[&str] = match provider {
        "anthropic" => &[
            "claude-sonnet-4-5",
            "claude-sonnet-4-5-20250929",
            "claude-sonnet-4-5-latest",
            "claude-sonnet-4-6",
            "claude-sonnet-4-7",
            "claude-sonnet-4-8",
            "claude-sonnet-4-9",
            "claude-sonnet-5",
            "claude-sonnet-5-5",
            "claude-opus-4-5",
            "claude-opus-4-5-20251101",
            "claude-opus-4-5-latest",
            "claude-opus-4-6",
            "claude-opus-4-7",
            "claude-opus-4-8",
            "claude-opus-4-9",
            "claude-opus-5",
            "claude-opus-5-5",
        ],
        "openai" => &[
            "gpt-5.4",
            "gpt-5.4-mini",
            "gpt-5.4-nano",
            "gpt-5.5",
            "gpt-5.6-sol",
            "gpt-5.6-terra",
            "gpt-5.6-luna",
            "gpt-6-astra",
            "gpt-6-sol",
            "gpt-6-luna",
            "gpt-6.1-sol",
            "gpt-6.1-terra",
        ],
        _ => &[],
    };
    let exact = if provider == "anthropic" {
        model.strip_suffix("[1m]").unwrap_or(&model)
    } else {
        &model
    };
    models.contains(&exact)
}

/// Environment variable naming the remote catalog document URL. Unset means
/// the engine never fetches — the disk cache and built-ins still apply.
pub const MODEL_CATALOG_URL_ENV: &str = "KLAUS_MODEL_CATALOG_URL";
/// Revalidation cadence once a source is configured.
pub const CATALOG_REFRESH_INTERVAL_MS: i64 = 4 * 60 * 60 * 1000;
/// Cache file name under the engine config directory.
pub const CATALOG_CACHE_FILE: &str = "model-catalog.json";
/// The document schema this engine understands.
pub const CATALOG_SCHEMA_VERSION: u32 = 1;

/// Field-level capability overrides. Absent fields keep the built-in value.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CapabilityOverrides {
    pub native_deferred_tools: Option<bool>,
    pub streaming: Option<bool>,
    pub tools: Option<bool>,
    pub thinking: Option<bool>,
    pub structured_output: Option<bool>,
    pub web_search: Option<bool>,
    pub prompt_cache_control: Option<bool>,
    pub context_window: Option<u64>,
    pub max_output_tokens: Option<u64>,
    pub native_pdf: Option<bool>,
    pub vision: Option<bool>,
    pub image_limits: Option<ImageLimits>,
    pub max_media_blocks: Option<NonZeroU32>,
}

impl CapabilityOverrides {
    /// Overlay the present fields onto `base`.
    pub fn apply(&self, base: &mut ProviderCapabilities) {
        if let Some(value) = self.streaming {
            base.streaming = value;
        }
        if let Some(value) = self.tools {
            base.tools = value;
        }
        if let Some(value) = self.thinking {
            base.thinking = value;
        }
        if let Some(value) = self.structured_output {
            base.structured_output = value;
        }
        if let Some(value) = self.web_search {
            base.web_search = value;
        }
        if let Some(value) = self.prompt_cache_control {
            base.prompt_cache_control = value;
        }
        if let Some(value) = self.context_window {
            base.context_window = value;
        }
        if let Some(value) = self.max_output_tokens {
            base.max_output_tokens = value;
        }
        if let Some(value) = self.native_pdf {
            base.native_pdf = value;
        }
        if let Some(value) = self.vision {
            base.vision = value;
        }
        if let Some(value) = &self.image_limits {
            base.image_limits.get_or_insert_default().overlay(value);
        }
        if let Some(value) = self.max_media_blocks {
            base.max_media_blocks = Some(value);
        }
    }
}

impl RuntimeEncode for CapabilityOverrides {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = self.native_deferred_tools {
            object.field("nativeDeferredTools", &value)?;
        }
        if let Some(value) = &self.streaming {
            object.field("streaming", value)?;
        }
        if let Some(value) = &self.tools {
            object.field("tools", value)?;
        }
        if let Some(value) = &self.thinking {
            object.field("thinking", value)?;
        }
        if let Some(value) = &self.structured_output {
            object.field("structuredOutput", value)?;
        }
        if let Some(value) = &self.web_search {
            object.field("webSearch", value)?;
        }
        if let Some(value) = &self.prompt_cache_control {
            object.field("promptCacheControl", value)?;
        }
        if let Some(value) = &self.context_window {
            object.field("contextWindow", value)?;
        }
        if let Some(value) = &self.max_output_tokens {
            object.field("maxOutputTokens", value)?;
        }
        if let Some(value) = &self.native_pdf {
            object.field("nativePdf", value)?;
        }
        if let Some(value) = &self.vision {
            object.field("vision", value)?;
        }
        if let Some(value) = &self.image_limits {
            object.field("imageLimits", value)?;
        }
        if let Some(value) = &self.max_media_blocks {
            object.field("maxMediaBlocks", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for CapabilityOverrides {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        // Permissive: a newer document may carry fields this engine ignores.
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                native_deferred_tools: object.optional("nativeDeferredTools", &[])?,
                streaming: object.optional("streaming", &[])?,
                tools: object.optional("tools", &[])?,
                thinking: object.optional("thinking", &[])?,
                structured_output: object.optional("structuredOutput", &[])?,
                web_search: object.optional("webSearch", &[])?,
                prompt_cache_control: object.optional("promptCacheControl", &[])?,
                context_window: object.optional("contextWindow", &[])?,
                max_output_tokens: object.optional("maxOutputTokens", &[])?,
                native_pdf: object.optional("nativePdf", &[])?,
                vision: object.optional("vision", &[])?,
                image_limits: object.optional("imageLimits", &[])?,
                max_media_blocks: object.optional("maxMediaBlocks", &[])?,
            })
        })
    }
}

/// How an entry's `model` field matches a requested model id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogMatch {
    Exact,
    Prefix,
}

/// One catalog entry: which provider/model it targets and what it overrides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogEntry {
    /// Provider identity the entry is scoped to (e.g. `anthropic`).
    pub provider: String,
    /// Model id (exact) or model-id prefix, per `match_kind`.
    pub model: String,
    pub match_kind: CatalogMatch,
    pub overrides: CapabilityOverrides,
}

impl CatalogEntry {
    fn matches(&self, provider: &str, model: &str) -> bool {
        if self.provider != provider {
            return false;
        }
        match self.match_kind {
            CatalogMatch::Exact => self.model == model,
            CatalogMatch::Prefix => model.starts_with(&self.model),
        }
    }

    /// Specificity for best-match selection: exact beats any prefix; longer
    /// prefixes beat shorter ones.
    fn specificity(&self) -> usize {
        match self.match_kind {
            CatalogMatch::Exact => usize::MAX,
            CatalogMatch::Prefix => self.model.len(),
        }
    }
}

impl RuntimeEncode for CatalogEntry {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("provider", &RuntimeString::from(self.provider.as_str()))?;
        object.field("model", &RuntimeString::from(self.model.as_str()))?;
        if self.match_kind == CatalogMatch::Prefix {
            object.field("match", &RuntimeString::from("prefix"))?;
        }
        object.field("capabilities", &self.overrides)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for CatalogEntry {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            let provider: RuntimeString = object.required("provider", &[])?;
            let model: RuntimeString = object.required("model", &[])?;
            let match_kind: Option<RuntimeString> = object.optional("match", &[])?;
            let match_kind = match match_kind {
                None => CatalogMatch::Exact,
                Some(kind) if kind == "exact" => CatalogMatch::Exact,
                Some(kind) if kind == "prefix" => CatalogMatch::Prefix,
                Some(_) => {
                    return Err(RuntimeCodecError::new(
                        crate::runtime_values::RuntimeCodecErrorKind::UnknownEnumVariant,
                    ))
                }
            };
            let overrides: Option<CapabilityOverrides> = object.optional("capabilities", &[])?;
            let provider = provider.try_to_utf8().map_err(|_| {
                RuntimeCodecError::new(crate::runtime_values::RuntimeCodecErrorKind::ExpectedString)
            })?;
            let model = model.try_to_utf8().map_err(|_| {
                RuntimeCodecError::new(crate::runtime_values::RuntimeCodecErrorKind::ExpectedString)
            })?;
            Ok(Self {
                provider,
                model,
                match_kind,
                overrides: overrides.unwrap_or_default(),
            })
        })
    }
}

/// The catalog document: a schema version plus entries. Individual entries
/// that fail to decode are SKIPPED (a partially newer document still serves
/// the rest), and a document with a newer major schema is ignored wholesale.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CatalogDocument {
    pub entries: Vec<CatalogEntry>,
}

impl CatalogDocument {
    fn parse(value: &RuntimeJson) -> Result<Self, String> {
        let object = value
            .as_object()
            .ok_or("catalog document must be an object")?;
        let schema_version = object
            .get("schemaVersion")
            .and_then(|v| v.as_f64())
            .ok_or("catalog document is missing schemaVersion")?;
        if schema_version as u32 > CATALOG_SCHEMA_VERSION {
            return Err(format!(
                "catalog schemaVersion {schema_version} is newer than the supported {CATALOG_SCHEMA_VERSION}"
            ));
        }
        let mut entries = Vec::new();
        if let Some(raw_entries) = object.get("entries").and_then(|v| v.as_array()) {
            for raw in raw_entries {
                // Tolerant: a malformed entry never poisons the document.
                if let Ok(entry) = runtime_codec::decode_value::<CatalogEntry>(raw) {
                    entries.push(entry);
                }
            }
        }
        Ok(Self { entries })
    }

    fn encode(&self) -> RuntimeJson {
        let mut object = crate::runtime_values::RuntimeObject::new();
        object.insert(
            "schemaVersion",
            RuntimeJson::from(CATALOG_SCHEMA_VERSION as f64),
        );
        let entries: Vec<RuntimeJson> = self
            .entries
            .iter()
            .filter_map(|entry| runtime_codec::encode_value(entry).ok())
            .collect();
        object.insert("entries", RuntimeJson::Array(entries));
        RuntimeJson::Object(object)
    }
}

/// Mutable catalog state guarded by the read/write lock.
#[derive(Debug, Default)]
struct CatalogState {
    document: CatalogDocument,
    etag: Option<String>,
    checked_at_ms: i64,
}

/// The refresh outcome of one revalidation round-trip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogRefresh {
    /// A new document was fetched, applied, and cached.
    Updated,
    /// The source confirmed the cached document is current (HTTP 304).
    NotModified,
}

/// A shared, refreshable model-capability catalog.
#[derive(Debug)]
pub struct ModelCatalog {
    state: RwLock<CatalogState>,
    /// Cache file location; `None` disables persistence (tests, ephemeral).
    cache_path: Option<String>,
}

impl ModelCatalog {
    /// An empty catalog that never persists — every lookup is the built-in.
    pub fn empty() -> Arc<Self> {
        Arc::new(Self {
            state: RwLock::new(CatalogState::default()),
            cache_path: None,
        })
    }

    /// Load the cached catalog from the engine config directory (explicit
    /// override, else `KLAUS_CONFIG_DIR`, else `~/.klaus`). A missing or
    /// unreadable cache yields an empty catalog; nothing here touches the
    /// network.
    pub fn load(config_dir: Option<&str>) -> Arc<Self> {
        let dir = config_dir
            .map(str::to_string)
            .or_else(|| {
                std::env::var("KLAUS_CONFIG_DIR")
                    .ok()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            })
            .unwrap_or_else(|| format!("{}/.klaus", home_dir()));
        let cache_path = format!("{dir}/{CATALOG_CACHE_FILE}");
        let state = read_cache(&cache_path).unwrap_or_default();
        Arc::new(Self {
            state: RwLock::new(state),
            cache_path: Some(cache_path),
        })
    }

    /// Overlay the best-matching entry (exact beats prefix, longer prefixes
    /// beat shorter) onto the provider's built-in capabilities.
    pub fn resolve(
        &self,
        provider: &str,
        model: &str,
        base: ProviderCapabilities,
    ) -> ProviderCapabilities {
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let best = state
            .document
            .entries
            .iter()
            .filter(|entry| entry.matches(provider, model))
            .max_by_key(|entry| entry.specificity());
        let mut resolved = base;
        if let Some(entry) = best {
            entry.overrides.apply(&mut resolved);
        }
        resolved
    }

    fn native_deferred_tools(&self, provider: &str, model: &str) -> Option<bool> {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .document
            .entries
            .iter()
            .rev()
            .find(|entry| entry.match_kind == CatalogMatch::Exact && entry.matches(provider, model))
            .and_then(|entry| entry.overrides.native_deferred_tools)
    }

    /// The configured remote source, when any.
    pub fn configured_source() -> Option<String> {
        std::env::var(MODEL_CATALOG_URL_ENV)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    }

    /// Whether a refresh is due for the given wall-clock time.
    pub fn is_stale(&self, now_ms: i64) -> bool {
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        now_ms.saturating_sub(state.checked_at_ms) >= CATALOG_REFRESH_INTERVAL_MS
    }

    /// Revalidate against the configured source when one exists AND the
    /// cached document has aged past the refresh interval. `None` means no
    /// round-trip was needed.
    pub async fn refresh_from_env_if_stale(
        self: &Arc<Self>,
        http: &reqwest::Client,
    ) -> Option<Result<CatalogRefresh, String>> {
        let url = Self::configured_source()?;
        if !self.is_stale(chrono::Utc::now().timestamp_millis()) {
            return None;
        }
        Some(self.refresh(http, &url).await)
    }

    /// [`Self::refresh_from_env_if_stale`] with an engine-owned HTTP client,
    /// for hosts that do not hold one.
    pub async fn refresh_from_env_default(
        self: &Arc<Self>,
    ) -> Option<Result<CatalogRefresh, String>> {
        let http = reqwest::Client::new();
        self.refresh_from_env_if_stale(&http).await
    }

    /// One conditional revalidation round-trip: `If-None-Match` with the
    /// cached ETag; 304 only touches the check timestamp, 200 replaces the
    /// document and rewrites the disk cache. Failures leave the cached
    /// document untouched.
    pub async fn refresh(
        &self,
        http: &reqwest::Client,
        url: &str,
    ) -> Result<CatalogRefresh, String> {
        let etag = {
            let state = self
                .state
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.etag.clone()
        };
        let mut request = http.get(url);
        if let Some(etag) = &etag {
            request = request.header("If-None-Match", etag);
        }
        let response = request
            .send()
            .await
            .map_err(|error| format!("model catalog fetch failed: {error}"))?;
        let status = response.status();
        if status.as_u16() == 304 {
            let now = chrono::Utc::now().timestamp_millis();
            {
                let mut state = self
                    .state
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.checked_at_ms = now;
            }
            self.persist();
            return Ok(CatalogRefresh::NotModified);
        }
        if !status.is_success() {
            return Err(format!("model catalog fetch failed with HTTP {status}"));
        }
        let new_etag = response
            .headers()
            .get("etag")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let body = response
            .text()
            .await
            .map_err(|error| format!("model catalog body read failed: {error}"))?;
        let value = runtime_json::parse(&RuntimeString::from(body.as_str()))
            .map_err(|_| "model catalog document is not valid JSON".to_string())?;
        let document = CatalogDocument::parse(&value)?;
        {
            let mut state = self
                .state
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.document = document;
            state.etag = new_etag;
            state.checked_at_ms = chrono::Utc::now().timestamp_millis();
        }
        self.persist();
        Ok(CatalogRefresh::Updated)
    }

    /// Best-effort atomic cache rewrite; persistence failures never fail a
    /// refresh (the in-memory document already advanced).
    fn persist(&self) {
        let Some(path) = &self.cache_path else {
            return;
        };
        let payload = {
            let state = self
                .state
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut object = crate::runtime_values::RuntimeObject::new();
            object.insert(
                "schemaVersion",
                RuntimeJson::from(CATALOG_SCHEMA_VERSION as f64),
            );
            if let Some(etag) = &state.etag {
                object.insert("etag", RuntimeJson::String(etag.as_str().into()));
            }
            object.insert("checkedAtMs", RuntimeJson::from(state.checked_at_ms as f64));
            object.insert("document", state.document.encode());
            runtime_json::stringify(&RuntimeJson::Object(object))
        };
        let bytes = payload.to_node_utf8();
        if let Some(parent) = std::path::Path::new(path).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let tmp = format!("{path}.tmp");
        if std::fs::write(&tmp, bytes).is_ok() {
            let _ = std::fs::rename(&tmp, path);
        }
    }

    /// Test/introspection: how many entries the catalog currently holds.
    pub fn entry_count(&self) -> usize {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .document
            .entries
            .len()
    }
}

/// Read and parse the disk cache; any failure means "no cache".
fn read_cache(path: &str) -> Option<CatalogState> {
    let bytes = std::fs::read(path).ok()?;
    let text = String::from_utf8(bytes).ok()?;
    let value = runtime_json::parse(&RuntimeString::from(text.as_str())).ok()?;
    let object = value.as_object()?;
    let document = object
        .get("document")
        .and_then(|d| CatalogDocument::parse(d).ok())?;
    Some(CatalogState {
        document,
        etag: object
            .get("etag")
            .and_then(|v| v.as_string())
            .and_then(|s| s.try_to_utf8().ok()),
        checked_at_ms: object
            .get("checkedAtMs")
            .and_then(|v| v.as_f64())
            .map(|v| v as i64)
            .unwrap_or(0),
    })
}

fn home_dir() -> String {
    #[allow(deprecated)]
    std::env::home_dir()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// Wrap a provider so its `capabilities()` answers pass through the catalog
/// overlay. Unbound direct requests receive the selected model's media facts;
/// captured requests retain their already frozen policies.
pub fn with_model_catalog(
    inner: Arc<dyn ModelProvider>,
    catalog: Arc<ModelCatalog>,
) -> Arc<dyn ModelProvider> {
    Arc::new(CatalogModelProvider { inner, catalog })
}

struct CatalogModelProvider {
    inner: Arc<dyn ModelProvider>,
    catalog: Arc<ModelCatalog>,
}

/// Freeze the complete capability values, not just the image/PDF flags. The
/// transport and its authorization remain live; only model facts are captured.
pub(crate) fn capture_model_provider(
    inner: Arc<dyn ModelProvider>,
    models: &[RuntimeString],
) -> Arc<dyn ModelProvider> {
    let capabilities = inner.capture_capabilities(models);
    let model_tool_budgets = capabilities
        .keys()
        .map(|model| (model.clone(), inner.uses_model_tool_budget(model)))
        .collect();
    Arc::new(CapturedModelProvider {
        info: inner.info().clone(),
        capabilities,
        model_tool_budgets,
        metadata: models
            .iter()
            .map(|model| {
                let mut metadata = inner.model_metadata(model);
                metadata.comp_hash = inner.compaction_compatibility_hash(model);
                metadata.effective_context_window_percent =
                    Some(i64::from(inner.effective_context_window_percent(model)));
                metadata.auto_compact_token_limit = inner.model_auto_compact_token_limit(model);
                (model.clone(), metadata)
            })
            .collect(),
        inner,
    })
}

struct CapturedModelProvider {
    inner: Arc<dyn ModelProvider>,
    info: ProviderInfo,
    capabilities: HashMap<RuntimeString, ProviderCapabilities>,
    model_tool_budgets: HashMap<RuntimeString, bool>,
    metadata: HashMap<RuntimeString, ModelCapabilities>,
}

pub(crate) fn uncaptured_model_error(model: &RuntimeString) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::InvalidRequest,
        message: RuntimeString::from("model ")
            .concat(model)
            .concat(&RuntimeString::from(
                " is not bound to the captured inference provider",
            )),
        status: None,
        code: None,
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
        retry_after_ms: None,
        request_id: None,
    }
}

pub(crate) fn uncaptured_model_stream(model: &RuntimeString) -> ModelStream {
    Box::pin(futures::stream::iter([ModelStreamEvent::Error {
        error: uncaptured_model_error(model),
    }]))
}

#[async_trait::async_trait]
impl ModelProvider for CapturedModelProvider {
    async fn native_checkpoint_compatibility(
        &self,
        checkpoint: &crate::schemas::message::NativeCompactionCheckpoint,
        model: &RuntimeString,
        signal: &crate::types::abort::AbortSignal,
    ) -> Result<bool, crate::types::errors::ProviderError> {
        if checkpoint
            .comp_hash
            .as_ref()
            .zip(self.compaction_compatibility_hash(model))
            .is_some_and(|(saved, current)| saved != &current)
        {
            return Ok(false);
        }
        self.inner
            .native_checkpoint_compatibility(checkpoint, model, signal)
            .await
    }

    fn native_compaction_context(
        &self,
        model: &RuntimeString,
    ) -> Option<crate::types::provider::NativeCompactionContext> {
        if self.remote_compaction_support(model)
            == crate::types::provider::RemoteCompactionSupport::Unsupported
        {
            return None;
        }
        self.inner
            .native_compaction_context(model)
            .map(|mut context| {
                context.identity = format!(
                    "{:?}:{:?}",
                    context.identity,
                    self.compaction_compatibility_hash(model)
                )
                .into();
                context
            })
    }

    fn remote_compaction_support(
        &self,
        model: &RuntimeString,
    ) -> crate::types::provider::RemoteCompactionSupport {
        if !self.has_model_capabilities(model) {
            return crate::types::provider::RemoteCompactionSupport::Unsupported;
        }
        self.inner.remote_compaction_support(model)
    }
    fn compaction_stream_max_retries(&self) -> Option<u32> {
        self.inner.compaction_stream_max_retries()
    }
    fn supports_compaction_model_fallback(&self) -> bool {
        self.inner.supports_compaction_model_fallback()
    }
    async fn compact_remote(
        &self,
        request: ModelRequest,
        signal: AbortSignal,
    ) -> Result<crate::types::provider::RemoteCompactionResult, ProviderError> {
        if !self.has_model_capabilities(&request.model) {
            return Err(crate::provider::native_compaction::unsupported_checkpoint());
        }
        let mut result = self.inner.compact_remote(request.clone(), signal).await?;
        result.checkpoint.comp_hash = self.compaction_compatibility_hash(&request.model);
        Ok(result)
    }

    async fn compact_remote_with_usage(
        &self,
        request: crate::types::provider::ModelRequest,
        signal: crate::types::abort::AbortSignal,
        idle_ms: u64,
        observer: crate::types::provider::NativeCompactionUsageObserver,
        expected_scope: Option<RuntimeString>,
    ) -> Result<crate::types::provider::RemoteCompactionResult, crate::types::errors::ProviderError>
    {
        if !self.has_model_capabilities(&request.model) {
            return Err(crate::provider::native_compaction::unsupported_checkpoint());
        }
        let model = request.model.clone();
        let mut result = self
            .inner
            .compact_remote_with_usage(request, signal, idle_ms, observer, expected_scope)
            .await?;
        result.checkpoint.comp_hash = self.compaction_compatibility_hash(&model);
        Ok(result)
    }

    async fn compact_remote_with_idle_timeout(
        &self,
        request: ModelRequest,
        signal: AbortSignal,
        idle_ms: u64,
    ) -> Result<crate::types::provider::RemoteCompactionResult, ProviderError> {
        if !self.has_model_capabilities(&request.model) {
            return Err(crate::provider::native_compaction::unsupported_checkpoint());
        }
        let model = request.model.clone();
        let mut result = self
            .inner
            .compact_remote_with_idle_timeout(request, signal, idle_ms)
            .await?;
        result.checkpoint.comp_hash = self.compaction_compatibility_hash(&model);
        Ok(result)
    }
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn uses_model_tool_budget(&self, model: &RuntimeString) -> bool {
        self.model_tool_budgets.get(model).copied().unwrap_or(false)
    }

    fn model_metadata(&self, model: &RuntimeString) -> ModelCapabilities {
        self.metadata.get(model).cloned().unwrap_or_default()
    }
    fn offline_model_catalog(&self) -> Vec<(RuntimeString, ModelCapabilities)> {
        self.inner.offline_model_catalog()
    }
    fn approval_review_preferred_model(&self) -> RuntimeString {
        self.inner.approval_review_preferred_model()
    }
    fn uses_official_api_catalog(&self) -> bool {
        self.inner.uses_official_api_catalog()
    }
    fn managed_review_cloud_auth_source(
        &self,
    ) -> Option<Arc<dyn crate::engine::approval_requirements_cloud::ManagedReviewCloudAuthSource>>
    {
        self.inner.managed_review_cloud_auth_source()
    }
    fn classifier_auth_changes(
        &self,
    ) -> Option<tokio::sync::watch::Receiver<crate::provider::auth::CredentialChangeState>> {
        self.inner.classifier_auth_changes()
    }
    fn approval_request_tokens(&self, request: &ModelRequest) -> Result<usize, RuntimeString> {
        self.inner.approval_request_tokens(request)
    }
    fn approval_request_tokens_with_metadata(
        &self,
        request: &ModelRequest,
        metadata: ModelCapabilities,
        raw_effort: Option<RuntimeString>,
    ) -> Result<usize, RuntimeString> {
        self.inner
            .approval_request_tokens_with_metadata(request, metadata, raw_effort)
    }

    fn create_approval_classifier_transport(
        &self,
        attribution: crate::provider::approval_classifier_transport::ClassifierAttribution,
        target: Option<crate::provider::approval_classifier_transport::ClassifierTarget>,
    ) -> Option<Arc<dyn crate::provider::approval_classifier_transport::ApprovalClassifierTransport>>
    {
        self.inner
            .create_approval_classifier_transport(attribution, target)
    }

    fn review_target_provider(&self) -> Option<Arc<dyn ModelProvider>> {
        Some(
            self.inner
                .review_target_provider()
                .unwrap_or_else(|| self.inner.clone()),
        )
    }

    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        self.capabilities
            .get(model)
            .cloned()
            .unwrap_or(ProviderCapabilities {
                supports_image_detail_original: false,
                streaming: false,
                tools: false,
                thinking: false,
                structured_output: false,
                web_search: false,
                prompt_cache_control: false,
                context_window: 0,
                max_output_tokens: 0,
                native_pdf: false,
                vision: false,
                image_limits: None,
                max_media_blocks: None,
            })
    }

    fn capture_capabilities(
        &self,
        models: &[RuntimeString],
    ) -> HashMap<RuntimeString, ProviderCapabilities> {
        models
            .iter()
            .filter_map(|model| {
                self.capabilities
                    .get(model)
                    .map(|caps| (model.clone(), caps.clone()))
            })
            .collect()
    }

    fn has_model_capabilities(&self, model: &RuntimeString) -> bool {
        self.capabilities.contains_key(model)
    }

    fn stream(&self, mut req: ModelRequest, signal: AbortSignal) -> ModelStream {
        let Some(capabilities) = self.capabilities.get(&req.model) else {
            return uncaptured_model_stream(&req.model);
        };
        let metadata = self.model_metadata(&req.model);
        if let Err(error) =
            crate::provider::native_media::validate_request(&req, &metadata.media_input_mime_types)
        {
            return crate::provider::native_media::rejected_stream(error);
        }
        req.bind_media_capabilities(capabilities);
        req.native_deferred_tools = metadata.native_deferred_tools;
        self.inner.stream(req, signal)
    }

    fn estimate_tokens(&self, req: &ModelRequest) -> Option<u64> {
        self.inner.estimate_tokens(req)
    }

    async fn resolve_auth(&self) -> Option<Result<AuthHeaders, ProviderError>> {
        self.inner.resolve_auth().await
    }

    fn credential_account_id(&self) -> Option<String> {
        self.inner.credential_account_id()
    }

    async fn rotate_credential(
        &self,
        failed_id: &str,
    ) -> Option<crate::provider::rotation::RotateOutcome> {
        self.inner.rotate_credential(failed_id).await
    }

    fn has_prewarm_endpoints(&self) -> bool {
        self.inner.has_prewarm_endpoints()
    }

    fn prewarm_transport(
        &self,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Vec<crate::provider::prewarm::PrewarmOutcome>> + Send>,
    > {
        self.inner.prewarm_transport()
    }
}

#[async_trait::async_trait]
impl ModelProvider for CatalogModelProvider {
    async fn native_checkpoint_compatibility(
        &self,
        checkpoint: &crate::schemas::message::NativeCompactionCheckpoint,
        model: &RuntimeString,
        signal: &crate::types::abort::AbortSignal,
    ) -> Result<bool, crate::types::errors::ProviderError> {
        if checkpoint
            .comp_hash
            .as_ref()
            .zip(self.compaction_compatibility_hash(model))
            .is_some_and(|(saved, current)| saved != &current)
        {
            return Ok(false);
        }
        self.inner
            .native_checkpoint_compatibility(checkpoint, model, signal)
            .await
    }

    fn native_compaction_context(
        &self,
        model: &RuntimeString,
    ) -> Option<crate::types::provider::NativeCompactionContext> {
        if self.remote_compaction_support(model)
            == crate::types::provider::RemoteCompactionSupport::Unsupported
        {
            return None;
        }
        self.inner
            .native_compaction_context(model)
            .map(|mut context| {
                context.identity = format!(
                    "{:?}:{:?}",
                    context.identity,
                    self.compaction_compatibility_hash(model)
                )
                .into();
                context
            })
    }

    fn remote_compaction_support(
        &self,
        model: &RuntimeString,
    ) -> crate::types::provider::RemoteCompactionSupport {
        if !self.has_model_capabilities(model) {
            return crate::types::provider::RemoteCompactionSupport::Unsupported;
        }
        self.inner.remote_compaction_support(model)
    }
    fn compaction_stream_max_retries(&self) -> Option<u32> {
        self.inner.compaction_stream_max_retries()
    }
    fn supports_compaction_model_fallback(&self) -> bool {
        self.inner.supports_compaction_model_fallback()
    }
    async fn compact_remote(
        &self,
        request: ModelRequest,
        signal: AbortSignal,
    ) -> Result<crate::types::provider::RemoteCompactionResult, ProviderError> {
        if !self.has_model_capabilities(&request.model) {
            return Err(crate::provider::native_compaction::unsupported_checkpoint());
        }
        let mut result = self.inner.compact_remote(request.clone(), signal).await?;
        result.checkpoint.comp_hash = self.compaction_compatibility_hash(&request.model);
        Ok(result)
    }

    async fn compact_remote_with_usage(
        &self,
        request: crate::types::provider::ModelRequest,
        signal: crate::types::abort::AbortSignal,
        idle_ms: u64,
        observer: crate::types::provider::NativeCompactionUsageObserver,
        expected_scope: Option<RuntimeString>,
    ) -> Result<crate::types::provider::RemoteCompactionResult, crate::types::errors::ProviderError>
    {
        if !self.has_model_capabilities(&request.model) {
            return Err(crate::provider::native_compaction::unsupported_checkpoint());
        }
        let model = request.model.clone();
        let mut result = self
            .inner
            .compact_remote_with_usage(request, signal, idle_ms, observer, expected_scope)
            .await?;
        result.checkpoint.comp_hash = self.compaction_compatibility_hash(&model);
        Ok(result)
    }

    async fn compact_remote_with_idle_timeout(
        &self,
        request: ModelRequest,
        signal: AbortSignal,
        idle_ms: u64,
    ) -> Result<crate::types::provider::RemoteCompactionResult, ProviderError> {
        if !self.has_model_capabilities(&request.model) {
            return Err(crate::provider::native_compaction::unsupported_checkpoint());
        }
        let model = request.model.clone();
        let mut result = self
            .inner
            .compact_remote_with_idle_timeout(request, signal, idle_ms)
            .await?;
        result.checkpoint.comp_hash = self.compaction_compatibility_hash(&model);
        Ok(result)
    }
    fn info(&self) -> &ProviderInfo {
        self.inner.info()
    }

    fn uses_model_tool_budget(&self, model: &RuntimeString) -> bool {
        self.inner.uses_model_tool_budget(model)
    }

    fn model_metadata(&self, model: &RuntimeString) -> ModelCapabilities {
        let mut metadata = self.inner.model_metadata(model);
        metadata.comp_hash = self.inner.compaction_compatibility_hash(model);
        metadata.effective_context_window_percent = Some(i64::from(
            self.inner.effective_context_window_percent(model),
        ));
        metadata.auto_compact_token_limit = self.inner.model_auto_compact_token_limit(model);
        if let (Ok(provider), Ok(model)) =
            (self.inner.info().name.try_to_utf8(), model.try_to_utf8())
        {
            metadata.native_deferred_tools = self
                .catalog
                .native_deferred_tools(&provider, &model)
                .or(metadata.native_deferred_tools);
        }
        metadata
    }
    fn offline_model_catalog(&self) -> Vec<(RuntimeString, ModelCapabilities)> {
        self.inner.offline_model_catalog()
    }
    fn approval_review_preferred_model(&self) -> RuntimeString {
        self.inner.approval_review_preferred_model()
    }
    fn uses_official_api_catalog(&self) -> bool {
        self.inner.uses_official_api_catalog()
    }
    fn managed_review_cloud_auth_source(
        &self,
    ) -> Option<Arc<dyn crate::engine::approval_requirements_cloud::ManagedReviewCloudAuthSource>>
    {
        self.inner.managed_review_cloud_auth_source()
    }
    fn classifier_auth_changes(
        &self,
    ) -> Option<tokio::sync::watch::Receiver<crate::provider::auth::CredentialChangeState>> {
        self.inner.classifier_auth_changes()
    }
    fn approval_request_tokens(&self, request: &ModelRequest) -> Result<usize, RuntimeString> {
        self.inner.approval_request_tokens(request)
    }
    fn approval_request_tokens_with_metadata(
        &self,
        request: &ModelRequest,
        metadata: ModelCapabilities,
        raw_effort: Option<RuntimeString>,
    ) -> Result<usize, RuntimeString> {
        self.inner
            .approval_request_tokens_with_metadata(request, metadata, raw_effort)
    }

    fn create_approval_classifier_transport(
        &self,
        attribution: crate::provider::approval_classifier_transport::ClassifierAttribution,
        target: Option<crate::provider::approval_classifier_transport::ClassifierTarget>,
    ) -> Option<Arc<dyn crate::provider::approval_classifier_transport::ApprovalClassifierTransport>>
    {
        self.inner
            .create_approval_classifier_transport(attribution, target)
    }

    fn review_target_provider(&self) -> Option<Arc<dyn ModelProvider>> {
        self.inner.review_target_provider()
    }
    fn stream_with_model_metadata(
        &self,
        req: ModelRequest,
        metadata: ModelCapabilities,
        raw_effort: Option<RuntimeString>,
        total_attempts: u32,
        signal: AbortSignal,
    ) -> ModelStream {
        self.inner
            .stream_with_model_metadata(req, metadata, raw_effort, total_attempts, signal)
    }

    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        let base = self.inner.capabilities(model);
        let provider = self.inner.info().name.try_to_utf8().unwrap_or_default();
        let model = match model.try_to_utf8() {
            Ok(model) => model,
            Err(_) => return base,
        };
        self.catalog.resolve(&provider, &model, base)
    }

    fn stream(&self, mut req: ModelRequest, signal: AbortSignal) -> ModelStream {
        // A captured request already owns both policies. Only unbound direct
        // calls read current catalog facts here.
        if req.image_limits.is_none() || req.max_media_blocks.is_none() {
            let capabilities = self.capabilities(&req.model);
            req.image_limits
                .get_or_insert(capabilities.image_limits.unwrap_or_default());
            req.max_media_blocks.get_or_insert(
                capabilities
                    .max_media_blocks
                    .unwrap_or(DEFAULT_MAX_MEDIA_BLOCKS),
            );
        }
        if req.native_deferred_tools.is_none() {
            req.native_deferred_tools = self.model_metadata(&req.model).native_deferred_tools;
        }
        self.inner.stream(req, signal)
    }

    fn capture_capabilities(
        &self,
        models: &[RuntimeString],
    ) -> HashMap<RuntimeString, ProviderCapabilities> {
        let mut captured = self.inner.capture_capabilities(models);
        let provider = self.inner.info().name.try_to_utf8().unwrap_or_default();
        let state = self
            .catalog
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (model, capabilities) in &mut captured {
            let Ok(model) = model.try_to_utf8() else {
                continue;
            };
            if let Some(entry) = state
                .document
                .entries
                .iter()
                .filter(|entry| entry.matches(&provider, &model))
                .max_by_key(|entry| entry.specificity())
            {
                entry.overrides.apply(capabilities);
            }
        }
        captured
    }

    fn has_model_capabilities(&self, model: &RuntimeString) -> bool {
        self.inner.has_model_capabilities(model)
    }

    fn estimate_tokens(&self, req: &ModelRequest) -> Option<u64> {
        self.inner.estimate_tokens(req)
    }

    async fn resolve_auth(&self) -> Option<Result<AuthHeaders, ProviderError>> {
        self.inner.resolve_auth().await
    }

    fn credential_account_id(&self) -> Option<String> {
        self.inner.credential_account_id()
    }

    async fn rotate_credential(
        &self,
        failed_id: &str,
    ) -> Option<crate::provider::rotation::RotateOutcome> {
        self.inner.rotate_credential(failed_id).await
    }

    fn has_prewarm_endpoints(&self) -> bool {
        self.inner.has_prewarm_endpoints()
    }

    fn prewarm_transport(
        &self,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Vec<crate::provider::prewarm::PrewarmOutcome>> + Send>,
    > {
        self.inner.prewarm_transport()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn temp_dir() -> String {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "model-catalog-{}-{}-{}",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::SeqCst),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.to_string_lossy().into_owned()
    }

    fn base_caps() -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: true,
            web_search: false,
            prompt_cache_control: true,
            context_window: 200_000,
            max_output_tokens: 8_192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn entry(
        provider: &str,
        model: &str,
        match_kind: CatalogMatch,
        overrides: CapabilityOverrides,
    ) -> CatalogEntry {
        CatalogEntry {
            provider: provider.to_string(),
            model: model.to_string(),
            match_kind,
            overrides,
        }
    }

    fn catalog_with(entries: Vec<CatalogEntry>) -> Arc<ModelCatalog> {
        let catalog = ModelCatalog::empty();
        {
            let mut state = catalog.state.write().unwrap();
            state.document = CatalogDocument { entries };
        }
        catalog
    }

    #[test]
    fn media_overrides_merge_declared_fields_and_preserve_raw_target_provenance() {
        let mut caps = base_caps();
        caps.image_limits = Some(ImageLimits {
            max_width: NonZeroU32::new(2_000),
            max_height: NonZeroU32::new(2_000),
            max_base64_size: NonZeroU32::new(5_242_880),
            target_raw_size: None,
        });
        let overrides: CapabilityOverrides = runtime_codec::decode_text(&RuntimeString::from(
            r#"{"imageLimits":{"maxWidth":1200,"maxBase64Size":10485760},"maxMediaBlocks":600}"#,
        ))
        .unwrap();
        overrides.apply(&mut caps);
        let limits = caps.image_limits.unwrap();
        assert_eq!(limits.max_width.unwrap().get(), 1_200);
        assert_eq!(limits.max_height.unwrap().get(), 2_000);
        assert_eq!(limits.max_base64_size.unwrap().get(), 10_485_760);
        assert_eq!(limits.target_raw_size, None);
        assert_eq!(caps.max_media_blocks.unwrap().get(), 600);

        caps.image_limits.as_mut().unwrap().target_raw_size = NonZeroU32::new(512_000);
        overrides.apply(&mut caps);
        assert_eq!(
            caps.image_limits.unwrap().target_raw_size.unwrap().get(),
            512_000
        );
    }

    #[test]
    fn catalog_media_selection_uses_exact_then_longest_prefix_without_cascading() {
        let media = |width| CapabilityOverrides {
            image_limits: Some(ImageLimits {
                max_width: NonZeroU32::new(width),
                ..Default::default()
            }),
            ..Default::default()
        };
        let catalog = catalog_with(vec![
            entry("fixture", "m-", CatalogMatch::Prefix, media(1_000)),
            entry("fixture", "m-long-", CatalogMatch::Prefix, media(1_500)),
            entry("fixture", "m-long-exact", CatalogMatch::Exact, media(2_000)),
            entry("other", "m-long-exact", CatalogMatch::Exact, media(3_000)),
        ]);
        for (model, width) in [
            ("m-short", 1_000),
            ("m-long-next", 1_500),
            ("m-long-exact", 2_000),
        ] {
            let caps = catalog.resolve("fixture", model, base_caps());
            assert_eq!(caps.image_limits.unwrap().max_width.unwrap().get(), width);
            assert_eq!(caps.image_limits.unwrap().max_height, None);
            assert_eq!(caps.image_limits.unwrap().target_raw_size, None);
        }
        assert_eq!(
            catalog
                .resolve("unmatched", "m-long-exact", base_caps())
                .image_limits,
            None
        );
    }

    #[test]
    fn malformed_media_catalog_entries_are_skipped_without_losing_valid_entries() {
        let value = runtime_json::parse(&RuntimeString::from(
            r#"{"schemaVersion":1,"entries":[
                {"provider":"fixture","model":"zero","capabilities":{"maxMediaBlocks":0}},
                {"provider":"fixture","model":"unknown","capabilities":{"imageLimits":{"maxWidht":2000}}},
                {"provider":"fixture","model":"valid","capabilities":{"imageLimits":{"maxBase64Size":10485760},"maxMediaBlocks":600}}
            ]}"#,
        )).unwrap();
        let document = CatalogDocument::parse(&value).unwrap();
        assert_eq!(document.entries.len(), 1);
        assert_eq!(document.entries[0].model, "valid");
        assert_eq!(
            document.entries[0]
                .overrides
                .image_limits
                .unwrap()
                .target_raw_size,
            None
        );
        let encoded = runtime_codec::encode_value(&document.entries[0].overrides).unwrap();
        assert_eq!(
            runtime_codec::decode_value::<CapabilityOverrides>(&encoded).unwrap(),
            document.entries[0].overrides,
        );
    }

    struct RecordingMediaProvider {
        info: ProviderInfo,
        requests: Arc<std::sync::Mutex<Vec<ModelRequest>>>,
    }

    impl ModelProvider for RecordingMediaProvider {
        fn info(&self) -> &ProviderInfo {
            &self.info
        }
        fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
            base_caps()
        }
        fn stream(&self, req: ModelRequest, _: AbortSignal) -> ModelStream {
            self.requests.lock().unwrap().push(req);
            Box::pin(futures::stream::empty())
        }
    }

    #[tokio::test]
    async fn unbound_catalog_requests_receive_selected_model_media_without_changing_bound_fields() {
        use futures::StreamExt;
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let inner = Arc::new(RecordingMediaProvider {
            info: ProviderInfo {
                name: "fixture".into(),
                default_model: "main".into(),
                base_url: None,
                env_key: None,
            },
            requests: requests.clone(),
        });
        let catalog = catalog_with(vec![entry(
            "fixture",
            "summary",
            CatalogMatch::Exact,
            CapabilityOverrides {
                image_limits: Some(ImageLimits {
                    max_width: NonZeroU32::new(1_200),
                    ..Default::default()
                }),
                max_media_blocks: NonZeroU32::new(42),
                ..Default::default()
            },
        )]);
        let live = with_model_catalog(inner, catalog);
        let request: ModelRequest = runtime_codec::decode_text(&RuntimeString::from(
            r#"{"model":"summary","system":[],"messages":[],"tools":[],"maxOutputTokens":100}"#,
        ))
        .unwrap();
        let _ = live
            .stream(request.clone(), AbortSignal::new())
            .collect::<Vec<_>>()
            .await;
        let mut partially_bound = request;
        partially_bound.image_limits = Some(ImageLimits::default());
        let _ = live
            .stream(partially_bound, AbortSignal::new())
            .collect::<Vec<_>>()
            .await;
        let sent = requests.lock().unwrap();
        assert_eq!(
            sent[0].image_limits.unwrap().max_width.unwrap().get(),
            1_200
        );
        assert_eq!(sent[0].max_media_blocks.unwrap().get(), 42);
        assert_eq!(sent[1].image_limits, Some(ImageLimits::default()));
        assert_eq!(sent[1].max_media_blocks.unwrap().get(), 42);
    }

    #[tokio::test]
    async fn native_tool_policy_is_exact_scoped_and_frozen_at_capture() {
        use futures::StreamExt;
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let inner = Arc::new(RecordingMediaProvider {
            info: ProviderInfo {
                name: "fixture".into(),
                default_model: "main".into(),
                base_url: None,
                env_key: None,
            },
            requests: requests.clone(),
        });
        let catalog = catalog_with(vec![
            entry(
                "fixture",
                "main",
                CatalogMatch::Exact,
                CapabilityOverrides {
                    native_deferred_tools: Some(true),
                    ..Default::default()
                },
            ),
            entry(
                "fixture",
                "unlisted-",
                CatalogMatch::Prefix,
                CapabilityOverrides {
                    native_deferred_tools: Some(true),
                    ..Default::default()
                },
            ),
        ]);
        let live = with_model_catalog(inner, catalog.clone());
        let frozen = capture_model_provider(live.clone(), &["main".into()]);
        catalog.state.write().unwrap().document.entries[0]
            .overrides
            .native_deferred_tools = Some(false);
        assert_eq!(
            frozen.model_metadata(&"main".into()).native_deferred_tools,
            Some(true)
        );
        assert_eq!(
            live.model_metadata(&"main".into()).native_deferred_tools,
            Some(false)
        );
        assert_eq!(
            live.model_metadata(&"unlisted-model".into())
                .native_deferred_tools,
            None
        );
        let request: ModelRequest = runtime_codec::decode_text(&RuntimeString::from(
            r#"{"model":"main","system":[],"messages":[],"tools":[],"maxOutputTokens":100}"#,
        ))
        .unwrap();
        frozen
            .stream(request.clone(), AbortSignal::new())
            .collect::<Vec<_>>()
            .await;
        live.stream(request, AbortSignal::new())
            .collect::<Vec<_>>()
            .await;
        let sent = requests.lock().unwrap();
        assert_eq!(sent[0].native_deferred_tools, Some(true));
        assert_eq!(sent[1].native_deferred_tools, Some(false));
        let metadata = ModelCapabilities {
            native_deferred_tools: Some(false),
            ..Default::default()
        };
        let encoded = runtime_codec::encode_value(&metadata).unwrap();
        assert_eq!(
            encoded.get("nativeDeferredTools"),
            Some(&crate::runtime_values::RuntimeJson::Bool(false))
        );
        assert_eq!(
            runtime_codec::decode_value::<ModelCapabilities>(&encoded)
                .unwrap()
                .native_deferred_tools,
            Some(false)
        );
    }

    #[tokio::test]
    async fn captured_send_boundary_keeps_old_media_facts_and_rebinds_each_request_model() {
        use futures::StreamExt;
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let inner = Arc::new(RecordingMediaProvider {
            info: ProviderInfo {
                name: "fixture".into(),
                default_model: "main".into(),
                base_url: None,
                env_key: None,
            },
            requests: requests.clone(),
        });
        let catalog = catalog_with(vec![entry(
            "fixture",
            "main",
            CatalogMatch::Exact,
            CapabilityOverrides {
                image_limits: Some(ImageLimits {
                    max_width: NonZeroU32::new(2_000),
                    max_base64_size: NonZeroU32::new(5_242_880),
                    ..Default::default()
                }),
                max_media_blocks: NonZeroU32::new(600),
                ..Default::default()
            },
        )]);
        let live = with_model_catalog(inner, catalog.clone());
        let models = ["main".into(), "summary".into()];
        let frozen = capture_model_provider(live.clone(), &models);
        catalog.state.write().unwrap().document.entries[0].overrides = CapabilityOverrides {
            image_limits: Some(ImageLimits {
                max_width: NonZeroU32::new(1_000),
                max_base64_size: NonZeroU32::new(10_485_760),
                ..Default::default()
            }),
            max_media_blocks: NonZeroU32::new(100),
            ..Default::default()
        };
        let request: ModelRequest = runtime_codec::decode_text(&RuntimeString::from(
            r#"{"model":"main","system":[],"messages":[],"tools":[],"maxOutputTokens":100}"#,
        ))
        .unwrap();
        let _ = frozen
            .stream(request.clone(), AbortSignal::new())
            .collect::<Vec<_>>()
            .await;
        let mut summary = request.clone();
        summary.model = "summary".into();
        summary.image_limits = requests.lock().unwrap()[0].image_limits;
        summary.max_media_blocks = NonZeroU32::new(600);
        let _ = frozen
            .stream(summary, AbortSignal::new())
            .collect::<Vec<_>>()
            .await;
        let next = capture_model_provider(live, &models);
        let _ = next
            .stream(request.clone(), AbortSignal::new())
            .collect::<Vec<_>>()
            .await;
        let mut unknown = request;
        unknown.model = "uncaptured".into();
        let denied = frozen
            .stream(unknown, AbortSignal::new())
            .collect::<Vec<_>>()
            .await;
        assert!(matches!(
            denied.as_slice(),
            [ModelStreamEvent::Error { .. }]
        ));
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert_eq!(
            requests[0].image_limits.unwrap().max_width.unwrap().get(),
            2_000
        );
        assert_eq!(
            requests[0]
                .image_limits
                .unwrap()
                .max_base64_size
                .unwrap()
                .get(),
            5_242_880
        );
        assert_eq!(requests[0].max_media_blocks.unwrap().get(), 600);
        assert_eq!(requests[1].image_limits, Some(ImageLimits::default()));
        assert_eq!(requests[1].max_media_blocks.unwrap().get(), 100);
        assert_eq!(
            requests[2].image_limits.unwrap().max_width.unwrap().get(),
            1_000
        );
        assert_eq!(
            requests[2]
                .image_limits
                .unwrap()
                .max_base64_size
                .unwrap()
                .get(),
            10_485_760
        );
        assert_eq!(requests[2].image_limits.unwrap().target_raw_size, None);
        assert_eq!(requests[2].max_media_blocks.unwrap().get(), 100);
    }

    #[test]
    fn overrides_apply_only_present_fields() {
        let overrides = CapabilityOverrides {
            context_window: Some(1_000_000),
            thinking: Some(true),
            ..Default::default()
        };
        let mut caps = base_caps();
        overrides.apply(&mut caps);
        assert_eq!(caps.context_window, 1_000_000);
        assert!(caps.thinking);
        // Untouched fields keep the built-in answer.
        assert_eq!(caps.max_output_tokens, 8_192);
        assert!(caps.streaming);
    }

    #[test]
    fn resolve_prefers_exact_then_longest_prefix_and_scopes_by_provider() {
        let catalog = catalog_with(vec![
            entry(
                "anthropic",
                "claude-",
                CatalogMatch::Prefix,
                CapabilityOverrides {
                    context_window: Some(111),
                    ..Default::default()
                },
            ),
            entry(
                "anthropic",
                "claude-omega-",
                CatalogMatch::Prefix,
                CapabilityOverrides {
                    context_window: Some(222),
                    ..Default::default()
                },
            ),
            entry(
                "anthropic",
                "claude-omega-9",
                CatalogMatch::Exact,
                CapabilityOverrides {
                    context_window: Some(333),
                    ..Default::default()
                },
            ),
            entry(
                "openai",
                "claude-omega-9",
                CatalogMatch::Exact,
                CapabilityOverrides {
                    context_window: Some(999),
                    ..Default::default()
                },
            ),
        ]);
        // Exact beats every prefix.
        assert_eq!(
            catalog
                .resolve("anthropic", "claude-omega-9", base_caps())
                .context_window,
            333
        );
        // The longest matching prefix wins.
        assert_eq!(
            catalog
                .resolve("anthropic", "claude-omega-8", base_caps())
                .context_window,
            222
        );
        assert_eq!(
            catalog
                .resolve("anthropic", "claude-lite", base_caps())
                .context_window,
            111
        );
        // Provider scoping: an openai entry never answers for anthropic.
        assert_eq!(
            catalog
                .resolve("gemini", "claude-omega-9", base_caps())
                .context_window,
            base_caps().context_window
        );
    }

    #[test]
    fn document_parse_skips_malformed_entries_and_rejects_newer_schema() {
        let value = runtime_json::parse(&RuntimeString::from(
            r#"{"schemaVersion":1,"entries":[
                {"provider":"anthropic","model":"m","capabilities":{"contextWindow":42},"futureField":true},
                {"model":"missing provider"},
                {"provider":"openai","model":"o","match":"prefix","capabilities":{}}
            ]}"#,
        ))
        .unwrap();
        let document = CatalogDocument::parse(&value).unwrap();
        assert_eq!(document.entries.len(), 2);
        assert_eq!(document.entries[0].provider, "anthropic");
        assert_eq!(document.entries[0].overrides.context_window, Some(42));
        assert_eq!(document.entries[1].match_kind, CatalogMatch::Prefix);

        let newer =
            runtime_json::parse(&RuntimeString::from(r#"{"schemaVersion":2,"entries":[]}"#))
                .unwrap();
        assert!(CatalogDocument::parse(&newer).is_err());
    }

    struct FixedProvider {
        info: ProviderInfo,
    }
    #[async_trait::async_trait]
    impl ModelProvider for FixedProvider {
        fn info(&self) -> &ProviderInfo {
            &self.info
        }
        fn capabilities(&self, _model: &RuntimeString) -> ProviderCapabilities {
            base_caps()
        }
        fn stream(&self, _req: ModelRequest, _signal: AbortSignal) -> ModelStream {
            Box::pin(futures::stream::iter(Vec::new()))
        }
    }

    #[test]
    fn decorator_overlays_capabilities_and_forwards_identity() {
        let inner = Arc::new(FixedProvider {
            info: ProviderInfo {
                name: "anthropic".into(),
                base_url: None,
                env_key: None,
                default_model: "claude-omega-9".into(),
            },
        });
        let catalog = catalog_with(vec![entry(
            "anthropic",
            "claude-omega-9",
            CatalogMatch::Exact,
            CapabilityOverrides {
                context_window: Some(1_000_000),
                native_pdf: Some(true),
                ..Default::default()
            },
        )]);
        let wrapped = with_model_catalog(inner, catalog);
        assert_eq!(wrapped.info().name, "anthropic");
        let caps = wrapped.capabilities(&RuntimeString::from("claude-omega-9"));
        assert_eq!(caps.context_window, 1_000_000);
        assert!(caps.native_pdf);
        // Unmatched models keep the built-in answer wholesale.
        let untouched = wrapped.capabilities(&RuntimeString::from("claude-other"));
        assert_eq!(untouched, base_caps());
    }

    /// Serve exactly `responses` (raw HTTP payloads) and record request heads.
    async fn serve_scripted(listener: TcpListener, responses: Vec<String>) -> Vec<String> {
        let mut heads = Vec::new();
        for response in responses {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            loop {
                let mut byte = [0_u8; 1];
                let read = socket.read(&mut byte).await.unwrap();
                assert_ne!(read, 0, "client closed before completing the request");
                head.push(byte[0]);
                if head.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            heads.push(String::from_utf8(head).unwrap());
            socket.write_all(response.as_bytes()).await.unwrap();
        }
        heads
    }

    #[tokio::test]
    async fn refresh_round_trips_etag_and_persists_the_cache() {
        let document = r#"{"schemaVersion":1,"entries":[{"provider":"anthropic","model":"claude-omega-9","capabilities":{"contextWindow":1000000}}]}"#;
        let ok = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nETag: \"v1\"\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{document}",
            document.len()
        );
        let not_modified =
            "HTTP/1.1 304 Not Modified\r\nETag: \"v1\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                .to_string();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_scripted(listener, vec![ok, not_modified]));
        let url = format!("http://{address}/catalog.json");
        let http = reqwest::Client::new();

        let dir = temp_dir();
        let catalog = ModelCatalog::load(Some(&dir));
        assert_eq!(catalog.entry_count(), 0);

        // First fetch applies and persists the document.
        assert_eq!(
            catalog.refresh(&http, &url).await.unwrap(),
            CatalogRefresh::Updated
        );
        assert_eq!(catalog.entry_count(), 1);
        assert_eq!(
            catalog
                .resolve("anthropic", "claude-omega-9", base_caps())
                .context_window,
            1_000_000
        );

        // Second fetch revalidates with the ETag and gets a 304.
        assert_eq!(
            catalog.refresh(&http, &url).await.unwrap(),
            CatalogRefresh::NotModified
        );
        assert_eq!(catalog.entry_count(), 1);

        let heads = server.await.unwrap();
        assert!(!heads[0].to_ascii_lowercase().contains("if-none-match"));
        assert!(heads[1]
            .to_ascii_lowercase()
            .contains("if-none-match: \"v1\""));

        // A fresh catalog instance reads the persisted cache from disk.
        let reloaded = ModelCatalog::load(Some(&dir));
        assert_eq!(reloaded.entry_count(), 1);
        assert!(!reloaded.is_stale(chrono::Utc::now().timestamp_millis()));
        assert_eq!(
            reloaded
                .resolve("anthropic", "claude-omega-9", base_caps())
                .context_window,
            1_000_000
        );
    }

    #[tokio::test]
    async fn refresh_failure_keeps_the_cached_document() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_scripted(
            listener,
            vec![
                "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .to_string(),
            ],
        ));
        let url = format!("http://{address}/catalog.json");
        let catalog = catalog_with(vec![entry(
            "anthropic",
            "m",
            CatalogMatch::Exact,
            CapabilityOverrides::default(),
        )]);
        let error = catalog
            .refresh(&reqwest::Client::new(), &url)
            .await
            .expect_err("HTTP 500 fails the refresh");
        assert!(error.contains("500"));
        assert_eq!(catalog.entry_count(), 1);
        let _ = server.await.unwrap();
    }
}
