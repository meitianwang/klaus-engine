//! The active prompt-catalog revision and its update chain.
//!
//! Startup order (spec: atomic activation, offline first): the most recent
//! valid distributed revision cached on disk activates first; the bundled
//! package is the fallback when no cache exists or the cached document fails
//! today's validation. A refresh is one conditional round-trip (ETag): a 304
//! only touches the check timestamp, a 200 parses and validates the whole
//! candidate before an atomic swap — an invalid candidate never activates and
//! leaves the current revision in place. Concurrent refreshers each validate
//! their own candidate and publish complete revisions only; readers never see
//! a half-applied document.
//!
//! The distribution endpoint is injected by the runtime environment
//! ([`PROMPT_CATALOG_URL_ENV`]); with no endpoint configured the store serves
//! the bundled package and never touches the network.

use std::sync::{Arc, OnceLock, RwLock};

use crate::runtime_values::{runtime_json, RuntimeJson, RuntimeObject, RuntimeString};

use super::catalog::PromptCatalog;

/// Runtime-environment injection of the distribution endpoint.
pub const PROMPT_CATALOG_URL_ENV: &str = "KLAUS_PROMPT_CATALOG_URL";
/// Cache of the most recent activated distributed revision.
pub const PROMPT_CATALOG_CACHE_FILE: &str = "prompt-catalog.json";
/// Revalidation cadence, mirroring the capability catalog's interval.
pub const PROMPT_CATALOG_REFRESH_INTERVAL_MS: i64 = 4 * 60 * 60 * 1000;

/// The refresh outcome of one revalidation round-trip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogRefresh {
    /// A new revision was validated, activated and cached.
    Updated,
    /// The source confirmed the active revision is current (HTTP 304).
    NotModified,
}

struct StoreState {
    catalog: Arc<PromptCatalog>,
    etag: Option<String>,
    checked_at_ms: i64,
    /// Raw distributed document, kept for cache rewrites. `None` while the
    /// bundled package is active (the bundled package is never cached).
    document: Option<RuntimeJson>,
}

/// Holder of the active revision. One per process; tests install revisions
/// explicitly through [`PromptCatalogStore::install`].
pub struct PromptCatalogStore {
    state: RwLock<StoreState>,
    cache_path: Option<String>,
}

fn read_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn config_dir(explicit: Option<&str>) -> String {
    explicit
        .map(str::to_string)
        .or_else(|| {
            std::env::var("KLAUS_CONFIG_DIR")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| format!("{}/.klaus", home_dir()))
}

fn home_dir() -> String {
    std::env::var("HOME")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "/tmp".to_string())
}

impl PromptCatalogStore {
    /// The process-wide store: cached distributed revision first, bundled
    /// package as the offline fallback.
    pub fn global() -> &'static Arc<PromptCatalogStore> {
        static GLOBAL: OnceLock<Arc<PromptCatalogStore>> = OnceLock::new();
        GLOBAL.get_or_init(|| Arc::new(Self::load(None)))
    }

    /// Load the startup revision without touching the network. An unreadable
    /// or invalid cache silently yields the bundled package.
    pub fn load(config_dir_override: Option<&str>) -> Self {
        let dir = config_dir(config_dir_override);
        let cache_path = format!("{dir}/{PROMPT_CATALOG_CACHE_FILE}");
        let cached = read_cache(&cache_path);
        let state = match cached {
            Some((document, etag, checked_at_ms)) => match decode_document(&document) {
                Ok(catalog) => StoreState {
                    catalog: Arc::new(catalog),
                    etag,
                    checked_at_ms,
                    document: Some(document),
                },
                Err(error) => {
                    tracing::warn!(
                        %error,
                        "cached prompt catalog failed validation; using the bundled package"
                    );
                    bundled_state()
                }
            },
            None => bundled_state(),
        };
        Self {
            state: RwLock::new(state),
            cache_path: Some(cache_path),
        }
    }

    /// An ephemeral store over the bundled package — no cache, no network.
    pub fn bundled_only() -> Arc<Self> {
        Arc::new(Self {
            state: RwLock::new(bundled_state()),
            cache_path: None,
        })
    }

    /// The active revision. Resolution reads this; every revision is complete
    /// and validated, so readers never observe a partial swap.
    pub fn active(&self) -> Arc<PromptCatalog> {
        read_lock(&self.state).catalog.clone()
    }

    /// Install a validated revision (test and trusted-publisher entry point).
    pub fn install(&self, catalog: PromptCatalog) {
        let revision = catalog.revision.clone();
        write_lock(&self.state).catalog = Arc::new(catalog);
        tracing::debug!(
            revision = revision.try_to_utf8().unwrap_or_default(),
            "prompt catalog revision installed"
        );
    }

    /// The distribution endpoint injected by the runtime environment.
    pub fn configured_source() -> Option<String> {
        std::env::var(PROMPT_CATALOG_URL_ENV)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    }

    pub fn is_stale(&self, now_ms: i64) -> bool {
        now_ms.saturating_sub(read_lock(&self.state).checked_at_ms)
            >= PROMPT_CATALOG_REFRESH_INTERVAL_MS
    }

    /// Revalidate against the configured endpoint when one exists and the
    /// active revision has aged past the refresh interval. `None` means no
    /// round-trip was needed.
    pub async fn refresh_from_env_if_stale(
        &self,
        http: &reqwest::Client,
    ) -> Option<Result<CatalogRefresh, String>> {
        let url = Self::configured_source()?;
        if !self.is_stale(chrono::Utc::now().timestamp_millis()) {
            return None;
        }
        Some(self.refresh(http, &url).await)
    }

    /// [`Self::refresh_from_env_if_stale`] with an engine-owned HTTP client.
    pub async fn refresh_from_env_default(&self) -> Option<Result<CatalogRefresh, String>> {
        let http = reqwest::Client::new();
        self.refresh_from_env_if_stale(&http).await
    }

    /// One conditional revalidation round-trip. The candidate is parsed and
    /// fully validated BEFORE activation; any failure keeps the active
    /// revision untouched.
    pub async fn refresh(
        &self,
        http: &reqwest::Client,
        url: &str,
    ) -> Result<CatalogRefresh, String> {
        let etag = read_lock(&self.state).etag.clone();
        let mut request = http.get(url);
        if let Some(etag) = &etag {
            request = request.header("If-None-Match", etag);
        }
        let response = request
            .send()
            .await
            .map_err(|error| format!("prompt catalog fetch failed: {error}"))?;
        let status = response.status();
        if status.as_u16() == 304 {
            let now = chrono::Utc::now().timestamp_millis();
            write_lock(&self.state).checked_at_ms = now;
            self.persist();
            return Ok(CatalogRefresh::NotModified);
        }
        if !status.is_success() {
            return Err(format!("prompt catalog fetch failed with HTTP {status}"));
        }
        let new_etag = response
            .headers()
            .get("etag")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let body = response
            .text()
            .await
            .map_err(|error| format!("prompt catalog body read failed: {error}"))?;
        let document = runtime_json::parse(&RuntimeString::from(body.as_str()))
            .map_err(|_| "prompt catalog document is not valid JSON".to_string())?;
        // Whole-document validation gates activation; a rejected candidate
        // leaves the active revision and the cache untouched.
        let catalog = decode_document(&document)?;
        {
            let mut state = write_lock(&self.state);
            state.catalog = Arc::new(catalog);
            state.etag = new_etag;
            state.checked_at_ms = chrono::Utc::now().timestamp_millis();
            state.document = Some(document);
        }
        self.persist();
        Ok(CatalogRefresh::Updated)
    }

    /// Best-effort atomic cache rewrite; persistence failures never fail a
    /// refresh (the in-memory revision already advanced).
    fn persist(&self) {
        let Some(path) = &self.cache_path else {
            return;
        };
        let payload = {
            let state = read_lock(&self.state);
            let Some(document) = state.document.clone() else {
                return;
            };
            let mut object = RuntimeObject::new();
            if let Some(etag) = &state.etag {
                object.insert(
                    "etag",
                    RuntimeJson::String(RuntimeString::from(etag.as_str())),
                );
            }
            object.insert("checkedAtMs", RuntimeJson::from(state.checked_at_ms as f64));
            object.insert("document", document);
            RuntimeJson::Object(object)
        };
        let bytes = runtime_json::stringify(&payload).to_node_utf8();
        if let Some(parent) = std::path::Path::new(path).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let tmp = format!("{path}.tmp");
        if std::fs::write(&tmp, bytes).is_err() {
            return;
        }
        if std::fs::rename(&tmp, path).is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
    }
}

fn bundled_state() -> StoreState {
    StoreState {
        catalog: Arc::new(super::bundled().clone()),
        etag: None,
        checked_at_ms: 0,
        document: None,
    }
}

fn decode_document(document: &RuntimeJson) -> Result<PromptCatalog, String> {
    PromptCatalog::decode(document).map_err(|error| error.to_string())
}

fn read_cache(path: &str) -> Option<(RuntimeJson, Option<String>, i64)> {
    let raw = std::fs::read_to_string(path).ok()?;
    let value = runtime_json::parse(&RuntimeString::from(raw.as_str())).ok()?;
    let document = value.get("document")?.clone();
    let etag = value
        .get("etag")
        .and_then(RuntimeJson::as_string)
        .and_then(|text| text.try_to_utf8().ok());
    let checked_at_ms = value
        .get("checkedAtMs")
        .and_then(RuntimeJson::as_i64)
        .unwrap_or(0);
    Some((document, etag, checked_at_ms))
}
