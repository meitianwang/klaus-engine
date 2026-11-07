// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified to bind authenticated native providers and administrator review requirements.

//! Authenticated subscription requirements, with identity-bound cache and refresh ownership.

use super::approval_requirements::{TrustedBundleRequirements, TrustedRequirementsBundleProvider};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::OnceCell;
use tokio::task::JoinHandle;

#[path = "approval_requirements_cloud/cache.rs"]
mod cache;
use cache::BundleCache;

const LOAD_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_ATTEMPTS: usize = 5;
const REFRESH_INTERVAL: Duration = Duration::from_secs(15 * 60);
const TIMEOUT_RETRY_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedReviewCloudIdentity {
    pub chatgpt_user_id: Option<String>,
    pub account_id: Option<String>,
    pub plan_type: Option<String>,
    pub backend_base_url: String,
    pub owner_generation: u64,
}

/// Produced only from the native subscription credential owner. Headers and
/// transport come from the same bound provider that performs model requests.
pub struct ManagedReviewCloudAuth {
    pub identity: ManagedReviewCloudIdentity,
    pub headers: HashMap<String, String>,
    pub client: reqwest::Client,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ManagedReviewCloudAuthRecovery {
    Refreshed,
    TransientFailure(String),
    PermanentFailure(String),
    Unavailable,
}

#[async_trait]
pub trait ManagedReviewCloudAuthSource: Send + Sync {
    async fn current(&self) -> Result<Option<ManagedReviewCloudAuth>, String>;
    async fn recover_unauthorized(
        &self,
        recovery_step: usize,
        expected: &ManagedReviewCloudIdentity,
    ) -> ManagedReviewCloudAuthRecovery;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CloudRequirementsErrorCode {
    Auth,
    RequestFailed,
    Timeout,
    InvalidBundle,
    StaleOwner,
    NotInitialized,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CloudRequirementsError {
    pub code: CloudRequirementsErrorCode,
    pub status_code: Option<u16>,
    pub message: String,
}

impl CloudRequirementsError {
    fn new(
        code: CloudRequirementsErrorCode,
        status_code: Option<u16>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code,
            status_code,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for CloudRequirementsError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}
impl std::error::Error for CloudRequirementsError {}

impl From<CloudRequirementsError> for io::Error {
    fn from(error: CloudRequirementsError) -> Self {
        let kind = match error.code {
            CloudRequirementsErrorCode::Auth | CloudRequirementsErrorCode::StaleOwner => {
                io::ErrorKind::PermissionDenied
            }
            CloudRequirementsErrorCode::InvalidBundle => io::ErrorKind::InvalidData,
            CloudRequirementsErrorCode::Timeout => io::ErrorKind::TimedOut,
            CloudRequirementsErrorCode::NotInitialized
            | CloudRequirementsErrorCode::RequestFailed => io::ErrorKind::Other,
        };
        io::Error::new(kind, error)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Default, Eq, PartialEq)]
pub(super) struct Bundle {
    pub config_toml: FragmentBucket,
    pub requirements_toml: FragmentBucket,
}

#[derive(Clone, Debug, Deserialize, Serialize, Default, Eq, PartialEq)]
pub(super) struct FragmentBucket {
    pub enterprise_managed: Vec<Fragment>,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub(super) struct Fragment {
    pub id: String,
    pub name: String,
    pub contents: String,
}

#[derive(Deserialize, Default)]
struct DeliveredBundle {
    config_toml: Option<DeliveredBucket>,
    requirements_toml: Option<DeliveredBucket>,
}

#[derive(Deserialize, Default)]
struct DeliveredBucket {
    enterprise_managed: Option<Vec<Fragment>>,
}

impl Bundle {
    fn from_network(bytes: &[u8]) -> Result<Self, String> {
        let delivered: DeliveredBundle =
            serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        Ok(Self {
            config_toml: FragmentBucket {
                enterprise_managed: delivered
                    .config_toml
                    .and_then(|bucket| bucket.enterprise_managed)
                    .unwrap_or_default(),
            },
            requirements_toml: FragmentBucket {
                enterprise_managed: delivered
                    .requirements_toml
                    .and_then(|bucket| bucket.enterprise_managed)
                    .unwrap_or_default(),
            },
        })
    }

    fn requirements(&self) -> Vec<TrustedBundleRequirements> {
        self.requirements_toml
            .enterprise_managed
            .iter()
            .rev()
            .map(|fragment| TrustedBundleRequirements {
                id: fragment.id.clone(),
                name: fragment.name.clone(),
                toml: fragment.contents.clone(),
            })
            .collect()
    }

    fn validate(&self) -> Result<(), CloudRequirementsError> {
        for fragment in &self.config_toml.enterprise_managed {
            fragment
                .contents
                .parse::<toml_edit::DocumentMut>()
                .map_err(|error| {
                    CloudRequirementsError::new(
                        CloudRequirementsErrorCode::InvalidBundle,
                        None,
                        format!(
                            "invalid cloud configuration fragment {}: {}",
                            fragment.id,
                            error.message()
                        ),
                    )
                })?;
        }
        super::approval_requirements::validate_trusted_bundle_requirements(self.requirements())
            .map_err(|error| {
                CloudRequirementsError::new(
                    CloudRequirementsErrorCode::InvalidBundle,
                    None,
                    format!("invalid cloud requirements bundle: {error}"),
                )
            })
    }
}

pub fn cloud_requirements_eligible(identity: &ManagedReviewCloudIdentity) -> bool {
    matches!(
        identity.plan_type.as_deref(),
        Some(
            "business"
                | "ent26"
                | "enterprise_cbp_automation"
                | "enterprise_cbp_usage_based"
                | "edu"
                | "edu_plus"
                | "edu_pro"
                | "enterprise"
        )
    )
}

/// The native Responses route may carry a final `/codex` component. Discovery
/// uses its backend root, preserving the provider's actual host and proxy route.
pub fn cloud_requirements_url(base_url: &str) -> String {
    let mut base = base_url.trim_end_matches('/').to_string();
    if base.ends_with("/codex") && base.contains("/backend-api") {
        base.truncate(base.len() - "/codex".len());
    }
    if (base.starts_with("https://chatgpt.com") || base.starts_with("https://chat.openai.com"))
        && !base.contains("/backend-api")
    {
        base.push_str("/backend-api");
    }
    if base.contains("/backend-api") {
        format!("{base}/wham/config/bundle")
    } else {
        format!("{base}/api/codex/config/bundle")
    }
}

#[derive(Clone)]
struct Snapshot {
    identity: Option<ManagedReviewCloudIdentity>,
    result: Result<Bundle, CloudRequirementsError>,
}

#[derive(Default)]
struct ObservedBundle {
    initialized: bool,
    revision: u64,
    bundle: Option<Bundle>,
    retired: bool,
}

#[derive(Clone)]
pub(super) struct CacheRevision {
    observed: Arc<Mutex<ObservedBundle>>,
    revision: u64,
}

impl CacheRevision {
    pub(super) fn commit(&self, commit: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
        let observed = self
            .observed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !observed.retired && observed.revision == self.revision {
            commit()
        } else {
            Ok(())
        }
    }
}

struct Service {
    auth: Arc<dyn ManagedReviewCloudAuthSource>,
    cache: BundleCache,
    latest: OnceCell<Mutex<Snapshot>>,
    observed: Arc<Mutex<ObservedBundle>>,
}

/// Keep this object with the session/provider that admitted it. Its lifetime
/// owns refresh cancellation; dropping the owner stops background requests.
pub struct CloudManagedRequirementsProvider {
    service: Arc<Service>,
    refresh_task: JoinHandle<()>,
}

impl CloudManagedRequirementsProvider {
    pub fn new(auth: Arc<dyn ManagedReviewCloudAuthSource>, klaus_home: PathBuf) -> Arc<Self> {
        let service = Arc::new(Service {
            auth,
            cache: BundleCache::new(klaus_home),
            latest: OnceCell::new(),
            observed: Arc::new(Mutex::new(ObservedBundle::default())),
        });
        let background = service.clone();
        let refresh_task = tokio::spawn(async move {
            let _ = background.latest().await;
            background.refresh_loop().await;
        });
        Arc::new(Self {
            service,
            refresh_task,
        })
    }

    /// Startup must await `load` before constructing a synchronous session.
    /// An uninitialized configured provider is an explicit admission error.
    pub fn snapshot_sync(&self) -> Result<Vec<TrustedBundleRequirements>, CloudRequirementsError> {
        let snapshot = self.service.latest.get().ok_or_else(|| {
            CloudRequirementsError::new(
                CloudRequirementsErrorCode::NotInitialized,
                None,
                "cloud requirements must be loaded before session construction",
            )
        })?;
        snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .result
            .as_ref()
            .map(Bundle::requirements)
            .map_err(Clone::clone)
    }

    pub async fn load(&self) -> Result<Vec<TrustedBundleRequirements>, CloudRequirementsError> {
        let snapshot = self
            .service
            .latest()
            .await
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Err(error) = &snapshot.result {
            return Err(error.clone());
        }
        let current = self.service.auth.current().await.map_err(|error| {
            CloudRequirementsError::new(CloudRequirementsErrorCode::Auth, None, error)
        })?;
        if snapshot.identity != current.as_ref().map(|auth| auth.identity.clone()) {
            self.service.retire();
            return Err(CloudRequirementsError::new(
                CloudRequirementsErrorCode::StaleOwner,
                None,
                "subscription identity changed after cloud requirements were admitted",
            ));
        }
        snapshot.result.map(|bundle| bundle.requirements())
    }
}

impl Drop for CloudManagedRequirementsProvider {
    fn drop(&mut self) {
        self.service.retire();
        self.refresh_task.abort();
    }
}

impl TrustedRequirementsBundleProvider for CloudManagedRequirementsProvider {
    fn load(
        &self,
    ) -> Pin<Box<dyn Future<Output = io::Result<Vec<TrustedBundleRequirements>>> + Send + '_>> {
        Box::pin(async move {
            CloudManagedRequirementsProvider::load(self)
                .await
                .map_err(io::Error::from)
        })
    }
}

impl Service {
    async fn latest(&self) -> &Mutex<Snapshot> {
        self.latest
            .get_or_init(|| async {
                let snapshot = self.load_startup().await;
                if let Ok(bundle) = &snapshot.result {
                    let _ = self.observe(bundle);
                }
                Mutex::new(snapshot)
            })
            .await
    }

    fn observe(&self, bundle: &Bundle) -> Option<CacheRevision> {
        let bundle = if bundle.config_toml.enterprise_managed.is_empty()
            && bundle.requirements_toml.enterprise_managed.is_empty()
        {
            None
        } else {
            Some(bundle.clone())
        };
        let mut observed = self
            .observed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if observed.retired {
            return None;
        }
        if !observed.initialized || observed.bundle != bundle {
            if observed.initialized {
                let Some(revision) = observed.revision.checked_add(1) else {
                    observed.retired = true;
                    return None;
                };
                observed.revision = revision;
            }
            observed.initialized = true;
            observed.bundle = bundle;
        }
        Some(CacheRevision {
            observed: self.observed.clone(),
            revision: observed.revision,
        })
    }

    fn retire(&self) {
        self.observed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retired = true;
    }

    async fn load_startup(&self) -> Snapshot {
        match tokio::time::timeout(LOAD_TIMEOUT, self.load_startup_inner()).await {
            Ok(snapshot) => snapshot,
            Err(_) => Snapshot {
                identity: None,
                result: Err(CloudRequirementsError::new(
                    CloudRequirementsErrorCode::Timeout,
                    None,
                    "timed out waiting for cloud requirements after 20s",
                )),
            },
        }
    }

    async fn load_startup_inner(&self) -> Snapshot {
        let auth = match self.auth.current().await {
            Ok(Some(auth)) => auth,
            Ok(None) => {
                return Snapshot {
                    identity: None,
                    result: Ok(Bundle::default()),
                }
            }
            Err(error) => {
                return Snapshot {
                    identity: None,
                    result: Err(CloudRequirementsError::new(
                        CloudRequirementsErrorCode::Auth,
                        None,
                        error,
                    )),
                }
            }
        };
        let identity = auth.identity.clone();
        if !cloud_requirements_eligible(&identity) {
            return Snapshot {
                identity: Some(identity),
                result: Ok(Bundle::default()),
            };
        }
        if let Ok(bundle) = self.cache.load(&identity).await {
            if bundle.validate().is_ok() {
                return Snapshot {
                    identity: Some(identity),
                    result: Ok(bundle),
                };
            }
        }
        Snapshot {
            identity: Some(identity),
            result: self.fetch(auth).await,
        }
    }

    async fn fetch(
        &self,
        mut auth: ManagedReviewCloudAuth,
    ) -> Result<Bundle, CloudRequirementsError> {
        let owner = auth.identity.clone();
        let mut recovery_step = 0;
        let mut attempt = 1;
        let mut last_status = None;
        while attempt <= MAX_ATTEMPTS {
            let mut request = auth
                .client
                .get(cloud_requirements_url(&auth.identity.backend_base_url));
            for (key, value) in &auth.headers {
                request = request.header(key, value);
            }
            let response = request.send().await;
            match response {
                Ok(response) if response.status().is_success() => {
                    let bytes = match response.bytes().await {
                        Ok(bytes) => bytes,
                        Err(_) => {
                            if attempt < MAX_ATTEMPTS {
                                retry_delay(attempt).await;
                                attempt += 1;
                                continue;
                            }
                            break;
                        }
                    };
                    let bundle = match Bundle::from_network(&bytes) {
                        Ok(bundle) => bundle,
                        Err(_) => {
                            if attempt < MAX_ATTEMPTS {
                                retry_delay(attempt).await;
                                attempt += 1;
                                continue;
                            }
                            break;
                        }
                    };
                    let revision = self.observe(&bundle).ok_or_else(|| {
                        CloudRequirementsError::new(
                            CloudRequirementsErrorCode::StaleOwner,
                            None,
                            "cloud requirements owner was retired",
                        )
                    })?;
                    bundle.validate()?;
                    let current = self.auth.current().await.map_err(|error| {
                        CloudRequirementsError::new(CloudRequirementsErrorCode::Auth, None, error)
                    })?;
                    if current.as_ref().map(|current| &current.identity) != Some(&owner) {
                        return Err(CloudRequirementsError::new(
                            CloudRequirementsErrorCode::StaleOwner,
                            None,
                            "subscription owner changed while cloud requirements were loading",
                        ));
                    }
                    if let Err(error) = self.cache.store(&owner, bundle.clone(), revision).await {
                        tracing::warn!(error = %error, "Failed to write cloud requirements cache");
                    }
                    return Ok(bundle);
                }
                Ok(response) if response.status() == reqwest::StatusCode::UNAUTHORIZED => {
                    last_status = Some(401);
                    let recovery = self.auth.recover_unauthorized(recovery_step, &owner).await;
                    recovery_step += 1;
                    match recovery {
                        ManagedReviewCloudAuthRecovery::Refreshed => {
                            auth = self.auth.current().await.map_err(|error| CloudRequirementsError::new(CloudRequirementsErrorCode::Auth, Some(401), error))?.ok_or_else(|| CloudRequirementsError::new(CloudRequirementsErrorCode::Auth, Some(401), "authentication recovery returned no subscription credential"))?;
                            if auth.identity != owner {
                                return Err(CloudRequirementsError::new(
                                    CloudRequirementsErrorCode::StaleOwner,
                                    Some(401),
                                    "subscription owner changed during authentication recovery",
                                ));
                            }
                            continue;
                        }
                        ManagedReviewCloudAuthRecovery::TransientFailure(_) => {}
                        ManagedReviewCloudAuthRecovery::PermanentFailure(error) => {
                            return Err(CloudRequirementsError::new(
                                CloudRequirementsErrorCode::Auth,
                                Some(401),
                                error,
                            ))
                        }
                        ManagedReviewCloudAuthRecovery::Unavailable => {
                            return Err(CloudRequirementsError::new(
                                CloudRequirementsErrorCode::Auth,
                                Some(401),
                                "authentication session could not be refreshed; sign in again",
                            ))
                        }
                    }
                }
                Ok(response) => {
                    last_status = Some(response.status().as_u16());
                }
                Err(_) => {}
            }
            if attempt < MAX_ATTEMPTS {
                retry_delay(attempt).await;
            }
            attempt += 1;
        }
        Err(CloudRequirementsError::new(
            CloudRequirementsErrorCode::RequestFailed,
            last_status,
            "failed to load subscription-managed requirements",
        ))
    }

    async fn refresh_loop(&self) {
        loop {
            let interval = if self.latest.get().is_some_and(|latest| {
                latest
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .result
                    .as_ref()
                    .is_err_and(|error| error.code == CloudRequirementsErrorCode::Timeout)
            }) {
                TIMEOUT_RETRY_INTERVAL
            } else {
                REFRESH_INTERVAL
            };
            tokio::time::sleep(interval).await;
            match tokio::time::timeout(LOAD_TIMEOUT, self.refresh_once()).await {
                Ok(true) => {}
                Ok(false) => break,
                Err(_) => self.publish_error(CloudRequirementsError::new(
                    CloudRequirementsErrorCode::Timeout,
                    None,
                    "timed out refreshing cloud requirements",
                )),
            }
        }
    }

    async fn refresh_once(&self) -> bool {
        let auth = match self.auth.current().await {
            Ok(Some(auth)) if cloud_requirements_eligible(&auth.identity) => auth,
            Ok(_) => return false,
            Err(error) => {
                self.publish_error(CloudRequirementsError::new(
                    CloudRequirementsErrorCode::Auth,
                    None,
                    error,
                ));
                return true;
            }
        };
        let identity = auth.identity.clone();
        if self.latest.get().is_some_and(|latest| {
            latest
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .identity
                .as_ref()
                .is_some_and(|admitted| admitted != &identity)
        }) {
            self.retire();
            return false;
        }
        let result = self.fetch(auth).await;
        if let Some(latest) = self.latest.get() {
            let mut latest = latest
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if latest
                .identity
                .as_ref()
                .is_some_and(|admitted| admitted != &identity)
            {
                self.retire();
                return false;
            }
            if result.is_ok() || latest.result.is_err() {
                latest.identity = Some(identity);
                latest.result = result;
            }
        }
        true
    }

    fn publish_error(&self, error: CloudRequirementsError) {
        if let Some(latest) = self.latest.get() {
            let mut latest = latest
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if latest.result.is_err() {
                latest.result = Err(error);
            }
        }
    }
}

async fn retry_delay(attempt: usize) {
    use rand::Rng;
    let jitter = rand::thread_rng().gen_range(0.9..1.1);
    let delay = (200.0 * 2_f64.powi(attempt.saturating_sub(1) as i32) * jitter) as u64;
    tokio::time::sleep(Duration::from_millis(delay)).await;
}

#[cfg(test)]
#[path = "approval_requirements_cloud/tests.rs"]
mod tests;
