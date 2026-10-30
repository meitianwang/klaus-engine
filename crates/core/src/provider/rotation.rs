//! Multi-account credential rotation for subscription sources.
//!
//! A subscription login may carry more than one account. The account array of
//! the host-owned credential file is the rotation queue: its head is the
//! account requests authenticate with. When the upstream refuses a request for
//! quota reasons (rate limit or exhausted allowance), the queue advances — the
//! failed account moves to the tail — and the caller retries the same request
//! against the new head. Once the queue wraps around to the account that
//! started the cycle and that account fails again, rotation reports exhaustion
//! and the caller surfaces the ordinary quota error; a later manual continue
//! starts a fresh cycle at whatever account the persisted order now leads
//! with.
//!
//! Each account keeps its own [`OAuthAuth`] slot, so token refresh stays
//! per-account: its credential namespace and its cross-process refresh lock
//! are keyed on the stable account credential id, never on token bytes.

use crate::credential_store::{CredentialStore, NamespacedCredentialStore};
use crate::fs::lockfile::{with_lock, LockOptions};
use crate::provider::auth::{AuthSource, ProviderAuthAccess};
use crate::provider::oauth::{OAuthAuth, OAuthAuthOptions, OAuthConfig, OAuthTokens};
use crate::runtime_values::RuntimeString;
use crate::types::abort::AbortSignal;
use crate::types::errors::{ProviderError, ProviderErrorKind};
use crate::types::provider::AuthHeaders;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// The upstream refusals that mean "this account's allowance is spent right
/// now": a plain rate limit or an exhausted quota. Auth failures are NOT here —
/// those belong to the refresh path, not to rotation.
pub fn is_quota_rotation_error(error: &ProviderError) -> bool {
    matches!(
        error.kind,
        ProviderErrorKind::RateLimit | ProviderErrorKind::Quota
    )
}

/// What one rotation attempt decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RotateOutcome {
    /// The queue advanced; requests should retry against the new head.
    Rotated(String),
    /// Another caller already advanced past `failed`; retry against the
    /// returned current head without consuming another cycle step.
    AlreadyAdvanced(String),
    /// The queue wrapped a full cycle and still failed: no account is left to
    /// try. The caller must surface its error as terminal.
    Exhausted,
}

impl RotateOutcome {
    /// Whether the caller may retry the request against the (possibly new)
    /// current head.
    pub fn retriable(&self) -> bool {
        matches!(self, Self::Rotated(_) | Self::AlreadyAdvanced(_))
    }

    pub fn current(&self) -> Option<&str> {
        match self {
            Self::Rotated(id) | Self::AlreadyAdvanced(id) => Some(id),
            Self::Exhausted => None,
        }
    }
}

/// One queued account: stable ids plus the tokens the host stored for it.
#[derive(Debug, Clone)]
pub struct RotationAccountSpec {
    pub id: String,
    /// Stable credential namespace id (`subscription-<id>` on the wire).
    pub credential_id: String,
    pub tokens: OAuthTokens,
}

/// Everything needed to build the account queue.
pub struct RotationInit {
    /// Absolute path of the credential file whose account array order is the
    /// queue; rotation persists its reorders here.
    pub file_path: String,
    pub config: OAuthConfig,
    pub base_url: Option<String>,
    pub extra_headers: HashMap<String, String>,
    /// The endpoint credential store; each account gets its own namespace.
    pub store: Arc<dyn CredentialStore>,
    /// Head first.
    pub accounts: Vec<RotationAccountSpec>,
}

struct RotationInner {
    file_path: String,
    config: OAuthConfig,
    base_url: Option<String>,
    extra_headers: HashMap<String, String>,
    store: Arc<dyn CredentialStore>,
    /// Current queue order, head first.
    accounts: Mutex<Vec<RotationAccountSpec>>,
    slots: Mutex<HashMap<String, Arc<OAuthAuth>>>,
    current: Mutex<String>,
    /// The account whose second failure ends the cycle.
    anchor: Mutex<Option<String>>,
    exhausted: Mutex<bool>,
    credential_changes: tokio::sync::watch::Sender<crate::provider::auth::CredentialChangeState>,
}

/// An [`AuthSource`] over a queue of same-source subscription accounts.
pub struct MultiAccountAuthSource {
    inner: Arc<RotationInner>,
}

impl MultiAccountAuthSource {
    pub fn new(init: RotationInit) -> Result<Self, ProviderError> {
        let head = init
            .accounts
            .first()
            .map(|account| account.id.clone())
            .ok_or_else(|| rotation_error("credential rotation requires at least one account"))?;
        Ok(Self {
            inner: Arc::new(RotationInner {
                file_path: init.file_path,
                config: init.config,
                base_url: init.base_url,
                extra_headers: init.extra_headers,
                store: init.store,
                accounts: Mutex::new(init.accounts),
                slots: Mutex::new(HashMap::new()),
                current: Mutex::new(head),
                anchor: Mutex::new(None),
                exhausted: Mutex::new(false),
                credential_changes: tokio::sync::watch::channel(Default::default()).0,
            }),
        })
    }

    pub fn current_account_id(&self) -> String {
        self.inner.current.lock().unwrap().clone()
    }

    pub fn account_count(&self) -> usize {
        self.inner.accounts.lock().unwrap().len()
    }

    /// Advance the queue past `failed_id`. Concurrent callers racing on the
    /// same failure observe [`RotateOutcome::AlreadyAdvanced`] and reuse the
    /// winner's new head instead of advancing twice.
    pub async fn rotate(&self, failed_id: &str) -> RotateOutcome {
        let outcome = {
            let mut accounts = self.inner.accounts.lock().unwrap();
            let mut current = self.inner.current.lock().unwrap();
            let mut anchor = self.inner.anchor.lock().unwrap();
            let mut exhausted = self.inner.exhausted.lock().unwrap();
            if *exhausted {
                return RotateOutcome::Exhausted;
            }
            if *current != failed_id {
                return RotateOutcome::AlreadyAdvanced(current.clone());
            }
            if accounts.len() < 2 {
                return RotateOutcome::Exhausted;
            }
            if anchor.as_deref() == Some(failed_id) {
                // The cycle wrapped and the account it started with failed
                // again: nothing left to try in this cycle.
                *exhausted = true;
                return RotateOutcome::Exhausted;
            }
            let position = match accounts.iter().position(|account| account.id == failed_id) {
                Some(position) => position,
                None => return RotateOutcome::Exhausted,
            };
            let failed = accounts.remove(position);
            accounts.push(failed);
            if anchor.is_none() {
                *anchor = Some(failed_id.to_string());
            }
            let next = accounts[0].id.clone();
            *current = next.clone();
            self.inner.credential_changes.send_modify(|state| {
                state.revision = state.revision.wrapping_add(1);
                state.owner_generation = state.owner_generation.wrapping_add(1);
            });
            let order: Vec<String> = accounts.iter().map(|account| account.id.clone()).collect();
            drop(accounts);
            RotateStep { next, order }
        };
        // Persist outside the queue locks: the file write takes its own
        // cross-process lock and must not nest behind in-process ones.
        let RotateStep { next, order } = outcome;
        self.inner.persist_order(&order).await;
        RotateOutcome::Rotated(next)
    }

    async fn slot(&self, id: &str) -> Result<Arc<OAuthAuth>, ProviderError> {
        if let Some(slot) = self.inner.slots.lock().unwrap().get(id) {
            return Ok(slot.clone());
        }
        let spec = {
            let accounts = self.inner.accounts.lock().unwrap();
            accounts
                .iter()
                .find(|account| account.id == id)
                .cloned()
                .ok_or_else(|| rotation_error(format!("unknown rotation account {id}")))?
        };
        let slot = self.inner.build_slot(&spec).await?;
        self.inner
            .slots
            .lock()
            .unwrap()
            .insert(spec.id, slot.clone());
        Ok(slot)
    }
}

struct RotateStep {
    next: String,
    order: Vec<String>,
}

impl RotationInner {
    /// A stored refresh wins over the login seed within its namespace. A new
    /// login already has a different credential id and seeds a new namespace.
    async fn build_slot(
        &self,
        spec: &RotationAccountSpec,
    ) -> Result<Arc<OAuthAuth>, ProviderError> {
        let namespace: Arc<dyn CredentialStore> = Arc::new(NamespacedCredentialStore::new(
            self.store.clone(),
            format!("provider/anthropic/{}", spec.credential_id),
        ));
        for _ in 0..4 {
            let prepared = OAuthAuth::capture_stored(&namespace, &self.config).await?;
            let needs_persist = prepared.is_none();
            let auth = OAuthAuth::new(OAuthAuthOptions {
                config: self.config.clone(),
                store: Some(namespace.clone()),
                tokens: Some(spec.tokens.clone()),
                prepared_credential: prepared,
                base_url: self.base_url.clone(),
                extra_headers: Some(self.extra_headers.clone()),
                refresh_lock_key: Some(format!("provider-anthropic-{}", spec.credential_id)),
                access: Some(ProviderAuthAccess::allow_all("oauth")),
                ..Default::default()
            });
            if !needs_persist {
                return Ok(Arc::new(auth));
            }
            match auth.save_if_absent().await {
                Ok(()) => return Ok(Arc::new(auth)),
                // Only a lost initial CAS can adopt the winning preparation.
                Err(error) if error.message == "OAuth credential changed during preparation" => {}
                Err(error) => return Err(error),
            }
        }
        Err(rotation_error(
            "OAuth credential changed during preparation",
        ))
    }

    /// Reorder the credential file's account array to `order` under the shared
    /// directory lock the host's writers use. Accounts the file knows but the
    /// queue does not (added by the host mid-turn) keep their place at the
    /// tail. Persistence failures never fail the rotation itself: the
    /// in-process queue already advanced, and the next session re-reads
    /// whatever order survived.
    async fn persist_order(&self, order: &[String]) {
        let Some(parent) = std::path::Path::new(&self.file_path).parent() else {
            return;
        };
        let lock_dir = parent.join(".locks").join("claude-accounts-rotation");
        let _ = tokio::fs::create_dir_all(&lock_dir).await;
        // with_lock appends `.lock`, so the directory both writers share is
        // `<configRoot>/.locks/claude-accounts-rotation.lock`.
        let lock_target = lock_dir.to_string_lossy().into_owned();
        let file_path = self.file_path.clone();
        let order = order.to_vec();
        let result = with_lock(
            &lock_target,
            || async move { reorder_credential_file(&file_path, &order).await },
            LockOptions {
                retries: Some(60),
                min_timeout_ms: Some(50),
                max_timeout_ms: Some(200),
                stale_ms: Some(120_000),
            },
        )
        .await;
        if let Err(error) = result {
            tracing::warn!(
                target: "provider.rotation",
                "failed to persist credential rotation order: {error}"
            );
        }
    }
}

async fn reorder_credential_file(file_path: &str, order: &[String]) -> std::io::Result<()> {
    let raw = match tokio::fs::read_to_string(file_path).await {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    let mut document: serde_json::Value = match serde_json::from_str(&raw) {
        Ok(document) => document,
        Err(_) => return Ok(()),
    };
    let Some(accounts) = document
        .get_mut("accounts")
        .and_then(|value| value.as_array_mut())
    else {
        return Ok(());
    };
    let original: Vec<serde_json::Value> = std::mem::take(accounts);
    let mut by_id: HashMap<String, serde_json::Value> = HashMap::new();
    let mut unnamed: Vec<serde_json::Value> = Vec::new();
    for entry in original {
        match entry.get("id").and_then(|value| value.as_str()) {
            Some(id) => {
                by_id.insert(id.to_string(), entry);
            }
            None => unnamed.push(entry),
        }
    }
    let mut reordered: Vec<serde_json::Value> = Vec::new();
    for id in order {
        if let Some(entry) = by_id.remove(id) {
            reordered.push(entry);
        }
    }
    // File-only accounts (the host appended them after this session started)
    // keep their relative order at the tail.
    for (_, entry) in by_id {
        reordered.push(entry);
    }
    reordered.extend(unnamed);
    *accounts = reordered;
    let serialized = serde_json::to_string_pretty(&document)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let temp_path = format!("{file_path}.rotation-tmp");
    tokio::fs::write(&temp_path, serialized.as_bytes()).await?;
    tokio::fs::rename(&temp_path, file_path).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(file_path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

fn rotation_error(message: impl Into<RuntimeString>) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Auth,
        message: message.into(),
        status: None,
        code: None,
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        retry_after_ms: None,
        request_id: None,
    }
}

#[async_trait::async_trait]
impl AuthSource for MultiAccountAuthSource {
    async fn managed_review_credentials(
        &self,
        signal: &AbortSignal,
    ) -> Result<Option<crate::provider::auth::ManagedReviewCredentials>, ProviderError> {
        let id = self.current_account_id();
        let slot = self.slot(&id).await?;
        let credentials = slot.managed_review_credentials(signal).await?;
        if self.current_account_id() != id {
            return Err(rotation_error(
                "OAuth owner changed during managed review authentication capture",
            ));
        }
        Ok(credentials)
    }

    async fn recover_managed_review_unauthorized(
        &self,
        recovery_step: usize,
        expected: &crate::provider::auth::ManagedReviewCredentialIdentity,
    ) -> crate::provider::auth::ManagedReviewCredentialRecovery {
        let id = self.current_account_id();
        let slot = match self.slot(&id).await {
            Ok(slot) => slot,
            Err(error) => {
                return crate::provider::auth::ManagedReviewCredentialRecovery::PermanentFailure(
                    error.to_string(),
                )
            }
        };
        if self.current_account_id() != id {
            return crate::provider::auth::ManagedReviewCredentialRecovery::PermanentFailure(
                "OAuth owner changed during managed review authentication recovery".into(),
            );
        }
        slot.recover_managed_review_unauthorized(recovery_step, expected)
            .await
    }

    fn is_subscription_oauth(&self) -> bool {
        true
    }
    fn credential_changes(
        &self,
    ) -> Option<tokio::sync::watch::Receiver<crate::provider::auth::CredentialChangeState>> {
        Some(self.inner.credential_changes.subscribe())
    }
    async fn get_headers(&self, signal: &AbortSignal) -> Result<AuthHeaders, ProviderError> {
        let id = self.current_account_id();
        let slot = self.slot(&id).await?;
        match slot.get_headers(signal).await {
            Ok(headers) => Ok(headers),
            Err(error) => {
                // A replaced-login lineage error means the cached slot is
                // stale: drop it and rebuild once from the current file copy.
                let message = error.message.try_to_utf8().unwrap_or_default();
                if message.contains("identity changed") {
                    self.inner.slots.lock().unwrap().remove(&id);
                    let slot = self.slot(&id).await?;
                    return slot.get_headers(signal).await;
                }
                Err(error)
            }
        }
    }

    fn base_url(&self) -> Option<&str> {
        self.inner.base_url.as_deref()
    }

    fn credential_account_id(&self) -> Option<String> {
        (self.account_count() > 1).then(|| self.current_account_id())
    }

    async fn rotate_credential(&self, failed_id: &str) -> Option<RotateOutcome> {
        Some(self.rotate(failed_id).await)
    }

    fn credential_request_succeeded(&self) {
        let mut anchor = self.inner.anchor.lock().unwrap();
        let mut exhausted = self.inner.exhausted.lock().unwrap();
        *anchor = None;
        *exhausted = false;
    }

    fn credential_request_started(&self) {
        let mut anchor = self.inner.anchor.lock().unwrap();
        let mut exhausted = self.inner.exhausted.lock().unwrap();
        if *exhausted {
            *anchor = None;
            *exhausted = false;
        }
    }

    async fn force_refresh(&self) -> Option<bool> {
        let id = self.current_account_id();
        let slot = match self.slot(&id).await {
            Ok(slot) => slot,
            Err(_) => return Some(false),
        };
        Some(slot.force_refresh().await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credential_store::CredentialStoreError;
    use std::sync::Mutex as StdMutex;

    #[derive(Default)]
    struct MemoryStore {
        values: StdMutex<HashMap<String, String>>,
    }

    #[async_trait::async_trait]
    impl CredentialStore for MemoryStore {
        async fn get(&self, key: &str) -> Result<Option<String>, CredentialStoreError> {
            Ok(self.values.lock().unwrap().get(key).cloned())
        }
        async fn set(&self, key: &str, value: &str) -> Result<(), CredentialStoreError> {
            self.values
                .lock()
                .unwrap()
                .insert(key.to_string(), value.to_string());
            Ok(())
        }
        async fn delete(&self, key: &str) -> Result<(), CredentialStoreError> {
            self.values.lock().unwrap().remove(key);
            Ok(())
        }
        async fn compare_exchange(
            &self,
            key: &str,
            expected: Option<&str>,
            replacement: &str,
        ) -> Result<bool, CredentialStoreError> {
            let mut values = self.values.lock().unwrap();
            let current = values.get(key).map(String::as_str);
            if current == expected {
                values.insert(key.to_string(), replacement.to_string());
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }

    fn spec(id: &str) -> RotationAccountSpec {
        RotationAccountSpec {
            id: id.to_string(),
            credential_id: format!("subscription-{id}"),
            tokens: OAuthTokens {
                access_token: RuntimeString::from(format!("access-{id}")),
                refresh_token: Some(RuntimeString::from(format!("refresh-{id}"))),
                expires_at: None,
                id_token: None,
            },
        }
    }

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "klaus-rotation-test-{tag}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    async fn source_with_file(dir: &std::path::Path, ids: &[&str]) -> MultiAccountAuthSource {
        let file_path = dir.join("claude-subscription-oauth.json");
        let accounts: Vec<serde_json::Value> = ids
            .iter()
            .map(|id| {
                serde_json::json!({
                    "id": id,
                    "email": format!("{id}@example.com"),
                    "tokens": { "accessToken": format!("access-{id}"), "refreshToken": format!("refresh-{id}") },
                })
            })
            .collect();
        tokio::fs::write(
            &file_path,
            serde_json::to_string_pretty(&serde_json::json!({ "accounts": accounts })).unwrap(),
        )
        .await
        .unwrap();
        MultiAccountAuthSource::new(RotationInit {
            file_path: file_path.to_string_lossy().into_owned(),
            config: OAuthConfig {
                client_id: "test-client".to_string(),
                authorize_url: "https://example.invalid/authorize".to_string(),
                token_url: "https://example.invalid/token".to_string(),
                redirect_uri: "http://127.0.0.1:1/callback".to_string(),
                scopes: None,
                extra_authorize_params: None,
            },
            base_url: None,
            extra_headers: HashMap::new(),
            store: Arc::new(MemoryStore::default()),
            accounts: ids.iter().map(|id| spec(id)).collect(),
        })
        .unwrap()
    }

    async fn file_order(dir: &std::path::Path) -> Vec<String> {
        let raw = tokio::fs::read_to_string(dir.join("claude-subscription-oauth.json"))
            .await
            .unwrap();
        let document: serde_json::Value = serde_json::from_str(&raw).unwrap();
        document["accounts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["id"].as_str().unwrap().to_string())
            .collect()
    }

    #[tokio::test]
    async fn rotation_wraps_a_full_cycle_then_exhausts() {
        let dir = temp_dir("case");
        let source = source_with_file(&dir, &["a", "b", "c"]).await;
        assert_eq!(source.current_account_id(), "a");

        assert_eq!(
            source.rotate("a").await,
            RotateOutcome::Rotated("b".to_string())
        );
        assert_eq!(file_order(&dir).await, vec!["b", "c", "a"]);
        assert_eq!(
            source.rotate("b").await,
            RotateOutcome::Rotated("c".to_string())
        );
        assert_eq!(
            source.rotate("c").await,
            RotateOutcome::Rotated("a".to_string())
        );
        assert_eq!(file_order(&dir).await, vec!["a", "b", "c"]);
        // The account that started the cycle failing again ends it.
        assert_eq!(source.rotate("a").await, RotateOutcome::Exhausted);
        // The turn must not restart the same exhausted cycle.
        assert_eq!(source.rotate("a").await, RotateOutcome::Exhausted);
        source.credential_request_started();
        // A later request can start a fresh cycle.
        assert_eq!(
            source.rotate("a").await,
            RotateOutcome::Rotated("b".to_string())
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn stale_failed_id_reuses_the_winner_without_advancing() {
        let dir = temp_dir("case");
        let source = source_with_file(&dir, &["a", "b"]).await;
        assert_eq!(
            source.rotate("a").await,
            RotateOutcome::Rotated("b".to_string())
        );
        // A second turn that observed the same quota error on `a` must not
        // burn another cycle step.
        assert_eq!(
            source.rotate("a").await,
            RotateOutcome::AlreadyAdvanced("b".to_string())
        );
        assert_eq!(source.current_account_id(), "b");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn single_account_queue_is_terminal() {
        let dir = temp_dir("case");
        let source = source_with_file(&dir, &["only"]).await;
        assert_eq!(source.rotate("only").await, RotateOutcome::Exhausted);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn persisted_order_keeps_host_only_accounts_at_the_tail() {
        let dir = temp_dir("case");
        let file_path = &dir.join("claude-subscription-oauth.json");
        tokio::fs::write(
            &file_path,
            serde_json::to_string_pretty(&serde_json::json!({
                "accounts": [
                    { "id": "a", "tokens": { "accessToken": "ta" } },
                    { "id": "b", "tokens": { "accessToken": "tb" } },
                    { "id": "host-added", "tokens": { "accessToken": "th" } },
                ]
            }))
            .unwrap(),
        )
        .await
        .unwrap();
        let source = MultiAccountAuthSource::new(RotationInit {
            file_path: file_path.to_string_lossy().into_owned(),
            config: OAuthConfig::default(),
            base_url: None,
            extra_headers: HashMap::new(),
            store: Arc::new(MemoryStore::default()),
            accounts: vec![spec("a"), spec("b")],
        })
        .unwrap();
        assert_eq!(
            source.rotate("a").await,
            RotateOutcome::Rotated("b".to_string())
        );
        assert_eq!(file_order(&dir).await, vec!["b", "a", "host-added"]);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
