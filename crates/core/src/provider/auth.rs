//! Pluggable auth for the model providers. The request/stream logic is
//! unchanged — only where the credentials come from differs: a static API key,
//! a static bearer token, or an OAuth subscription token (see [`super::oauth`]).

use crate::provider::access_policy::{
    evaluate_credential_access, CredentialAccessDenial, CredentialAccessGrant,
    CredentialAccessRequest, CredentialOperation, ProviderAccessPolicy, ProviderCredentialKind,
    ProviderIdentity,
};
use crate::runtime_values::RuntimeString;
use crate::types::abort::AbortSignal;
use crate::types::errors::{ProviderError, ProviderErrorKind};
use crate::types::provider::AuthHeaders;
use std::collections::{HashMap, HashSet};

const MAX_PROVIDER_EXTRA_HEADERS: usize = 32;
const MAX_PROVIDER_EXTRA_HEADER_VALUE_BYTES: usize = 8 * 1024;
const MAX_PROVIDER_EXTRA_HEADER_BYTES: usize = 32 * 1024;

fn protected_provider_header(name: &str) -> bool {
    matches!(
        name,
        "accept"
            | "api-key"
            | "authorization"
            | "connection"
            | "content-length"
            | "content-type"
            | "cookie"
            | "expect"
            | "host"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "proxy-connection"
            | "set-cookie"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "x-api-key"
            | "x-goog-api-key"
    )
}

/// Validate caller-supplied provider headers without ever including values in
/// diagnostics. Authentication, framing, routing, and hop-by-hop headers remain
/// owned by the transport/auth layers.
pub fn validate_provider_extra_headers(headers: &HashMap<String, String>) -> Result<(), String> {
    if headers.len() > MAX_PROVIDER_EXTRA_HEADERS {
        return Err(format!(
            "provider extraHeaders supports at most {MAX_PROVIDER_EXTRA_HEADERS} entries"
        ));
    }
    let mut normalized = HashSet::new();
    let mut total_bytes = 0usize;
    for (name, value) in headers {
        let parsed_name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| format!("provider extraHeaders contains invalid header name '{name}'"))?;
        let normalized_name = parsed_name.as_str().to_string();
        if !normalized.insert(normalized_name.clone()) {
            return Err(format!(
                "provider extraHeaders contains duplicate header name '{normalized_name}'"
            ));
        }
        if protected_provider_header(&normalized_name) {
            return Err(format!(
                "provider extraHeaders cannot set protected header '{normalized_name}'"
            ));
        }
        if value.len() > MAX_PROVIDER_EXTRA_HEADER_VALUE_BYTES {
            return Err(format!(
                "provider extraHeaders value for '{normalized_name}' exceeds {MAX_PROVIDER_EXTRA_HEADER_VALUE_BYTES} bytes"
            ));
        }
        reqwest::header::HeaderValue::from_str(value).map_err(|_| {
            format!("provider extraHeaders value for '{normalized_name}' is not a valid HTTP header value")
        })?;
        total_bytes = total_bytes
            .checked_add(name.len())
            .and_then(|total| total.checked_add(value.len()))
            .ok_or_else(|| "provider extraHeaders size overflow".to_string())?;
        if total_bytes > MAX_PROVIDER_EXTRA_HEADER_BYTES {
            return Err(format!(
                "provider extraHeaders exceeds {MAX_PROVIDER_EXTRA_HEADER_BYTES} total bytes"
            ));
        }
    }
    Ok(())
}

fn invalid_extra_headers(error: String) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::InvalidRequest,
        message: error.into(),
        status: Some(400),
        code: None,
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        retry_after_ms: None,
        request_id: None,
    }
}

/// Credential-access context shared by concrete [`AuthSource`] implementations.
#[derive(Debug, Clone)]
pub struct ProviderAuthAccess {
    provider_id: String,
    policy: ProviderAccessPolicy,
    identity: Option<ProviderIdentity>,
}

impl ProviderAuthAccess {
    pub fn new(provider_id: impl Into<String>, policy: ProviderAccessPolicy) -> Self {
        Self {
            provider_id: provider_id.into(),
            policy,
            identity: None,
        }
    }

    pub fn allow_all(provider_id: impl Into<String>) -> Self {
        Self::new(provider_id, ProviderAccessPolicy::allow_all())
    }

    pub fn with_identity(mut self, identity: ProviderIdentity) -> Self {
        self.identity = Some(identity);
        self
    }

    pub fn with_optional_identity(mut self, identity: Option<ProviderIdentity>) -> Self {
        self.identity = identity;
        self
    }

    pub fn evaluate(
        &self,
        kind: ProviderCredentialKind,
        operation: CredentialOperation,
        credential_available: bool,
        token_expired: bool,
        refresh_token_available: bool,
    ) -> Result<CredentialAccessGrant, ProviderError> {
        evaluate_credential_access(
            &self.policy,
            CredentialAccessRequest {
                provider_id: self.provider_id.clone(),
                kind,
                operation,
                credential_available,
                token_expired,
                refresh_token_available,
                identity: self.identity.clone(),
            },
        )
        .map_err(credential_access_error)
    }
}

impl Default for ProviderAuthAccess {
    fn default() -> Self {
        Self::allow_all("provider")
    }
}

fn credential_access_error(denial: CredentialAccessDenial) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Auth,
        message: RuntimeString::from(format!("provider credential access denied: {denial:?}")),
        status: None,
        code: None,
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        retry_after_ms: None,
        request_id: None,
    }
}

/// Account metadata captured from the current OAuth grant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedReviewCredentialIdentity {
    pub chatgpt_user_id: Option<String>,
    pub account_id: Option<String>,
    pub plan_type: Option<String>,
    pub owner_generation: u64,
}

pub struct ManagedReviewCredentials {
    pub identity: ManagedReviewCredentialIdentity,
    pub headers: AuthHeaders,
}

impl std::fmt::Debug for ManagedReviewCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedReviewCredentials")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagedReviewCredentialRecovery {
    Refreshed,
    TransientFailure(String),
    PermanentFailure(String),
    Unavailable,
}

/// Credential refresh and ownership revisions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CredentialChangeState {
    pub revision: u64,
    pub owner_generation: u64,
}

/// A source of request credentials held by a native provider.
#[async_trait::async_trait]
pub trait AuthSource: Send + Sync {
    /// Headers to attach to each request (e.g. `Authorization` / `x-api-key`).
    /// May refresh transparently.
    async fn get_headers(&self, signal: &AbortSignal) -> Result<AuthHeaders, ProviderError>;

    /// True for a subscription login, rather than a configured bearer credential.
    fn is_subscription_oauth(&self) -> bool {
        false
    }

    /// Refreshes invalidate idle connections; owner changes also cancel active work.
    fn credential_changes(&self) -> Option<tokio::sync::watch::Receiver<CredentialChangeState>> {
        None
    }

    async fn managed_review_credentials(
        &self,
        _signal: &AbortSignal,
    ) -> Result<Option<ManagedReviewCredentials>, ProviderError> {
        Ok(None)
    }

    async fn recover_managed_review_unauthorized(
        &self,
        _recovery_step: usize,
        _expected: &ManagedReviewCredentialIdentity,
    ) -> ManagedReviewCredentialRecovery {
        ManagedReviewCredentialRecovery::Unavailable
    }

    /// Optional base-URL override (subscription backends differ from the public
    /// API).
    fn base_url(&self) -> Option<&str> {
        None
    }

    /// Reactively recover from a 401/403: force a credential refresh REGARDLESS
    /// of the local expiry check (the server rejected a token that isn't locally
    /// expired — e.g. a server-side revocation or a clock skew the proactive
    /// refresh missed), so the caller can retry once. `Some(true)` when a fresh
    /// credential was obtained. Present only where the credential is refreshable
    /// (OAuth) — a static API key / bearer returns `None`, leaving a 401
    /// terminal.
    async fn force_refresh(&self) -> Option<bool> {
        None
    }

    /// Account identity captured before a request, when another credential is available.
    fn credential_account_id(&self) -> Option<String> {
        None
    }

    /// Advance past the failed request's account without double-advancing concurrent failures.
    async fn rotate_credential(
        &self,
        _failed_id: &str,
    ) -> Option<crate::provider::rotation::RotateOutcome> {
        None
    }

    /// A new request may retry a previously exhausted queue, not an active recovery cycle.
    fn credential_request_started(&self) {}

    /// A completed response starts a fresh allowance-recovery cycle.
    fn credential_request_succeeded(&self) {}
}

/// The credential scheme for [`ApiKeyAuth`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ApiKeyScheme {
    /// `x-api-key: <key>` (Anthropic).
    #[default]
    XApiKey,
    /// `Authorization: Bearer <key>` (OpenAI).
    Bearer,
}

/// Standard API-key auth (`x-api-key` for Anthropic, `Authorization: Bearer` for
/// OpenAI).
pub struct ApiKeyAuth {
    key: String,
    scheme: ApiKeyScheme,
    base_url: Option<String>,
    access: ProviderAuthAccess,
}

impl ApiKeyAuth {
    pub fn new(key: impl Into<String>, scheme: ApiKeyScheme, base_url: Option<String>) -> Self {
        Self {
            key: key.into(),
            scheme,
            base_url,
            access: ProviderAuthAccess::allow_all("api-key"),
        }
    }

    pub fn with_auth_access(mut self, access: ProviderAuthAccess) -> Self {
        self.access = access;
        self
    }
}

#[async_trait::async_trait]
impl AuthSource for ApiKeyAuth {
    async fn get_headers(&self, _signal: &AbortSignal) -> Result<AuthHeaders, ProviderError> {
        self.access.evaluate(
            ProviderCredentialKind::ApiKey,
            CredentialOperation::Use,
            !self.key.is_empty(),
            false,
            false,
        )?;
        let mut headers = HashMap::new();
        match self.scheme {
            ApiKeyScheme::Bearer => {
                headers.insert("Authorization".to_string(), format!("Bearer {}", self.key));
            }
            ApiKeyScheme::XApiKey => {
                headers.insert("x-api-key".to_string(), self.key.clone());
            }
        }
        Ok(headers)
    }

    fn base_url(&self) -> Option<&str> {
        self.base_url.as_deref()
    }
}

/// A fixed bearer token plus optional extra headers (e.g. a beta flag).
pub struct StaticBearerAuth {
    token: String,
    base_url: Option<String>,
    extra_headers: HashMap<String, String>,
    access: ProviderAuthAccess,
}

impl StaticBearerAuth {
    pub fn new(
        token: impl Into<String>,
        base_url: Option<String>,
        extra_headers: HashMap<String, String>,
    ) -> Self {
        Self {
            token: token.into(),
            base_url,
            extra_headers,
            access: ProviderAuthAccess::allow_all("bearer-token"),
        }
    }

    pub fn with_auth_access(mut self, access: ProviderAuthAccess) -> Self {
        self.access = access;
        self
    }
}

#[async_trait::async_trait]
impl AuthSource for StaticBearerAuth {
    async fn get_headers(&self, _signal: &AbortSignal) -> Result<AuthHeaders, ProviderError> {
        validate_provider_extra_headers(&self.extra_headers).map_err(invalid_extra_headers)?;
        self.access.evaluate(
            ProviderCredentialKind::BearerToken,
            CredentialOperation::Use,
            !self.token.is_empty(),
            false,
            false,
        )?;
        let mut headers = self.extra_headers.clone();
        headers.insert(
            "Authorization".to_string(),
            format!("Bearer {}", self.token),
        );
        Ok(headers)
    }

    fn base_url(&self) -> Option<&str> {
        self.base_url.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::access_policy::ProviderAccessPolicy;
    use crate::types::errors::ProviderErrorKind;

    #[tokio::test]
    async fn static_bearer_auth_cannot_be_overridden_by_extra_headers() {
        let auth = StaticBearerAuth::new(
            "trusted-token",
            None,
            HashMap::from([("authorization".to_string(), "Bearer attacker".to_string())]),
        );

        let error = auth
            .get_headers(&AbortSignal::new())
            .await
            .expect_err("protected headers must fail closed");
        let message = String::from_utf16_lossy(error.message.as_utf16());
        assert!(message.contains("authorization"));
        assert!(!message.contains("attacker"));
    }

    #[tokio::test]
    async fn api_key_auth_enforces_provider_access_policy() {
        let auth = ApiKeyAuth::new("secret", ApiKeyScheme::Bearer, None).with_auth_access(
            ProviderAuthAccess::new(
                "openai",
                ProviderAccessPolicy::allow_all().allow_providers(["anthropic"]),
            ),
        );

        let err = auth
            .get_headers(&AbortSignal::new())
            .await
            .expect_err("provider policy should deny credential access");

        assert_eq!(err.kind, ProviderErrorKind::Auth);
        assert!(err.message.contains("ProviderDenied"), "{err}");
    }
}
