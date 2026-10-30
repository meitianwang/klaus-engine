//! OAuth 2.0 Authorization-Code-with-PKCE credentials (RFC 6749 + RFC 7636).
//!
//! Subscription logins (Claude / ChatGPT) are ordinary OAuth providers; supply
//! the provider's endpoints + client id in an [`OAuthConfig`]. Nothing here is
//! provider-specific: PKCE challenge generation, the authorize-URL builder, the
//! token exchange/refresh round-trips, and the [`OAuthAuth`] credential source
//! that attaches the access token and transparently refreshes it.

use crate::fs::lockfile::{with_lock, LockOptions};
use crate::fs::paths::{config_home, sanitize_path_component};
use crate::provider::access_policy::{CredentialOperation, ProviderCredentialKind};
use crate::provider::auth::{
    validate_provider_extra_headers, AuthSource, ManagedReviewCredentialIdentity,
    ManagedReviewCredentialRecovery, ManagedReviewCredentials, ProviderAuthAccess,
};
use crate::runtime_process::RuntimeProcessContext;
use crate::runtime_values::{
    runtime_codec, runtime_json, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder,
    RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};
use crate::types::abort::AbortSignal;
use crate::types::errors::{ProviderError, ProviderErrorKind};
use crate::types::provider::AuthHeaders;
use futures::future::{BoxFuture, Shared};
use futures::FutureExt;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub use crate::credential_store::CredentialStore;

/// A clock returning epoch milliseconds. Injected in tests; defaults to the
/// system clock.
pub type Now = Arc<dyn Fn() -> i64 + Send + Sync>;

/// Standard OAuth 2.0 Authorization-Code-with-PKCE config.
#[derive(Debug, Clone, Default)]
pub struct OAuthConfig {
    pub client_id: String,
    pub authorize_url: String,
    pub token_url: String,
    pub redirect_uri: String,
    pub scopes: Option<Vec<String>>,
    /// Extra params some providers require on the authorize request (e.g.
    /// audience). Order-preserving.
    pub extra_authorize_params: Option<Vec<(String, String)>>,
}

/// OAuth tokens, persisted as JSON in the credential store.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OAuthTokens {
    pub access_token: RuntimeString,
    pub refresh_token: Option<RuntimeString>,
    /// Epoch ms when the access token expires.
    pub expires_at: Option<i64>,
    pub id_token: Option<RuntimeString>,
}

impl RuntimeEncode for OAuthTokens {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("accessToken", &self.access_token)?;
        if let Some(value) = &self.refresh_token {
            object.field("refreshToken", value)?;
        }
        if let Some(value) = &self.expires_at {
            object.field("expiresAt", value)?;
        }
        if let Some(value) = &self.id_token {
            object.field("idToken", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for OAuthTokens {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                access_token: object.required("accessToken", &[])?,
                refresh_token: object.optional("refreshToken", &[])?,
                expires_at: object.optional("expiresAt", &[])?,
                id_token: object.optional("idToken", &[])?,
            })
        })
    }
}

/// An RFC 7636 S256 PKCE pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pkce {
    pub verifier: String,
    pub challenge: String,
}

// --- error helpers ---------------------------------------------------------

fn auth_error(message: impl Into<RuntimeString>) -> ProviderError {
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

fn network_error(message: impl Into<RuntimeString>) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Network,
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

fn runtime_oauth_json_to_credential_store_host(
    value: &RuntimeString,
) -> Result<String, ProviderError> {
    value.try_to_utf8().map_err(|error| {
        auth_error(format!(
            "persisted OAuth token JSON is not a Unicode scalar string: {error}"
        ))
    })
}

fn runtime_oauth_refresh_token_to_form_host(
    value: &RuntimeString,
) -> Result<String, ProviderError> {
    value.try_to_utf8().map_err(|error| {
        auth_error(format!(
            "OAuth refresh token is not a Unicode scalar string: {error}"
        ))
    })
}

fn runtime_oauth_access_token_to_header_host(
    value: &RuntimeString,
) -> Result<String, ProviderError> {
    value.try_to_utf8().map_err(|error| {
        auth_error(format!(
            "OAuth access token is not a Unicode scalar string: {error}"
        ))
    })
}

#[cfg(test)]
fn encode_persisted_tokens(tokens: &OAuthTokens) -> Result<String, ProviderError> {
    let value =
        runtime_codec::encode_text(tokens).map_err(|error| auth_error(error.to_string()))?;
    runtime_oauth_json_to_credential_store_host(&value)
}

#[cfg(test)]
fn decode_persisted_tokens(source: &str) -> Option<OAuthTokens> {
    runtime_codec::decode_text(&RuntimeString::from(source)).ok()
}

/// Private credentials captured during provider preparation. Neither the
/// generation nor the serialized credential is part of a public model snapshot.
#[derive(Clone)]
pub struct OAuthCredentialSnapshot {
    tokens: OAuthTokens,
    generation: String,
    raw: String,
}

impl std::fmt::Debug for OAuthCredentialSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthCredentialSnapshot")
            .finish_non_exhaustive()
    }
}

impl OAuthCredentialSnapshot {
    pub fn tokens(&self) -> OAuthTokens {
        self.tokens.clone()
    }
}

#[derive(Clone)]
struct CredentialState {
    tokens: OAuthTokens,
    generation: String,
    raw: Option<String>,
}

fn credential_lineage_generation(lineage: &str) -> u64 {
    let digest = Sha256::digest(lineage.as_bytes());
    u64::from_be_bytes(
        digest[..8]
            .try_into()
            .expect("SHA-256 prefix has eight bytes"),
    )
}

impl From<OAuthCredentialSnapshot> for CredentialState {
    fn from(snapshot: OAuthCredentialSnapshot) -> Self {
        Self {
            tokens: snapshot.tokens,
            generation: snapshot.generation,
            raw: Some(snapshot.raw),
        }
    }
}

fn encode_generation_tokens(
    tokens: &OAuthTokens,
    generation: &str,
) -> Result<String, ProviderError> {
    let mut value = runtime_codec::encode_value(tokens)
        .map_err(|_| auth_error("failed to encode OAuth credential"))?;
    let RuntimeJson::Object(object) = &mut value else {
        return Err(auth_error("invalid OAuth credential"));
    };
    object.insert(
        "credentialGeneration",
        RuntimeJson::String(generation.into()),
    );
    runtime_oauth_json_to_credential_store_host(&runtime_json::stringify(&value))
}

fn decode_generation_tokens(raw: &str) -> Result<(OAuthTokens, Option<String>), ProviderError> {
    let value = runtime_json::parse(&RuntimeString::from(raw))
        .map_err(|_| auth_error("invalid stored OAuth credential"))?;
    let tokens = OAuthTokens::decode_runtime(&value)
        .map_err(|_| auth_error("invalid stored OAuth credential"))?;
    if tokens.access_token.is_empty() {
        return Err(auth_error("stored OAuth access token is missing"));
    }
    let generation = match value.get("credentialGeneration") {
        None => None,
        Some(value) => {
            let generation = value
                .as_str()
                .and_then(|value| value.try_to_utf8().ok())
                .filter(|value| uuid::Uuid::parse_str(value).is_ok())
                .ok_or_else(|| auth_error("invalid OAuth credential generation"))?;
            Some(generation)
        }
    };
    Ok((tokens, generation))
}

async fn capture_stored_credential(
    store: &Arc<dyn CredentialStore>,
    key: &str,
) -> Result<Option<OAuthCredentialSnapshot>, ProviderError> {
    for _ in 0..4 {
        let Some(raw) = store
            .get(key)
            .await
            .map_err(|_| auth_error("failed to read stored OAuth credential"))?
        else {
            return Ok(None);
        };
        let (tokens, generation) = decode_generation_tokens(&raw)?;
        if let Some(generation) = generation {
            return Ok(Some(OAuthCredentialSnapshot {
                tokens,
                generation,
                raw,
            }));
        }
        // Legacy records acquire a lineage exactly once under the backend CAS.
        // A concurrent login or migration wins rather than being overwritten.
        let generation = uuid::Uuid::new_v4().to_string();
        let replacement = encode_generation_tokens(&tokens, &generation)?;
        if store
            .compare_exchange(key, Some(&raw), &replacement)
            .await
            .map_err(|_| auth_error("OAuth credential store requires atomic conditional writes"))?
        {
            return Ok(Some(OAuthCredentialSnapshot {
                tokens,
                generation,
                raw: replacement,
            }));
        }
    }
    Err(auth_error("OAuth credential changed during preparation"))
}

// --- HTTP seam -------------------------------------------------------------

/// A single outbound HTTP request.
pub struct HttpRequest {
    pub method: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

/// A completed HTTP response. `ok` is true for a 2xx status.
pub struct HttpResponse {
    pub ok: bool,
    pub status: u16,
    pub body: String,
}

impl HttpResponse {
    /// Parse the body as JSON.
    pub fn json(&self) -> Result<RuntimeJson, ProviderError> {
        runtime_json::parse(&RuntimeString::from(&self.body)).map_err(|e| auth_error(e.to_string()))
    }
}

/// The HTTP transport seam, injectable in tests.
#[async_trait::async_trait]
pub trait HttpFetch: Send + Sync {
    async fn fetch(&self, url: &str, req: HttpRequest) -> Result<HttpResponse, ProviderError>;
}

/// The production transport over `reqwest`.
pub struct ReqwestFetch {
    client: reqwest::Client,
}

impl ReqwestFetch {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl Default for ReqwestFetch {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl HttpFetch for ReqwestFetch {
    async fn fetch(&self, url: &str, req: HttpRequest) -> Result<HttpResponse, ProviderError> {
        let method = reqwest::Method::from_bytes(req.method.as_bytes())
            .map_err(|e| network_error(e.to_string()))?;
        let mut builder = self.client.request(method, url);
        for (key, value) in &req.headers {
            builder = builder.header(key, value);
        }
        let response = builder
            .body(req.body)
            .send()
            .await
            .map_err(|e| network_error(e.to_string()))?;
        let status = response.status().as_u16();
        let ok = response.status().is_success();
        let body = response
            .text()
            .await
            .map_err(|e| network_error(e.to_string()))?;
        Ok(HttpResponse { ok, status, body })
    }
}

/// Optional transport + clock overrides for the standalone token calls.
#[derive(Clone)]
pub struct RequestOpts {
    pub fetch: Arc<dyn HttpFetch>,
    pub now: Now,
}

impl Default for RequestOpts {
    fn default() -> Self {
        Self {
            fetch: default_fetch(),
            now: default_now(),
        }
    }
}

fn default_fetch() -> Arc<dyn HttpFetch> {
    Arc::new(ReqwestFetch::new())
}

fn default_now() -> Now {
    Arc::new(|| chrono::Utc::now().timestamp_millis())
}

// --- encoding helpers ------------------------------------------------------

/// URL-safe base64 with no padding (`+`→`-`, `/`→`_`, trailing `=` stripped).
fn base64url(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// `n` cryptographically-random bytes.
fn random_bytes(n: usize) -> Vec<u8> {
    use rand::RngCore;
    let mut buf = vec![0u8; n];
    rand::rngs::OsRng.fill_bytes(&mut buf);
    buf
}

/// `application/x-www-form-urlencoded` component serialization (WHATWG form
/// serializer): unreserved bytes pass through, space becomes `+`, everything
/// else is percent-encoded.
fn encode_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'*' | b'-' | b'.' | b'_' => out.push(b as char),
            b'0'..=b'9' | b'A'..=b'Z' | b'a'..=b'z' => out.push(b as char),
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

/// Serialize key/value pairs into a form-urlencoded body/query string.
fn form_serialize(pairs: &[(&str, &str)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", encode_component(k), encode_component(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// Decode one form-urlencoded component (`+`→space, `%XX`→byte).
fn decode_component(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hi = (bytes[i + 1] as char).to_digit(16);
                let lo = (bytes[i + 2] as char).to_digit(16);
                match (hi, lo) {
                    (Some(h), Some(l)) => {
                        out.push((h * 16 + l) as u8);
                        i += 3;
                    }
                    _ => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Look up a query parameter by key, matching the URL search-params decoder.
fn query_get(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };
        if decode_component(k) == key {
            return Some(decode_component(v));
        }
    }
    None
}

// --- PKCE + authorize URL --------------------------------------------------

/// Generate an RFC 7636 S256 PKCE pair.
pub fn generate_pkce() -> Pkce {
    let verifier = base64url(&random_bytes(32));
    let mut hasher = Sha256::new();
    hasher.update(verifier.as_bytes());
    let challenge = base64url(&hasher.finalize());
    Pkce {
        verifier,
        challenge,
    }
}

/// Build the authorize-request URL with the standard PKCE query parameters.
pub fn build_authorize_url(config: &OAuthConfig, challenge: &str, state: &str) -> String {
    let mut pairs: Vec<(String, String)> = vec![
        ("response_type".to_string(), "code".to_string()),
        ("client_id".to_string(), config.client_id.clone()),
        ("redirect_uri".to_string(), config.redirect_uri.clone()),
        ("code_challenge".to_string(), challenge.to_string()),
        ("code_challenge_method".to_string(), "S256".to_string()),
        ("state".to_string(), state.to_string()),
    ];
    if let Some(scopes) = &config.scopes {
        if !scopes.is_empty() {
            pairs.push(("scope".to_string(), scopes.join(" ")));
        }
    }
    if let Some(extra) = &config.extra_authorize_params {
        for (k, v) in extra {
            pairs.push((k.clone(), v.clone()));
        }
    }
    let query = pairs
        .iter()
        .map(|(k, v)| format!("{}={}", encode_component(k), encode_component(v)))
        .collect::<Vec<_>>()
        .join("&");
    format!("{}?{}", config.authorize_url, query)
}

// --- token exchange / refresh ----------------------------------------------

fn coerce_string(value: Option<&RuntimeJson>) -> RuntimeString {
    match value {
        None | Some(RuntimeJson::Null) => RuntimeString::default(),
        Some(RuntimeJson::String(value)) => value.clone(),
        Some(other) => runtime_json::stringify(other),
    }
}

fn parse_token_response(json: &RuntimeJson, now: i64) -> OAuthTokens {
    let access_token = coerce_string(json.get("access_token"));
    let mut tokens = OAuthTokens {
        access_token,
        refresh_token: None,
        expires_at: None,
        id_token: None,
    };
    if let Some(v) = json.get("refresh_token") {
        if let Some(s) = v.as_str() {
            tokens.refresh_token = Some(s.clone());
        }
    }
    if let Some(v) = json.get("id_token") {
        if let Some(s) = v.as_str() {
            tokens.id_token = Some(s.clone());
        }
    }
    if let Some(v) = json.get("expires_in") {
        if let Some(n) = v.as_f64() {
            tokens.expires_at = Some(now + (n * 1000.0) as i64);
        }
    }
    tokens
}

/// Exchange an authorization code for tokens (PKCE — no client secret).
pub async fn exchange_code(
    config: &OAuthConfig,
    code: &str,
    verifier: &str,
    opts: RequestOpts,
) -> Result<OAuthTokens, ProviderError> {
    let body = form_serialize(&[
        ("grant_type", "authorization_code"),
        ("code", code),
        ("redirect_uri", &config.redirect_uri),
        ("client_id", &config.client_id),
        ("code_verifier", verifier),
    ]);
    let res = opts
        .fetch
        .fetch(
            &config.token_url,
            HttpRequest {
                method: "POST".to_string(),
                headers: vec![(
                    "content-type".to_string(),
                    "application/x-www-form-urlencoded".to_string(),
                )],
                body,
            },
        )
        .await?;
    if !res.ok {
        return Err(auth_error(format!(
            "OAuth token exchange failed: {}",
            res.status
        )));
    }
    Ok(parse_token_response(&res.json()?, (*opts.now)()))
}

/// Refresh an access token using a refresh token.
pub async fn refresh_tokens(
    config: &OAuthConfig,
    refresh_token: &str,
    opts: RequestOpts,
) -> Result<OAuthTokens, ProviderError> {
    let body = form_serialize(&[
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh_token),
        ("client_id", &config.client_id),
    ]);
    let res = opts
        .fetch
        .fetch(
            &config.token_url,
            HttpRequest {
                method: "POST".to_string(),
                headers: vec![(
                    "content-type".to_string(),
                    "application/x-www-form-urlencoded".to_string(),
                )],
                body,
            },
        )
        .await?;
    if !res.ok {
        return Err(auth_error(format!("OAuth refresh failed: {}", res.status)));
    }
    let mut tokens = parse_token_response(&res.json()?, (*opts.now)());
    // Some providers omit the refresh token on refresh — keep the old one.
    if tokens.refresh_token.is_none() {
        tokens.refresh_token = Some(refresh_token.into());
    }
    Ok(tokens)
}

// --- OAuthAuth credential source -------------------------------------------

/// Options for constructing an [`OAuthAuth`].
#[derive(Default)]
pub struct OAuthAuthOptions {
    pub config: OAuthConfig,
    /// Persist/restore tokens (keyed by client id).
    pub store: Option<Arc<dyn CredentialStore>>,
    /// Initial tokens (e.g. from a prior login), if not loading from the store.
    pub tokens: Option<OAuthTokens>,
    /// Store lineage captured before publication, including its private tokens.
    pub prepared_credential: Option<OAuthCredentialSnapshot>,
    /// Base-URL override for the subscription backend.
    pub base_url: Option<String>,
    /// Extra static headers to send (e.g. a beta flag).
    pub extra_headers: Option<HashMap<String, String>>,
    /// Refresh when fewer than this many ms remain (default 60s).
    pub refresh_skew_ms: Option<i64>,
    pub fetch: Option<Arc<dyn HttpFetch>>,
    pub now: Option<Now>,
    /// Directory for cross-process refresh locks. Defaults to the Klaus config
    /// home so separate stdio endpoints coordinate token rotation.
    pub refresh_lock_dir: Option<String>,
    /// Stable non-secret lock identity. Defaults to the OAuth client id.
    pub refresh_lock_key: Option<String>,
    /// Optional provider credential access policy. Defaults to allow-all.
    pub access: Option<ProviderAuthAccess>,
}

type RefreshFuture = Shared<BoxFuture<'static, bool>>;

struct OAuthInner {
    config: OAuthConfig,
    store: Option<Arc<dyn CredentialStore>>,
    base_url: Option<String>,
    extra_headers: HashMap<String, String>,
    skew: i64,
    fetch: Arc<dyn HttpFetch>,
    now: Now,
    refresh_lock_dir: String,
    refresh_lock_key: String,
    access: ProviderAuthAccess,
    credential: Mutex<Option<CredentialState>>,
    /// The in-flight single-flight force-refresh, tagged with a generation so it
    /// clears only itself on settle.
    pending: Mutex<Option<(u64, RefreshFuture)>>,
    gen_counter: AtomicU64,
    credential_changes: tokio::sync::watch::Sender<crate::provider::auth::CredentialChangeState>,
}

impl OAuthInner {
    fn credential_owner_generation(&self) -> Option<u64> {
        self.credential
            .lock()
            .unwrap()
            .as_ref()
            .map(|state| credential_lineage_generation(&state.generation))
    }

    fn managed_review_snapshot(&self) -> Result<Option<ManagedReviewCredentials>, ProviderError> {
        validate_provider_extra_headers(&self.extra_headers).map_err(auth_error)?;
        let Some(state) = self.credential.lock().unwrap().clone() else {
            return Ok(None);
        };
        let claims = managed_review_token_claims(
            state
                .tokens
                .id_token
                .as_ref()
                .unwrap_or(&state.tokens.access_token),
        );
        let auth = claims.get("https://api.openai.com/auth");
        let string = |key: &str| {
            auth.and_then(|auth| auth.get(key))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        };
        let mut headers = self.extra_headers.clone();
        headers.insert(
            "Authorization".into(),
            format!(
                "Bearer {}",
                runtime_oauth_access_token_to_header_host(&state.tokens.access_token)?
            ),
        );
        let account_id = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("ChatGPT-Account-Id"))
            .map(|(_, value)| value.clone())
            .or_else(|| string("chatgpt_account_id"));
        Ok(Some(ManagedReviewCredentials {
            identity: ManagedReviewCredentialIdentity {
                chatgpt_user_id: string("chatgpt_user_id").or_else(|| string("user_id")),
                account_id,
                plan_type: string("chatgpt_plan_type"),
                owner_generation: credential_lineage_generation(&state.generation),
            },
            headers,
        }))
    }

    async fn managed_review_credentials(&self) -> Result<ManagedReviewCredentials, ProviderError> {
        for _ in 0..3 {
            let tokens = self.ensure_fresh().await?;
            let captured = self
                .managed_review_snapshot()?
                .ok_or_else(|| auth_error("OAuth credential is unavailable"))?;
            let unchanged = self
                .credential
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|state| {
                    state.tokens == tokens
                        && credential_lineage_generation(&state.generation)
                            == captured.identity.owner_generation
                });
            if unchanged {
                return Ok(captured);
            }
        }
        Err(auth_error(
            "OAuth credential changed while capturing managed review authentication",
        ))
    }

    fn require_managed_review_owner(
        &self,
        expected: &ManagedReviewCredentialIdentity,
    ) -> Result<(), ProviderError> {
        let current = self
            .managed_review_snapshot()?
            .ok_or_else(|| auth_error("OAuth credential is unavailable"))?;
        if current.identity.owner_generation != expected.owner_generation
            || current.identity.account_id != expected.account_id
            || current.identity.chatgpt_user_id != expected.chatgpt_user_id
        {
            return Err(auth_error(
                "OAuth owner changed during managed review authentication recovery",
            ));
        }
        Ok(())
    }

    fn store_key(&self) -> String {
        format!("oauth:{}", self.config.client_id)
    }

    /// Explicitly persist submitted login credentials. Once bound, even public
    /// save calls cannot overwrite a replacement login's generation.
    async fn save(&self) -> Result<(), ProviderError> {
        self.persist_initial(false).await
    }

    async fn save_if_absent(&self) -> Result<(), ProviderError> {
        self.persist_initial(true).await
    }

    async fn persist_initial(&self, require_absent: bool) -> Result<(), ProviderError> {
        let snapshot = { self.credential.lock().unwrap().clone() };
        let Some(mut snapshot) = snapshot else {
            return Ok(());
        };
        if let Some(store) = &self.store {
            let raw = encode_generation_tokens(&snapshot.tokens, &snapshot.generation)?;
            if let Some(expected) = snapshot.raw.as_deref() {
                if !store
                    .compare_exchange(&self.store_key(), Some(expected), &raw)
                    .await
                    .map_err(|_| {
                        auth_error("OAuth credential store requires atomic conditional writes")
                    })?
                {
                    return Err(auth_error("OAuth credential changed before save"));
                }
            } else if require_absent {
                if !store
                    .compare_exchange(&self.store_key(), None, &raw)
                    .await
                    .map_err(|_| {
                        auth_error("OAuth credential store requires atomic conditional writes")
                    })?
                {
                    return Err(auth_error("OAuth credential changed during preparation"));
                }
            } else {
                store
                    .set(&self.store_key(), &raw)
                    .await
                    .map_err(|_| auth_error("failed to persist OAuth login credential"))?;
            }
            snapshot.raw = Some(raw);
            self.install_refreshed(snapshot)?;
        }
        Ok(())
    }

    async fn replace_tokens(&self, tokens: OAuthTokens) -> Result<(), ProviderError> {
        let mut snapshot = CredentialState {
            tokens,
            generation: uuid::Uuid::new_v4().to_string(),
            raw: None,
        };
        if let Some(store) = &self.store {
            let raw = encode_generation_tokens(&snapshot.tokens, &snapshot.generation)?;
            store
                .set(&self.store_key(), &raw)
                .await
                .map_err(|_| auth_error("failed to persist OAuth login credential"))?;
            snapshot.raw = Some(raw);
        }
        *self.credential.lock().unwrap() = Some(snapshot);
        let owner_generation = self.credential_owner_generation().unwrap_or_default();
        self.credential_changes.send_modify(|state| {
            state.revision = state.revision.wrapping_add(1);
            state.owner_generation = owner_generation;
        });
        Ok(())
    }

    fn install_refreshed(&self, snapshot: CredentialState) -> Result<(), ProviderError> {
        let mut current = self.credential.lock().unwrap();
        if current
            .as_ref()
            .is_none_or(|current| current.generation != snapshot.generation)
        {
            return Err(auth_error("OAuth credential changed during refresh"));
        }
        let changed = current
            .as_ref()
            .is_none_or(|current| current.tokens != snapshot.tokens);
        *current = Some(snapshot);
        if changed {
            self.credential_changes.send_modify(|state| {
                state.revision = state.revision.wrapping_add(1);
            });
        }
        Ok(())
    }

    async fn load(&self) -> Result<Option<OAuthTokens>, ProviderError> {
        if let Some(snapshot) = self.credential.lock().unwrap().as_ref() {
            return Ok(Some(snapshot.tokens.clone()));
        }
        self.load_from_store().await
    }

    /// Import peer rotation only when the captured store lineage proves it is
    /// the same login. Token contents and unverified ID-token claims are not an
    /// identity oracle for a replacement record.
    async fn load_from_store(&self) -> Result<Option<OAuthTokens>, ProviderError> {
        let Some(store) = &self.store else {
            return Ok(self
                .credential
                .lock()
                .unwrap()
                .as_ref()
                .map(|snapshot| snapshot.tokens.clone()));
        };
        let stored = capture_stored_credential(store, &self.store_key()).await?;
        let mut current = self.credential.lock().unwrap();
        let Some(stored) = stored else {
            return match current.as_ref() {
                Some(snapshot) if snapshot.raw.is_none() => Ok(Some(snapshot.tokens.clone())),
                Some(_) => Err(auth_error("captured OAuth credential is no longer stored")),
                None => Ok(None),
            };
        };
        if let Some(snapshot) = current.as_ref() {
            let same_lineage = snapshot.raw.is_some() && snapshot.generation == stored.generation;
            let identical_submitted = snapshot.raw.is_none() && snapshot.tokens == stored.tokens;
            if !same_lineage && !identical_submitted {
                return Err(auth_error(
                    "OAuth credential identity changed; prepare a new provider",
                ));
            }
        }
        let tokens = stored.tokens.clone();
        let owner_generation = credential_lineage_generation(&stored.generation);
        let changed = current.as_ref().is_none_or(|current| {
            current.tokens != tokens
                || credential_lineage_generation(&current.generation) != owner_generation
        });
        *current = Some(stored.into());
        if changed {
            self.credential_changes.send_modify(|state| {
                state.revision = state.revision.wrapping_add(1);
                state.owner_generation = owner_generation;
            });
        }
        Ok(Some(tokens))
    }

    async fn refresh_and_save(&self, tokens: OAuthTokens) -> Result<OAuthTokens, ProviderError> {
        let mut snapshot = self
            .credential
            .lock()
            .unwrap()
            .clone()
            .filter(|snapshot| snapshot.tokens == tokens)
            .ok_or_else(|| auth_error("OAuth credential changed before refresh"))?;
        let refresh_token = tokens
            .refresh_token
            .as_ref()
            .ok_or_else(|| auth_error("OAuth refresh token is missing"))?;
        let refresh_token = runtime_oauth_refresh_token_to_form_host(refresh_token)?;
        let opts = RequestOpts {
            fetch: self.fetch.clone(),
            now: self.now.clone(),
        };
        let mut refreshed = refresh_tokens(&self.config, &refresh_token, opts)
            .await
            .map_err(|error| ProviderError {
                kind: error.kind,
                message: "OAuth token refresh failed".into(),
                status: error.status,
                code: error.code,
                error_type: error.error_type,
                media_origins: None,
                media_repair_complete: false,
                retry_after_ms: error.retry_after_ms,
                request_id: None,
            })?;
        if refreshed.id_token.is_none() {
            refreshed.id_token = tokens.id_token;
        }
        if let Some(store) = &self.store {
            let raw = encode_generation_tokens(&refreshed, &snapshot.generation)?;
            if !store
                .compare_exchange(&self.store_key(), snapshot.raw.as_deref(), &raw)
                .await
                .map_err(|_| {
                    auth_error("OAuth credential store requires atomic conditional writes")
                })?
            {
                // A same-generation peer may have rotated first. A different
                // login is rejected by load_from_store and never overwritten.
                let peer = self
                    .load_from_store()
                    .await?
                    .ok_or_else(|| auth_error("captured OAuth credential is no longer stored"))?;
                let expired = peer
                    .expires_at
                    .map(|expires_at| expires_at - (*self.now)() < self.skew)
                    .unwrap_or(false);
                if expired {
                    return Err(auth_error(
                        "OAuth credential rotated during refresh; retry authentication",
                    ));
                }
                return Ok(peer);
            }
            snapshot.raw = Some(raw);
        }
        snapshot.tokens = refreshed.clone();
        self.install_refreshed(snapshot)?;
        Ok(refreshed)
    }

    async fn refresh_coordinated(
        &self,
        observed: OAuthTokens,
        force: bool,
    ) -> Result<OAuthTokens, ProviderError> {
        if self.store.is_none() {
            return self.refresh_and_save(observed).await;
        }

        tokio::fs::create_dir_all(&self.refresh_lock_dir)
            .await
            .map_err(|err| auth_error(format!("failed to create OAuth lock directory: {err}")))?;
        let target = format!(
            "{}/provider-refresh-{}",
            self.refresh_lock_dir,
            sanitize_path_component(&self.refresh_lock_key)
        );
        with_lock(
            &target,
            || async {
                let current = self
                    .load_from_store()
                    .await
                    .map_err(std::io::Error::other)?
                    .unwrap_or_else(|| observed.clone());
                let peer_refreshed = current.access_token != observed.access_token
                    || current.refresh_token != observed.refresh_token
                    || current.expires_at != observed.expires_at;
                let expired = current
                    .expires_at
                    .map(|expires_at| expires_at - (*self.now)() < self.skew)
                    .unwrap_or(false);
                if !expired && (peer_refreshed || !force) {
                    return Ok(current);
                }
                self.refresh_and_save(current)
                    .await
                    .map_err(std::io::Error::other)
            },
            LockOptions {
                retries: Some(60),
                min_timeout_ms: Some(50),
                max_timeout_ms: Some(1_000),
                stale_ms: Some(120_000),
            },
        )
        .await
        .map_err(|err| {
            err.get_ref()
                .and_then(|source| source.downcast_ref::<ProviderError>())
                .cloned()
                .unwrap_or_else(|| auth_error(format!("OAuth refresh coordination failed: {err}")))
        })
    }

    async fn ensure_fresh(&self) -> Result<OAuthTokens, ProviderError> {
        let tokens = match self.load().await? {
            Some(tokens) => tokens,
            None => {
                self.access.evaluate(
                    ProviderCredentialKind::OAuthAccessToken,
                    CredentialOperation::Use,
                    false,
                    false,
                    false,
                )?;
                return Err(auth_error(
                    "Not logged in — run the subscription login flow first.",
                ));
            }
        };
        let expired = match tokens.expires_at {
            Some(exp) => exp - (*self.now)() < self.skew,
            None => false,
        };
        self.access.evaluate(
            ProviderCredentialKind::OAuthAccessToken,
            CredentialOperation::Use,
            true,
            expired,
            tokens.refresh_token.is_some(),
        )?;
        if expired && tokens.refresh_token.is_some() {
            return self.refresh_coordinated(tokens, false).await;
        }
        Ok(tokens)
    }

    async fn get_headers(&self) -> Result<AuthHeaders, ProviderError> {
        validate_provider_extra_headers(&self.extra_headers).map_err(|message| ProviderError {
            kind: ProviderErrorKind::InvalidRequest,
            message: message.into(),
            status: Some(400),
            code: None,
            error_type: None,
            media_origins: None,
            media_repair_complete: false,
            retry_after_ms: None,
            request_id: None,
        })?;
        let tokens = self.ensure_fresh().await?;
        let access_token = runtime_oauth_access_token_to_header_host(&tokens.access_token)?;
        let mut headers = self.extra_headers.clone();
        headers.insert(
            "Authorization".to_string(),
            format!("Bearer {access_token}"),
        );
        Ok(headers)
    }

    /// The refresh body of `force_refresh`: refresh regardless of local expiry,
    /// swallowing refresh/save failures into `false`.
    async fn do_force_refresh(inner: &Arc<OAuthInner>) -> bool {
        let tokens = match inner.load().await {
            Ok(tokens) => tokens,
            Err(_) => return false,
        };
        let tokens = match tokens {
            Some(tokens) => tokens,
            None => {
                let _ = inner.access.evaluate(
                    ProviderCredentialKind::OAuthAccessToken,
                    CredentialOperation::Refresh,
                    false,
                    true,
                    false,
                );
                return false;
            }
        };
        let refresh_token_available = tokens.refresh_token.is_some();
        if inner
            .access
            .evaluate(
                ProviderCredentialKind::OAuthAccessToken,
                CredentialOperation::Refresh,
                true,
                true,
                refresh_token_available,
            )
            .is_err()
        {
            return false;
        }
        if tokens.refresh_token.is_none() {
            return false;
        }
        inner.refresh_coordinated(tokens, true).await.is_ok()
    }

    /// Register (or join) the single-flight force-refresh and hand back the
    /// shared future. Concurrent callers share one round-trip; the dedup window
    /// closes once it settles.
    fn begin_force_refresh(inner: &Arc<OAuthInner>) -> RefreshFuture {
        let mut pending = inner.pending.lock().unwrap();
        if let Some((_, shared)) = pending.as_ref() {
            return shared.clone();
        }
        let my_gen = inner.gen_counter.fetch_add(1, Ordering::SeqCst);
        let inner2 = Arc::clone(inner);
        let fut: BoxFuture<'static, bool> = Box::pin(async move {
            let result = OAuthInner::do_force_refresh(&inner2).await;
            {
                let mut pending = inner2.pending.lock().unwrap();
                if let Some((g, _)) = pending.as_ref() {
                    if *g == my_gen {
                        *pending = None;
                    }
                }
            }
            result
        });
        let shared = fut.shared();
        *pending = Some((my_gen, shared.clone()));
        shared
    }
}

/// A credential source backed by OAuth tokens. Loads from the store, attaches
/// the access token as a Bearer header, and transparently refreshes when
/// expired.
#[derive(Clone)]
pub struct OAuthAuth {
    inner: Arc<OAuthInner>,
}

impl OAuthAuth {
    /// Capture credentials and their lineage before publishing a provider.
    /// Legacy records are upgraded with an atomic compare-exchange.
    pub async fn capture_stored(
        store: &Arc<dyn CredentialStore>,
        config: &OAuthConfig,
    ) -> Result<Option<OAuthCredentialSnapshot>, ProviderError> {
        capture_stored_credential(store, &format!("oauth:{}", config.client_id)).await
    }

    pub fn new(opts: OAuthAuthOptions) -> Self {
        let now = opts.now.unwrap_or_else(default_now);
        let skew = opts.refresh_skew_ms.unwrap_or(60_000);
        let fetch = opts.fetch.unwrap_or_else(default_fetch);
        let extra_headers = opts.extra_headers.unwrap_or_default();
        let base_url = opts.base_url.filter(|s| !s.is_empty());
        let access = opts
            .access
            .unwrap_or_else(|| ProviderAuthAccess::allow_all("oauth"));
        let refresh_lock_dir = opts.refresh_lock_dir.unwrap_or_else(config_home);
        let refresh_lock_key = opts
            .refresh_lock_key
            .unwrap_or_else(|| opts.config.client_id.clone());
        let credential = opts
            .prepared_credential
            .map(CredentialState::from)
            .or_else(|| {
                opts.tokens.map(|tokens| CredentialState {
                    tokens,
                    generation: uuid::Uuid::new_v4().to_string(),
                    raw: None,
                })
            });
        let owner_generation = credential
            .as_ref()
            .map(|state| credential_lineage_generation(&state.generation))
            .unwrap_or_default();
        Self {
            inner: Arc::new(OAuthInner {
                config: opts.config,
                store: opts.store,
                base_url,
                extra_headers,
                skew,
                fetch,
                now,
                refresh_lock_dir,
                refresh_lock_key,
                access,
                credential: Mutex::new(credential),
                pending: Mutex::new(None),
                gen_counter: AtomicU64::new(0),
                credential_changes: tokio::sync::watch::channel(
                    crate::provider::auth::CredentialChangeState {
                        revision: 0,
                        owner_generation,
                    },
                )
                .0,
            }),
        }
    }

    /// Persist the current tokens (if a store is configured).
    pub async fn save(&self) -> Result<(), ProviderError> {
        self.inner.save().await
    }

    /// Preparation observed an absent credential, not authorization to replace a login.
    pub async fn save_if_absent(&self) -> Result<(), ProviderError> {
        self.inner.save_if_absent().await
    }

    /// Set tokens after an interactive login and persist them.
    pub async fn set_tokens(&self, tokens: OAuthTokens) -> Result<(), ProviderError> {
        self.inner.replace_tokens(tokens).await
    }

    /// Force a token refresh regardless of the local expiry check (reactive
    /// 401/403 recovery). Single-flight: concurrent callers share one refresh,
    /// so a burst of 401s triggers just one round-trip. `true` when a fresh
    /// access token was obtained, `false` when there's no refresh token to use.
    pub fn force_refresh(&self) -> BoxFuture<'static, bool> {
        OAuthInner::begin_force_refresh(&self.inner).boxed()
    }
}

fn managed_review_token_claims(token: &RuntimeString) -> serde_json::Value {
    use base64::Engine;
    let Ok(token) = token.try_to_utf8() else {
        return serde_json::Value::Null;
    };
    let Some(payload) = token
        .split('.')
        .nth(1)
        .filter(|payload| payload.len() <= 1024 * 1024)
    else {
        return serde_json::Value::Null;
    };
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(payload))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or(serde_json::Value::Null)
}

fn managed_review_recovery_failure(error: ProviderError) -> ManagedReviewCredentialRecovery {
    if matches!(
        error.kind,
        ProviderErrorKind::Network
            | ProviderErrorKind::Timeout
            | ProviderErrorKind::Overloaded
            | ProviderErrorKind::RateLimit
    ) {
        ManagedReviewCredentialRecovery::TransientFailure(error.to_string())
    } else {
        ManagedReviewCredentialRecovery::PermanentFailure(error.to_string())
    }
}

#[async_trait::async_trait]
impl AuthSource for OAuthAuth {
    fn is_subscription_oauth(&self) -> bool {
        true
    }
    fn credential_account_id(&self) -> Option<String> {
        self.inner
            .managed_review_snapshot()
            .ok()
            .flatten()
            .and_then(|snapshot| snapshot.identity.account_id)
    }
    fn credential_changes(
        &self,
    ) -> Option<tokio::sync::watch::Receiver<crate::provider::auth::CredentialChangeState>> {
        Some(self.inner.credential_changes.subscribe())
    }
    async fn managed_review_credentials(
        &self,
        _signal: &AbortSignal,
    ) -> Result<Option<ManagedReviewCredentials>, ProviderError> {
        self.inner.managed_review_credentials().await.map(Some)
    }

    async fn recover_managed_review_unauthorized(
        &self,
        recovery_step: usize,
        expected: &ManagedReviewCredentialIdentity,
    ) -> ManagedReviewCredentialRecovery {
        if let Err(error) = self.inner.require_managed_review_owner(expected) {
            return managed_review_recovery_failure(error);
        }
        let result = match recovery_step {
            0 => self
                .inner
                .load_from_store()
                .await
                .map(|tokens| tokens.is_some()),
            1 => {
                let tokens = match self.inner.load().await {
                    Ok(Some(tokens)) => tokens,
                    Ok(None) => return ManagedReviewCredentialRecovery::Unavailable,
                    Err(error) => return managed_review_recovery_failure(error),
                };
                if tokens.refresh_token.is_none() {
                    return ManagedReviewCredentialRecovery::Unavailable;
                }
                if let Err(error) = self.inner.access.evaluate(
                    ProviderCredentialKind::OAuthAccessToken,
                    CredentialOperation::Refresh,
                    true,
                    true,
                    true,
                ) {
                    return managed_review_recovery_failure(error);
                }
                if let Err(error) = self.inner.require_managed_review_owner(expected) {
                    return managed_review_recovery_failure(error);
                }
                self.inner
                    .refresh_coordinated(tokens, true)
                    .await
                    .map(|_| true)
            }
            _ => return ManagedReviewCredentialRecovery::Unavailable,
        };
        match result {
            Ok(true) => match self.inner.require_managed_review_owner(expected) {
                Ok(()) => ManagedReviewCredentialRecovery::Refreshed,
                Err(error) => managed_review_recovery_failure(error),
            },
            Ok(false) => ManagedReviewCredentialRecovery::Unavailable,
            Err(error) => managed_review_recovery_failure(error),
        }
    }

    async fn get_headers(&self, _signal: &AbortSignal) -> Result<AuthHeaders, ProviderError> {
        self.inner.get_headers().await
    }

    fn base_url(&self) -> Option<&str> {
        self.inner.base_url.as_deref()
    }

    async fn force_refresh(&self) -> Option<bool> {
        Some(OAuthAuth::force_refresh(self).await)
    }
}

// --- interactive loopback login --------------------------------------------

/// Options for [`login_with_loopback`].
pub struct LoginOpts {
    /// Open the authorize URL in a browser (default true). When false, the URL
    /// is printed instead.
    pub open: bool,
    pub fetch: Option<Arc<dyn HttpFetch>>,
    pub now: Option<Now>,
    pub timeout_ms: Option<u64>,
}

impl Default for LoginOpts {
    fn default() -> Self {
        Self {
            open: true,
            fetch: None,
            now: None,
            timeout_ms: None,
        }
    }
}

enum Conn {
    NotMatched,
    Matched {
        code: Option<String>,
        state: Option<String>,
    },
}

/// Interactive login: start a loopback HTTP server on the redirect URI, open the
/// authorize URL in the browser, wait for the callback, and exchange the code.
pub async fn login_with_loopback(
    process_context: &RuntimeProcessContext,
    config: &OAuthConfig,
    opts: LoginOpts,
) -> Result<OAuthTokens, ProviderError> {
    let pkce = generate_pkce();
    let state = base64url(&random_bytes(16));
    let (redirect_path, port) = parse_redirect(&config.redirect_uri)?;
    let authorize_url = build_authorize_url(config, &pkce.challenge, &state);

    let listener = std::net::TcpListener::bind(("127.0.0.1", port))
        .map_err(|e| network_error(e.to_string()))?;
    listener
        .set_nonblocking(true)
        .map_err(|e| network_error(e.to_string()))?;

    if opts.open {
        open_in_browser(process_context, &authorize_url).await;
    } else {
        print!("Open this URL to authorize:\n{}\n", authorize_url);
    }

    let timeout = Duration::from_millis(opts.timeout_ms.unwrap_or(300_000));
    let deadline = tokio::time::Instant::now() + timeout;

    let (code, returned_state) = loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(auth_error("OAuth login timed out"));
        }
        match listener.accept() {
            Ok((stream, _addr)) => {
                let path = redirect_path.clone();
                let handled = tokio::task::spawn_blocking(move || handle_connection(stream, &path))
                    .await
                    .map_err(|e| auth_error(e.to_string()))?;
                match handled {
                    Conn::NotMatched => continue,
                    Conn::Matched { code, state } => break (code, state),
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                tokio::time::sleep(Duration::from_millis(20)).await;
                continue;
            }
            Err(e) => return Err(network_error(e.to_string())),
        }
    };

    let code_ok = code.as_deref().map(|c| !c.is_empty()).unwrap_or(false);
    if !code_ok || returned_state.as_deref() != Some(state.as_str()) {
        return Err(auth_error("OAuth callback missing code or state mismatch"));
    }
    let mut request_opts = RequestOpts::default();
    if let Some(fetch) = opts.fetch {
        request_opts.fetch = fetch;
    }
    if let Some(now) = opts.now {
        request_opts.now = now;
    }
    exchange_code(
        config,
        &code.unwrap_or_default(),
        &pkce.verifier,
        request_opts,
    )
    .await
}

/// Extract the path and port from a redirect URI (no query/fragment on the
/// path; an empty port maps to 0).
fn parse_redirect(redirect_uri: &str) -> Result<(String, u16), ProviderError> {
    let after_scheme = match redirect_uri.find("://") {
        Some(i) => &redirect_uri[i + 3..],
        None => redirect_uri,
    };
    let (authority, path) = match after_scheme.find('/') {
        Some(i) => (&after_scheme[..i], &after_scheme[i..]),
        None => (after_scheme, "/"),
    };
    let path_end = path.find(['?', '#']).unwrap_or(path.len());
    let path = &path[..path_end];
    let port: u16 = match authority.rfind(':') {
        Some(i) => {
            let raw = &authority[i + 1..];
            if raw.is_empty() {
                0
            } else {
                raw.parse()
                    .map_err(|_| auth_error("invalid redirect port"))?
            }
        }
        None => 0,
    };
    Ok((path.to_string(), port))
}

/// Handle a single loopback connection: 404 non-matching paths, and on the
/// redirect path respond with the close-window page and capture `code`/`state`.
fn handle_connection(mut stream: std::net::TcpStream, redirect_path: &str) -> Conn {
    use std::io::{Read, Write};
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
    loop {
        if find_crlf(&buf).is_some() || buf.len() > 8192 {
            break;
        }
        match stream.read(&mut tmp) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(_) => break,
        }
    }
    let line_end = find_crlf(&buf).unwrap_or(buf.len());
    let request_line = String::from_utf8_lossy(&buf[..line_end]).into_owned();
    let target = request_line.split_whitespace().nth(1).unwrap_or("/");
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p, q),
        None => (target, ""),
    };
    if path != redirect_path {
        let _ = stream
            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        let _ = stream.flush();
        return Conn::NotMatched;
    }
    let code = query_get(query, "code");
    let state = query_get(query, "state");
    let body = "<html><body>You may close this window and return to the terminal.</body></html>";
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
    Conn::Matched { code, state }
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

async fn open_in_browser(process_context: &RuntimeProcessContext, url: &str) {
    let cmd = if cfg!(target_os = "macos") {
        "open"
    } else if cfg!(target_os = "windows") {
        "start"
    } else {
        "xdg-open"
    };
    // Best effort; if it fails, the user can copy the URL from the log.
    if let Ok(mut child) = process_context
        .spawn_oauth_opener(cmd.into(), vec![url.into()])
        .await
    {
        tokio::spawn(async move {
            let _ = child.wait().await;
        });
    }
    print!("Opening browser to authorize:\n{}\n", url);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credential_store::{CredentialStore, NamespacedCredentialStore};
    use crate::mcp::credential_store::InMemoryCredentialStore;
    use crate::provider::access_policy::ProviderAccessPolicy;
    use crate::provider::auth::{AuthSource, ProviderAuthAccess};
    use crate::types::errors::ProviderErrorKind;
    use std::sync::atomic::{AtomicUsize, Ordering as O};

    fn managed_claim_token() -> RuntimeString {
        use base64::Engine;
        let claims=serde_json::json!({"https://api.openai.com/auth":{"chatgpt_user_id":"user","chatgpt_account_id":"account","chatgpt_plan_type":"business"}}).to_string();
        format!(
            "header.{}.signature",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(claims.as_bytes())
        )
        .into()
    }

    #[tokio::test]
    async fn managed_credentials_keep_lineage_across_refresh_and_change_it_on_login() {
        let auth = OAuthAuth::new(OAuthAuthOptions {
            tokens: Some(OAuthTokens {
                access_token: "initial".into(),
                id_token: Some(managed_claim_token()),
                ..Default::default()
            }),
            ..Default::default()
        });
        let initial = auth
            .managed_review_credentials(&AbortSignal::new())
            .await
            .unwrap()
            .unwrap();
        let watch = auth.credential_changes().unwrap();
        assert_eq!(
            watch.borrow().owner_generation,
            initial.identity.owner_generation
        );
        let mut refreshed = auth.inner.credential.lock().unwrap().clone().unwrap();
        refreshed.tokens.access_token = "refreshed".into();
        auth.inner.install_refreshed(refreshed).unwrap();
        let next = auth
            .managed_review_credentials(&AbortSignal::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(next.identity, initial.identity);
        assert_eq!(
            watch.borrow().owner_generation,
            initial.identity.owner_generation
        );
        assert_eq!(
            next.headers.get("Authorization").map(String::as_str),
            Some("Bearer refreshed")
        );
        assert_eq!(watch.borrow().revision, 1);
        auth.set_tokens(OAuthTokens {
            access_token: "replacement".into(),
            id_token: Some(managed_claim_token()),
            ..Default::default()
        })
        .await
        .unwrap();
        let replacement = auth
            .managed_review_credentials(&AbortSignal::new())
            .await
            .unwrap()
            .unwrap();
        assert_ne!(
            replacement.identity.owner_generation,
            initial.identity.owner_generation
        );
        assert_eq!(
            watch.borrow().owner_generation,
            replacement.identity.owner_generation
        );
        assert!(matches!(
            auth.recover_managed_review_unauthorized(0, &initial.identity)
                .await,
            ManagedReviewCredentialRecovery::PermanentFailure(_)
        ));
    }

    #[tokio::test]
    async fn managed_recovery_preserves_transient_failures_through_the_refresh_lock() {
        for (response, expect_transient) in
            [(Resp::Throw, true), (Resp::NotOk { status: 401 }, false)]
        {
            let calls = Arc::new(AtomicUsize::new(0));
            let fetch: Arc<dyn HttpFetch> = Arc::new(FakeFetch {
                calls: calls.clone(),
                resp: response,
                yielding: false,
            });
            let scratch = OAuthScratch::new();
            let store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
            let auth = OAuthAuth::new(OAuthAuthOptions {
                config: cfg(),
                store: Some(store),
                tokens: Some(OAuthTokens {
                    access_token: "access".into(),
                    id_token: Some(managed_claim_token()),
                    refresh_token: Some("refresh".into()),
                    expires_at: Some(4_000_000_000_000),
                }),
                fetch: Some(fetch),
                refresh_lock_dir: Some(scratch.path()),
                ..Default::default()
            });
            auth.save().await.unwrap();
            let identity = auth
                .managed_review_credentials(&AbortSignal::new())
                .await
                .unwrap()
                .unwrap()
                .identity;
            assert!(matches!(
                auth.recover_managed_review_unauthorized(0, &identity).await,
                ManagedReviewCredentialRecovery::Refreshed
            ));
            assert_eq!(calls.load(O::SeqCst), 0);
            let recovery = auth.recover_managed_review_unauthorized(1, &identity).await;
            assert_eq!(calls.load(O::SeqCst), 1);
            if expect_transient {
                assert!(matches!(
                    recovery,
                    ManagedReviewCredentialRecovery::TransientFailure(_)
                ));
            } else {
                assert!(matches!(
                    recovery,
                    ManagedReviewCredentialRecovery::PermanentFailure(_)
                ));
            }
            assert!(matches!(
                auth.recover_managed_review_unauthorized(2, &identity).await,
                ManagedReviewCredentialRecovery::Unavailable
            ));
        }
    }

    fn cfg() -> OAuthConfig {
        OAuthConfig {
            client_id: "c".to_string(),
            authorize_url: "https://as/auth".to_string(),
            token_url: "https://as/token".to_string(),
            redirect_uri: "http://localhost/cb".to_string(),
            ..Default::default()
        }
    }

    struct OAuthScratch(std::path::PathBuf);

    impl OAuthScratch {
        fn new() -> Self {
            Self(
                std::env::temp_dir()
                    .join(format!("klaus-oauth-generation-{}", uuid::Uuid::new_v4())),
            )
        }

        fn path(&self) -> String {
            self.0.to_string_lossy().into_owned()
        }
    }

    impl Drop for OAuthScratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn generation_tokens(access: &str, refresh: &str, expires_at: i64) -> OAuthTokens {
        OAuthTokens {
            access_token: access.into(),
            refresh_token: Some(refresh.into()),
            expires_at: Some(expires_at),
            id_token: Some("private-id-token".into()),
        }
    }

    async fn login_generation(store: Arc<dyn CredentialStore>, tokens: OAuthTokens) -> OAuthAuth {
        let auth = OAuthAuth::new(OAuthAuthOptions {
            config: cfg(),
            store: Some(store),
            tokens: Some(tokens),
            ..Default::default()
        });
        auth.save().await.unwrap();
        auth
    }

    async fn prepare_generation(
        store: Arc<dyn CredentialStore>,
        fetch: Arc<dyn HttpFetch>,
        scratch: &OAuthScratch,
        lock_key: &str,
    ) -> OAuthAuth {
        let captured = OAuthAuth::capture_stored(&store, &cfg())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(format!("{captured:?}"), "OAuthCredentialSnapshot { .. }");
        OAuthAuth::new(OAuthAuthOptions {
            config: cfg(),
            store: Some(store),
            tokens: Some(captured.tokens()),
            prepared_credential: Some(captured),
            fetch: Some(fetch),
            now: Some(Arc::new(|| 1_000_000)),
            refresh_lock_dir: Some(scratch.path()),
            refresh_lock_key: Some(lock_key.to_string()),
            ..Default::default()
        })
    }

    fn rotation_fetch(calls: Arc<AtomicUsize>, access: &str, refresh: &str) -> Arc<dyn HttpFetch> {
        Arc::new(FakeFetch {
            calls,
            resp: Resp::Ok {
                access: access.to_string(),
                refresh: refresh.to_string(),
                expires_in: 3600,
            },
            yielding: false,
        })
    }

    #[tokio::test]
    async fn prepared_first_request_keeps_captured_login_after_store_replacement() {
        let store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
        login_generation(
            store.clone(),
            generation_tokens("account-a", "refresh-a", 9_000_000),
        )
        .await;
        let scratch = OAuthScratch::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let old = prepare_generation(
            store.clone(),
            rotation_fetch(calls.clone(), "unused", "unused"),
            &scratch,
            "old",
        )
        .await;
        login_generation(
            store.clone(),
            generation_tokens("account-b", "refresh-b", 9_000_000),
        )
        .await;
        let headers = old.get_headers(&AbortSignal::new()).await.unwrap();
        assert_eq!(
            headers.get("Authorization").map(String::as_str),
            Some("Bearer account-a")
        );
        assert_eq!(calls.load(O::SeqCst), 0);
        let stored = OAuthAuth::capture_stored(&store, &cfg())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.tokens.access_token, "account-b");
        assert!(old.save().await.is_err());
        assert_eq!(
            store.get("oauth:c").await.unwrap().as_deref(),
            Some(stored.raw.as_str())
        );
    }

    #[tokio::test]
    async fn prepared_expired_request_rejects_replacement_identity_before_refresh() {
        let store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
        login_generation(
            store.clone(),
            generation_tokens("account-a", "refresh-a", 0),
        )
        .await;
        let scratch = OAuthScratch::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let old = prepare_generation(
            store.clone(),
            rotation_fetch(calls.clone(), "unused", "unused"),
            &scratch,
            "old",
        )
        .await;
        login_generation(
            store.clone(),
            generation_tokens("account-b", "refresh-b", 9_000_000),
        )
        .await;
        let error = old.get_headers(&AbortSignal::new()).await.unwrap_err();
        assert_eq!(error.kind, ProviderErrorKind::Auth);
        for secret in [
            "account-a",
            "account-b",
            "refresh-a",
            "refresh-b",
            "private-id-token",
        ] {
            assert!(!error.message.contains(secret));
        }
        assert_eq!(calls.load(O::SeqCst), 0);
    }

    struct DelayedRotation {
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl HttpFetch for DelayedRotation {
        async fn fetch(
            &self,
            _url: &str,
            _request: HttpRequest,
        ) -> Result<HttpResponse, ProviderError> {
            self.entered.notify_one();
            self.release.notified().await;
            Ok(HttpResponse {
                ok: true,
                status: 200,
                body: r#"{"access_token":"late-access-a","refresh_token":"late-refresh-a","expires_in":3600}"#.to_string(),
            })
        }
    }

    #[tokio::test]
    async fn delayed_refresh_cannot_overwrite_a_new_login_generation() {
        let store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
        let login = login_generation(
            store.clone(),
            generation_tokens("account-a", "refresh-a", 0),
        )
        .await;
        let scratch = OAuthScratch::new();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let old = prepare_generation(
            store.clone(),
            Arc::new(DelayedRotation {
                entered: entered.clone(),
                release: release.clone(),
            }),
            &scratch,
            "old",
        )
        .await;
        let pending = tokio::spawn(old.force_refresh());
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        login
            .set_tokens(generation_tokens("account-b", "refresh-b", 9_000_000))
            .await
            .unwrap();
        let replacement = store.get("oauth:c").await.unwrap().unwrap();
        release.notify_one();
        assert!(!tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .unwrap()
            .unwrap());
        assert_eq!(
            store.get("oauth:c").await.unwrap().as_deref(),
            Some(replacement.as_str())
        );
        assert!(old.get_headers(&AbortSignal::new()).await.is_err());
    }

    #[tokio::test]
    async fn same_generation_peer_rotation_is_reused_without_reauthentication() {
        let store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
        login_generation(
            store.clone(),
            generation_tokens("account-a", "refresh-a", 0),
        )
        .await;
        let scratch = OAuthScratch::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch = rotation_fetch(calls.clone(), "fresh-account-a", "rotated-refresh-a");
        let first = prepare_generation(store.clone(), fetch.clone(), &scratch, "same-login").await;
        let second = prepare_generation(store.clone(), fetch, &scratch, "same-login").await;
        let before = OAuthAuth::capture_stored(&store, &cfg())
            .await
            .unwrap()
            .unwrap();
        assert!(first.force_refresh().await);
        assert!(second.force_refresh().await);
        let after = OAuthAuth::capture_stored(&store, &cfg())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(before.generation, after.generation);
        assert_eq!(
            after.tokens.id_token.as_ref(),
            Some(&RuntimeString::from("private-id-token"))
        );
        assert_eq!(calls.load(O::SeqCst), 1);
        let headers = second.get_headers(&AbortSignal::new()).await.unwrap();
        assert_eq!(
            headers.get("Authorization").map(String::as_str),
            Some("Bearer fresh-account-a")
        );
    }

    #[tokio::test]
    async fn losing_refresh_cas_accepts_only_same_generation_peer_rotation() {
        let store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
        login_generation(
            store.clone(),
            generation_tokens("account-a", "refresh-a", 0),
        )
        .await;
        let scratch = OAuthScratch::new();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let delayed = prepare_generation(
            store.clone(),
            Arc::new(DelayedRotation {
                entered: entered.clone(),
                release: release.clone(),
            }),
            &scratch,
            "delayed-lock",
        )
        .await;
        let peer = prepare_generation(
            store.clone(),
            rotation_fetch(
                Arc::new(AtomicUsize::new(0)),
                "peer-access-a",
                "peer-refresh-a",
            ),
            &scratch,
            "peer-lock",
        )
        .await;
        let pending = tokio::spawn(delayed.force_refresh());
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        assert!(peer.force_refresh().await);
        let rotated = store.get("oauth:c").await.unwrap().unwrap();
        release.notify_one();
        assert!(tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .unwrap()
            .unwrap());
        assert_eq!(
            store.get("oauth:c").await.unwrap().as_deref(),
            Some(rotated.as_str())
        );
        assert_eq!(
            delayed
                .get_headers(&AbortSignal::new())
                .await
                .unwrap()
                .get("Authorization")
                .map(String::as_str),
            Some("Bearer peer-access-a")
        );
    }

    #[tokio::test]
    async fn legacy_credential_migration_is_shared_but_unproven_rotation_is_rejected() {
        let store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
        let legacy =
            encode_persisted_tokens(&generation_tokens("legacy-a", "legacy-refresh", 0)).unwrap();
        store.set("oauth:c", &legacy).await.unwrap();
        let config = cfg();
        let (first, second) = tokio::join!(
            OAuthAuth::capture_stored(&store, &config),
            OAuthAuth::capture_stored(&store, &config)
        );
        let first = first.unwrap().unwrap();
        let second = second.unwrap().unwrap();
        assert_eq!(first.generation, second.generation);
        assert_eq!(first.tokens.access_token, "legacy-a");
        let scratch = OAuthScratch::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let old = prepare_generation(
            store.clone(),
            rotation_fetch(calls.clone(), "unused", "unused"),
            &scratch,
            "legacy",
        )
        .await;
        store
            .set(
                "oauth:c",
                &encode_persisted_tokens(&generation_tokens("peer-a", "peer-refresh", 9_000_000))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(old.get_headers(&AbortSignal::new()).await.is_err());
        assert_eq!(calls.load(O::SeqCst), 0);
    }

    struct NonAtomicStore(InMemoryCredentialStore);

    #[async_trait::async_trait]
    impl CredentialStore for NonAtomicStore {
        async fn get(
            &self,
            key: &str,
        ) -> Result<Option<String>, crate::credential_store::CredentialStoreError> {
            self.0.get(key).await
        }
        async fn set(
            &self,
            key: &str,
            value: &str,
        ) -> Result<(), crate::credential_store::CredentialStoreError> {
            self.0.set(key, value).await
        }
        async fn delete(
            &self,
            key: &str,
        ) -> Result<(), crate::credential_store::CredentialStoreError> {
            self.0.delete(key).await
        }
    }

    #[tokio::test]
    async fn non_atomic_backend_fails_closed_for_legacy_migration_and_refresh() {
        let store: Arc<dyn CredentialStore> =
            Arc::new(NonAtomicStore(InMemoryCredentialStore::new()));
        let legacy =
            encode_persisted_tokens(&generation_tokens("legacy-a", "legacy-refresh", 0)).unwrap();
        store.set("oauth:c", &legacy).await.unwrap();
        assert!(OAuthAuth::capture_stored(&store, &cfg()).await.is_err());
        assert_eq!(
            store.get("oauth:c").await.unwrap().as_deref(),
            Some(legacy.as_str())
        );
        login_generation(
            store.clone(),
            generation_tokens("account-a", "refresh-a", 0),
        )
        .await;
        let before = store.get("oauth:c").await.unwrap();
        let scratch = OAuthScratch::new();
        let old = prepare_generation(
            store.clone(),
            rotation_fetch(Arc::new(AtomicUsize::new(0)), "fresh-a", "fresh-refresh-a"),
            &scratch,
            "unsupported",
        )
        .await;
        let error = old.get_headers(&AbortSignal::new()).await.unwrap_err();
        assert_eq!(error.kind, ProviderErrorKind::Auth);
        assert_eq!(store.get("oauth:c").await.unwrap(), before);
    }

    #[tokio::test]
    async fn oauth_generation_updates_do_not_touch_other_credential_namespaces() {
        let backing: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
        let first: Arc<dyn CredentialStore> = Arc::new(NamespacedCredentialStore::new(
            backing.clone(),
            "provider/openai/a",
        ));
        let second: Arc<dyn CredentialStore> =
            Arc::new(NamespacedCredentialStore::new(backing, "provider/openai/b"));
        login_generation(
            first.clone(),
            generation_tokens("account-a", "refresh-a", 0),
        )
        .await;
        login_generation(
            second.clone(),
            generation_tokens("account-b", "refresh-b", 0),
        )
        .await;
        let before = second.get("oauth:c").await.unwrap();
        let scratch = OAuthScratch::new();
        let auth = prepare_generation(
            first,
            rotation_fetch(Arc::new(AtomicUsize::new(0)), "fresh-a", "rotated-a"),
            &scratch,
            "a",
        )
        .await;
        assert!(auth.force_refresh().await);
        assert_eq!(second.get("oauth:c").await.unwrap(), before);
    }

    struct SecretErrorFetch;

    #[async_trait::async_trait]
    impl HttpFetch for SecretErrorFetch {
        async fn fetch(
            &self,
            _url: &str,
            _request: HttpRequest,
        ) -> Result<HttpResponse, ProviderError> {
            Err(auth_error(
                "private-access-token private-refresh-token private-id-token",
            ))
        }
    }

    struct SecretErrorStore;

    #[async_trait::async_trait]
    impl CredentialStore for SecretErrorStore {
        async fn get(
            &self,
            _key: &str,
        ) -> Result<Option<String>, crate::credential_store::CredentialStoreError> {
            Err("private-access-token private-refresh-token private-id-token".into())
        }
        async fn set(
            &self,
            _key: &str,
            _value: &str,
        ) -> Result<(), crate::credential_store::CredentialStoreError> {
            Err("private-access-token private-refresh-token private-id-token".into())
        }
        async fn delete(
            &self,
            _key: &str,
        ) -> Result<(), crate::credential_store::CredentialStoreError> {
            Err("private-access-token private-refresh-token private-id-token".into())
        }
    }

    #[tokio::test]
    async fn oauth_prepare_and_refresh_errors_do_not_expose_credential_values() {
        let auth = OAuthAuth::new(OAuthAuthOptions {
            config: cfg(),
            tokens: Some(generation_tokens(
                "private-access-token",
                "private-refresh-token",
                0,
            )),
            fetch: Some(Arc::new(SecretErrorFetch)),
            now: Some(Arc::new(|| 1_000_000)),
            ..Default::default()
        });
        let refresh_error = auth.get_headers(&AbortSignal::new()).await.unwrap_err();
        let store: Arc<dyn CredentialStore> = Arc::new(SecretErrorStore);
        let prepare_error = OAuthAuth::capture_stored(&store, &cfg()).await.unwrap_err();
        for error in [refresh_error, prepare_error] {
            assert_eq!(error.kind, ProviderErrorKind::Auth);
            for secret in [
                "private-access-token",
                "private-refresh-token",
                "private-id-token",
            ] {
                assert!(!error.message.contains(secret));
            }
        }
    }

    #[tokio::test]
    async fn oauth_auth_cannot_be_overridden_by_extra_headers() {
        let refresh_calls = Arc::new(AtomicUsize::new(0));
        let auth = OAuthAuth::new(OAuthAuthOptions {
            config: cfg(),
            tokens: Some(OAuthTokens {
                access_token: "trusted-token".into(),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(0),
                id_token: None,
            }),
            extra_headers: Some(HashMap::from([(
                "AUTHORIZATION".to_string(),
                "Bearer private-attacker-value".to_string(),
            )])),
            fetch: Some(Arc::new(FakeFetch {
                calls: refresh_calls.clone(),
                resp: Resp::Throw,
                yielding: false,
            })),
            ..Default::default()
        });

        let error = auth
            .get_headers(&AbortSignal::new())
            .await
            .expect_err("protected headers must fail closed");
        let message = String::from_utf16_lossy(error.message.as_utf16());
        assert!(message.contains("authorization"));
        assert!(!message.contains("private-attacker-value"));
        assert_eq!(refresh_calls.load(O::SeqCst), 0);
    }

    enum Resp {
        Ok {
            access: String,
            refresh: String,
            expires_in: i64,
        },
        NotOk {
            status: u16,
        },
        Throw,
    }

    struct FakeFetch {
        calls: Arc<AtomicUsize>,
        resp: Resp,
        yielding: bool,
    }

    #[async_trait::async_trait]
    impl HttpFetch for FakeFetch {
        async fn fetch(
            &self,
            _url: &str,
            _req: HttpRequest,
        ) -> Result<HttpResponse, ProviderError> {
            self.calls.fetch_add(1, O::SeqCst);
            if self.yielding {
                tokio::task::yield_now().await;
            }
            match &self.resp {
                Resp::Throw => Err(network_error("should not fetch")),
                Resp::NotOk { status } => Ok(HttpResponse {
                    ok: false,
                    status: *status,
                    body: String::new(),
                }),
                Resp::Ok {
                    access,
                    refresh,
                    expires_in,
                } => {
                    let body = serde_json::json!({
                        "access_token": access,
                        "refresh_token": refresh,
                        "expires_in": expires_in,
                    })
                    .to_string();
                    Ok(HttpResponse {
                        ok: true,
                        status: 200,
                        body,
                    })
                }
            }
        }
    }

    #[tokio::test]
    async fn force_refresh_bypasses_expiry_and_updates_bearer() {
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch: Arc<dyn HttpFetch> = Arc::new(FakeFetch {
            calls: calls.clone(),
            resp: Resp::Ok {
                access: "fresh".to_string(),
                refresh: "r-new".to_string(),
                expires_in: 3600,
            },
            yielding: false,
        });
        let now: Now = Arc::new(|| 1_000_000_000_000_i64);
        let auth = OAuthAuth::new(OAuthAuthOptions {
            config: cfg(),
            // A token that is NOT locally expired — proactive get_headers would
            // not refresh it.
            tokens: Some(OAuthTokens {
                access_token: "old".into(),
                refresh_token: Some("r0".into()),
                expires_at: Some(4_000_000_000_000),
                id_token: None,
            }),
            now: Some(now),
            fetch: Some(fetch),
            ..Default::default()
        });

        let signal = AbortSignal::new();
        let before = auth.get_headers(&signal).await.unwrap();
        assert_eq!(
            before.get("Authorization").map(String::as_str),
            Some("Bearer old")
        );
        assert_eq!(calls.load(O::SeqCst), 0);

        assert!(auth.force_refresh().await);
        assert_eq!(calls.load(O::SeqCst), 1);
        let after = auth.get_headers(&signal).await.unwrap();
        assert_eq!(
            after.get("Authorization").map(String::as_str),
            Some("Bearer fresh")
        );
    }

    #[tokio::test]
    async fn refreshed_tokens_survive_namespaced_auth_restart() {
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch: Arc<dyn HttpFetch> = Arc::new(FakeFetch {
            calls: calls.clone(),
            resp: Resp::Ok {
                access: "fresh-access".to_string(),
                refresh: "rotated-refresh".to_string(),
                expires_in: 3600,
            },
            yielding: false,
        });
        let backing: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
        let store: Arc<dyn CredentialStore> = Arc::new(NamespacedCredentialStore::new(
            backing,
            "provider/openai/desktop-account-a",
        ));
        let now: Now = Arc::new(|| 1_000_000_000_000_i64);
        let first = OAuthAuth::new(OAuthAuthOptions {
            config: cfg(),
            store: Some(store.clone()),
            tokens: Some(OAuthTokens {
                access_token: "expired-access".into(),
                refresh_token: Some("old-refresh".into()),
                expires_at: Some(1_000_000_000_000),
                id_token: None,
            }),
            now: Some(now.clone()),
            fetch: Some(fetch.clone()),
            ..Default::default()
        });

        let first_headers = first.get_headers(&AbortSignal::new()).await.unwrap();
        assert_eq!(
            first_headers.get("Authorization").map(String::as_str),
            Some("Bearer fresh-access")
        );
        drop(first);

        let resumed = OAuthAuth::new(OAuthAuthOptions {
            config: cfg(),
            store: Some(store),
            tokens: None,
            now: Some(now),
            fetch: Some(fetch),
            ..Default::default()
        });
        let resumed_headers = resumed.get_headers(&AbortSignal::new()).await.unwrap();
        assert_eq!(
            resumed_headers.get("Authorization").map(String::as_str),
            Some("Bearer fresh-access")
        );
        assert_eq!(calls.load(O::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_auth_instances_refresh_rotating_token_once() {
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch: Arc<dyn HttpFetch> = Arc::new(FakeFetch {
            calls: calls.clone(),
            resp: Resp::Ok {
                access: "fresh-access".to_string(),
                refresh: "rotated-refresh".to_string(),
                expires_in: 3600,
            },
            yielding: true,
        });
        let backing: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
        let store: Arc<dyn CredentialStore> = Arc::new(NamespacedCredentialStore::new(
            backing,
            "provider/openai/concurrent-account",
        ));
        let now: Now = Arc::new(|| 1_000_000_000_000_i64);
        let lock_dir = std::env::temp_dir().join(format!(
            "klaus-provider-oauth-lock-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let lock_dir = lock_dir.to_string_lossy().into_owned();
        let seed = OAuthAuth::new(OAuthAuthOptions {
            config: cfg(),
            store: Some(store.clone()),
            now: Some(now.clone()),
            fetch: Some(fetch.clone()),
            refresh_lock_dir: Some(lock_dir.clone()),
            refresh_lock_key: Some("provider-openai-concurrent-account".into()),
            ..Default::default()
        });
        seed.set_tokens(OAuthTokens {
            access_token: "expired-access".into(),
            refresh_token: Some("old-refresh".into()),
            expires_at: Some(1_000_000_000_000),
            id_token: None,
        })
        .await
        .unwrap();

        let make_auth = || {
            OAuthAuth::new(OAuthAuthOptions {
                config: cfg(),
                store: Some(store.clone()),
                tokens: None,
                now: Some(now.clone()),
                fetch: Some(fetch.clone()),
                refresh_lock_dir: Some(lock_dir.clone()),
                refresh_lock_key: Some("provider-openai-concurrent-account".into()),
                ..Default::default()
            })
        };
        let first = make_auth();
        let second = make_auth();
        let first_signal = AbortSignal::new();
        let second_signal = AbortSignal::new();
        let (first_headers, second_headers) = tokio::join!(
            first.get_headers(&first_signal),
            second.get_headers(&second_signal)
        );

        assert_eq!(
            first_headers
                .unwrap()
                .get("Authorization")
                .map(String::as_str),
            Some("Bearer fresh-access")
        );
        assert_eq!(
            second_headers
                .unwrap()
                .get("Authorization")
                .map(String::as_str),
            Some("Bearer fresh-access")
        );
        assert_eq!(calls.load(O::SeqCst), 1);
        let _ = std::fs::remove_dir_all(lock_dir);
    }

    #[tokio::test]
    async fn get_headers_denies_refresh_when_policy_disallows_it() {
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch: Arc<dyn HttpFetch> = Arc::new(FakeFetch {
            calls: calls.clone(),
            resp: Resp::Throw,
            yielding: false,
        });
        let now: Now = Arc::new(|| 1_000_000_000_000_i64);
        let auth = OAuthAuth::new(OAuthAuthOptions {
            config: cfg(),
            tokens: Some(OAuthTokens {
                access_token: "old".into(),
                refresh_token: Some("r0".into()),
                expires_at: Some(1_000_000_000_000),
                id_token: None,
            }),
            now: Some(now),
            fetch: Some(fetch),
            access: Some(ProviderAuthAccess::new(
                "openai",
                ProviderAccessPolicy::allow_all().allow_refresh(false),
            )),
            ..Default::default()
        });

        let err = auth
            .get_headers(&AbortSignal::new())
            .await
            .expect_err("refresh policy should deny expired-token refresh");

        assert_eq!(err.kind, ProviderErrorKind::Auth);
        assert!(err.message.contains("RefreshDenied"), "{:?}", err.message);
        assert_eq!(calls.load(O::SeqCst), 0);
    }

    #[tokio::test]
    async fn force_refresh_is_single_flight() {
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch: Arc<dyn HttpFetch> = Arc::new(FakeFetch {
            calls: calls.clone(),
            resp: Resp::Ok {
                access: "fresh".to_string(),
                refresh: "r-new".to_string(),
                expires_in: 3600,
            },
            yielding: true,
        });
        let auth = OAuthAuth::new(OAuthAuthOptions {
            config: cfg(),
            tokens: Some(OAuthTokens {
                access_token: "old".into(),
                refresh_token: Some("r0".into()),
                ..Default::default()
            }),
            fetch: Some(fetch),
            ..Default::default()
        });

        let results = futures::future::join_all(vec![
            auth.force_refresh(),
            auth.force_refresh(),
            auth.force_refresh(),
        ])
        .await;
        assert_eq!(results, vec![true, true, true]);
        assert_eq!(calls.load(O::SeqCst), 1);

        // The dedup window closes once it settles: a later call refreshes again.
        let _ = auth.force_refresh().await;
        assert_eq!(calls.load(O::SeqCst), 2);
    }

    #[tokio::test]
    async fn force_refresh_false_when_no_refresh_token() {
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch: Arc<dyn HttpFetch> = Arc::new(FakeFetch {
            calls: calls.clone(),
            resp: Resp::Throw,
            yielding: false,
        });
        let auth = OAuthAuth::new(OAuthAuthOptions {
            config: cfg(),
            tokens: Some(OAuthTokens {
                access_token: "old".into(),
                ..Default::default()
            }),
            fetch: Some(fetch),
            ..Default::default()
        });
        assert!(!auth.force_refresh().await);
        assert_eq!(calls.load(O::SeqCst), 0);
    }

    #[tokio::test]
    async fn force_refresh_false_when_request_fails() {
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch: Arc<dyn HttpFetch> = Arc::new(FakeFetch {
            calls: calls.clone(),
            resp: Resp::NotOk { status: 400 },
            yielding: false,
        });
        let auth = OAuthAuth::new(OAuthAuthOptions {
            config: cfg(),
            tokens: Some(OAuthTokens {
                access_token: "old".into(),
                refresh_token: Some("r0".into()),
                ..Default::default()
            }),
            fetch: Some(fetch),
            ..Default::default()
        });
        assert!(!auth.force_refresh().await);
    }

    #[test]
    fn pkce_challenge_matches_sha256_of_verifier() {
        let pkce = generate_pkce();
        let mut hasher = Sha256::new();
        hasher.update(pkce.verifier.as_bytes());
        assert_eq!(pkce.challenge, base64url(&hasher.finalize()));
        assert!(!pkce.verifier.contains('='));
        assert!(!pkce.verifier.contains('+'));
        assert!(!pkce.verifier.contains('/'));
    }

    #[test]
    fn authorize_url_carries_expected_params() {
        let mut config = cfg();
        config.scopes = Some(vec!["a".to_string(), "b".to_string()]);
        let url = build_authorize_url(&config, "chal", "st");
        assert!(url.starts_with("https://as/auth?"));
        assert!(url.contains("response_type=code"));
        assert!(url.contains("client_id=c"));
        assert!(url.contains("redirect_uri=http%3A%2F%2Flocalhost%2Fcb"));
        assert!(url.contains("code_challenge=chal"));
        assert!(url.contains("code_challenge_method=S256"));
        assert!(url.contains("state=st"));
        assert!(url.contains("scope=a+b"));
    }

    #[test]
    fn parse_token_response_maps_fields() {
        let value = runtime_json::parse(&RuntimeString::from(
            r#"{"access_token":"a","refresh_token":"r","expires_in":10}"#,
        ))
        .unwrap();
        let tokens = parse_token_response(&value, 1000);
        assert_eq!(tokens.access_token, "a");
        assert_eq!(
            tokens.refresh_token.as_ref(),
            Some(&RuntimeString::from("r"))
        );
        assert_eq!(tokens.expires_at, Some(1000 + 10 * 1000));

        let empty =
            parse_token_response(&runtime_json::parse(&RuntimeString::from("{}")).unwrap(), 0);
        assert_eq!(empty.access_token, "");
        assert!(empty.refresh_token.is_none());
        assert!(empty.expires_at.is_none());
    }

    #[test]
    fn oauth_token_codec_preserves_lone_surrogates_and_field_order() {
        let source = runtime_json::parse(&RuntimeString::from(
            r#"{"access_token":"\ud800x","refresh_token":"r","expires_in":10,"id_token":"\udc00"}"#,
        ))
        .unwrap();
        let tokens = parse_token_response(&source, 1_000);
        assert_eq!(tokens.access_token.as_utf16(), &[0xd800, u16::from(b'x')]);
        assert_eq!(tokens.id_token.as_ref().unwrap().as_utf16(), &[0xdc00]);

        let encoded = runtime_codec::encode_value(&tokens).unwrap();
        let RuntimeJson::Object(object) = &encoded else {
            panic!("OAuth tokens must encode as an object");
        };
        let keys = object
            .entries()
            .map(|(key, _)| key.try_to_utf8().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            ["accessToken", "refreshToken", "expiresAt", "idToken"]
        );

        let persisted = encode_persisted_tokens(&tokens).unwrap();
        assert_eq!(
            persisted,
            r#"{"accessToken":"\ud800x","refreshToken":"r","expiresAt":11000,"idToken":"\udc00"}"#
        );
        assert_eq!(decode_persisted_tokens(&persisted), Some(tokens.clone()));
        assert!(runtime_oauth_access_token_to_header_host(&tokens.access_token).is_err());
    }

    #[test]
    fn oauth_token_codec_preserves_required_default_skip_and_unknown_field_rules() {
        let tokens = OAuthTokens {
            access_token: RuntimeString::from("a"),
            ..Default::default()
        };
        assert_eq!(
            encode_persisted_tokens(&tokens).unwrap(),
            r#"{"accessToken":"a"}"#
        );

        let with_unknown = RuntimeString::from(
            r#"{"accessToken":"a","refreshToken":null,"expiresAt":null,"idToken":null,"future":true}"#,
        );
        assert_eq!(
            runtime_codec::decode_text::<OAuthTokens>(&with_unknown).unwrap(),
            tokens
        );
        assert!(runtime_codec::decode_text::<OAuthTokens>(&RuntimeString::from("{}")).is_err());
    }

    #[test]
    fn form_codec_round_trips() {
        assert_eq!(decode_component("a%2Bb+c"), "a+b c");
        assert_eq!(encode_component("http://x/y"), "http%3A%2F%2Fx%2Fy");
        assert_eq!(query_get("code=xyz&state=s", "state").as_deref(), Some("s"));
    }
}
