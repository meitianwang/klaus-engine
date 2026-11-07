//! A [`CredentialStore`]-backed [`OAuthClientProvider`] for MCP servers. It
//! persists the pieces that must survive across `auth()` calls — the registered
//! client, the tokens (+ derived expiry), and the discovered authorization-server
//! metadata — plus the in-memory, single-session PKCE verifier and CSRF state.
//!
//! On top of the flow's connect-time refresh it adds a PROACTIVE refresh: when
//! [`tokens`](OAuthClientProvider::tokens) is read with the access token near
//! expiry, it refreshes ahead of time (serialized across processes by a refresh
//! lockfile; the last writer re-reads first).

use crate::credential_store::CredentialStoreError;
use crate::fs::lockfile::{with_lock, LockOptions};
use crate::fs::paths::sanitize_path_component;
use crate::mcp::credential_store::CredentialStore;
use crate::mcp::needs_auth_cache::default_needs_auth_cache;
use crate::mcp::oauth::{
    base64url, random_bytes, refresh_authorization, select_resource_url, AuthError,
    InvalidateScope, McpFetch, OAuthClientInformationFull, OAuthClientMetadata,
    OAuthClientProvider, OAuthDiscoveryState, OAuthTokens, RefreshAuthorizationOptions,
};
use crate::mcp::types::McpServerConfig;
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObject, RuntimeObjectEncoder, RuntimeString,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use url::Url;

fn decode_provider_unit_enum<T: Copy>(
    value: &RuntimeJson,
    variants: &[RuntimeEnumVariant<'_>],
    values: &[T],
) -> Result<T, RuntimeCodecError> {
    runtime_codec::decode_external(value, variants, |variant| match variant {
        RuntimeExternalVariant::Unit { variant } => values
            .get(variant)
            .copied()
            .ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownEnumVariant)),
        RuntimeExternalVariant::Payload { .. } => Err(RuntimeCodecError::new(
            RuntimeCodecErrorKind::InvalidEnumShape,
        )),
    })
}

/// Refresh the access token proactively when it has at most this long to live.
const REFRESH_LEEWAY_MS: i64 = 300_000; // 5 min

/// Default absolute lifetime when a token grant omits `expires_in`.
const DEFAULT_TOKEN_TTL_MS: i64 = 3_600_000; // 1h

/// A persisted token grant plus the absolute instant it expires.
#[derive(Debug, Clone)]
struct StoredTokenBundle {
    tokens: OAuthTokens,
    expires_at: i64,
}

impl RuntimeEncode for StoredTokenBundle {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("tokens", &self.tokens)?;
        object.field("expiresAt", &self.expires_at)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for StoredTokenBundle {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                tokens: object.required("tokens", &[])?,
                expires_at: object.required("expiresAt", &[])?,
            })
        })
    }
}

fn runtime_oauth_credential_json_to_store_host(value: &RuntimeString) -> Result<String, AuthError> {
    value.try_to_utf8().map_err(|error| {
        AuthError::Message(format!(
            "MCP OAuth credential JSON is not a Unicode-scalar store value: {error}"
        ))
    })
}

fn runtime_oauth_access_token_to_status_host(value: &RuntimeString) -> Result<String, AuthError> {
    value.try_to_utf8().map_err(|error| {
        AuthError::Message(format!(
            "MCP OAuth access token is not a Unicode-scalar status value: {error}"
        ))
    })
}

fn runtime_oauth_authorization_server_url_to_refresh_host(
    value: &RuntimeString,
) -> Result<String, AuthError> {
    value.try_to_utf8().map_err(|error| {
        AuthError::Message(format!(
            "MCP OAuth authorization-server URL is not a Unicode-scalar refresh value: {error}"
        ))
    })
}

/// Secret-free authentication status for the engine's MCP auth status surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum McpAuthStatus {
    Unsupported,
    NotLoggedIn,
    BearerToken,
    OAuth,
}

impl RuntimeEncode for McpAuthStatus {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Unsupported => "unsupported",
            Self::NotLoggedIn => "notLoggedIn",
            Self::BearerToken => "bearerToken",
            Self::OAuth => "oAuth",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for McpAuthStatus {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("unsupported", &[]),
                RuntimeEnumVariant::new("notLoggedIn", &[]),
                RuntimeEnumVariant::new("bearerToken", &[]),
                RuntimeEnumVariant::new("oAuth", &[]),
            ],
            &[
                Self::Unsupported,
                Self::NotLoggedIn,
                Self::BearerToken,
                Self::OAuth,
            ],
        )
    }
}

/// The server-identifying inputs to [`mcp_oauth_server_key`].
#[derive(Debug, Clone)]
pub struct McpOAuthConfig {
    /// Transport discriminator — part of the server-key material.
    pub transport: String,
    pub name: String,
    pub url: String,
    pub headers: Option<BTreeMap<String, String>>,
}

/// Stable per-server storage key: `<name>|<sha256(JSON{type,url,headers}).slice(0,16)>`.
pub fn mcp_oauth_server_key(config: &McpOAuthConfig) -> String {
    struct Material<'a> {
        r#type: &'a RuntimeString,
        url: &'a RuntimeString,
        headers: RuntimeJson,
    }

    impl RuntimeEncode for Material<'_> {
        fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
            let mut object = RuntimeObjectEncoder::new();
            object.field("type", self.r#type)?;
            object.field("url", self.url)?;
            object.field("headers", &self.headers)?;
            out.emit(RuntimeJson::Object(object.finish()))
        }
    }

    let mut headers = RuntimeObject::new();
    if let Some(values) = &config.headers {
        for (name, value) in values {
            headers.insert(name.as_str(), RuntimeJson::from(value.as_str()));
        }
    }
    let transport = RuntimeString::from(config.transport.as_str());
    let url = RuntimeString::from(config.url.as_str());
    let material = runtime_codec::encode_text(&Material {
        r#type: &transport,
        url: &url,
        headers: RuntimeJson::Object(headers),
    })
    .expect("MCP OAuth server-key material has an infallible runtime encoding");
    let mut hasher = Sha256::new();
    hasher.update(material.to_node_utf8());
    let hash = to_hex(&hasher.finalize());
    format!("{}|{}", config.name, &hash[..16])
}

/// Return the effective authentication status without exposing credential data.
pub async fn mcp_auth_status(
    config: &McpServerConfig,
    credential_store: Option<Arc<dyn CredentialStore>>,
) -> Result<McpAuthStatus, CredentialStoreError> {
    match config {
        McpServerConfig::Http {
            name,
            url,
            headers,
            oauth,
            ..
        }
        | McpServerConfig::Sse {
            name,
            url,
            headers,
            oauth,
            ..
        } => {
            if oauth.is_some() {
                let Some(store) = credential_store else {
                    return Ok(McpAuthStatus::NotLoggedIn);
                };
                let server_key = mcp_oauth_server_key(&McpOAuthConfig {
                    transport: config.transport().to_string(),
                    name: name.clone(),
                    url: url.clone(),
                    headers: headers.clone().map(|values| values.into_iter().collect()),
                });
                let raw = store.get(&format!("mcp:oauth:{server_key}:tokens")).await?;
                let has_valid_tokens = raw
                    .and_then(|raw| {
                        runtime_codec::decode_text::<StoredTokenBundle>(&RuntimeString::from(raw))
                            .ok()
                    })
                    .and_then(|bundle| {
                        runtime_oauth_access_token_to_status_host(&bundle.tokens.access_token).ok()
                    })
                    .is_some_and(|access_token| !access_token.trim().is_empty());
                return Ok(if has_valid_tokens {
                    McpAuthStatus::OAuth
                } else {
                    McpAuthStatus::NotLoggedIn
                });
            }

            let configured_bearer = headers.as_ref().is_some_and(|headers| {
                headers.iter().any(|(name, value)| {
                    name.eq_ignore_ascii_case("authorization") && !value.trim().is_empty()
                })
            });
            let stored_bearer = match credential_store {
                Some(store) => store
                    .get(&format!("mcp:{name}:token"))
                    .await?
                    .is_some_and(|value| !value.trim().is_empty()),
                None => false,
            };
            Ok(if configured_bearer || stored_bearer {
                McpAuthStatus::BearerToken
            } else {
                McpAuthStatus::Unsupported
            })
        }
        McpServerConfig::Stdio { .. }
        | McpServerConfig::InProcess { .. }
        | McpServerConfig::HostManaged { .. } => Ok(McpAuthStatus::Unsupported),
    }
}

/// Delete every persisted credential owned by one MCP authorization lifecycle.
pub async fn revoke_mcp_authorization(
    config: &McpServerConfig,
    credential_store: Arc<dyn CredentialStore>,
    oauth_lock_dir: Option<&str>,
) -> Result<McpAuthStatus, CredentialStoreError> {
    match config {
        McpServerConfig::Http {
            name,
            url,
            headers,
            oauth: Some(_),
            ..
        }
        | McpServerConfig::Sse {
            name,
            url,
            headers,
            oauth: Some(_),
            ..
        } => {
            let server_key = mcp_oauth_server_key(&McpOAuthConfig {
                transport: config.transport().to_string(),
                name: name.clone(),
                url: url.clone(),
                headers: headers.clone().map(|values| values.into_iter().collect()),
            });
            for part in ["tokens", "client", "discovery"] {
                credential_store
                    .delete(&format!("mcp:oauth:{server_key}:{part}"))
                    .await?;
            }
            default_needs_auth_cache(oauth_lock_dir)
                .clear(&server_key)
                .await;
            Ok(McpAuthStatus::NotLoggedIn)
        }
        McpServerConfig::Http { name, .. } | McpServerConfig::Sse { name, .. } => {
            credential_store
                .delete(&format!("mcp:{name}:token"))
                .await?;
            mcp_auth_status(config, Some(credential_store)).await
        }
        McpServerConfig::Stdio { .. }
        | McpServerConfig::InProcess { .. }
        | McpServerConfig::HostManaged { .. } => Ok(McpAuthStatus::Unsupported),
    }
}

fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// Construction options for an [`McpOAuthProvider`].
pub struct McpOAuthProviderOptions {
    pub credential_store: Arc<dyn CredentialStore>,
    /// The [`mcp_oauth_server_key`] for this server.
    pub server_key: String,
    pub server_name: String,
    pub server_url: String,
    /// The redirect_uri the host will receive the authorization code on.
    pub redirect_uri: String,
    /// Requested OAuth scopes (joined into `clientMetadata.scope`).
    pub scopes: Option<Vec<String>>,
    /// Invoked with the authorization URL when interactive auth is required.
    pub on_authorization_url: Arc<dyn Fn(Url) + Send + Sync>,
    /// Directory for cross-process refresh lockfiles (default `~/.klaus`).
    pub lock_dir: Option<String>,
    /// Custom fetch (tests inject a mock authorization server).
    pub fetch: Option<Arc<dyn McpFetch>>,
}

/// A credential-store-backed [`OAuthClientProvider`].
pub struct McpOAuthProvider {
    opts: McpOAuthProviderOptions,
    lock_dir: String,
    state: Mutex<Option<String>>,
    code_verifier: Mutex<Option<String>>,
}

impl McpOAuthProvider {
    pub fn new(opts: McpOAuthProviderOptions) -> Self {
        let lock_dir = opts
            .lock_dir
            .clone()
            .unwrap_or_else(|| format!("{}/.klaus", home_dir()));
        Self {
            opts,
            lock_dir,
            state: Mutex::new(None),
            code_verifier: Mutex::new(None),
        }
    }

    fn key(&self, part: &str) -> String {
        format!("mcp:oauth:{}:{}", self.opts.server_key, part)
    }

    async fn read_json<T: RuntimeDecode>(&self, key: &str) -> Result<Option<T>, AuthError> {
        let raw = self
            .opts
            .credential_store
            .get(key)
            .await
            .map_err(|e| AuthError::Message(e.to_string()))?;
        match raw {
            None => Ok(None),
            // A corrupt entry behaves as absent.
            Some(raw) => Ok(runtime_codec::decode_text::<T>(&RuntimeString::from(raw)).ok()),
        }
    }

    async fn load_bundle(&self) -> Result<Option<StoredTokenBundle>, AuthError> {
        self.read_json::<StoredTokenBundle>(&self.key("tokens"))
            .await
    }

    /// Refresh under a cross-process lockfile, re-reading first so a peer's
    /// refresh wins. On any failure keep the stale token so the request still
    /// goes out; a resulting 401 drives the full re-auth.
    async fn refresh_locked(&self, bundle: StoredTokenBundle) -> OAuthTokens {
        let disc = self.discovery_state().await.ok().flatten();
        let client = self.client_information().await.ok().flatten();
        let (disc, client) = match (disc, client) {
            (Some(d), Some(c)) if d.authorization_server_url.is_some() => (d, c),
            _ => return bundle.tokens,
        };
        let lock_target = format!(
            "{}/mcp-refresh-{}",
            self.lock_dir,
            sanitize_path_component(&self.opts.server_key)
        );
        // Ensure the lock's parent dir exists first; a creation failure falls
        // through to the stale-token path.
        let _ = tokio::fs::create_dir_all(&self.lock_dir).await;

        let result: std::io::Result<OAuthTokens> = with_lock(
            &lock_target,
            || async {
                let current = self
                    .load_bundle()
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| bundle.clone());
                // A peer refreshed while we waited for the lock.
                if current.expires_at - now_ms() > REFRESH_LEEWAY_MS {
                    return Ok(current.tokens);
                }
                let refresh_token = match current.tokens.refresh_token.clone() {
                    Some(rt) => rt,
                    None => return Ok(current.tokens),
                };
                let resource = select_resource_url(
                    &self.opts.server_url,
                    self,
                    disc.resource_metadata.as_ref(),
                )
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
                let authorization_server_url =
                    runtime_oauth_authorization_server_url_to_refresh_host(
                        disc.authorization_server_url.as_ref().unwrap(),
                    )
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                let refreshed = refresh_authorization(
                    &authorization_server_url,
                    RefreshAuthorizationOptions {
                        metadata: disc.authorization_server_metadata.clone(),
                        client_information: Some(client.clone()),
                        refresh_token,
                        resource,
                        fetch: self.opts.fetch.clone(),
                    },
                )
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
                self.save_tokens(refreshed.clone())
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                Ok(refreshed)
            },
            LockOptions {
                retries: Some(5),
                ..Default::default()
            },
        )
        .await;

        match result {
            Ok(t) => t,
            Err(_) => bundle.tokens,
        }
    }
}

#[async_trait]
impl OAuthClientProvider for McpOAuthProvider {
    fn redirect_url(&self) -> Option<String> {
        Some(self.opts.redirect_uri.clone())
    }

    fn client_metadata(&self) -> OAuthClientMetadata {
        let mut meta = OAuthClientMetadata {
            client_name: Some(RuntimeString::from(format!(
                "klaus-agent ({})",
                self.opts.server_name
            ))),
            redirect_uris: vec![RuntimeString::from(self.opts.redirect_uri.clone())],
            grant_types: Some(vec![
                RuntimeString::from("authorization_code"),
                RuntimeString::from("refresh_token"),
            ]),
            response_types: Some(vec![RuntimeString::from("code")]),
            token_endpoint_auth_method: Some(RuntimeString::from("none")),
            scope: None,
        };
        if let Some(scopes) = &self.opts.scopes {
            if !scopes.is_empty() {
                meta.scope = Some(RuntimeString::from(scopes.join(" ")));
            }
        }
        meta
    }

    async fn state(&self) -> Result<Option<String>, AuthError> {
        let mut guard = self.state.lock().unwrap();
        if guard.is_none() {
            *guard = Some(base64url(&random_bytes(32)));
        }
        Ok(guard.clone())
    }

    async fn client_information(&self) -> Result<Option<OAuthClientInformationFull>, AuthError> {
        self.read_json(&self.key("client")).await
    }

    async fn save_client_information(
        &self,
        info: OAuthClientInformationFull,
    ) -> Result<(), AuthError> {
        let json =
            runtime_codec::encode_text(&info).map_err(|e| AuthError::Message(e.to_string()))?;
        let json = runtime_oauth_credential_json_to_store_host(&json)?;
        self.opts
            .credential_store
            .set(&self.key("client"), &json)
            .await
            .map_err(|e| AuthError::Message(e.to_string()))
    }

    async fn tokens(&self) -> Result<Option<OAuthTokens>, AuthError> {
        let bundle = match self.load_bundle().await? {
            Some(b) => b,
            None => return Ok(None),
        };
        let needs_refresh = bundle.tokens.refresh_token.is_some()
            && (bundle.expires_at - now_ms()) <= REFRESH_LEEWAY_MS;
        if !needs_refresh {
            return Ok(Some(bundle.tokens));
        }
        Ok(Some(self.refresh_locked(bundle).await))
    }

    async fn save_tokens(&self, tokens: OAuthTokens) -> Result<(), AuthError> {
        let ttl = tokens
            .expires_in
            .map(|e| (e * 1000.0) as i64)
            .unwrap_or(DEFAULT_TOKEN_TTL_MS);
        let bundle = StoredTokenBundle {
            tokens,
            expires_at: now_ms() + ttl,
        };
        let json =
            runtime_codec::encode_text(&bundle).map_err(|e| AuthError::Message(e.to_string()))?;
        let json = runtime_oauth_credential_json_to_store_host(&json)?;
        self.opts
            .credential_store
            .set(&self.key("tokens"), &json)
            .await
            .map_err(|e| AuthError::Message(e.to_string()))
    }

    async fn redirect_to_authorization(&self, url: Url) -> Result<(), AuthError> {
        // Headless: hand the URL to the host; it captures the redirect out-of-band.
        (self.opts.on_authorization_url)(url);
        Ok(())
    }

    async fn save_code_verifier(&self, code_verifier: String) -> Result<(), AuthError> {
        *self.code_verifier.lock().unwrap() = Some(code_verifier);
        Ok(())
    }

    async fn code_verifier(&self) -> Result<String, AuthError> {
        self.code_verifier.lock().unwrap().clone().ok_or_else(|| {
            AuthError::Message("No PKCE code verifier saved for this MCP OAuth session".to_string())
        })
    }

    async fn save_discovery_state(&self, state: OAuthDiscoveryState) -> Result<(), AuthError> {
        let json =
            runtime_codec::encode_text(&state).map_err(|e| AuthError::Message(e.to_string()))?;
        let json = runtime_oauth_credential_json_to_store_host(&json)?;
        self.opts
            .credential_store
            .set(&self.key("discovery"), &json)
            .await
            .map_err(|e| AuthError::Message(e.to_string()))
    }

    async fn discovery_state(&self) -> Result<Option<OAuthDiscoveryState>, AuthError> {
        self.read_json(&self.key("discovery")).await
    }

    async fn invalidate_credentials(&self, scope: InvalidateScope) -> Result<(), AuthError> {
        let store = &self.opts.credential_store;
        if matches!(scope, InvalidateScope::All | InvalidateScope::Tokens) {
            store
                .delete(&self.key("tokens"))
                .await
                .map_err(|e| AuthError::Message(e.to_string()))?;
        }
        if matches!(scope, InvalidateScope::All | InvalidateScope::Client) {
            store
                .delete(&self.key("client"))
                .await
                .map_err(|e| AuthError::Message(e.to_string()))?;
        }
        if matches!(scope, InvalidateScope::All | InvalidateScope::Discovery) {
            store
                .delete(&self.key("discovery"))
                .await
                .map_err(|e| AuthError::Message(e.to_string()))?;
        }
        if matches!(scope, InvalidateScope::All | InvalidateScope::Verifier) {
            *self.code_verifier.lock().unwrap() = None;
        }
        Ok(())
    }
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn home_dir() -> String {
    #[allow(deprecated)]
    std::env::home_dir()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::credential_store::InMemoryCredentialStore;
    use crate::mcp::oauth::{
        auth, AuthOptions, AuthResult, FetchError, FetchRequest, FetchResponse, McpFetch,
        OAuthDiscoveryState, OAuthMetadata,
    };
    use crate::mcp::types::{McpOAuthOption, McpServerConfig, McpServerOptions};
    use regex::Regex;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;

    const MCP_URL: &str = "https://mcp.test/mcp";
    const AS: &str = "https://as.test";

    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn temp_dir(tag: &str) -> String {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let uniq = format!(
            "ae-mcp-oauth-{}-{}-{}-{}",
            tag,
            std::process::id(),
            n,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = std::env::temp_dir().join(uniq);
        std::fs::create_dir_all(&dir).unwrap();
        dir.to_string_lossy().into_owned()
    }

    struct MockAuthServer {
        token_grants: Arc<StdMutex<Vec<String>>>,
        issued: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl McpFetch for MockAuthServer {
        async fn fetch(&self, url: &str, req: FetchRequest) -> Result<FetchResponse, FetchError> {
            let parsed = Url::parse(url).unwrap();
            let path = parsed.path();
            let method = req.method.to_uppercase();
            let json = |body: serde_json::Value, status: u16| FetchResponse {
                status,
                headers: vec![("content-type".to_string(), "application/json".to_string())],
                body: body.to_string(),
            };
            if path == "/mcp" {
                return Ok(FetchResponse {
                    status: 401,
                    headers: vec![(
                        "WWW-Authenticate".to_string(),
                        "Bearer resource_metadata=\"https://mcp.test/.well-known/oauth-protected-resource\""
                            .to_string(),
                    )],
                    body: "Unauthorized".to_string(),
                });
            }
            if path.starts_with("/.well-known/oauth-protected-resource") {
                return Ok(json(
                    serde_json::json!({"resource": MCP_URL, "authorization_servers": [AS]}),
                    200,
                ));
            }
            if path.contains("/.well-known/oauth-authorization-server")
                || path.contains("/.well-known/openid-configuration")
            {
                return Ok(json(
                    serde_json::json!({
                        "issuer": AS,
                        "authorization_endpoint": format!("{}/authorize", AS),
                        "token_endpoint": format!("{}/token", AS),
                        "registration_endpoint": format!("{}/register", AS),
                        "response_types_supported": ["code"],
                        "grant_types_supported": ["authorization_code", "refresh_token"],
                        "code_challenge_methods_supported": ["S256"],
                        "token_endpoint_auth_methods_supported": ["none"],
                    }),
                    200,
                ));
            }
            if path == "/register" && method == "POST" {
                return Ok(json(
                    serde_json::json!({
                        "client_id": "dyn-client-1",
                        "redirect_uris": ["http://127.0.0.1:33418/callback"],
                        "grant_types": ["authorization_code", "refresh_token"],
                        "response_types": ["code"],
                        "token_endpoint_auth_method": "none",
                    }),
                    201,
                ));
            }
            if path == "/token" && method == "POST" {
                let body = req.body.clone().unwrap_or_default();
                let grant = url::form_urlencoded::parse(body.as_bytes())
                    .find(|(k, _)| k == "grant_type")
                    .map(|(_, v)| v.into_owned())
                    .unwrap_or_default();
                self.token_grants.lock().unwrap().push(grant);
                let n = self.issued.fetch_add(1, Ordering::SeqCst) + 1;
                return Ok(json(
                    serde_json::json!({
                        "access_token": format!("AT{}", n),
                        "token_type": "Bearer",
                        "expires_in": 3600,
                        "refresh_token": format!("RT{}", n),
                        "scope": "read",
                    }),
                    200,
                ));
            }
            Ok(FetchResponse {
                status: 404,
                headers: vec![],
                body: "not found".to_string(),
            })
        }
    }

    fn mock_auth_server() -> (Arc<dyn McpFetch>, Arc<StdMutex<Vec<String>>>) {
        let grants = Arc::new(StdMutex::new(Vec::new()));
        let server = MockAuthServer {
            token_grants: grants.clone(),
            issued: Arc::new(AtomicUsize::new(0)),
        };
        (Arc::new(server), grants)
    }

    fn make_provider(
        store: Arc<InMemoryCredentialStore>,
        fetch: Arc<dyn McpFetch>,
        on_url: Arc<dyn Fn(Url) + Send + Sync>,
        dir: String,
    ) -> McpOAuthProvider {
        McpOAuthProvider::new(McpOAuthProviderOptions {
            credential_store: store,
            server_key: mcp_oauth_server_key(&McpOAuthConfig {
                transport: "http".to_string(),
                name: "srv".to_string(),
                url: MCP_URL.to_string(),
                headers: None,
            }),
            server_name: "srv".to_string(),
            server_url: MCP_URL.to_string(),
            redirect_uri: "http://127.0.0.1:33418/callback".to_string(),
            scopes: None,
            on_authorization_url: on_url,
            lock_dir: Some(dir),
            fetch: Some(fetch),
        })
    }

    #[test]
    fn server_key_is_stable_across_calls_but_varies_with_config() {
        let k1 = mcp_oauth_server_key(&McpOAuthConfig {
            transport: "http".to_string(),
            name: "srv".to_string(),
            url: MCP_URL.to_string(),
            headers: None,
        });
        let k2 = mcp_oauth_server_key(&McpOAuthConfig {
            transport: "http".to_string(),
            name: "srv".to_string(),
            url: MCP_URL.to_string(),
            headers: None,
        });
        let k3 = mcp_oauth_server_key(&McpOAuthConfig {
            transport: "sse".to_string(),
            name: "srv".to_string(),
            url: MCP_URL.to_string(),
            headers: None,
        });
        assert!(Regex::new(r"^srv\|[0-9a-f]{16}$").unwrap().is_match(&k1));
        assert_eq!(k1, "srv|206bdee151b6f029");
        assert_eq!(k1, k2);
        assert_ne!(k3, k1);
    }

    #[tokio::test]
    async fn discovers_registers_and_produces_a_pkce_authorization_url() {
        let dir = temp_dir("redirect");
        let (fetch, _grants) = mock_auth_server();
        let store = Arc::new(InMemoryCredentialStore::new());
        let captured: Arc<StdMutex<Option<Url>>> = Arc::new(StdMutex::new(None));
        let cap = captured.clone();
        let provider = make_provider(
            store,
            fetch.clone(),
            Arc::new(move |u: Url| *cap.lock().unwrap() = Some(u)),
            dir.clone(),
        );

        let result = auth(
            &provider,
            AuthOptions {
                server_url: MCP_URL.to_string(),
                fetch: Some(fetch.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(result, AuthResult::Redirect);

        let client = provider.client_information().await.unwrap().unwrap();
        assert_eq!(client.client_id, "dyn-client-1");
        assert!(!provider.code_verifier().await.unwrap().is_empty());

        let u = captured.lock().unwrap().clone().unwrap();
        assert_eq!(
            format!("{}{}", u.origin().ascii_serialization(), u.path()),
            format!("{}/authorize", AS)
        );
        let q: HashMap<String, String> = u.query_pairs().into_owned().collect();
        assert_eq!(q.get("response_type").map(String::as_str), Some("code"));
        assert_eq!(q.get("client_id").map(String::as_str), Some("dyn-client-1"));
        assert!(q
            .get("code_challenge")
            .map(|s| !s.is_empty())
            .unwrap_or(false));
        assert_eq!(
            q.get("code_challenge_method").map(String::as_str),
            Some("S256")
        );
        assert_eq!(
            q.get("state").cloned(),
            Some(provider.state().await.unwrap().unwrap())
        );
        assert_eq!(
            q.get("redirect_uri").map(String::as_str),
            Some("http://127.0.0.1:33418/callback")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn exchanges_an_authorization_code_for_tokens_and_persists_them() {
        let dir = temp_dir("exchange");
        let (fetch, grants) = mock_auth_server();
        let store = Arc::new(InMemoryCredentialStore::new());
        let provider = make_provider(store, fetch.clone(), Arc::new(|_u| {}), dir.clone());

        auth(
            &provider,
            AuthOptions {
                server_url: MCP_URL.to_string(),
                fetch: Some(fetch.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let result = auth(
            &provider,
            AuthOptions {
                server_url: MCP_URL.to_string(),
                authorization_code: Some("the-code".to_string()),
                fetch: Some(fetch.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(result, AuthResult::Authorized);
        assert!(grants
            .lock()
            .unwrap()
            .contains(&"authorization_code".to_string()));

        let tokens = provider.tokens().await.unwrap().unwrap();
        assert_eq!(tokens.access_token, "AT1");
        assert!(tokens
            .refresh_token
            .as_ref()
            .is_some_and(|token| token == "RT1"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn proactively_refreshes_a_near_expiry_token() {
        let dir = temp_dir("refresh");
        let (fetch, grants) = mock_auth_server();
        let store = Arc::new(InMemoryCredentialStore::new());
        let provider = make_provider(store, fetch.clone(), Arc::new(|_u| {}), dir.clone());

        provider
            .save_discovery_state(OAuthDiscoveryState {
                authorization_server_url: Some(RuntimeString::from(AS)),
                resource_metadata_url: None,
                resource_metadata: None,
                authorization_server_metadata: Some(OAuthMetadata {
                    issuer: RuntimeString::from(AS),
                    authorization_endpoint: RuntimeString::from(format!("{}/authorize", AS)),
                    token_endpoint: RuntimeString::from(format!("{}/token", AS)),
                    response_types_supported: vec![RuntimeString::from("code")],
                    ..Default::default()
                }),
            })
            .await
            .unwrap();
        provider
            .save_client_information(OAuthClientInformationFull {
                client_id: RuntimeString::from("dyn-client-1"),
                redirect_uris: vec![RuntimeString::from("http://127.0.0.1:33418/callback")],
                ..Default::default()
            })
            .await
            .unwrap();
        provider
            .save_tokens(OAuthTokens {
                access_token: RuntimeString::from("STALE"),
                token_type: RuntimeString::from("Bearer"),
                expires_in: Some(10.0),
                refresh_token: Some(RuntimeString::from("RT-seed")),
                ..Default::default()
            })
            .await
            .unwrap();

        let tokens = provider.tokens().await.unwrap().unwrap();
        assert!(grants
            .lock()
            .unwrap()
            .contains(&"refresh_token".to_string()));
        assert_eq!(tokens.access_token, "AT1");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn invalidate_credentials_clears_the_right_stored_keys() {
        let dir = temp_dir("invalidate");
        let (fetch, _grants) = mock_auth_server();
        let store = Arc::new(InMemoryCredentialStore::new());
        let provider = make_provider(store, fetch.clone(), Arc::new(|_u| {}), dir.clone());

        provider
            .save_client_information(OAuthClientInformationFull {
                client_id: RuntimeString::from("c"),
                redirect_uris: vec![RuntimeString::from("x")],
                ..Default::default()
            })
            .await
            .unwrap();
        provider
            .save_tokens(OAuthTokens {
                access_token: RuntimeString::from("a"),
                token_type: RuntimeString::from("Bearer"),
                ..Default::default()
            })
            .await
            .unwrap();

        provider
            .invalidate_credentials(InvalidateScope::Tokens)
            .await
            .unwrap();
        assert!(provider.tokens().await.unwrap().is_none());
        assert_eq!(
            provider
                .client_information()
                .await
                .unwrap()
                .unwrap()
                .client_id,
            "c"
        );

        provider
            .invalidate_credentials(InvalidateScope::All)
            .await
            .unwrap();
        assert!(provider.client_information().await.unwrap().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn credential_status_and_revoke_cover_the_full_persisted_oauth_lifecycle() {
        let dir = temp_dir("status-revoke");
        let (fetch, _grants) = mock_auth_server();
        let store = Arc::new(InMemoryCredentialStore::new());
        let config = McpServerConfig::Http {
            name: "srv".to_string(),
            url: MCP_URL.to_string(),
            headers: None,
            oauth: Some(McpOAuthOption::Advertised),
            options: McpServerOptions::default(),
        };

        assert_eq!(
            mcp_auth_status(&config, Some(store.clone())).await.unwrap(),
            McpAuthStatus::NotLoggedIn
        );

        let provider = make_provider(store.clone(), fetch, Arc::new(|_u| {}), dir.clone());
        provider
            .save_client_information(OAuthClientInformationFull {
                client_id: RuntimeString::from("persisted-client"),
                redirect_uris: vec![RuntimeString::from("http://127.0.0.1:33418/callback")],
                ..Default::default()
            })
            .await
            .unwrap();
        provider
            .save_discovery_state(OAuthDiscoveryState {
                authorization_server_url: Some(RuntimeString::from(AS)),
                resource_metadata_url: None,
                resource_metadata: None,
                authorization_server_metadata: None,
            })
            .await
            .unwrap();
        provider
            .save_tokens(OAuthTokens {
                access_token: RuntimeString::from("status-access-secret"),
                refresh_token: Some(RuntimeString::from("status-refresh-secret")),
                token_type: RuntimeString::from("Bearer"),
                expires_in: Some(3600.0),
                ..Default::default()
            })
            .await
            .unwrap();
        let server_key = mcp_oauth_server_key(&McpOAuthConfig {
            transport: "http".to_string(),
            name: "srv".to_string(),
            url: MCP_URL.to_string(),
            headers: None,
        });
        let cache = crate::mcp::needs_auth_cache::default_needs_auth_cache(Some(&dir));
        cache.mark(&server_key, 1_000_000).await;

        assert_eq!(
            mcp_auth_status(&config, Some(store.clone())).await.unwrap(),
            McpAuthStatus::OAuth
        );

        let status = revoke_mcp_authorization(&config, store.clone(), Some(&dir))
            .await
            .unwrap();
        assert_eq!(status, McpAuthStatus::NotLoggedIn);
        for part in ["tokens", "client", "discovery"] {
            assert!(store
                .get(&format!("mcp:oauth:{server_key}:{part}"))
                .await
                .unwrap()
                .is_none());
        }
        assert!(
            !cache
                .is_fresh(
                    &server_key,
                    crate::mcp::needs_auth_cache::NEEDS_AUTH_TTL_MS,
                    1_000_001
                )
                .await
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
