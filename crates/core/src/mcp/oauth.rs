//! The MCP OAuth 2.0 protocol machinery: RFC 9728 protected-resource metadata
//! discovery, RFC 8414 / OpenID Connect authorization-server metadata discovery,
//! RFC 7591 dynamic client registration, PKCE authorization-URL construction,
//! and the authorization-code / refresh-token exchanges — all orchestrated by
//! [`auth`]. The persistence of the pieces that must survive across calls is
//! delegated to an [`OAuthClientProvider`] implementation; `oauth_provider`
//! builds that implementation on top of this module.
//!
//! MCP protocol strings (the `MCP-Protocol-Version` header, the
//! `.well-known/*` paths, and the protocol-version value) are behavior and kept
//! exact.

use async_trait::async_trait;
use rand::RngCore;
use sha2::{Digest, Sha256};
use std::fmt;
use std::sync::Arc;
use std::sync::LazyLock;
use url::Url;

use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder, RuntimeJson,
    RuntimeObjectEncoder, RuntimeString,
};

/// The MCP protocol version advertised during metadata discovery.
pub const LATEST_PROTOCOL_VERSION: &str = "2025-11-25";

const AUTHORIZATION_CODE_RESPONSE_TYPE: &str = "code";
const AUTHORIZATION_CODE_CHALLENGE_METHOD: &str = "S256";

// --- schemas ---------------------------------------------------------------

/// OAuth 2.1 token response.
#[derive(Debug, Clone, Default)]
pub struct OAuthTokens {
    pub access_token: RuntimeString,
    pub id_token: Option<RuntimeString>,
    pub token_type: RuntimeString,
    pub expires_in: Option<f64>,
    pub scope: Option<RuntimeString>,
    pub refresh_token: Option<RuntimeString>,
}

impl RuntimeEncode for OAuthTokens {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("access_token", &self.access_token)?;
        encode_optional_runtime_field(&mut object, "id_token", &self.id_token)?;
        object.field("token_type", &self.token_type)?;
        encode_optional_runtime_field(&mut object, "expires_in", &self.expires_in)?;
        encode_optional_runtime_field(&mut object, "scope", &self.scope)?;
        encode_optional_runtime_field(&mut object, "refresh_token", &self.refresh_token)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for OAuthTokens {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                access_token: object.required("access_token", &[])?,
                id_token: object.optional("id_token", &[])?,
                token_type: object.required("token_type", &[])?,
                expires_in: object.optional("expires_in", &[])?,
                scope: object.optional("scope", &[])?,
                refresh_token: object.optional("refresh_token", &[])?,
            })
        })
    }
}

/// RFC 7591 client registration metadata (the request body for DCR).
#[derive(Debug, Clone, Default)]
pub struct OAuthClientMetadata {
    pub client_name: Option<RuntimeString>,
    pub redirect_uris: Vec<RuntimeString>,
    pub grant_types: Option<Vec<RuntimeString>>,
    pub response_types: Option<Vec<RuntimeString>>,
    pub token_endpoint_auth_method: Option<RuntimeString>,
    pub scope: Option<RuntimeString>,
}

impl RuntimeEncode for OAuthClientMetadata {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        encode_optional_runtime_field(&mut object, "client_name", &self.client_name)?;
        object.field("redirect_uris", &self.redirect_uris)?;
        encode_optional_runtime_field(&mut object, "grant_types", &self.grant_types)?;
        encode_optional_runtime_field(&mut object, "response_types", &self.response_types)?;
        encode_optional_runtime_field(
            &mut object,
            "token_endpoint_auth_method",
            &self.token_endpoint_auth_method,
        )?;
        encode_optional_runtime_field(&mut object, "scope", &self.scope)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for OAuthClientMetadata {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                client_name: object.optional("client_name", &[])?,
                redirect_uris: object.required("redirect_uris", &[])?,
                grant_types: object.optional("grant_types", &[])?,
                response_types: object.optional("response_types", &[])?,
                token_endpoint_auth_method: object.optional("token_endpoint_auth_method", &[])?,
                scope: object.optional("scope", &[])?,
            })
        })
    }
}

/// RFC 7591 client information (registration response).
#[derive(Debug, Clone, Default)]
pub struct OAuthClientInformationFull {
    pub client_id: RuntimeString,
    pub client_secret: Option<RuntimeString>,
    pub client_id_issued_at: Option<f64>,
    pub client_secret_expires_at: Option<f64>,
    pub redirect_uris: Vec<RuntimeString>,
    pub token_endpoint_auth_method: Option<RuntimeString>,
    pub grant_types: Option<Vec<RuntimeString>>,
    pub response_types: Option<Vec<RuntimeString>>,
    pub client_name: Option<RuntimeString>,
    pub scope: Option<RuntimeString>,
}

impl RuntimeEncode for OAuthClientInformationFull {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("client_id", &self.client_id)?;
        encode_optional_runtime_field(&mut object, "client_secret", &self.client_secret)?;
        encode_optional_runtime_field(
            &mut object,
            "client_id_issued_at",
            &self.client_id_issued_at,
        )?;
        encode_optional_runtime_field(
            &mut object,
            "client_secret_expires_at",
            &self.client_secret_expires_at,
        )?;
        object.field("redirect_uris", &self.redirect_uris)?;
        encode_optional_runtime_field(
            &mut object,
            "token_endpoint_auth_method",
            &self.token_endpoint_auth_method,
        )?;
        encode_optional_runtime_field(&mut object, "grant_types", &self.grant_types)?;
        encode_optional_runtime_field(&mut object, "response_types", &self.response_types)?;
        encode_optional_runtime_field(&mut object, "client_name", &self.client_name)?;
        encode_optional_runtime_field(&mut object, "scope", &self.scope)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for OAuthClientInformationFull {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                client_id: object.required("client_id", &[])?,
                client_secret: object.optional("client_secret", &[])?,
                client_id_issued_at: object.optional("client_id_issued_at", &[])?,
                client_secret_expires_at: object.optional("client_secret_expires_at", &[])?,
                redirect_uris: object.defaulted("redirect_uris", &[], Vec::new)?,
                token_endpoint_auth_method: object.optional("token_endpoint_auth_method", &[])?,
                grant_types: object.optional("grant_types", &[])?,
                response_types: object.optional("response_types", &[])?,
                client_name: object.optional("client_name", &[])?,
                scope: object.optional("scope", &[])?,
            })
        })
    }
}

/// RFC 8414 authorization-server metadata (also used for OIDC provider metadata,
/// keeping only the fields the flow reads).
#[derive(Debug, Clone, Default)]
pub struct OAuthMetadata {
    pub issuer: RuntimeString,
    pub authorization_endpoint: RuntimeString,
    pub token_endpoint: RuntimeString,
    pub registration_endpoint: Option<RuntimeString>,
    pub scopes_supported: Option<Vec<RuntimeString>>,
    pub response_types_supported: Vec<RuntimeString>,
    pub grant_types_supported: Option<Vec<RuntimeString>>,
    pub token_endpoint_auth_methods_supported: Option<Vec<RuntimeString>>,
    pub code_challenge_methods_supported: Option<Vec<RuntimeString>>,
    pub client_id_metadata_document_supported: Option<bool>,
}

impl RuntimeEncode for OAuthMetadata {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("issuer", &self.issuer)?;
        object.field("authorization_endpoint", &self.authorization_endpoint)?;
        object.field("token_endpoint", &self.token_endpoint)?;
        encode_optional_runtime_field(
            &mut object,
            "registration_endpoint",
            &self.registration_endpoint,
        )?;
        encode_optional_runtime_field(&mut object, "scopes_supported", &self.scopes_supported)?;
        object.field("response_types_supported", &self.response_types_supported)?;
        encode_optional_runtime_field(
            &mut object,
            "grant_types_supported",
            &self.grant_types_supported,
        )?;
        encode_optional_runtime_field(
            &mut object,
            "token_endpoint_auth_methods_supported",
            &self.token_endpoint_auth_methods_supported,
        )?;
        encode_optional_runtime_field(
            &mut object,
            "code_challenge_methods_supported",
            &self.code_challenge_methods_supported,
        )?;
        encode_optional_runtime_field(
            &mut object,
            "client_id_metadata_document_supported",
            &self.client_id_metadata_document_supported,
        )?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for OAuthMetadata {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                issuer: object.required("issuer", &[])?,
                authorization_endpoint: object.required("authorization_endpoint", &[])?,
                token_endpoint: object.required("token_endpoint", &[])?,
                registration_endpoint: object.optional("registration_endpoint", &[])?,
                scopes_supported: object.optional("scopes_supported", &[])?,
                response_types_supported: object.defaulted(
                    "response_types_supported",
                    &[],
                    Vec::new,
                )?,
                grant_types_supported: object.optional("grant_types_supported", &[])?,
                token_endpoint_auth_methods_supported: object
                    .optional("token_endpoint_auth_methods_supported", &[])?,
                code_challenge_methods_supported: object
                    .optional("code_challenge_methods_supported", &[])?,
                client_id_metadata_document_supported: object
                    .optional("client_id_metadata_document_supported", &[])?,
            })
        })
    }
}

/// RFC 9728 protected-resource metadata.
#[derive(Debug, Clone, Default)]
pub struct OAuthProtectedResourceMetadata {
    pub resource: RuntimeString,
    pub authorization_servers: Option<Vec<RuntimeString>>,
    pub scopes_supported: Option<Vec<RuntimeString>>,
}

impl RuntimeEncode for OAuthProtectedResourceMetadata {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("resource", &self.resource)?;
        encode_optional_runtime_field(
            &mut object,
            "authorization_servers",
            &self.authorization_servers,
        )?;
        encode_optional_runtime_field(&mut object, "scopes_supported", &self.scopes_supported)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for OAuthProtectedResourceMetadata {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                resource: object.required("resource", &[])?,
                authorization_servers: object.optional("authorization_servers", &[])?,
                scopes_supported: object.optional("scopes_supported", &[])?,
            })
        })
    }
}

/// Cached discovery state persisted between `auth()` calls.
#[derive(Debug, Clone, Default)]
pub struct OAuthDiscoveryState {
    pub authorization_server_url: Option<RuntimeString>,
    pub resource_metadata_url: Option<RuntimeString>,
    pub resource_metadata: Option<OAuthProtectedResourceMetadata>,
    pub authorization_server_metadata: Option<OAuthMetadata>,
}

impl RuntimeEncode for OAuthDiscoveryState {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        encode_optional_runtime_field(
            &mut object,
            "authorizationServerUrl",
            &self.authorization_server_url,
        )?;
        encode_optional_runtime_field(
            &mut object,
            "resourceMetadataUrl",
            &self.resource_metadata_url,
        )?;
        encode_optional_runtime_field(&mut object, "resourceMetadata", &self.resource_metadata)?;
        encode_optional_runtime_field(
            &mut object,
            "authorizationServerMetadata",
            &self.authorization_server_metadata,
        )?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for OAuthDiscoveryState {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                authorization_server_url: object.optional("authorizationServerUrl", &[])?,
                resource_metadata_url: object.optional("resourceMetadataUrl", &[])?,
                resource_metadata: object.optional("resourceMetadata", &[])?,
                authorization_server_metadata: object
                    .optional("authorizationServerMetadata", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone)]
struct OAuthErrorResponse {
    error: RuntimeString,
    error_description: Option<RuntimeString>,
    error_uri: Option<RuntimeString>,
}

impl RuntimeDecode for OAuthErrorResponse {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                error: object.required("error", &[])?,
                error_description: object.optional("error_description", &[])?,
                error_uri: object.optional("error_uri", &[])?,
            })
        })
    }
}

fn encode_optional_runtime_field<T: RuntimeEncode>(
    object: &mut RuntimeObjectEncoder,
    name: &str,
    value: &Option<T>,
) -> Result<(), RuntimeCodecError> {
    if let Some(value) = value {
        object.field(name, value)?;
    }
    Ok(())
}

// --- errors ----------------------------------------------------------------

/// The OAuth 2.0 error codes recognized when parsing an error response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OAuthErrorCode {
    InvalidRequest,
    InvalidClient,
    InvalidGrant,
    UnauthorizedClient,
    UnsupportedGrantType,
    InvalidScope,
    AccessDenied,
    ServerError,
    TemporarilyUnavailable,
    UnsupportedResponseType,
    UnsupportedTokenType,
    InvalidToken,
    MethodNotAllowed,
    TooManyRequests,
    InvalidClientMetadata,
    InsufficientScope,
    InvalidTarget,
}

fn code_from_error_string(s: &RuntimeString) -> OAuthErrorCode {
    if s == "invalid_request" {
        OAuthErrorCode::InvalidRequest
    } else if s == "invalid_client" {
        OAuthErrorCode::InvalidClient
    } else if s == "invalid_grant" {
        OAuthErrorCode::InvalidGrant
    } else if s == "unauthorized_client" {
        OAuthErrorCode::UnauthorizedClient
    } else if s == "unsupported_grant_type" {
        OAuthErrorCode::UnsupportedGrantType
    } else if s == "invalid_scope" {
        OAuthErrorCode::InvalidScope
    } else if s == "access_denied" {
        OAuthErrorCode::AccessDenied
    } else if s == "temporarily_unavailable" {
        OAuthErrorCode::TemporarilyUnavailable
    } else if s == "unsupported_response_type" {
        OAuthErrorCode::UnsupportedResponseType
    } else if s == "unsupported_token_type" {
        OAuthErrorCode::UnsupportedTokenType
    } else if s == "invalid_token" {
        OAuthErrorCode::InvalidToken
    } else if s == "method_not_allowed" {
        OAuthErrorCode::MethodNotAllowed
    } else if s == "too_many_requests" {
        OAuthErrorCode::TooManyRequests
    } else if s == "invalid_client_metadata" {
        OAuthErrorCode::InvalidClientMetadata
    } else if s == "insufficient_scope" {
        OAuthErrorCode::InsufficientScope
    } else if s == "invalid_target" {
        OAuthErrorCode::InvalidTarget
    } else {
        // Unknown codes fall back to ServerError (OAUTH_ERRORS[error] || ServerError).
        OAuthErrorCode::ServerError
    }
}

/// A typed OAuth error parsed from a server error response.
#[derive(Debug, Clone)]
pub struct OAuthError {
    pub code: OAuthErrorCode,
    pub message: String,
    pub error_uri: Option<String>,
}

/// The error type for the OAuth machinery: either a typed OAuth error or a plain
/// message.
#[derive(Debug)]
pub enum AuthError {
    OAuth(OAuthError),
    Message(String),
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthError::OAuth(e) => write!(f, "{}", e.message),
            AuthError::Message(m) => write!(f, "{}", m),
        }
    }
}

impl std::error::Error for AuthError {}

impl From<url::ParseError> for AuthError {
    fn from(e: url::ParseError) -> Self {
        AuthError::Message(e.to_string())
    }
}

/// The typed error a transport throws after `auth()` returns `REDIRECT`, telling
/// the host that interactive authorization is required.
#[derive(Debug, Clone)]
pub struct UnauthorizedError {
    pub message: String,
}

impl UnauthorizedError {
    pub fn new(message: Option<String>) -> Self {
        Self {
            message: message.unwrap_or_else(|| "Unauthorized".to_string()),
        }
    }
}

impl fmt::Display for UnauthorizedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for UnauthorizedError {}

fn project_oauth_runtime_scalar(
    value: &RuntimeString,
    surface: &'static str,
) -> Result<String, AuthError> {
    value.try_to_utf8().map_err(|error| {
        AuthError::Message(format!(
            "OAuth {surface} is not a Unicode-scalar host value: {error}"
        ))
    })
}

fn runtime_oauth_error_description_to_public_host(
    value: &RuntimeString,
) -> Result<String, AuthError> {
    project_oauth_runtime_scalar(value, "error_description")
}

fn runtime_oauth_error_uri_to_public_host(value: &RuntimeString) -> Result<String, AuthError> {
    project_oauth_runtime_scalar(value, "error_uri")
}

fn runtime_oauth_url_to_url_host(value: &RuntimeString) -> Result<String, AuthError> {
    project_oauth_runtime_scalar(value, "URL")
}

fn runtime_oauth_form_value_to_http_host(value: &RuntimeString) -> Result<String, AuthError> {
    project_oauth_runtime_scalar(value, "form value")
}

fn runtime_oauth_scope_to_provider_host(value: &RuntimeString) -> Result<String, AuthError> {
    project_oauth_runtime_scalar(value, "scope")
}

fn runtime_oauth_scopes_to_provider_host(values: &[RuntimeString]) -> Result<String, AuthError> {
    values
        .iter()
        .map(runtime_oauth_scope_to_provider_host)
        .collect::<Result<Vec<_>, _>>()
        .map(|values| values.join(" "))
}

fn runtime_oauth_json_body_to_http_host(value: &RuntimeString) -> Result<String, AuthError> {
    project_oauth_runtime_scalar(value, "JSON request body")
}

fn parse_error_response(status: Option<u16>, body: String) -> OAuthError {
    let parsed = runtime_codec::decode_text::<OAuthErrorResponse>(&RuntimeString::from(&body))
        .map_err(|error| AuthError::Message(error.to_string()));
    match parsed.and_then(|response| {
        let code = code_from_error_string(&response.error);
        let message = response
            .error_description
            .as_ref()
            .map(runtime_oauth_error_description_to_public_host)
            .transpose()?
            .unwrap_or_default();
        let error_uri = response
            .error_uri
            .as_ref()
            .map(runtime_oauth_error_uri_to_public_host)
            .transpose()?;
        Ok((code, message, error_uri))
    }) {
        Ok((code, message, error_uri)) => OAuthError {
            code,
            message,
            error_uri,
        },
        Err(e) => OAuthError {
            code: OAuthErrorCode::ServerError,
            message: format!(
                "{}Invalid OAuth error response: {}. Raw body: {}",
                status.map(|s| format!("HTTP {}: ", s)).unwrap_or_default(),
                e,
                body
            ),
            error_uri: None,
        },
    }
}

// --- fetch seam ------------------------------------------------------------

/// A single outbound HTTP request.
pub struct FetchRequest {
    pub method: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<String>,
}

/// A completed HTTP response.
pub struct FetchResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl FetchResponse {
    /// True for a 2xx status.
    pub fn ok(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

/// A transport failure. `cors` marks a browser-style CORS failure that triggers
/// a header-less retry; a native transport never sets it.
#[derive(Debug)]
pub struct FetchError {
    pub message: String,
    pub cors: bool,
}

/// The HTTP transport seam, injectable in tests.
#[async_trait]
pub trait McpFetch: Send + Sync {
    async fn fetch(&self, url: &str, req: FetchRequest) -> Result<FetchResponse, FetchError>;
}

/// The production transport over `reqwest`.
pub struct ReqwestMcpFetch {
    client: reqwest::Client,
}

impl ReqwestMcpFetch {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl Default for ReqwestMcpFetch {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl McpFetch for ReqwestMcpFetch {
    async fn fetch(&self, url: &str, req: FetchRequest) -> Result<FetchResponse, FetchError> {
        let method =
            reqwest::Method::from_bytes(req.method.as_bytes()).map_err(|e| FetchError {
                message: e.to_string(),
                cors: false,
            })?;
        let mut b = self.client.request(method, url);
        for (k, v) in &req.headers {
            b = b.header(k.as_str(), v.as_str());
        }
        if let Some(body) = req.body {
            b = b.body(body);
        }
        let resp = b.send().await.map_err(|e| FetchError {
            message: e.to_string(),
            cors: false,
        })?;
        let status = resp.status().as_u16();
        let mut headers = Vec::new();
        for (k, v) in resp.headers().iter() {
            if let Ok(s) = v.to_str() {
                headers.push((k.as_str().to_string(), s.to_string()));
            }
        }
        let body = resp.text().await.map_err(|e| FetchError {
            message: e.to_string(),
            cors: false,
        })?;
        Ok(FetchResponse {
            status,
            headers,
            body,
        })
    }
}

static DEFAULT_FETCH: LazyLock<ReqwestMcpFetch> = LazyLock::new(ReqwestMcpFetch::new);

async fn perform_fetch(
    fetch: &Option<Arc<dyn McpFetch>>,
    url: &str,
    req: FetchRequest,
) -> Result<FetchResponse, FetchError> {
    match fetch {
        Some(f) => f.fetch(url, req).await,
        None => DEFAULT_FETCH.fetch(url, req).await,
    }
}

async fn fetch_with_cors_retry(
    fetch: &Option<Arc<dyn McpFetch>>,
    url: &str,
    headers: Option<Vec<(String, String)>>,
) -> Result<Option<FetchResponse>, AuthError> {
    let mut headers = headers;
    loop {
        let req = FetchRequest {
            method: "GET".to_string(),
            headers: headers.clone().unwrap_or_default(),
            body: None,
        };
        match perform_fetch(fetch, url, req).await {
            Ok(r) => return Ok(Some(r)),
            Err(e) if e.cors => {
                if headers.is_some() {
                    headers = None;
                    continue;
                }
                return Ok(None);
            }
            Err(e) => return Err(AuthError::Message(e.message)),
        }
    }
}

// --- encoding helpers ------------------------------------------------------

/// URL-safe base64 with no padding.
pub(crate) fn base64url(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// `n` cryptographically-random bytes.
pub(crate) fn random_bytes(n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    rand::rngs::OsRng.fill_bytes(&mut buf);
    buf
}

fn pkce_challenge() -> (String, String) {
    let verifier = base64url(&random_bytes(32));
    let mut hasher = Sha256::new();
    hasher.update(verifier.as_bytes());
    let challenge = base64url(&hasher.finalize());
    (verifier, challenge)
}

fn form_encode(params: &[(String, String)]) -> String {
    let mut s = url::form_urlencoded::Serializer::new(String::new());
    for (k, v) in params {
        s.append_pair(k, v);
    }
    s.finish()
}

/// Replace-or-append a form parameter (mirrors `URLSearchParams.set`).
fn set_param(params: &mut Vec<(String, String)>, key: &str, value: &str) {
    params.retain(|(k, _)| k != key);
    params.push((key.to_string(), value.to_string()));
}

/// Replace-or-append a query parameter on a URL, preserving other params.
fn set_search_param(url: &mut Url, key: &str, value: &str) {
    let existing: Vec<(String, String)> = url
        .query_pairs()
        .filter(|(k, _)| k != key)
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    {
        let mut qp = url.query_pairs_mut();
        qp.clear();
        for (k, v) in &existing {
            qp.append_pair(k, v);
        }
        qp.append_pair(key, value);
    }
}

fn append_search_param(url: &mut Url, key: &str, value: &str) {
    url.query_pairs_mut().append_pair(key, value);
}

fn truthy(v: Option<String>) -> Option<String> {
    v.filter(|s| !s.is_empty())
}

// --- auth-utils ------------------------------------------------------------

fn resource_url_from_server_url(url: &str) -> Result<Url, AuthError> {
    let mut u = Url::parse(url)?;
    u.set_fragment(None);
    Ok(u)
}

fn check_resource_allowed(requested: &str, configured: &str) -> Result<bool, AuthError> {
    let requested = Url::parse(requested)?;
    let configured = Url::parse(configured)?;
    if requested.origin() != configured.origin() {
        return Ok(false);
    }
    let rp = requested.path();
    let cp = configured.path();
    if rp.len() < cp.len() {
        return Ok(false);
    }
    let rp = if rp.ends_with('/') {
        rp.to_string()
    } else {
        format!("{}/", rp)
    };
    let cp = if cp.ends_with('/') {
        cp.to_string()
    } else {
        format!("{}/", cp)
    };
    Ok(rp.starts_with(&cp))
}

/// SEP-991: validate a URL-based client id is HTTPS with a non-root path.
pub fn is_https_url(value: &str) -> bool {
    match Url::parse(value) {
        Ok(u) => u.scheme() == "https" && u.path() != "/",
        Err(_) => false,
    }
}

/// Determine the RFC 8707 `resource` for token requests.
pub async fn select_resource_url(
    server_url: &str,
    provider: &dyn OAuthClientProvider,
    resource_metadata: Option<&OAuthProtectedResourceMetadata>,
) -> Result<Option<Url>, AuthError> {
    let default_resource = resource_url_from_server_url(server_url)?;
    let metadata_resource = resource_metadata
        .map(|metadata| runtime_oauth_url_to_url_host(&metadata.resource))
        .transpose()?;
    if let Some(res) = provider
        .validate_resource_url(default_resource.as_str(), metadata_resource.as_deref())
        .await
    {
        return res;
    }
    let rm = match resource_metadata {
        Some(rm) => rm,
        None => return Ok(None),
    };
    let resource = runtime_oauth_url_to_url_host(&rm.resource)?;
    if !check_resource_allowed(default_resource.as_str(), &resource)? {
        return Err(AuthError::Message(format!(
            "Protected resource {} does not match expected {} (or origin)",
            resource, default_resource
        )));
    }
    Ok(Some(Url::parse(&resource)?))
}

// --- discovery -------------------------------------------------------------

fn build_well_known_path(prefix: &str, pathname: &str, prepend: bool) -> String {
    let mut pathname = pathname.to_string();
    if pathname.ends_with('/') {
        pathname.pop();
    }
    if prepend {
        format!("{}/.well-known/{}", pathname, prefix)
    } else {
        format!("/.well-known/{}{}", prefix, pathname)
    }
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum DiscoveryKind {
    OAuth,
    Oidc,
}

fn build_discovery_urls(
    authorization_server_url: &str,
) -> Result<Vec<(Url, DiscoveryKind)>, AuthError> {
    let url = Url::parse(authorization_server_url)?;
    let has_path = url.path() != "/";
    let mut out = Vec::new();
    if !has_path {
        out.push((
            url.join("/.well-known/oauth-authorization-server")?,
            DiscoveryKind::OAuth,
        ));
        out.push((
            url.join("/.well-known/openid-configuration")?,
            DiscoveryKind::Oidc,
        ));
        return Ok(out);
    }
    let mut pathname = url.path().to_string();
    if pathname.ends_with('/') {
        pathname.pop();
    }
    out.push((
        url.join(&format!(
            "/.well-known/oauth-authorization-server{}",
            pathname
        ))?,
        DiscoveryKind::OAuth,
    ));
    out.push((
        url.join(&format!("/.well-known/openid-configuration{}", pathname))?,
        DiscoveryKind::Oidc,
    ));
    out.push((
        url.join(&format!("{}/.well-known/openid-configuration", pathname))?,
        DiscoveryKind::Oidc,
    ));
    Ok(out)
}

async fn try_metadata_discovery(
    url: &Url,
    protocol_version: &str,
    fetch: &Option<Arc<dyn McpFetch>>,
) -> Result<Option<FetchResponse>, AuthError> {
    let headers = vec![(
        "MCP-Protocol-Version".to_string(),
        protocol_version.to_string(),
    )];
    fetch_with_cors_retry(fetch, url.as_str(), Some(headers)).await
}

fn should_attempt_fallback(resp: Option<&FetchResponse>, pathname: &str) -> bool {
    match resp {
        None => true,
        Some(r) => (400..500).contains(&r.status) && pathname != "/",
    }
}

async fn discover_metadata_with_fallback(
    server_url: &str,
    well_known_type: &str,
    fetch: &Option<Arc<dyn McpFetch>>,
    metadata_url: Option<&str>,
) -> Result<Option<FetchResponse>, AuthError> {
    let issuer = Url::parse(server_url)?;
    let protocol_version = LATEST_PROTOCOL_VERSION;
    let url = if let Some(mu) = metadata_url {
        Url::parse(mu)?
    } else {
        let well_known_path = build_well_known_path(well_known_type, issuer.path(), false);
        let mut u = issuer.join(&well_known_path)?;
        u.set_query(issuer.query());
        u
    };
    let mut response = try_metadata_discovery(&url, protocol_version, fetch).await?;
    if metadata_url.is_none() && should_attempt_fallback(response.as_ref(), issuer.path()) {
        let root_url = issuer.join(&format!("/.well-known/{}", well_known_type))?;
        response = try_metadata_discovery(&root_url, protocol_version, fetch).await?;
    }
    Ok(response)
}

async fn discover_oauth_protected_resource_metadata(
    server_url: &str,
    resource_metadata_url: Option<&str>,
    fetch: &Option<Arc<dyn McpFetch>>,
) -> Result<OAuthProtectedResourceMetadata, AuthError> {
    let response = discover_metadata_with_fallback(
        server_url,
        "oauth-protected-resource",
        fetch,
        resource_metadata_url,
    )
    .await?;
    match &response {
        None => {
            return Err(AuthError::Message(
                "Resource server does not implement OAuth 2.0 Protected Resource Metadata."
                    .to_string(),
            ))
        }
        Some(r) if r.status == 404 => {
            return Err(AuthError::Message(
                "Resource server does not implement OAuth 2.0 Protected Resource Metadata."
                    .to_string(),
            ))
        }
        Some(r) if !r.ok() => {
            return Err(AuthError::Message(format!(
                "HTTP {} trying to load well-known OAuth protected resource metadata.",
                r.status
            )))
        }
        _ => {}
    }
    let r = response.unwrap();
    runtime_codec::decode_text::<OAuthProtectedResourceMetadata>(&RuntimeString::from(&r.body))
        .map_err(|e| AuthError::Message(e.to_string()))
}

async fn discover_authorization_server_metadata(
    authorization_server_url: &str,
    fetch: &Option<Arc<dyn McpFetch>>,
) -> Result<Option<OAuthMetadata>, AuthError> {
    let headers = vec![
        (
            "MCP-Protocol-Version".to_string(),
            LATEST_PROTOCOL_VERSION.to_string(),
        ),
        ("Accept".to_string(), "application/json".to_string()),
    ];
    let urls = build_discovery_urls(authorization_server_url)?;
    for (endpoint_url, kind) in urls {
        let response =
            fetch_with_cors_retry(fetch, endpoint_url.as_str(), Some(headers.clone())).await?;
        let response = match response {
            Some(r) => r,
            None => continue,
        };
        if !response.ok() {
            if (400..500).contains(&response.status) {
                continue;
            }
            return Err(AuthError::Message(format!(
                "HTTP {} trying to load {} metadata from {}",
                response.status,
                if kind == DiscoveryKind::OAuth {
                    "OAuth"
                } else {
                    "OpenID provider"
                },
                endpoint_url
            )));
        }
        return runtime_codec::decode_text::<OAuthMetadata>(&RuntimeString::from(&response.body))
            .map(Some)
            .map_err(|e| AuthError::Message(e.to_string()));
    }
    Ok(None)
}

struct ServerInfo {
    authorization_server_url: String,
    authorization_server_metadata: Option<OAuthMetadata>,
    resource_metadata: Option<OAuthProtectedResourceMetadata>,
}

async fn discover_oauth_server_info(
    server_url: &str,
    resource_metadata_url: Option<&str>,
    fetch: &Option<Arc<dyn McpFetch>>,
) -> Result<ServerInfo, AuthError> {
    let mut resource_metadata = None;
    let mut authorization_server_url = None;
    if let Ok(rm) =
        discover_oauth_protected_resource_metadata(server_url, resource_metadata_url, fetch).await
    {
        if let Some(servers) = &rm.authorization_servers {
            if let Some(first) = servers.first() {
                authorization_server_url = Some(runtime_oauth_url_to_url_host(first)?);
            }
        }
        resource_metadata = Some(rm);
    }
    let authorization_server_url = match authorization_server_url {
        Some(u) => u,
        None => Url::parse(server_url)?.join("/")?.to_string(),
    };
    let authorization_server_metadata =
        discover_authorization_server_metadata(&authorization_server_url, fetch).await?;
    Ok(ServerInfo {
        authorization_server_url,
        authorization_server_metadata,
        resource_metadata,
    })
}

// --- client authentication -------------------------------------------------

fn select_client_auth_method(
    ci: &OAuthClientInformationFull,
    supported: &[RuntimeString],
) -> String {
    let has_client_secret = ci.client_secret.is_some();
    if let Some(m) = &ci.token_endpoint_auth_method {
        if !m.is_empty()
            && (m == "client_secret_basic" || m == "client_secret_post" || m == "none")
            && (supported.is_empty() || supported.iter().any(|s| s == m))
        {
            return if m == "client_secret_basic" {
                "client_secret_basic".to_string()
            } else if m == "client_secret_post" {
                "client_secret_post".to_string()
            } else {
                "none".to_string()
            };
        }
    }
    if supported.is_empty() {
        return if has_client_secret {
            "client_secret_basic".to_string()
        } else {
            "none".to_string()
        };
    }
    if has_client_secret && supported.iter().any(|s| s == "client_secret_basic") {
        return "client_secret_basic".to_string();
    }
    if has_client_secret && supported.iter().any(|s| s == "client_secret_post") {
        return "client_secret_post".to_string();
    }
    if supported.iter().any(|s| s == "none") {
        return "none".to_string();
    }
    if has_client_secret {
        "client_secret_post".to_string()
    } else {
        "none".to_string()
    }
}

fn apply_client_authentication(
    method: &str,
    ci: &OAuthClientInformationFull,
    headers: &mut Vec<(String, String)>,
    params: &mut Vec<(String, String)>,
) -> Result<(), AuthError> {
    match method {
        "client_secret_basic" => {
            let secret = ci.client_secret.as_ref().ok_or_else(|| {
                AuthError::Message(
                    "client_secret_basic authentication requires a client_secret".to_string(),
                )
            })?;
            let client_id = runtime_oauth_form_value_to_http_host(&ci.client_id)?;
            let secret = runtime_oauth_form_value_to_http_host(secret)?;
            use base64::Engine;
            let creds = base64::engine::general_purpose::STANDARD
                .encode(format!("{}:{}", client_id, secret));
            headers.retain(|(k, _)| !k.eq_ignore_ascii_case("authorization"));
            headers.push(("Authorization".to_string(), format!("Basic {}", creds)));
        }
        "client_secret_post" => {
            let client_id = runtime_oauth_form_value_to_http_host(&ci.client_id)?;
            set_param(params, "client_id", &client_id);
            if let Some(secret) = &ci.client_secret {
                let secret = runtime_oauth_form_value_to_http_host(secret)?;
                set_param(params, "client_secret", &secret);
            }
        }
        "none" => {
            let client_id = runtime_oauth_form_value_to_http_host(&ci.client_id)?;
            set_param(params, "client_id", &client_id);
        }
        other => {
            return Err(AuthError::Message(format!(
                "Unsupported client authentication method: {}",
                other
            )))
        }
    }
    Ok(())
}

// --- token requests --------------------------------------------------------

fn prepare_authorization_code_request(
    authorization_code: &str,
    code_verifier: &str,
    redirect_uri: &str,
) -> Vec<(String, String)> {
    vec![
        ("grant_type".to_string(), "authorization_code".to_string()),
        ("code".to_string(), authorization_code.to_string()),
        ("code_verifier".to_string(), code_verifier.to_string()),
        ("redirect_uri".to_string(), redirect_uri.to_string()),
    ]
}

async fn execute_token_request(
    authorization_server_url: &str,
    metadata: Option<&OAuthMetadata>,
    mut params: Vec<(String, String)>,
    client_information: Option<&OAuthClientInformationFull>,
    resource: Option<&Url>,
    fetch: &Option<Arc<dyn McpFetch>>,
) -> Result<OAuthTokens, AuthError> {
    let token_endpoint = metadata.and_then(|m| {
        if m.token_endpoint.is_empty() {
            None
        } else {
            Some(m.token_endpoint.clone())
        }
    });
    let token_url = match token_endpoint {
        Some(te) => Url::parse(&runtime_oauth_url_to_url_host(&te)?)?,
        None => Url::parse(authorization_server_url)?.join("/token")?,
    };
    let mut headers = vec![
        (
            "Content-Type".to_string(),
            "application/x-www-form-urlencoded".to_string(),
        ),
        ("Accept".to_string(), "application/json".to_string()),
    ];
    if let Some(r) = resource {
        set_param(&mut params, "resource", r.as_str());
    }
    if let Some(ci) = client_information {
        let supported = metadata
            .and_then(|m| m.token_endpoint_auth_methods_supported.clone())
            .unwrap_or_default();
        let method = select_client_auth_method(ci, &supported);
        apply_client_authentication(&method, ci, &mut headers, &mut params)?;
    }
    let body = form_encode(&params);
    let response = perform_fetch(
        fetch,
        token_url.as_str(),
        FetchRequest {
            method: "POST".to_string(),
            headers,
            body: Some(body),
        },
    )
    .await
    .map_err(|e| AuthError::Message(e.message))?;
    if !response.ok() {
        return Err(AuthError::OAuth(parse_error_response(
            Some(response.status),
            response.body,
        )));
    }
    runtime_codec::decode_text::<OAuthTokens>(&RuntimeString::from(&response.body))
        .map_err(|e| AuthError::Message(e.to_string()))
}

/// Options for [`refresh_authorization`].
pub struct RefreshAuthorizationOptions {
    pub metadata: Option<OAuthMetadata>,
    pub client_information: Option<OAuthClientInformationFull>,
    pub refresh_token: RuntimeString,
    pub resource: Option<Url>,
    pub fetch: Option<Arc<dyn McpFetch>>,
}

/// Exchange a refresh token for an updated access token, preserving the original
/// refresh token if the server does not return a new one.
pub async fn refresh_authorization(
    authorization_server_url: &str,
    opts: RefreshAuthorizationOptions,
) -> Result<OAuthTokens, AuthError> {
    let refresh_token = runtime_oauth_form_value_to_http_host(&opts.refresh_token)?;
    let params = vec![
        ("grant_type".to_string(), "refresh_token".to_string()),
        ("refresh_token".to_string(), refresh_token),
    ];
    let mut tokens = execute_token_request(
        authorization_server_url,
        opts.metadata.as_ref(),
        params,
        opts.client_information.as_ref(),
        opts.resource.as_ref(),
        &opts.fetch,
    )
    .await?;
    if tokens.refresh_token.is_none() {
        tokens.refresh_token = Some(opts.refresh_token.clone());
    }
    Ok(tokens)
}

struct StartAuthorizationOptions {
    metadata: Option<OAuthMetadata>,
    client_information: OAuthClientInformationFull,
    state: Option<String>,
    redirect_url: Option<String>,
    scope: Option<String>,
    resource: Option<Url>,
}

struct StartAuthorizationResult {
    authorization_url: Url,
    code_verifier: String,
}

fn start_authorization(
    authorization_server_url: &str,
    opts: StartAuthorizationOptions,
) -> Result<StartAuthorizationResult, AuthError> {
    let mut authorization_url = if let Some(m) = &opts.metadata {
        let authorization_endpoint = runtime_oauth_url_to_url_host(&m.authorization_endpoint)?;
        let u = Url::parse(&authorization_endpoint)?;
        if !m
            .response_types_supported
            .iter()
            .any(|r| r == AUTHORIZATION_CODE_RESPONSE_TYPE)
        {
            return Err(AuthError::Message(format!(
                "Incompatible auth server: does not support response type {}",
                AUTHORIZATION_CODE_RESPONSE_TYPE
            )));
        }
        if let Some(methods) = &m.code_challenge_methods_supported {
            if !methods
                .iter()
                .any(|c| c == AUTHORIZATION_CODE_CHALLENGE_METHOD)
            {
                return Err(AuthError::Message(format!(
                    "Incompatible auth server: does not support code challenge method {}",
                    AUTHORIZATION_CODE_CHALLENGE_METHOD
                )));
            }
        }
        u
    } else {
        Url::parse(authorization_server_url)?.join("/authorize")?
    };
    let (code_verifier, code_challenge) = pkce_challenge();
    let client_id = runtime_oauth_form_value_to_http_host(&opts.client_information.client_id)?;
    set_search_param(
        &mut authorization_url,
        "response_type",
        AUTHORIZATION_CODE_RESPONSE_TYPE,
    );
    set_search_param(&mut authorization_url, "client_id", &client_id);
    set_search_param(&mut authorization_url, "code_challenge", &code_challenge);
    set_search_param(
        &mut authorization_url,
        "code_challenge_method",
        AUTHORIZATION_CODE_CHALLENGE_METHOD,
    );
    set_search_param(
        &mut authorization_url,
        "redirect_uri",
        &opts.redirect_url.clone().unwrap_or_default(),
    );
    if let Some(s) = &opts.state {
        set_search_param(&mut authorization_url, "state", s);
    }
    if let Some(sc) = &opts.scope {
        set_search_param(&mut authorization_url, "scope", sc);
    }
    if opts
        .scope
        .as_deref()
        .map(|s| s.contains("offline_access"))
        .unwrap_or(false)
    {
        append_search_param(&mut authorization_url, "prompt", "consent");
    }
    if let Some(r) = &opts.resource {
        set_search_param(&mut authorization_url, "resource", r.as_str());
    }
    Ok(StartAuthorizationResult {
        authorization_url,
        code_verifier,
    })
}

async fn register_client(
    authorization_server_url: &str,
    metadata: Option<&OAuthMetadata>,
    client_metadata: &OAuthClientMetadata,
    scope: Option<String>,
    fetch: &Option<Arc<dyn McpFetch>>,
) -> Result<OAuthClientInformationFull, AuthError> {
    let registration_url = match metadata {
        Some(m) => {
            let re = m
                .registration_endpoint
                .as_ref()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    AuthError::Message(
                        "Incompatible auth server: does not support dynamic client registration"
                            .to_string(),
                    )
                })?;
            Url::parse(&runtime_oauth_url_to_url_host(re)?)?
        }
        None => Url::parse(authorization_server_url)?.join("/register")?,
    };
    let mut registration_metadata = client_metadata.clone();
    if let Some(sc) = scope {
        registration_metadata.scope = Some(RuntimeString::from(sc));
    }
    let body = runtime_codec::encode_text(&registration_metadata)
        .map_err(|e| AuthError::Message(e.to_string()))?;
    let body = runtime_oauth_json_body_to_http_host(&body)?;
    let response = perform_fetch(
        fetch,
        registration_url.as_str(),
        FetchRequest {
            method: "POST".to_string(),
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: Some(body),
        },
    )
    .await
    .map_err(|e| AuthError::Message(e.message))?;
    if !response.ok() {
        return Err(AuthError::OAuth(parse_error_response(
            Some(response.status),
            response.body,
        )));
    }
    runtime_codec::decode_text::<OAuthClientInformationFull>(&RuntimeString::from(&response.body))
        .map_err(|e| AuthError::Message(e.to_string()))
}

async fn fetch_token(
    provider: &dyn OAuthClientProvider,
    authorization_server_url: &str,
    metadata: Option<&OAuthMetadata>,
    resource: Option<&Url>,
    authorization_code: Option<&str>,
    fetch: &Option<Arc<dyn McpFetch>>,
) -> Result<OAuthTokens, AuthError> {
    let scope = provider
        .client_metadata()
        .scope
        .as_ref()
        .map(runtime_oauth_scope_to_provider_host)
        .transpose()?;
    let mut token_request_params = provider.prepare_token_request(scope).await;
    if token_request_params.is_none() {
        let code = authorization_code.ok_or_else(|| {
            AuthError::Message(
                "Either provider.prepareTokenRequest() or authorizationCode is required"
                    .to_string(),
            )
        })?;
        let redirect = provider.redirect_url().ok_or_else(|| {
            AuthError::Message("redirectUrl is required for authorization_code flow".to_string())
        })?;
        let verifier = provider.code_verifier().await?;
        token_request_params = Some(prepare_authorization_code_request(
            code, &verifier, &redirect,
        ));
    }
    let client_information = provider.client_information().await?;
    execute_token_request(
        authorization_server_url,
        metadata,
        token_request_params.unwrap(),
        client_information.as_ref(),
        resource,
        fetch,
    )
    .await
}

// --- provider trait --------------------------------------------------------

/// The scope of a credential invalidation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidateScope {
    All,
    Client,
    Tokens,
    Verifier,
    Discovery,
}

/// The persistence + interaction seam the OAuth flow drives. An implementation
/// owns storing/restoring the registered client, tokens, code verifier, and
/// discovered metadata.
#[async_trait]
pub trait OAuthClientProvider: Send + Sync {
    fn redirect_url(&self) -> Option<String>;
    fn client_metadata(&self) -> OAuthClientMetadata;
    fn client_metadata_url(&self) -> Option<String> {
        None
    }
    async fn state(&self) -> Result<Option<String>, AuthError> {
        Ok(None)
    }
    async fn client_information(&self) -> Result<Option<OAuthClientInformationFull>, AuthError>;
    async fn save_client_information(
        &self,
        info: OAuthClientInformationFull,
    ) -> Result<(), AuthError>;
    async fn tokens(&self) -> Result<Option<OAuthTokens>, AuthError>;
    async fn save_tokens(&self, tokens: OAuthTokens) -> Result<(), AuthError>;
    async fn redirect_to_authorization(&self, url: Url) -> Result<(), AuthError>;
    async fn save_code_verifier(&self, code_verifier: String) -> Result<(), AuthError>;
    async fn code_verifier(&self) -> Result<String, AuthError>;
    async fn save_discovery_state(&self, state: OAuthDiscoveryState) -> Result<(), AuthError>;
    async fn discovery_state(&self) -> Result<Option<OAuthDiscoveryState>, AuthError>;
    async fn invalidate_credentials(&self, scope: InvalidateScope) -> Result<(), AuthError>;
    /// Return `Some(..)` to override resource selection; `None` to use the default.
    async fn validate_resource_url(
        &self,
        _default_resource: &str,
        _metadata_resource: Option<&str>,
    ) -> Option<Result<Option<Url>, AuthError>> {
        None
    }
    /// Return `Some(params)` to fetch tokens with a custom grant; `None` for the
    /// authorization_code default.
    async fn prepare_token_request(&self, _scope: Option<String>) -> Option<Vec<(String, String)>> {
        None
    }
}

// --- orchestrator ----------------------------------------------------------

/// The terminal state of an `auth()` run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthResult {
    Authorized,
    Redirect,
}

/// Options for [`auth`].
#[derive(Default, Clone)]
pub struct AuthOptions {
    pub server_url: String,
    pub authorization_code: Option<String>,
    pub scope: Option<String>,
    pub resource_metadata_url: Option<String>,
    pub fetch: Option<Arc<dyn McpFetch>>,
}

/// Orchestrate the full auth flow with a server.
pub async fn auth(
    provider: &dyn OAuthClientProvider,
    options: AuthOptions,
) -> Result<AuthResult, AuthError> {
    match auth_internal(provider, &options).await {
        Ok(r) => Ok(r),
        Err(e) => match &e {
            AuthError::OAuth(o)
                if matches!(
                    o.code,
                    OAuthErrorCode::InvalidClient | OAuthErrorCode::UnauthorizedClient
                ) =>
            {
                provider
                    .invalidate_credentials(InvalidateScope::All)
                    .await?;
                auth_internal(provider, &options).await
            }
            AuthError::OAuth(o) if matches!(o.code, OAuthErrorCode::InvalidGrant) => {
                provider
                    .invalidate_credentials(InvalidateScope::Tokens)
                    .await?;
                auth_internal(provider, &options).await
            }
            _ => Err(e),
        },
    }
}

async fn auth_internal(
    provider: &dyn OAuthClientProvider,
    options: &AuthOptions,
) -> Result<AuthResult, AuthError> {
    let server_url = &options.server_url;
    let fetch = &options.fetch;
    let cached_state = provider.discovery_state().await?;

    let mut effective_resource_metadata_url = options.resource_metadata_url.clone();
    if effective_resource_metadata_url.is_none() {
        if let Some(cs) = &cached_state {
            if let Some(rmu) = &cs.resource_metadata_url {
                effective_resource_metadata_url = Some(runtime_oauth_url_to_url_host(rmu)?);
            }
        }
    }

    let use_cache = cached_state
        .as_ref()
        .map(|cs| cs.authorization_server_url.is_some())
        .unwrap_or(false);

    let resource_metadata: Option<OAuthProtectedResourceMetadata>;
    let authorization_server_url: String;
    let metadata: Option<OAuthMetadata>;

    if use_cache {
        let cs = cached_state.as_ref().unwrap();
        authorization_server_url =
            runtime_oauth_url_to_url_host(cs.authorization_server_url.as_ref().unwrap())?;
        let mut rm = cs.resource_metadata.clone();
        let mut resource_recomputed = false;
        let md;
        let metadata_recomputed;
        if let Some(m) = cs.authorization_server_metadata.clone() {
            md = Some(m);
            metadata_recomputed = false;
        } else {
            md = discover_authorization_server_metadata(&authorization_server_url, fetch).await?;
            metadata_recomputed = true;
        }
        if rm.is_none() {
            if let Ok(fetched) = discover_oauth_protected_resource_metadata(
                server_url,
                effective_resource_metadata_url.as_deref(),
                fetch,
            )
            .await
            {
                rm = Some(fetched);
                resource_recomputed = true;
            }
        }
        if metadata_recomputed || resource_recomputed {
            provider
                .save_discovery_state(OAuthDiscoveryState {
                    authorization_server_url: Some(RuntimeString::from(
                        authorization_server_url.clone(),
                    )),
                    resource_metadata_url: effective_resource_metadata_url
                        .clone()
                        .map(RuntimeString::from),
                    resource_metadata: rm.clone(),
                    authorization_server_metadata: md.clone(),
                })
                .await?;
        }
        resource_metadata = rm;
        metadata = md;
    } else {
        let server_info = discover_oauth_server_info(
            server_url,
            effective_resource_metadata_url.as_deref(),
            fetch,
        )
        .await?;
        authorization_server_url = server_info.authorization_server_url;
        metadata = server_info.authorization_server_metadata;
        resource_metadata = server_info.resource_metadata;
        provider
            .save_discovery_state(OAuthDiscoveryState {
                authorization_server_url: Some(RuntimeString::from(
                    authorization_server_url.clone(),
                )),
                resource_metadata_url: effective_resource_metadata_url
                    .clone()
                    .map(RuntimeString::from),
                resource_metadata: resource_metadata.clone(),
                authorization_server_metadata: metadata.clone(),
            })
            .await?;
    }

    let resource = select_resource_url(server_url, provider, resource_metadata.as_ref()).await?;

    let client_metadata = provider.client_metadata();
    let resource_scope = resource_metadata
        .as_ref()
        .and_then(|metadata| metadata.scopes_supported.as_deref())
        .map(runtime_oauth_scopes_to_provider_host)
        .transpose()?;
    let client_scope = client_metadata
        .scope
        .as_ref()
        .map(runtime_oauth_scope_to_provider_host)
        .transpose()?;
    let resolved_scope = truthy(options.scope.clone())
        .or_else(|| truthy(resource_scope))
        .or_else(|| truthy(client_scope));

    let mut client_information = provider.client_information().await?;
    if client_information.is_none() {
        if options.authorization_code.is_some() {
            return Err(AuthError::Message(
                "Existing OAuth client information is required when exchanging an authorization code"
                    .to_string(),
            ));
        }
        let supports_url_based = metadata
            .as_ref()
            .map(|m| m.client_id_metadata_document_supported == Some(true))
            .unwrap_or(false);
        let client_metadata_url = provider.client_metadata_url();
        if let Some(cmu) = &client_metadata_url {
            if !is_https_url(cmu) {
                return Err(AuthError::OAuth(OAuthError {
                    code: OAuthErrorCode::InvalidClientMetadata,
                    message: format!(
                        "clientMetadataUrl must be a valid HTTPS URL with a non-root pathname, got: {}",
                        cmu
                    ),
                    error_uri: None,
                }));
            }
        }
        let should_use_url_based = supports_url_based && client_metadata_url.is_some();
        if should_use_url_based {
            let ci = OAuthClientInformationFull {
                client_id: RuntimeString::from(client_metadata_url.clone().unwrap()),
                ..Default::default()
            };
            provider.save_client_information(ci.clone()).await?;
            client_information = Some(ci);
        } else {
            let full = register_client(
                &authorization_server_url,
                metadata.as_ref(),
                &client_metadata,
                resolved_scope.clone(),
                fetch,
            )
            .await?;
            provider.save_client_information(full.clone()).await?;
            client_information = Some(full);
        }
    }

    let non_interactive_flow = provider.redirect_url().is_none();

    if options.authorization_code.is_some() || non_interactive_flow {
        let tokens = fetch_token(
            provider,
            &authorization_server_url,
            metadata.as_ref(),
            resource.as_ref(),
            options.authorization_code.as_deref(),
            fetch,
        )
        .await?;
        provider.save_tokens(tokens).await?;
        return Ok(AuthResult::Authorized);
    }

    let tokens = provider.tokens().await?;
    if let Some(t) = &tokens {
        if let Some(rt) = &t.refresh_token {
            match refresh_authorization(
                &authorization_server_url,
                RefreshAuthorizationOptions {
                    metadata: metadata.clone(),
                    client_information: client_information.clone(),
                    refresh_token: rt.clone(),
                    resource: resource.clone(),
                    fetch: fetch.clone(),
                },
            )
            .await
            {
                Ok(new_tokens) => {
                    provider.save_tokens(new_tokens).await?;
                    return Ok(AuthResult::Authorized);
                }
                Err(e) => match &e {
                    // ServerError (or a non-OAuth error) — log-and-continue.
                    AuthError::OAuth(o) if o.code == OAuthErrorCode::ServerError => {}
                    AuthError::OAuth(_) => return Err(e),
                    _ => {}
                },
            }
        }
    }

    let state = provider.state().await?;
    let client = client_information
        .clone()
        .ok_or_else(|| AuthError::Message("OAuth client information is required".to_string()))?;
    let sa = start_authorization(
        &authorization_server_url,
        StartAuthorizationOptions {
            metadata: metadata.clone(),
            client_information: client,
            state,
            redirect_url: provider.redirect_url(),
            scope: resolved_scope.clone(),
            resource: resource.clone(),
        },
    )?;
    provider.save_code_verifier(sa.code_verifier).await?;
    provider
        .redirect_to_authorization(sa.authorization_url)
        .await?;
    Ok(AuthResult::Redirect)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oauth_runtime_codec_preserves_field_order_and_non_scalar_strings() {
        let tokens = OAuthTokens {
            access_token: RuntimeString::from_utf16(vec![0xd800]),
            id_token: Some(RuntimeString::from("id")),
            token_type: RuntimeString::from("Bearer"),
            expires_in: Some(1.0),
            scope: Some(RuntimeString::from("read")),
            refresh_token: Some(RuntimeString::from("refresh")),
        };

        let encoded = runtime_codec::encode_text(&tokens).unwrap();
        assert_eq!(
            encoded,
            r#"{"access_token":"\ud800","id_token":"id","token_type":"Bearer","expires_in":1,"scope":"read","refresh_token":"refresh"}"#
        );
        let decoded = runtime_codec::decode_text::<OAuthTokens>(&encoded).unwrap();
        assert_eq!(decoded.access_token, tokens.access_token);
        assert_eq!(decoded.refresh_token, tokens.refresh_token);
    }

    #[test]
    fn oauth_runtime_codec_preserves_camel_case_discovery_wire() {
        let state = OAuthDiscoveryState {
            authorization_server_url: Some(RuntimeString::from("https://as.test")),
            resource_metadata_url: Some(RuntimeString::from("https://mcp.test/metadata")),
            ..Default::default()
        };

        assert_eq!(
            runtime_codec::encode_text(&state).unwrap(),
            r#"{"authorizationServerUrl":"https://as.test","resourceMetadataUrl":"https://mcp.test/metadata"}"#
        );
    }

    #[test]
    fn oauth_url_projection_rejects_non_scalar_runtime_values() {
        let url = RuntimeString::from_utf16(vec![0xd800]);
        let error = runtime_oauth_url_to_url_host(&url).unwrap_err();
        assert!(error
            .to_string()
            .contains("not a Unicode-scalar host value"));
    }

    #[test]
    fn build_discovery_urls_root_and_pathful() {
        let root = build_discovery_urls("https://as.test").unwrap();
        assert_eq!(
            root[0].0.as_str(),
            "https://as.test/.well-known/oauth-authorization-server"
        );
        assert_eq!(
            root[1].0.as_str(),
            "https://as.test/.well-known/openid-configuration"
        );
        let pathful = build_discovery_urls("https://as.test/tenant1").unwrap();
        assert_eq!(
            pathful[0].0.as_str(),
            "https://as.test/.well-known/oauth-authorization-server/tenant1"
        );
        assert_eq!(
            pathful[2].0.as_str(),
            "https://as.test/tenant1/.well-known/openid-configuration"
        );
    }

    #[test]
    fn check_resource_allowed_matches_subpaths_only() {
        assert!(check_resource_allowed("https://a.test/api/x", "https://a.test/api").unwrap());
        assert!(!check_resource_allowed("https://a.test/api123", "https://a.test/api").unwrap());
        assert!(!check_resource_allowed("https://a.test/x", "https://b.test/x").unwrap());
    }

    #[test]
    fn select_client_auth_method_prefers_dcr_hint_then_none() {
        let mut ci = OAuthClientInformationFull {
            client_id: RuntimeString::from("c"),
            token_endpoint_auth_method: Some(RuntimeString::from("none")),
            ..Default::default()
        };
        assert_eq!(
            select_client_auth_method(&ci, &[RuntimeString::from("none")]),
            "none"
        );
        ci.token_endpoint_auth_method = None;
        assert_eq!(select_client_auth_method(&ci, &[]), "none");
    }
}
