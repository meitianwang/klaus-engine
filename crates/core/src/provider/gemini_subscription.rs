//! Gemini Code Assist subscription provider factory.
//!
//! Builds a Gemini provider that authenticates with a Gemini Code Assist
//! subscription via OAuth instead of an API key, routing through the Code Assist
//! backend (the `{ model, project, request }` envelope). The request/stream logic
//! is the ordinary Gemini provider — only the credential source and transport
//! differ. Returns the provider plus its OAuth credential source.

use crate::provider::access_policy::{ProviderAccessPolicy, ProviderIdentity};
use crate::provider::auth::{AuthSource, ProviderAuthAccess};
use crate::provider::gemini::{CodeAssistOptions, GeminiProvider, GeminiProviderOptions};
use crate::provider::oauth::{
    CredentialStore, OAuthAuth, OAuthAuthOptions, OAuthConfig, OAuthTokens,
};
use std::sync::Arc;

/// Google OAuth defaults for the Gemini Code Assist subscription: Google's OAuth
/// issuer, the `cloud-platform` scope, and a loopback redirect. These are PUBLIC
/// Google endpoints — you still supply your own `client_id` (a Google Cloud OAuth
/// client you register). `port` defaults to `8976` when `None`.
pub fn gemini_oauth_defaults(client_id: &str, port: Option<u16>) -> OAuthConfig {
    let port = port.unwrap_or(8976);
    OAuthConfig {
        client_id: client_id.to_string(),
        authorize_url: "https://accounts.google.com/o/oauth2/v2/auth".to_string(),
        token_url: "https://oauth2.googleapis.com/token".to_string(),
        redirect_uri: format!("http://127.0.0.1:{port}/oauth2callback"),
        scopes: Some(vec![
            "https://www.googleapis.com/auth/cloud-platform".to_string()
        ]),
        ..Default::default()
    }
}

/// Options for [`create_gemini_subscription_provider`].
pub struct GeminiSubscriptionOptions {
    /// OAuth client config. Use [`gemini_oauth_defaults`] for the Google endpoints.
    pub oauth: OAuthConfig,
    /// Persist/restore tokens.
    pub store: Option<Arc<dyn CredentialStore>>,
    /// Tokens from a prior login.
    pub tokens: Option<OAuthTokens>,
    /// GCP project id Code Assist bills/routes through (host-supplied).
    pub project: Option<String>,
    /// Override the Code Assist endpoint (default `cloudcode-pa.googleapis.com`).
    pub endpoint: Option<String>,
    /// Stable session id echoed into the Code Assist request envelope.
    pub session_id: Option<String>,
    /// Default model id for the provider.
    pub default_model: Option<String>,
    /// Optional credential access policy for this provider auth source.
    pub access_policy: Option<ProviderAccessPolicy>,
    /// Optional identity attached to credential access decisions.
    pub identity: Option<ProviderIdentity>,
}

/// Build a Code-Assist-routed Gemini provider bound to a Gemini-subscription OAuth
/// credential source. Returns the provider plus the shared [`OAuthAuth`].
pub fn create_gemini_subscription_provider(
    opts: GeminiSubscriptionOptions,
) -> (GeminiProvider, Arc<OAuthAuth>) {
    // The Code Assist transport uses its own endpoint, so no base-URL override is
    // threaded into the credential source (unlike the Anthropic/OpenAI factories).
    let auth = Arc::new(OAuthAuth::new(OAuthAuthOptions {
        config: opts.oauth,
        store: opts.store,
        tokens: opts.tokens,
        access: opts.access_policy.map(|policy| {
            ProviderAuthAccess::new("gemini", policy).with_optional_identity(opts.identity)
        }),
        ..Default::default()
    }));

    let auth_source: Arc<dyn AuthSource> = auth.clone();
    let provider = GeminiProvider::new(GeminiProviderOptions {
        auth_source: Some(auth_source),
        code_assist: Some(CodeAssistOptions {
            endpoint: opts.endpoint,
            project: opts.project,
            session_id: opts.session_id,
        }),
        default_model: opts.default_model,
        ..Default::default()
    });

    (provider, auth)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::provider::ModelProvider;

    #[test]
    fn gemini_oauth_defaults_use_google_endpoints_and_cloud_platform_scope() {
        let cfg = gemini_oauth_defaults("client-x", None);
        assert_eq!(cfg.client_id, "client-x");
        assert!(cfg.authorize_url.contains("accounts.google.com"));
        assert!(cfg.token_url.contains("oauth2.googleapis.com"));
        assert!(cfg
            .scopes
            .as_ref()
            .unwrap()
            .iter()
            .any(|s| s == "https://www.googleapis.com/auth/cloud-platform"));
        assert!(cfg.redirect_uri.contains("127.0.0.1"));
    }

    #[test]
    fn builds_a_code_assist_routed_provider() {
        let (provider, _auth) = create_gemini_subscription_provider(GeminiSubscriptionOptions {
            oauth: gemini_oauth_defaults("client-x", None),
            store: None,
            tokens: Some(OAuthTokens {
                access_token: "oauth-tok".into(),
                ..Default::default()
            }),
            project: Some("proj-2".to_string()),
            endpoint: None,
            session_id: None,
            default_model: None,
            access_policy: None,
            identity: None,
        });
        assert_eq!(provider.info().name, "gemini");
    }
}
