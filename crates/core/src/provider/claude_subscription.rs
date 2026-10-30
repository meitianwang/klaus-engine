//! Claude-subscription provider factory.
//!
//! Builds an Anthropic provider that authenticates with a Claude subscription via
//! OAuth instead of an API key. The request/stream/caching logic is the ordinary
//! Anthropic provider, unchanged — only the credential source differs: a
//! refreshed bearer token attached per request. The factory returns the provider
//! plus the OAuth credential source so the host can drive the login and set
//! tokens.

use crate::provider::access_policy::{ProviderAccessPolicy, ProviderIdentity};
use crate::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use crate::provider::auth::{AuthSource, ProviderAuthAccess};
use crate::provider::oauth::{
    CredentialStore, OAuthAuth, OAuthAuthOptions, OAuthConfig, OAuthTokens,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Options for [`create_claude_subscription_provider`].
pub struct ClaudeSubscriptionOptions {
    /// OAuth client config for the subscription login (generic OAuth 2.0 + PKCE;
    /// supply your own client id and endpoints — no proprietary id is embedded).
    pub oauth: OAuthConfig,
    /// Persist/restore tokens (OS keychain or encrypted file).
    pub store: Option<Arc<dyn CredentialStore>>,
    /// Tokens from a prior login, if not loading from `store`.
    pub tokens: Option<OAuthTokens>,
    /// Anthropic API base used by the subscription (default `api.anthropic.com`).
    pub base_url: Option<String>,
    /// Any `anthropic-beta` flags the subscription endpoint requires.
    pub beta_header: Option<String>,
    /// Default model id for the provider.
    pub default_model: Option<String>,
    /// Optional credential access policy for this provider auth source.
    pub access_policy: Option<ProviderAccessPolicy>,
    /// Optional identity attached to credential access decisions.
    pub identity: Option<ProviderIdentity>,
}

/// Build an Anthropic provider bound to a Claude-subscription OAuth credential
/// source. Returns the provider plus the shared [`OAuthAuth`] (so the caller can
/// drive login and `set_tokens`); both reference the same credential state.
pub fn create_claude_subscription_provider(
    opts: ClaudeSubscriptionOptions,
) -> (AnthropicProvider, Arc<OAuthAuth>) {
    let mut extra_headers: HashMap<String, String> = HashMap::new();
    if let Some(beta) = opts.beta_header {
        extra_headers.insert("anthropic-beta".to_string(), beta);
    }

    let auth = Arc::new(OAuthAuth::new(OAuthAuthOptions {
        config: opts.oauth,
        extra_headers: Some(extra_headers),
        store: opts.store,
        tokens: opts.tokens,
        base_url: opts.base_url,
        access: opts.access_policy.map(|policy| {
            ProviderAuthAccess::new("anthropic", policy).with_optional_identity(opts.identity)
        }),
        ..Default::default()
    }));

    let auth_source: Arc<dyn AuthSource> = auth.clone();
    let provider = AnthropicProvider::new(AnthropicProviderOptions {
        auth_source: Some(auth_source),
        default_model: opts.default_model,
        ..Default::default()
    });

    (provider, auth)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::provider::ModelProvider;

    fn config() -> OAuthConfig {
        OAuthConfig {
            client_id: "my-client".to_string(),
            authorize_url: "https://auth.example.com/authorize".to_string(),
            token_url: "https://auth.example.com/token".to_string(),
            redirect_uri: "http://localhost:1455/auth/callback".to_string(),
            scopes: Some(vec!["openid".to_string(), "offline_access".to_string()]),
            ..Default::default()
        }
    }

    #[test]
    fn builds_a_claude_subscription_provider() {
        let (provider, _auth) = create_claude_subscription_provider(ClaudeSubscriptionOptions {
            oauth: config(),
            store: None,
            tokens: Some(OAuthTokens {
                access_token: "t".into(),
                ..Default::default()
            }),
            base_url: None,
            beta_header: Some("oauth-2025-01".to_string()),
            default_model: None,
            access_policy: None,
            identity: None,
        });
        // Construction wires an Anthropic provider around the OAuth credential source.
        assert!(!provider.info().default_model.is_empty());
    }
}
