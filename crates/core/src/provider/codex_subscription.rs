//! Codex-subscription provider factory.
//!
//! Builds an OpenAI provider that authenticates with a ChatGPT/codex subscription
//! via OAuth instead of an API key, driving the Responses API surface. The
//! request/stream logic is the ordinary OpenAI provider — only the credential
//! source and base URL differ. Returns the provider plus its OAuth credential
//! source.

use crate::provider::access_policy::{ProviderAccessPolicy, ProviderIdentity};
use crate::provider::auth::{AuthSource, ProviderAuthAccess};
use crate::provider::oauth::{
    CredentialStore, OAuthAuth, OAuthAuthOptions, OAuthConfig, OAuthTokens,
};
use crate::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use std::sync::Arc;

/// OAuth defaults for the Codex subscription source: the OpenAI OAuth issuer
/// plus a loopback `/auth/callback`. You still supply your own `client_id`.
/// `port` defaults to `1455` when `None`.
pub fn codex_oauth_defaults(client_id: &str, port: Option<u16>) -> OAuthConfig {
    let port = port.unwrap_or(1455);
    OAuthConfig {
        client_id: client_id.to_string(),
        authorize_url: "https://auth.openai.com/oauth/authorize".to_string(),
        token_url: "https://auth.openai.com/oauth/token".to_string(),
        redirect_uri: format!("http://localhost:{port}/auth/callback"),
        scopes: Some(vec![
            "openid".to_string(),
            "profile".to_string(),
            "email".to_string(),
            "offline_access".to_string(),
        ]),
        ..Default::default()
    }
}

/// Options for [`create_codex_subscription_provider`].
pub struct CodexSubscriptionOptions {
    /// OAuth client config. Use [`codex_oauth_defaults`] for the codex-style endpoints.
    pub oauth: OAuthConfig,
    /// Persist/restore tokens.
    pub store: Option<Arc<dyn CredentialStore>>,
    /// Tokens from a prior login.
    pub tokens: Option<OAuthTokens>,
    /// Backend base URL the subscription routes through (the Responses API host).
    pub base_url: Option<String>,
    /// Default model id for the provider.
    pub default_model: Option<String>,
    /// Optional credential access policy for this provider auth source.
    pub access_policy: Option<ProviderAccessPolicy>,
    /// Optional identity attached to credential access decisions.
    pub identity: Option<ProviderIdentity>,
}

/// Build an OpenAI provider (Responses API) bound to a codex-subscription OAuth
/// credential source. Returns the provider plus the shared [`OAuthAuth`].
pub fn create_codex_subscription_provider(
    opts: CodexSubscriptionOptions,
) -> (OpenAIProvider, Arc<OAuthAuth>) {
    let auth = Arc::new(OAuthAuth::new(OAuthAuthOptions {
        config: opts.oauth,
        store: opts.store,
        tokens: opts.tokens,
        base_url: opts.base_url,
        access: opts.access_policy.map(|policy| {
            ProviderAuthAccess::new("openai", policy).with_optional_identity(opts.identity)
        }),
        ..Default::default()
    }));

    let auth_source: Arc<dyn AuthSource> = auth.clone();
    let provider = OpenAIProvider::new(OpenAIProviderOptions {
        auth_source: Some(auth_source),
        api: Some(OpenAiApi::Responses),
        remote_compaction: crate::types::provider::RemoteCompactionSupport::ResponsesV2,
        default_model: opts.default_model,
        ..Default::default()
    });

    (provider, auth)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::auth::AuthSource;
    use crate::types::abort::AbortSignal;
    use crate::types::errors::ProviderErrorKind;
    use crate::types::provider::ModelProvider;

    #[test]
    fn codex_oauth_defaults_use_the_openai_issuer_and_loopback() {
        let c = codex_oauth_defaults("cid", Some(1455));
        assert!(c.authorize_url.contains("auth.openai.com"));
        assert_eq!(c.redirect_uri, "http://localhost:1455/auth/callback");
        assert!(c
            .scopes
            .as_ref()
            .unwrap()
            .iter()
            .any(|s| s == "offline_access"));
    }

    #[test]
    fn builds_a_codex_subscription_provider() {
        let (provider, _auth) = create_codex_subscription_provider(CodexSubscriptionOptions {
            oauth: codex_oauth_defaults("my-codex-client", None),
            store: None,
            tokens: Some(OAuthTokens {
                access_token: "t".into(),
                ..Default::default()
            }),
            base_url: None,
            default_model: None,
            access_policy: None,
            identity: None,
        });
        assert!(!provider.info().default_model.is_empty());
    }

    #[tokio::test]
    async fn codex_subscription_auth_enforces_access_policy() {
        let (_provider, auth) = create_codex_subscription_provider(CodexSubscriptionOptions {
            oauth: codex_oauth_defaults("my-codex-client", None),
            store: None,
            tokens: Some(OAuthTokens {
                access_token: "t".into(),
                ..Default::default()
            }),
            base_url: None,
            default_model: None,
            access_policy: Some(ProviderAccessPolicy::allow_all().allow_providers(["anthropic"])),
            identity: None,
        });

        let err = auth
            .get_headers(&AbortSignal::new())
            .await
            .expect_err("factory-provided policy should deny credential access");

        assert_eq!(err.kind, ProviderErrorKind::Auth);
        assert!(err.message.contains("ProviderDenied"), "{:?}", err.message);
    }
}
