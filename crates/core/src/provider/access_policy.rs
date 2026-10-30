//! Pure provider credential access policy.

use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProviderCredentialKind {
    ApiKey,
    BearerToken,
    OAuthAccessToken,
    OAuthRefreshToken,
    AgentIdentityAssertion,
    McpBearerToken,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialOperation {
    Use,
    Refresh,
    Store,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderIdentity {
    pub account_id: String,
    pub subject_id: Option<String>,
    pub task_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderAccessPolicy {
    allowed_providers: BTreeSet<String>,
    allowed_kinds: BTreeSet<ProviderCredentialKind>,
    bound_identity: Option<ProviderIdentity>,
    require_identity_match: bool,
    allow_refresh: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialAccessRequest {
    pub provider_id: String,
    pub kind: ProviderCredentialKind,
    pub operation: CredentialOperation,
    pub credential_available: bool,
    pub token_expired: bool,
    pub refresh_token_available: bool,
    pub identity: Option<ProviderIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialAccessGrant {
    pub provider_id: String,
    pub kind: ProviderCredentialKind,
    pub operation: CredentialOperation,
    pub identity: Option<ProviderIdentity>,
    pub refresh_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialAccessDenial {
    MissingCredential {
        provider_id: String,
    },
    ProviderDenied {
        provider_id: String,
    },
    CredentialKindDenied {
        kind: ProviderCredentialKind,
    },
    IdentityRequired,
    IdentityMismatch {
        expected: ProviderIdentity,
        actual: Option<ProviderIdentity>,
    },
    RefreshDenied,
    MissingRefreshToken,
}

impl ProviderIdentity {
    pub fn account(account_id: impl Into<String>) -> Self {
        Self {
            account_id: account_id.into(),
            subject_id: None,
            task_id: None,
        }
    }
}

impl ProviderAccessPolicy {
    pub fn allow_all() -> Self {
        Self {
            allowed_providers: BTreeSet::new(),
            allowed_kinds: BTreeSet::new(),
            bound_identity: None,
            require_identity_match: false,
            allow_refresh: true,
        }
    }

    pub fn allow_providers(
        mut self,
        providers: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.allowed_providers = providers.into_iter().map(Into::into).collect();
        self
    }

    pub fn allow_kinds(mut self, kinds: impl IntoIterator<Item = ProviderCredentialKind>) -> Self {
        self.allowed_kinds = kinds.into_iter().collect();
        self
    }

    pub fn bind_identity(mut self, identity: ProviderIdentity) -> Self {
        self.bound_identity = Some(identity);
        self.require_identity_match = true;
        self
    }

    pub fn allow_refresh(mut self, allow_refresh: bool) -> Self {
        self.allow_refresh = allow_refresh;
        self
    }
}

impl CredentialAccessRequest {
    pub fn use_credential(provider_id: impl Into<String>, kind: ProviderCredentialKind) -> Self {
        Self {
            provider_id: provider_id.into(),
            kind,
            operation: CredentialOperation::Use,
            credential_available: true,
            token_expired: false,
            refresh_token_available: false,
            identity: None,
        }
    }
}

// Keep the public denial enum source-compatible for callers that construct and
// destructure `IdentityMismatch`; boxing its fields would break that API.
#[allow(clippy::result_large_err)]
pub fn evaluate_credential_access(
    policy: &ProviderAccessPolicy,
    request: CredentialAccessRequest,
) -> Result<CredentialAccessGrant, CredentialAccessDenial> {
    if !policy.allowed_providers.is_empty()
        && !policy.allowed_providers.contains(&request.provider_id)
    {
        return Err(CredentialAccessDenial::ProviderDenied {
            provider_id: request.provider_id,
        });
    }

    if !policy.allowed_kinds.is_empty() && !policy.allowed_kinds.contains(&request.kind) {
        return Err(CredentialAccessDenial::CredentialKindDenied { kind: request.kind });
    }

    if !request.credential_available {
        return Err(CredentialAccessDenial::MissingCredential {
            provider_id: request.provider_id,
        });
    }

    if policy.require_identity_match {
        let expected = policy
            .bound_identity
            .as_ref()
            .expect("identity match requires a bound identity");
        match request.identity.as_ref() {
            None => return Err(CredentialAccessDenial::IdentityRequired),
            Some(actual) if actual != expected => {
                return Err(CredentialAccessDenial::IdentityMismatch {
                    expected: expected.clone(),
                    actual: Some(actual.clone()),
                });
            }
            Some(_) => {}
        }
    }

    let refresh_required =
        request.kind == ProviderCredentialKind::OAuthAccessToken && request.token_expired;
    if refresh_required {
        if !policy.allow_refresh {
            return Err(CredentialAccessDenial::RefreshDenied);
        }
        if !request.refresh_token_available {
            return Err(CredentialAccessDenial::MissingRefreshToken);
        }
    }

    Ok(CredentialAccessGrant {
        provider_id: request.provider_id,
        kind: request.kind,
        operation: request.operation,
        identity: request.identity,
        refresh_required,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn openai_request(kind: ProviderCredentialKind) -> CredentialAccessRequest {
        CredentialAccessRequest::use_credential("openai", kind)
    }

    #[test]
    fn identity_mismatch_expected_remains_an_owned_provider_identity() {
        let denial = CredentialAccessDenial::IdentityMismatch {
            expected: ProviderIdentity::account("expected-account"),
            actual: Some(ProviderIdentity::account("actual-account")),
        };

        let (expected, actual) = match denial {
            CredentialAccessDenial::IdentityMismatch { expected, actual } => (expected, actual),
            _ => panic!("expected an identity mismatch denial"),
        };
        let expected: ProviderIdentity = expected;

        assert_eq!(expected.account_id, "expected-account");
        assert_eq!(
            actual.map(|identity| identity.account_id),
            Some("actual-account".to_string())
        );
    }

    #[test]
    fn missing_credential_is_reported() {
        let mut request = openai_request(ProviderCredentialKind::ApiKey);
        request.credential_available = false;

        let err = evaluate_credential_access(&ProviderAccessPolicy::allow_all(), request)
            .expect_err("missing credential should fail");

        assert_eq!(
            err,
            CredentialAccessDenial::MissingCredential {
                provider_id: "openai".to_string(),
            }
        );
    }

    #[test]
    fn provider_and_kind_allowlists_deny_access() {
        let policy = ProviderAccessPolicy::allow_all()
            .allow_providers(["anthropic"])
            .allow_kinds([ProviderCredentialKind::OAuthAccessToken]);

        let provider_err =
            evaluate_credential_access(&policy, openai_request(ProviderCredentialKind::ApiKey))
                .expect_err("provider should be denied");
        assert_eq!(
            provider_err,
            CredentialAccessDenial::ProviderDenied {
                provider_id: "openai".to_string(),
            }
        );

        let kind_err = evaluate_credential_access(
            &policy,
            CredentialAccessRequest::use_credential("anthropic", ProviderCredentialKind::ApiKey),
        )
        .expect_err("credential kind should be denied");
        assert_eq!(
            kind_err,
            CredentialAccessDenial::CredentialKindDenied {
                kind: ProviderCredentialKind::ApiKey,
            }
        );
    }

    #[test]
    fn expired_oauth_token_requires_refresh_when_allowed() {
        let mut request = openai_request(ProviderCredentialKind::OAuthAccessToken);
        request.token_expired = true;
        request.refresh_token_available = true;

        let grant = evaluate_credential_access(&ProviderAccessPolicy::allow_all(), request)
            .expect("refresh should be allowed");

        assert!(grant.refresh_required);
        assert_eq!(grant.operation, CredentialOperation::Use);
    }

    #[test]
    fn expired_oauth_token_respects_refresh_policy_and_refresh_token_presence() {
        let mut request = openai_request(ProviderCredentialKind::OAuthAccessToken);
        request.token_expired = true;
        request.refresh_token_available = true;

        let err = evaluate_credential_access(
            &ProviderAccessPolicy::allow_all().allow_refresh(false),
            request,
        )
        .expect_err("refresh disabled should fail");
        assert_eq!(err, CredentialAccessDenial::RefreshDenied);

        let mut request = openai_request(ProviderCredentialKind::OAuthAccessToken);
        request.token_expired = true;
        request.refresh_token_available = false;
        let err = evaluate_credential_access(&ProviderAccessPolicy::allow_all(), request)
            .expect_err("missing refresh token should fail");
        assert_eq!(err, CredentialAccessDenial::MissingRefreshToken);
    }

    #[test]
    fn bound_identity_must_match_request_identity() {
        let policy = ProviderAccessPolicy::allow_all().bind_identity(ProviderIdentity {
            account_id: "acct-1".to_string(),
            subject_id: Some("user-1".to_string()),
            task_id: Some("task-1".to_string()),
        });

        let missing = evaluate_credential_access(
            &policy,
            CredentialAccessRequest::use_credential(
                "openai",
                ProviderCredentialKind::AgentIdentityAssertion,
            ),
        )
        .expect_err("identity should be required");
        assert_eq!(missing, CredentialAccessDenial::IdentityRequired);

        let mut mismatch = CredentialAccessRequest::use_credential(
            "openai",
            ProviderCredentialKind::AgentIdentityAssertion,
        );
        mismatch.identity = Some(ProviderIdentity::account("acct-2"));
        let err = evaluate_credential_access(&policy, mismatch)
            .expect_err("mismatched identity should fail");
        assert!(matches!(
            err,
            CredentialAccessDenial::IdentityMismatch { .. }
        ));

        let mut matched = CredentialAccessRequest::use_credential(
            "openai",
            ProviderCredentialKind::AgentIdentityAssertion,
        );
        matched.identity = Some(ProviderIdentity {
            account_id: "acct-1".to_string(),
            subject_id: Some("user-1".to_string()),
            task_id: Some("task-1".to_string()),
        });
        let grant = evaluate_credential_access(&policy, matched).expect("identity matches");
        assert_eq!(
            grant.identity.as_ref().map(|id| id.account_id.as_str()),
            Some("acct-1")
        );
    }
}
