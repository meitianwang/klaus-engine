//! Subscription usage reads share the inference credential lineage and refresh lock.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use futures::{future::join_all, StreamExt};
use klaus_agent_core::credential_store::{CredentialStore, NamespacedCredentialStore};
use klaus_agent_core::provider::auth::AuthSource;
use klaus_agent_core::provider::oauth::{
    HttpFetch, HttpRequest, HttpResponse, OAuthAuth, OAuthAuthOptions, OAuthConfig, OAuthTokens,
};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{ProviderError, ProviderErrorKind};
use serde_json::{json, Value};

use super::model_sources;

const USAGE_URL: &str = "https://api.anthropic.com/api/oauth/usage";
const ACCOUNT_BUDGET: Duration = Duration::from_secs(12);
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub(super) struct UsageOptions {
    endpoint: String,
    config: OAuthConfig,
    refresh_lock_dir: Option<String>,
    account_budget: Duration,
}

impl Default for UsageOptions {
    fn default() -> Self {
        Self {
            endpoint: USAGE_URL.to_string(),
            config: model_sources::claude_oauth_config(),
            refresh_lock_dir: None,
            account_budget: ACCOUNT_BUDGET,
        }
    }
}

#[cfg(test)]
impl UsageOptions {
    pub(super) fn fixture(endpoint: String, token_url: String, lock_dir: String) -> Self {
        Self {
            endpoint,
            config: OAuthConfig {
                token_url,
                ..model_sources::claude_oauth_config()
            },
            refresh_lock_dir: Some(lock_dir),
            account_budget: ACCOUNT_BUDGET,
        }
    }

    pub(super) fn with_budget(mut self, budget: Duration) -> Self {
        self.account_budget = budget;
        self
    }
}

fn validate_endpoint(endpoint: &str) -> Result<(), String> {
    let url = url::Url::parse(endpoint).map_err(|_| "invalid Claude usage endpoint")?;
    let loopback = url.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || !(url.scheme() == "https" || (url.scheme() == "http" && loopback))
    {
        return Err("Claude usage endpoints require HTTPS or loopback HTTP".to_string());
    }
    Ok(())
}

struct UsageFetch(reqwest::Client);

impl UsageFetch {
    fn new() -> Result<Self, String> {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map(Self)
            .map_err(|_| "could not create the Claude usage client".to_string())
    }
}

fn transport_error() -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Network,
        message: "Claude usage transport failed".into(),
        status: None,
        code: None,
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
        retry_after_ms: None,
        request_id: None,
    }
}

#[async_trait::async_trait]
impl HttpFetch for UsageFetch {
    async fn fetch(&self, url: &str, request: HttpRequest) -> Result<HttpResponse, ProviderError> {
        let method = reqwest::Method::from_bytes(request.method.as_bytes())
            .map_err(|_| transport_error())?;
        let mut builder = self.0.request(method, url);
        for (name, value) in request.headers {
            builder = builder.header(name, value);
        }
        let response = builder
            .body(request.body)
            .send()
            .await
            .map_err(|_| transport_error())?;
        let status = response.status();
        if response
            .content_length()
            .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
        {
            return Err(transport_error());
        }
        let mut stream = response.bytes_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| transport_error())?;
            if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
                return Err(transport_error());
            }
            bytes.extend_from_slice(&chunk);
        }
        let body = String::from_utf8(bytes).map_err(|_| transport_error())?;
        Ok(HttpResponse {
            ok: status.is_success(),
            status: status.as_u16(),
            body,
        })
    }
}

pub(super) async fn usage(
    root: &Path,
    store: Option<Arc<dyn CredentialStore>>,
    options: &UsageOptions,
) -> Result<Value, String> {
    let accounts = model_sources::claude_accounts(root)?;
    if accounts.is_empty() {
        return Ok(json!({"signedIn": false, "accounts": []}));
    }
    validate_endpoint(&options.endpoint)?;
    validate_endpoint(&options.config.token_url)?;
    let fetch: Arc<dyn HttpFetch> = Arc::new(UsageFetch::new()?);
    let results = join_all(accounts.into_iter().enumerate().map(|(index, account)| {
        let store = store.clone();
        let fetch = fetch.clone();
        async move {
            let id = account["id"].as_str().unwrap_or("legacy");
            let label = ["email", "displayName"]
                .into_iter()
                .find_map(|key| account[key].as_str().filter(|label| !label.is_empty()))
                .or_else(|| {
                    (id == "legacy")
                        .then(|| account["label"].as_str())
                        .flatten()
                })
                .filter(|label| !label.is_empty())
                .unwrap_or("Claude");
            let mut result = json!({"id": id, "label": label, "isCurrent": index == 0});
            if let Some(plan) = account
                .get("plan")
                .or_else(|| account.pointer("/tokens/subscriptionType"))
                .and_then(Value::as_str)
            {
                result["plan"] = json!(plan);
            }
            let outcome = tokio::time::timeout(
                options.account_budget,
                account_usage(&account, store, options, fetch),
            )
            .await
            .unwrap_or_else(|_| Err("Usage request failed (timeout)".to_string()));
            match outcome {
                Ok(usage) => {
                    result["state"] = json!("ok");
                    result["windows"] = json!(usage.windows);
                    if let Some(extra_usage) = usage.extra_usage {
                        result["extraUsage"] = extra_usage;
                    }
                }
                Err(error) => {
                    result["state"] = json!("error");
                    result["error"] = json!(error);
                }
            }
            result
        }
    }))
    .await;
    Ok(json!({"signedIn": true, "accounts": results}))
}

async fn prepare_auth(
    account: &Value,
    store: Arc<dyn CredentialStore>,
    options: &UsageOptions,
    fetch: Arc<dyn HttpFetch>,
) -> Result<OAuthAuth, String> {
    let tokens = account["tokens"]
        .as_object()
        .ok_or("Claude account has no login seed")?;
    let credential_id = model_sources::account_credential_id("subscription", tokens);
    let namespace: Arc<dyn CredentialStore> = Arc::new(NamespacedCredentialStore::new(
        store,
        format!("provider/anthropic/{credential_id}"),
    ));
    // An absent namespace may be seeded concurrently by inference or another read.
    // The CAS winner is recaptured; no login seed can replace a stored refresh.
    for _ in 0..4 {
        let prepared = OAuthAuth::capture_stored(&namespace, &options.config)
            .await
            .map_err(|_| "could not read Claude account credentials")?;
        let needs_persist = prepared.is_none();
        let active_tokens = prepared.as_ref().map_or_else(
            || OAuthTokens {
                access_token: tokens["accessToken"].as_str().unwrap_or_default().into(),
                refresh_token: tokens
                    .get("refreshToken")
                    .and_then(Value::as_str)
                    .filter(|token| !token.is_empty())
                    .map(Into::into),
                expires_at: tokens.get("expiresAt").and_then(Value::as_i64),
                id_token: None,
            },
            |snapshot| snapshot.tokens(),
        );
        if active_tokens.refresh_token.is_none()
            && active_tokens
                .expires_at
                .is_some_and(|expires| expires <= chrono::Utc::now().timestamp_millis())
        {
            return Err("Claude login needs renewal; sign in again".to_string());
        }
        let auth = OAuthAuth::new(OAuthAuthOptions {
            config: options.config.clone(),
            store: Some(namespace.clone()),
            prepared_credential: prepared,
            tokens: Some(active_tokens),
            fetch: Some(fetch.clone()),
            refresh_lock_key: Some(format!("provider-anthropic-{credential_id}")),
            refresh_lock_dir: options.refresh_lock_dir.clone(),
            ..Default::default()
        });
        if !needs_persist {
            return Ok(auth);
        }
        match auth.save_if_absent().await {
            Ok(()) => return Ok(auth),
            Err(error) if error.message == "OAuth credential changed during preparation" => {}
            Err(_) => return Err("could not persist Claude account credentials".to_string()),
        }
    }
    Err("could not prepare Claude account credentials".to_string())
}

/// Reuse the inference namespace and refresh lock for a standalone account operation.
pub(super) async fn current_auth(
    root: &Path,
    store: Option<Arc<dyn CredentialStore>>,
    options: &UsageOptions,
) -> Result<OAuthAuth, String> {
    let accounts = model_sources::claude_accounts(root)?;
    let account = accounts.first().ok_or("Claude is not signed in")?;
    let store = store.ok_or("Claude requires the endpoint credential store")?;
    validate_endpoint(&options.config.token_url)?;
    prepare_auth(account, store, options, Arc::new(UsageFetch::new()?)).await
}

async fn account_usage(
    account: &Value,
    store: Option<Arc<dyn CredentialStore>>,
    options: &UsageOptions,
    fetch: Arc<dyn HttpFetch>,
) -> Result<AccountUsage, String> {
    let store = store.ok_or("Claude usage requires the endpoint credential store")?;
    let auth = prepare_auth(account, store, options, fetch.clone()).await?;
    for attempt in 0..2 {
        // A token exchange already in flight must settle its store write after a deadline.
        let header_auth = auth.clone();
        let headers =
            tokio::spawn(async move { header_auth.get_headers(&AbortSignal::new()).await })
                .await
                .map_err(|_| "Usage request failed")?
                .map_err(|_| "Claude login needs renewal; sign in again")?;
        let response = fetch
            .fetch(
                &options.endpoint,
                HttpRequest {
                    method: "GET".to_string(),
                    headers: headers
                        .into_iter()
                        .chain([
                            ("Accept".to_string(), "application/json".to_string()),
                            ("anthropic-beta".to_string(), "oauth-2025-04-20".to_string()),
                        ])
                        .collect(),
                    body: String::new(),
                },
            )
            .await
            .map_err(|_| "Usage request failed")?;
        if response.status == 401 && attempt == 0 {
            // Let the shared refresh settle even if the usage deadline expires.
            // Dropping its last polling caller would retain a pending refresh lock.
            if tokio::spawn(auth.force_refresh()).await.unwrap_or(false) {
                continue;
            }
            return Err("Claude login needs renewal; sign in again".to_string());
        }
        if !response.ok {
            // Upstream bodies may echo credentials; only the HTTP status crosses the facade.
            return Err(format!("Usage request failed (HTTP {})", response.status));
        }
        let value: Value =
            serde_json::from_str(&response.body).map_err(|_| "Invalid usage response")?;
        return Ok(AccountUsage {
            windows: project_windows(&value)?,
            extra_usage: project_extra_usage(&value),
        });
    }
    Err("Usage request failed (HTTP 401)".to_string())
}

struct AccountUsage {
    windows: Vec<Value>,
    extra_usage: Option<Value>,
}

/// Extra credits are separately billed and must not appear as included-plan windows.
fn project_extra_usage(value: &Value) -> Option<Value> {
    let extra = value.get("extra_usage")?.as_object()?;
    let mut projected = json!({"billing":"extraCredits"});
    if let Some(enabled) = extra.get("is_enabled").and_then(Value::as_bool) {
        projected["enabled"] = json!(enabled);
    }
    for (native, public) in [
        ("monthly_limit", "monthlyLimit"),
        ("used_credits", "usedCredits"),
    ] {
        if let Some(number) = extra
            .get(native)
            .and_then(Value::as_f64)
            .filter(|number| number.is_finite() && *number >= 0.0)
        {
            projected[public] = json!(number);
        }
    }
    Some(projected)
}

fn project_windows(value: &Value) -> Result<Vec<Value>, String> {
    let object = value.as_object().ok_or("Invalid usage response")?;
    let mut windows = Vec::new();
    for (id, window) in object {
        if id == "extra_usage" {
            continue;
        }
        let used = ["utilization", "used_percentage"]
            .into_iter()
            .find_map(|key| {
                let used = window.get(key)?;
                used.as_f64()
                    .or_else(|| used.as_str()?.parse().ok())
                    .filter(|used| used.is_finite())
            });
        let Some(used) = used else { continue };
        let mut result = json!({"id": id, "usedPercent": used.clamp(0.0, 100.0)});
        if let Some(reset) = ["resets_at", "reset_at"]
            .into_iter()
            .find_map(|key| chrono::DateTime::parse_from_rfc3339(window.get(key)?.as_str()?).ok())
        {
            result["resetsAt"] = json!(reset.timestamp_millis());
        }
        windows.push(result);
    }
    Ok(windows)
}

#[cfg(test)]
mod tests {
    use super::{project_extra_usage, project_windows, validate_endpoint};
    use serde_json::json;

    #[test]
    fn extra_credits_keep_billing_and_enablement_separate_from_included_windows() {
        let value = json!({
            "five_hour":{"utilization":25},
            "extra_usage":{"is_enabled":false,"monthly_limit":2000,"used_credits":125,"utilization":6.25,"access_token":"must-not-escape"}
        });
        assert_eq!(
            project_windows(&value).unwrap(),
            vec![json!({"id":"five_hour","usedPercent":25.0})]
        );
        assert_eq!(
            project_extra_usage(&value),
            Some(
                json!({"billing":"extraCredits","enabled":false,"monthlyLimit":2000.0,"usedCredits":125.0})
            )
        );
        assert_eq!(project_extra_usage(&json!({})), None);
        assert_eq!(
            project_extra_usage(&json!({"extra_usage":{"is_enabled":"true","used_credits":-1}})),
            Some(json!({"billing":"extraCredits"}))
        );
    }

    #[test]
    fn usage_windows_preserve_named_windows_and_tolerate_non_window_metadata() {
        let windows = project_windows(&json!({
            "future_window": {"used_percentage": "123.5", "reset_at": "2026-10-15T00:00:00Z"},
            "five_hour": {"utilization": "-5"},
            "seven_day": {"utilization": 10},
            "dual_window": {"utilization": null, "used_percentage": "15", "resets_at": null, "reset_at": "2026-10-15T00:00:00Z"},
            "other": {"extra": true},
            "invalid": {"utilization": "not a number"},
            "null_window": null,
        }))
        .unwrap();
        assert_eq!(
            windows,
            vec![
                json!({"id": "future_window", "usedPercent": 100.0, "resetsAt": 1792022400000_i64}),
                json!({"id": "five_hour", "usedPercent": 0.0}),
                json!({"id": "seven_day", "usedPercent": 10.0}),
                json!({"id": "dual_window", "usedPercent": 15.0, "resetsAt": 1792022400000_i64}),
            ]
        );
        assert_eq!(
            project_windows(&json!([])).unwrap_err(),
            "Invalid usage response"
        );
    }

    #[test]
    fn usage_endpoints_require_https_or_loopback_without_userinfo() {
        for endpoint in [
            "https://api.anthropic.com/api/oauth/usage",
            "http://127.0.0.1:8080/usage",
            "http://[::1]:8080/usage",
            "http://localhost:8080/usage",
        ] {
            assert!(validate_endpoint(endpoint).is_ok(), "{endpoint}");
        }
        for endpoint in [
            "http://example.com/usage",
            "http://127.0.0.1.example.com/usage",
            "https://user:secret@example.com/usage",
            "file:///tmp/usage",
            "https://example.com/usage#secret",
        ] {
            assert!(validate_endpoint(endpoint).is_err(), "{endpoint}");
        }
    }
}
