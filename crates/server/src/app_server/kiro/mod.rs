//! Kiro subscriptions use the normal inference loop with source-local wire adaptation.
pub mod auth;
mod bridge;
mod catalog;
mod errors;
pub mod login;
mod provider;
use futures::StreamExt;
use serde_json::{json, Map, Value};
use std::{
    path::{Path, PathBuf},
    sync::{OnceLock, RwLock},
    time::Duration,
};
static CONFIG_ROOT: RwLock<Option<PathBuf>> = RwLock::new(None);
pub fn set_config_root(root: &Path) {
    let _gate = login::ownership_gate();
    let mut current = CONFIG_ROOT.write().unwrap_or_else(|p| p.into_inner());
    if current.as_deref() != Some(root) {
        *current = Some(root.to_path_buf());
        drop(current);
        login::invalidate_pending_locked();
        forget_catalog();
    }
}
pub(super) fn config_root() -> PathBuf {
    CONFIG_ROOT
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
        .unwrap_or_else(|| {
            std::env::var_os("KLAUS_CONFIG_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| {
                    PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(".klaus")
                })
        })
}
fn endpoint(name: &str, fallback: &str) -> String {
    if std::env::var("KLAUS_TEST").as_deref() != Ok("1") {
        return fallback.trim_end_matches('/').into();
    }
    std::env::var(name)
        .ok()
        .filter(|value| {
            url::Url::parse(value).is_ok_and(|u| {
                u.scheme() == "http"
                    && matches!(
                        u.host_str(),
                        Some("127.0.0.1" | "localhost" | "[::1]" | "::1")
                    )
                    && u.username().is_empty()
                    && u.password().is_none()
                    && u.query().is_none()
                    && u.fragment().is_none()
            })
        })
        .unwrap_or_else(|| fallback.into())
        .trim_end_matches('/')
        .into()
}
pub(super) fn auth_base() -> String {
    endpoint(
        "KLAUS_KIRO_AUTH_URL",
        "https://prod.us-east-1.auth.desktop.kiro.dev",
    )
}
fn region(c: &auth::Credential) -> String {
    c.profile_arn
        .as_deref()
        .and_then(|v| v.split(':').nth(3))
        .or(c.region.as_deref())
        .filter(|v| {
            !v.is_empty()
                && v.len() <= 32
                && v.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        })
        .unwrap_or("us-east-1")
        .into()
}
pub(super) fn management_base(c: &auth::Credential) -> String {
    endpoint(
        "KLAUS_KIRO_MANAGEMENT_URL",
        &format!("https://management.{}.kiro.dev", region(c)),
    )
}
pub(super) fn runtime_base(c: &auth::Credential) -> String {
    endpoint(
        "KLAUS_KIRO_RUNTIME_URL",
        &format!("https://runtime.{}.kiro.dev", region(c)),
    )
}
pub(super) fn client() -> reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(15))
                .timeout(Duration::from_secs(90))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap_or_default()
        })
        .clone()
}
pub(super) fn headers(c: &auth::Credential) -> Result<reqwest::header::HeaderMap, String> {
    use reqwest::header::{HeaderMap, HeaderValue};
    let mut headers = HeaderMap::new();
    headers.insert(
        "authorization",
        HeaderValue::from_str(&format!("Bearer {}", c.access_token))
            .map_err(|_| "kiro_credential_invalid")?,
    );
    headers.insert("content-type", HeaderValue::from_static("application/json"));
    headers.insert("x-amzn-kiro-origin", HeaderValue::from_static("AI_EDITOR"));
    headers.insert(
        "x-amzn-codewhisperer-optout",
        HeaderValue::from_static("true"),
    );
    if let Some(profile) = &c.profile_arn {
        headers.insert(
            "x-amzn-kiro-profile",
            HeaderValue::from_str(profile).map_err(|_| "kiro_credential_invalid")?,
        );
    }
    let token_type = match c.auth_method.as_str() {
        "IdC" => Some("SSO_OIDC"),
        "external_idp" => Some("EXTERNAL_IDP"),
        "machine_token" => Some("KIRO_MACHINE_TOKEN"),
        "api_key" => Some("API_KEY"),
        "social" => None,
        _ => return Err("kiro_auth_method_unsupported".into()),
    };
    if let Some(t) = token_type {
        headers.insert("tokentype", HeaderValue::from_static(t));
    }
    Ok(headers)
}
pub(super) async fn bounded_json(response: reqwest::Response) -> Result<Value, String> {
    let status = response.status();
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| "kiro_response_transport_failed")?;
        if bytes.len().saturating_add(chunk.len()) > 4 * 1024 * 1024 {
            return Err("kiro_response_too_large".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    let value: Value = serde_json::from_slice(&bytes)
        .map_err(|_| format!("kiro_response_invalid (HTTP {})", status.as_u16()))?;
    if !status.is_success() {
        let code = value["__type"]
            .as_str()
            .or(value["code"].as_str())
            .unwrap_or("request_rejected");
        let safe: String = code
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '#'))
            .take(128)
            .collect();
        return Err(format!("kiro_{safe} (HTTP {})", status.as_u16()));
    }
    Ok(value)
}
pub(super) fn forget_catalog() {
    catalog::forget();
    auth::notify_changed();
}
pub(crate) fn decorate(
    inner: std::sync::Arc<dyn klaus_agent_core::types::provider::ModelProvider>,
) -> std::sync::Arc<dyn klaus_agent_core::types::provider::ModelProvider> {
    provider::decorate(inner)
}
pub async fn models(warm: bool) -> Result<Value, String> {
    Ok(super::conversation_catalog::response(
        "kiro",
        catalog::rows(warm).await?,
    ))
}
pub async fn main_model(selection: &str, tiers: &Map<String, Value>) -> Result<String, String> {
    let selection = selection.trim();
    if let Some(explicit) = selection.strip_prefix("kiro:") {
        if catalog::is_routing_alias(explicit) {
            return Err("kiro_model_unavailable".into());
        }
        return Ok(explicit.into());
    }
    let selected = match selection {
        "" | "auto" => None,
        "high" | "medium" | "low" => tiers.get(selection).and_then(Value::as_str),
        other => Some(other),
    };
    if let Some(selected) = selected {
        if catalog::is_routing_alias(selected) {
            return Err("kiro_model_unavailable".into());
        }
        return Ok(selected.strip_prefix("kiro:").unwrap_or(selected).into());
    }
    let rows = catalog::rows(true).await?;
    let concrete: Vec<_> = rows
        .iter()
        .filter(|row| {
            !row["key"].as_str().is_some_and(catalog::is_routing_alias)
                && super::conversation_catalog::is_conversation_model("kiro", row)
        })
        .collect();
    // A service routing default is not a concrete model. Preserve native order
    // when selecting an initial concrete model; explicit selections never fall back.
    concrete
        .iter()
        .find(|row| row["isDefault"] == true)
        .or_else(|| concrete.iter().find(|row| row["available"] != false))
        .and_then(|row| row["key"].as_str())
        .map(str::to_owned)
        .ok_or_else(|| "kiro_default_model_unavailable".into())
}
pub async fn resolve(
    selection: &str,
    tiers: &Map<String, Value>,
) -> Result<super::model_sources::Resolved, String> {
    let generation = auth::generation().ok_or("kiro_not_signed_in")?;
    auth::credential(Some(&generation)).await?;
    let model = main_model(selection, tiers).await?;
    let rows = catalog::expanded_rows(&catalog::rows(true).await?);
    let selected = rows
        .iter()
        .find(|v| v["key"].as_str() == Some(&model))
        .ok_or("kiro_model_unavailable")?;
    if !super::conversation_catalog::is_conversation_model("kiro", selected) {
        return Err("kiro_model_purpose_unsupported".into());
    }
    let (base, token) = bridge::ensure_started(&generation).await?;
    let mut model_tiers = Map::new();
    for tier in ["high", "medium", "low"] {
        if let Some(v) = tiers.get(tier).and_then(Value::as_str) {
            let v = v.strip_prefix("kiro:").unwrap_or(v);
            if rows.iter().any(|r| r["key"].as_str() == Some(v)) {
                model_tiers.insert(tier.into(), json!(v));
            }
        }
    }
    Ok(super::model_sources::Resolved {
        provider: json!({"type":"openai","api":"responses","model":model,"baseUrl":base,"compatibility":{"responsesFunctionTools":true}}),
        auth: json!({"type":"static_bearer","token":token}),
        capabilities: selected["capabilities"].clone(),
        capabilities_by_model: rows
            .iter()
            .filter(|r| r["key"].as_str() != Some(&model))
            .map(|r| json!({"model":r["key"],"capabilities":r["capabilities"]}))
            .collect(),
        model_tiers,
        pricing: None,
    })
}
pub async fn usage() -> Result<Value, String> {
    let Some(generation) = auth::generation() else {
        return Ok(json!({"signedIn":false}));
    };
    let c = auth::credential(Some(&generation)).await?;
    let mut request = client()
        .get(format!("{}/Get-Usage-Limits", management_base(&c)))
        .query(&[("origin", "AI_EDITOR"), ("isEmailRequired", "true")])
        .headers(headers(&c)?);
    if let Some(profile) = &c.profile_arn {
        request = request.query(&[("profileArn", profile)]);
    }
    let value = bounded_json(
        request
            .send()
            .await
            .map_err(|_| "kiro_usage_transport_failed")?,
    )
    .await?;
    if !auth::is_current(&generation) {
        return Err("kiro_credential_changed".into());
    }
    Ok(
        json!({"signedIn":true,"account":value["userInfo"]["email"],"subscriptionInfo":value["subscriptionInfo"],"nextDateReset":value["nextDateReset"],"usageBreakdownList":value["usageBreakdownList"],"overageConfiguration":value["overageConfiguration"]}),
    )
}
pub async fn capabilities() -> Result<Value, String> {
    let connected = auth::status()["signedIn"] == true;
    Ok(
        json!({"source":"kiro","connected":connected,"signedIn":connected,"search":{"available":connected,"models":[{"id":"","name":"Service default"}]},"image":{"available":false,"models":[]},"imageEdit":{"available":false,"models":[]},"video":{"available":false,"models":[]},"audio":{"available":false,"models":[]},"transcription":{"available":false,"models":[]},"music":{"available":false,"models":[]},"realtime":{"available":false,"models":[]}}),
    )
}
pub(super) fn valid_search_query(query: &str) -> bool {
    !query.is_empty() && query.chars().count() <= 200
}
pub async fn search(
    query: &str,
    signal: klaus_agent_core::types::abort::AbortSignal,
) -> Result<Value, String> {
    if !valid_search_query(query) {
        return Err("kiro_search_query_invalid".into());
    }
    let generation = auth::generation().ok_or("kiro_not_signed_in")?;
    let c = auth::credential(Some(&generation)).await?;
    let mut h = headers(&c)?;
    if let Some(profile) = &c.profile_arn {
        h.insert(
            "x-amzn-kiro-profile-arn",
            reqwest::header::HeaderValue::from_str(profile)
                .map_err(|_| "kiro_credential_invalid")?,
        );
    }
    let mut changes = auth::changes();
    let request=client().post(format!("{}/mcp",runtime_base(&c))).headers(h).json(&json!({"jsonrpc":"2.0","id":uuid::Uuid::new_v4().to_string(),"method":"tools/call","params":{"name":"web_search","arguments":{"query":query}}}));
    let value = tokio::select! {r=async{bounded_json(request.send().await.map_err(|_|"kiro_search_transport_failed")?).await}=>r?,_=signal.cancelled()=>return Err("kiro_search_cancelled".into()),_=changes.changed()=>return Err("kiro_credential_changed".into())};
    if value.get("error").is_some_and(|v| !v.is_null()) || value["result"]["isError"] == true {
        return Err("kiro_search_rejected".into());
    }
    if !auth::is_current(&generation) {
        return Err("kiro_credential_changed".into());
    }
    let mut results = Vec::new();
    for content in value["result"]["content"].as_array().into_iter().flatten() {
        let Some(text) = content["text"].as_str() else {
            continue;
        };
        let payload: Value =
            serde_json::from_str(text).map_err(|_| "kiro_search_response_invalid")?;
        let rows = payload["results"]
            .as_array()
            .ok_or("kiro_search_response_invalid")?;
        for row in rows {
            if row["title"].is_string()
                && row["url"].as_str().is_some_and(|v| {
                    url::Url::parse(v).is_ok_and(|u| matches!(u.scheme(), "http" | "https"))
                })
            {
                results.push(row.clone());
            }
        }
    }
    Ok(json!({"results":results}))
}
#[cfg(test)]
mod tests;
