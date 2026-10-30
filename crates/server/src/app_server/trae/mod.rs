//! Trae subscription models run in Klaus's own inference and tool loop.

pub mod audio;
pub mod auth;
mod bridge;
mod catalog;
mod device;
mod errors;
pub mod login;
pub mod native;
mod provider;

use futures::StreamExt;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::{OnceLock, RwLock};
use std::time::Duration;

static CONFIG_ROOT: RwLock<Option<PathBuf>> = RwLock::new(None);

pub fn set_config_root(root: &Path) {
    let mut current = CONFIG_ROOT.write().unwrap_or_else(|p| p.into_inner());
    if current.as_deref() != Some(root) {
        *current = Some(root.to_path_buf());
        drop(current);
        login::invalidate_pending();
        forget_catalog();
    }
}

#[cfg(test)]
pub(crate) fn test_env_lock() -> &'static tokio::sync::Mutex<()> {
    tests::root_lock()
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

fn endpoint(variable: &str, fallback: &str) -> String {
    std::env::var(variable)
        .ok()
        .filter(|value| {
            url::Url::parse(value).is_ok_and(|url| {
                url.scheme() == "http"
                    && matches!(
                        url.host_str(),
                        Some("localhost" | "127.0.0.1" | "[::1]" | "::1")
                    )
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
                    && url.fragment().is_none()
            })
        })
        .unwrap_or_else(|| fallback.into())
        .trim_end_matches('/')
        .to_owned()
}
pub(super) fn account_base() -> String {
    endpoint("KLAUS_TRAE_ACCOUNT_URL", "https://api.trae.cn")
}
pub(super) fn cloud_base() -> String {
    endpoint("KLAUS_TRAE_CLOUD_URL", "https://trae-api-cn.mchost.guru")
}
pub(super) fn client() -> reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(15))
                .timeout(Duration::from_secs(90))
                .pool_max_idle_per_host(4)
                .pool_idle_timeout(Duration::from_secs(30))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap_or_default()
        })
        .clone()
}

pub(super) fn cloud_headers(
    credential: &auth::Credential,
) -> Result<reqwest::header::HeaderMap, String> {
    use reqwest::header::{HeaderMap, HeaderValue};
    let mut headers = HeaderMap::new();
    for (key, value) in [
        (
            "authorization",
            format!("Cloud-IDE-JWT {}", credential.access_token),
        ),
        ("content-type", "application/json".into()),
        ("x-app-id", "6eefa01c-1036-4c7e-9ca5-d891f63bfcd8".into()),
        ("x-ide-version-code", "20260924".into()),
        ("x-trae-client-type", "ide".into()),
        ("x-ide-version", credential.client_version.clone()),
        ("x-ide-version-type", "stable".into()),
        ("x-device-id", credential.device_id.clone()),
        ("x-machine-id", credential.machine_id.clone()),
    ] {
        headers.insert(
            key,
            HeaderValue::from_str(&value).map_err(|_| "trae_credential_invalid")?,
        );
    }
    Ok(headers)
}

pub(super) async fn bounded_json(response: reqwest::Response) -> Result<Value, String> {
    let status = response.status();
    if !status.is_success() {
        return Err(errors::from_response(response)
            .await
            .provider_error()
            .message
            .try_to_utf8()
            .unwrap_or_else(|_| "trae_request_rejected".into()));
    }
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| "trae_response_transport_failed")?;
        if bytes.len().saturating_add(chunk.len()) > 4 * 1024 * 1024 {
            return Err("trae_response_too_large".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    let value: Value = serde_json::from_slice(&bytes)
        .map_err(|_| format!("trae_response_invalid (HTTP {})", status.as_u16()))?;
    if value
        .pointer("/ResponseMetadata/Error")
        .is_some_and(|error| !error.is_null())
    {
        let code = value
            .pointer("/ResponseMetadata/Error/Code")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        return Err(format!("trae_account_refused ({})", safe_code(code)));
    }
    if value
        .get("code")
        .and_then(Value::as_i64)
        .filter(|code| *code != 0)
        .is_some()
    {
        return Err(errors::ServiceError::from_native(&value, None, None)
            .provider_error()
            .message
            .try_to_utf8()
            .unwrap_or_else(|_| "trae_request_rejected".into()));
    }
    Ok(value)
}
fn safe_code(code: &str) -> String {
    code.chars()
        .filter(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.')
        })
        .take(64)
        .collect()
}

pub(super) async fn http_json(
    base: &str,
    path: &str,
    method: reqwest::Method,
    body: Option<Value>,
    credential: Option<(&str, bool)>,
) -> Result<Value, String> {
    let mut request = client()
        .request(method, format!("{base}{path}"))
        .header("content-type", "application/json");
    if let Some((token, account)) = credential {
        request = if account {
            request.header("x-cloudide-token", token)
        } else {
            request.header("authorization", format!("Cloud-IDE-JWT {token}"))
        };
    }
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request
        .send()
        .await
        .map_err(|_| "trae_request_transport_failed")?;
    bounded_json(response).await
}

pub(super) fn forget_catalog() {
    catalog::forget();
    auth::notify_changed();
}

pub(super) fn decorate(
    inner: std::sync::Arc<dyn klaus_agent_core::types::provider::ModelProvider>,
) -> std::sync::Arc<dyn klaus_agent_core::types::provider::ModelProvider> {
    provider::decorate(inner)
}

pub async fn models(warm: bool) -> Result<Value, String> {
    let rows = catalog::rows(warm).await?;
    Ok(super::conversation_catalog::response("trae", rows))
}

pub async fn main_model(
    selection: &str,
    tiers: &serde_json::Map<String, Value>,
) -> Result<String, String> {
    let selected = match selection {
        "" | "auto" | "high" | "medium" | "low" => tiers
            .get(selection)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty()),
        other => Some(other),
    };
    if let Some(selected) = selected {
        return Ok(selected
            .strip_prefix("trae:")
            .unwrap_or(selected)
            .to_owned());
    }
    let rows = catalog::rows(true).await?;
    rows.iter()
        .filter(|row| super::conversation_catalog::is_conversation_model("trae", row))
        .find(|row| row["isDefault"] == true)
        .or_else(|| {
            rows.iter()
                .find(|row| super::conversation_catalog::is_conversation_model("trae", row))
        })
        .and_then(|row| row["key"].as_str())
        .map(str::to_owned)
        .ok_or_else(|| "trae_no_models".into())
}

pub async fn resolve(
    selection: &str,
    tiers: &serde_json::Map<String, Value>,
) -> Result<super::model_sources::Resolved, String> {
    let credential = auth::credential(None).await?;
    let generation = auth::generation().ok_or("trae_not_signed_in")?;
    let model = main_model(selection, tiers).await?;
    let rows = catalog::expanded_rows(&catalog::rows(true).await?);
    let selected = rows
        .iter()
        .find(|row| row["key"].as_str() == Some(&model))
        .ok_or("trae_model_unavailable")?;
    if !super::conversation_catalog::is_conversation_model("trae", selected) {
        return Err("trae_model_purpose_unsupported".into());
    }
    let (base, token) = bridge::ensure_started(&generation).await?;
    let mut model_tiers = serde_json::Map::new();
    for tier in ["high", "medium", "low"] {
        if let Some(id) = tiers.get(tier).and_then(Value::as_str) {
            let id = id.strip_prefix("trae:").unwrap_or(id);
            if rows.iter().any(|row| {
                row["key"].as_str() == Some(id)
                    && super::conversation_catalog::is_conversation_model("trae", row)
            }) {
                model_tiers.insert(tier.into(), json!(id));
            }
        }
    }
    if !auth::is_current(&generation) || credential.region != "cn" {
        return Err("trae_credential_changed".into());
    }
    Ok(super::model_sources::Resolved {
        provider: json!({"type":"openai","api":"chat","model":model,"baseUrl":base,
            "compatibility":{"instructionRole":"system"}}),
        auth: json!({"type":"static_bearer","token":token}),
        capabilities: selected["capabilities"].clone(),
        capabilities_by_model: rows
            .iter()
            .filter(|row| {
                row["key"].as_str() != Some(&model)
                    && super::conversation_catalog::is_conversation_model("trae", row)
            })
            .map(|row| json!({"model":row["key"],"capabilities":row["capabilities"]}))
            .collect(),
        model_tiers,
        pricing: None,
    })
}

pub async fn usage() -> Result<Value, String> {
    let Some(generation) = auth::generation() else {
        return Ok(json!({"signedIn":false}));
    };
    let credential = auth::credential(Some(&generation)).await?;
    let status = http_json(
        &account_base(),
        "/trae/api/v2/pay/ide_user_pay_status",
        reqwest::Method::POST,
        Some(json!({"trae_client":"IDE","device_id":credential.device_id})),
        Some((&credential.access_token, false)),
    )
    .await?;
    let usage = http_json(
        &account_base(),
        "/trae/api/v2/pay/ide_user_ent_usage",
        reqwest::Method::POST,
        Some(json!({"require_usage":true,"req_source":1})),
        Some((&credential.access_token, false)),
    )
    .await?;
    if !auth::is_current(&generation) {
        return Err("trae_credential_changed".into());
    }
    Ok(catalog::normalize_usage(
        &usage,
        &status,
        credential.account.as_deref(),
    ))
}

pub async fn capabilities() -> Result<Value, String> {
    let connected = auth::status()["signedIn"] == true;
    let service_default = json!({"id":"","name":"Service default","label":"Service default"});
    Ok(
        json!({"signedIn":connected,"connected":connected,"source":"trae",
        "image":{"available":connected,"models":[service_default],"referenceImages":true},
        "imageEdit":{"available":connected,"models":[service_default],"referenceImages":true},
        "video":{"available":connected,"models":[service_default],"referenceImages":true},
        "search":{"available":connected,"models":[service_default]},
        "transcription":{"available":connected,"models":[service_default],"streaming":true,"protocol":"cloud-bidi"},
        "audio":{"available":false,"models":[]},"music":{"available":false,"models":[]},
        "realtime":{"available":false,"models":[],"reason":"trae_realtime_protocol_unavailable"}}),
    )
}

#[cfg(test)]
mod tests;
