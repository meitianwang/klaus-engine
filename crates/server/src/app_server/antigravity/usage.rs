//! Fast, low-overhead quota and plan reads for the active Antigravity account.
//!
//! Usage queries touch only the single active head account (`entries[0]`) so
//! standby accounts in the pool remain silent. For the active account, tier
//! metadata (`loadCodeAssist`) is cached per credential generation, cold or
//! forced reads issue `retrieveUserQuotaSummary` and `loadCodeAssist`
//! concurrently, and the legacy per-model `retrieveUserQuota` request is
//! skipped whenever the grouped summary already supplies percentage pools.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use klaus_agent_core::types::abort::AbortSignal;
use serde_json::{json, Value};

use super::{auth, catalog, cloud};

const METADATA_TTL_MS: i64 = 30 * 60 * 1000;
const ACCOUNT_USAGE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Default)]
struct AccountMetadata {
    plan: Option<String>,
    available_credits: Option<Value>,
    fetched_at_ms: i64,
}

fn metadata_cache() -> &'static Mutex<HashMap<String, AccountMetadata>> {
    static CACHE: OnceLock<Mutex<HashMap<String, AccountMetadata>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub(super) fn clear_all() {
    metadata_cache()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clear();
}

pub(super) fn prune_generation(generation: &str) {
    metadata_cache()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(generation);
}

pub(super) fn retain_generations(valid: &[String]) {
    metadata_cache()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .retain(|gen, _| valid.contains(gen));
}

fn extract_plan(metadata: &Value) -> Option<String> {
    metadata
        .get("paidTier")
        .or_else(|| metadata.get("currentTier"))
        .and_then(|tier| tier.get("name").or_else(|| tier.get("id")))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|plan| !plan.is_empty())
        .map(str::to_string)
}

fn extract_available_credits(metadata: &Value) -> Option<Value> {
    metadata
        .pointer("/paidTier/availableCredits")
        .or_else(|| metadata.pointer("/currentTier/availableCredits"))
        .cloned()
}

/// Opportunistically seed tier metadata when `project()` or the head-account
/// control-plane heartbeat already called `loadCodeAssist`.
pub(super) fn cache_load_code_assist(generation: &str, payload: &Value) {
    if !auth::is_current(generation) {
        return;
    }
    let plan = extract_plan(payload);
    let available_credits = extract_available_credits(payload);
    if plan.is_none() && available_credits.is_none() {
        return;
    }
    metadata_cache()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(
            generation.to_string(),
            AccountMetadata {
                plan,
                available_credits,
                fetched_at_ms: now_ms(),
            },
        );
}

fn fresh_metadata(generation: &str, now: i64) -> Option<AccountMetadata> {
    let guard = metadata_cache().lock().unwrap_or_else(|p| p.into_inner());
    let entry = guard.get(generation)?;
    if now.saturating_sub(entry.fetched_at_ms) < METADATA_TTL_MS {
        Some(entry.clone())
    } else {
        None
    }
}

fn store_metadata(generation: &str, metadata: AccountMetadata) {
    metadata_cache()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(generation.to_string(), metadata);
}

/// True when `retrieveUserQuotaSummary` already supplies grouped percentage
/// pools (`remainingFraction`), making a separate `retrieveUserQuota` request
/// redundant because the host projection selects grouped pools exclusively.
fn summary_has_percentage_groups(summary: &Value) -> bool {
    let Some(groups) = summary.get("groups").and_then(Value::as_array) else {
        return false;
    };
    let mut found_group = false;
    for group in groups {
        let has_label = group
            .get("displayName")
            .and_then(Value::as_str)
            .is_some_and(|label| !label.trim().is_empty());
        if !has_label {
            continue;
        }
        let Some(buckets) = group.get("buckets").and_then(Value::as_array) else {
            continue;
        };
        let has_fraction = buckets.iter().any(|bucket| {
            bucket
                .get("remainingFraction")
                .and_then(|value| {
                    value
                        .as_f64()
                        .or_else(|| value.as_str()?.trim().parse::<f64>().ok())
                })
                .is_some_and(f64::is_finite)
        });
        if has_fraction {
            found_group = true;
        } else if !buckets.is_empty() {
            return false;
        }
    }
    found_group
}

fn is_terminal_credential_error(error: &str) -> bool {
    matches!(
        error,
        "antigravity_login_expired"
            | "antigravity_refresh_unavailable"
            | "antigravity_credential_changed"
            | "antigravity_not_signed_in"
            | "antigravity_authentication_required"
            | "antigravity_account_disabled"
            | "antigravity_account_restricted"
    )
}

async fn fetch_account_usage(entry: &auth::StoredAccount, force: bool) -> Result<Value, String> {
    let generation = entry.generation.as_str();
    let signal = AbortSignal::new();
    let project = super::project(Some(generation), &signal).await?;
    let cached_meta = if force {
        None
    } else {
        fresh_metadata(generation, now_ms())
    };
    let (summary, meta) = match cached_meta {
        Some(meta) => {
            let summary = cloud::rpc(
                "retrieveUserQuotaSummary",
                json!({"project": project}),
                Some(generation),
                &signal,
            )
            .await;
            (summary, meta)
        }
        None => {
            let summary_fut = cloud::rpc(
                "retrieveUserQuotaSummary",
                json!({"project": project}),
                Some(generation),
                &signal,
            );
            let meta_fut = cloud::rpc(
                "loadCodeAssist",
                json!({
                    "cloudaicompanionProject": project,
                    "metadata": { "ideType": "ANTIGRAVITY" }
                }),
                Some(generation),
                &signal,
            );
            let (summary, metadata_res) = tokio::join!(summary_fut, meta_fut);
            let metadata = metadata_res.unwrap_or(Value::Null);
            let parsed = AccountMetadata {
                plan: extract_plan(&metadata),
                available_credits: extract_available_credits(&metadata),
                fetched_at_ms: now_ms(),
            };
            if summary.is_ok() && auth::is_current(generation) {
                store_metadata(generation, parsed.clone());
            }
            (summary, parsed)
        }
    };

    let needs_quota = match &summary {
        Ok(value) => !summary_has_percentage_groups(value),
        Err(_) => true,
    };
    let quota = if needs_quota {
        match &summary {
            Err(err) if is_terminal_credential_error(err) => Err(err.clone()),
            _ => {
                cloud::rpc(
                    "retrieveUserQuota",
                    json!({"project": project}),
                    Some(generation),
                    &signal,
                )
                .await
            }
        }
    } else {
        Ok(Value::Null)
    };

    if quota.is_err() && summary.is_err() {
        return Err(quota
            .err()
            .or_else(|| summary.err())
            .unwrap_or_else(|| "antigravity_usage_unavailable".into()));
    }
    if !auth::is_current(generation) {
        return Err("antigravity_credential_changed".into());
    }

    let mut summary = summary.unwrap_or(Value::Null);
    if summary.is_null() {
        summary = json!({});
    }
    let live_credits = super::credit_cache()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(generation)
        .cloned();
    let credits = live_credits
        .as_ref()
        .and_then(|value| value.get("remainingCredits"))
        .or(meta.available_credits.as_ref());
    if let Some(credits) = credits {
        summary["availableCredits"] = credits.clone();
    }
    if !auth::is_current(generation) {
        return Err("antigravity_credential_changed".into());
    }

    let mut usage = catalog::normalize_usage(
        &quota.unwrap_or(Value::Null),
        &summary,
        entry.credential.account.as_deref(),
        meta.plan.as_deref(),
    );
    if let Some(consumed) = live_credits
        .as_ref()
        .and_then(|value| value.get("consumedCredits"))
    {
        usage["lastRequestConsumedCredits"] = consumed.clone();
    }

    Ok(usage)
}

pub(super) async fn usage(force: bool) -> Result<Value, String> {
    let entries = auth::accounts();
    let Some(current) = entries.first() else {
        return Ok(json!({"signedIn": false, "accounts": []}));
    };
    if entries.len() == 1 {
        return tokio::time::timeout(ACCOUNT_USAGE_TIMEOUT, fetch_account_usage(current, force))
            .await
            .unwrap_or_else(|_| Err("antigravity_catalog_timeout".into()));
    }
    let label = current
        .credential
        .account
        .as_deref()
        .filter(|account| !account.is_empty())
        .unwrap_or("Antigravity")
        .to_string();
    let outcome = tokio::time::timeout(ACCOUNT_USAGE_TIMEOUT, fetch_account_usage(current, force))
        .await
        .unwrap_or_else(|_| Err("antigravity_catalog_timeout".into()));
    let row = match outcome {
        Ok(mut row) => {
            if let Some(obj) = row.as_object_mut() {
                obj.insert("id".into(), json!(current.id));
                obj.insert("label".into(), json!(label));
                obj.insert("isCurrent".into(), json!(true));
                obj.insert("state".into(), json!("ok"));
            }
            row
        }
        Err(error) => json!({
            "id": current.id,
            "label": label,
            "isCurrent": true,
            "state": "error",
            "error": error,
        }),
    };
    let mut top = row.clone();
    if let Some(obj) = top.as_object_mut() {
        obj.insert("signedIn".into(), json!(true));
        obj.insert("accounts".into(), json!([row]));
    }
    Ok(top)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use klaus_agent_core::runtime_process::{
        remove_process_environment_var, set_process_environment_var,
    };
    use serde_json::{json, Value};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    struct UsageMockServer {
        endpoint: String,
        calls: Arc<Mutex<Vec<(String, String)>>>,
        include_summary_fraction: Arc<Mutex<bool>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for UsageMockServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn spawn_usage_server(delay_ms: u64) -> UsageMockServer {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let calls = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
        let include_summary_fraction = Arc::new(Mutex::new(true));
        let calls_ref = Arc::clone(&calls);
        let fraction_ref = Arc::clone(&include_summary_fraction);
        let task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let calls_ref = Arc::clone(&calls_ref);
                let fraction_ref = Arc::clone(&fraction_ref);
                tokio::spawn(async move {
                    let mut buf = Vec::with_capacity(4096);
                    let mut chunk = [0u8; 2048];
                    let header_end = loop {
                        let Ok(n) = socket.read(&mut chunk).await else {
                            return;
                        };
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break pos + 4;
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
                    let path = head
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or_default()
                        .to_string();
                    let token = head
                        .lines()
                        .find_map(|line| {
                            let (k, v) = line.split_once(':')?;
                            if k.eq_ignore_ascii_case("authorization") {
                                Some(
                                    v.trim()
                                        .strip_prefix("Bearer ")
                                        .unwrap_or(v.trim())
                                        .to_string(),
                                )
                            } else {
                                None
                            }
                        })
                        .unwrap_or_default();
                    let content_length = head
                        .lines()
                        .find_map(|line| {
                            let (k, v) = line.split_once(':')?;
                            k.eq_ignore_ascii_case("content-length")
                                .then(|| v.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap_or(0);
                    while buf.len() < header_end + content_length {
                        let Ok(n) = socket.read(&mut chunk).await else {
                            break;
                        };
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                    }
                    calls_ref
                        .lock()
                        .unwrap()
                        .push((token.clone(), path.clone()));
                    if delay_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    }
                    let reset_iso = (chrono::Utc::now() + chrono::Duration::hours(3)).to_rfc3339();
                    let with_fraction = *fraction_ref.lock().unwrap();
                    let payload: Value = if path.ends_with(":loadCodeAssist") {
                        json!({
                            "cloudaicompanionProject": format!("proj-{token}"),
                            "paidTier": {
                                "name": "Google AI Ultra",
                                "availableCredits": [{
                                    "creditType": "GOOGLE_ONE_AI",
                                    "creditAmount": "25000",
                                }]
                            }
                        })
                    } else if path.ends_with(":retrieveUserQuotaSummary") {
                        let bucket = if with_fraction {
                            json!({
                                "remainingFraction": 0.75,
                                "resetTime": reset_iso,
                                "windowSeconds": 18000,
                            })
                        } else {
                            json!({
                                "resetTime": reset_iso,
                                "windowSeconds": 18000,
                            })
                        };
                        json!({
                            "groups": [{
                                "displayName": "Gemini Models",
                                "buckets": [bucket]
                            }]
                        })
                    } else if path.ends_with(":retrieveUserQuota") {
                        json!({
                            "buckets": [{
                                "modelId": "gemini-2.5-pro",
                                "remainingFraction": 0.5,
                                "resetTime": reset_iso,
                            }]
                        })
                    } else {
                        Value::Null
                    };
                    let body = serde_json::to_vec(&payload).unwrap();
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.write_all(&body).await;
                });
            }
        });
        UsageMockServer {
            endpoint,
            calls,
            include_summary_fraction,
            task,
        }
    }

    fn adopt_credential(account: &str, token: &str) -> auth::Credential {
        auth::Credential {
            access_token: token.to_string(),
            refresh_token: Some(format!("refresh-{token}")),
            expires_at: Some(chrono::Utc::now().timestamp_millis() + 3_600_000),
            account: Some(account.to_string()),
            project: Some("proj-fast".to_string()),
            auth_method: "consumer".to_string(),
            client_id: None,
            client_secret: None,
        }
    }

    #[tokio::test]
    async fn summary_first_skips_legacy_quota_and_caches_tier_metadata() {
        let server = spawn_usage_server(0).await;
        let root = tempfile::tempdir().unwrap();
        super::super::set_config_root(root.path());
        set_process_environment_var("KLAUS_ANTIGRAVITY_API_URL", &server.endpoint);

        auth::adopt(adopt_credential("solo@example.invalid", "tok-solo"))
            .await
            .unwrap();

        let first = usage(false).await.unwrap();
        assert_eq!(first["plan"], "Google AI Ultra");
        assert_eq!(first["credits"]["remaining"], 25000.0);
        assert!(first["windows"]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w["groupLabel"] == "Gemini Models" && w["usedPercent"] == 25.0));

        {
            let calls = server.calls.lock().unwrap();
            let summary_n = calls
                .iter()
                .filter(|(_, p)| p.ends_with(":retrieveUserQuotaSummary"))
                .count();
            let assist_n = calls
                .iter()
                .filter(|(_, p)| p.ends_with(":loadCodeAssist"))
                .count();
            let quota_n = calls
                .iter()
                .filter(|(_, p)| p.ends_with(":retrieveUserQuota"))
                .count();
            assert_eq!(summary_n, 1);
            assert_eq!(assist_n, 1);
            assert_eq!(
                quota_n, 0,
                "legacy retrieveUserQuota must be skipped when summary groups carry percentages"
            );
        }

        // Second non-forced read reuses cached loadCodeAssist metadata -> single RPC!
        let second = usage(false).await.unwrap();
        assert_eq!(second["plan"], "Google AI Ultra");
        {
            let calls = server.calls.lock().unwrap();
            let summary_n = calls
                .iter()
                .filter(|(_, p)| p.ends_with(":retrieveUserQuotaSummary"))
                .count();
            let assist_n = calls
                .iter()
                .filter(|(_, p)| p.ends_with(":loadCodeAssist"))
                .count();
            let quota_n = calls
                .iter()
                .filter(|(_, p)| p.ends_with(":retrieveUserQuota"))
                .count();
            assert_eq!(summary_n, 2);
            assert_eq!(
                assist_n, 1,
                "warm read must reuse cached loadCodeAssist metadata"
            );
            assert_eq!(quota_n, 0);
        }

        // Explicit force=true re-queries loadCodeAssist alongside summary while still skipping legacy quota.
        let forced = usage(true).await.unwrap();
        assert_eq!(forced["plan"], "Google AI Ultra");
        {
            let calls = server.calls.lock().unwrap();
            let assist_n = calls
                .iter()
                .filter(|(_, p)| p.ends_with(":loadCodeAssist"))
                .count();
            let quota_n = calls
                .iter()
                .filter(|(_, p)| p.ends_with(":retrieveUserQuota"))
                .count();
            assert_eq!(assist_n, 2);
            assert_eq!(quota_n, 0);
        }

        // When summary omits remainingFraction, usage falls back cleanly to retrieveUserQuota.
        *server.include_summary_fraction.lock().unwrap() = false;
        let fallback = usage(false).await.unwrap();
        assert!(fallback["windows"]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w["usedPercent"] == 50.0));
        {
            let calls = server.calls.lock().unwrap();
            let quota_n = calls
                .iter()
                .filter(|(_, p)| p.ends_with(":retrieveUserQuota"))
                .count();
            assert_eq!(quota_n, 1);
        }

        auth::logout().await.unwrap();
        remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
    }

    #[tokio::test]
    async fn multi_account_pool_queries_only_active_head_and_reuses_tier_cache() {
        let server = spawn_usage_server(40).await;
        let root = tempfile::tempdir().unwrap();
        super::super::set_config_root(root.path());
        set_process_environment_var("KLAUS_ANTIGRAVITY_API_URL", &server.endpoint);

        auth::adopt(adopt_credential("head@example.invalid", "tok-head"))
            .await
            .unwrap();
        auth::adopt(adopt_credential("standby-1@example.invalid", "tok-sb1"))
            .await
            .unwrap();
        auth::adopt(adopt_credential("standby-2@example.invalid", "tok-sb2"))
            .await
            .unwrap();
        let standby_id = auth::status()["accounts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["email"] == "standby-1@example.invalid")
            .and_then(|row| row["id"].as_str())
            .unwrap()
            .to_string();

        server.calls.lock().unwrap().clear();
        let snap = usage(false).await.unwrap();
        let rows = snap["accounts"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["label"], "head@example.invalid");
        assert_eq!(rows[0]["isCurrent"], true);
        assert_eq!(rows[0]["state"], "ok");
        {
            let calls = server.calls.lock().unwrap();
            assert!(
                calls.iter().all(|(tok, _)| tok == "tok-head"),
                "standby accounts must never be polled during usage reads: {calls:?}"
            );
            assert_eq!(
                calls.len(),
                2,
                "cold head read runs summary + loadCodeAssist in parallel"
            );
        }

        // Warm read on the same head account sends only 1 RPC (`retrieveUserQuotaSummary`).
        server.calls.lock().unwrap().clear();
        let warm = usage(false).await.unwrap();
        assert_eq!(warm["accounts"][0]["label"], "head@example.invalid");
        {
            let calls = server.calls.lock().unwrap();
            assert_eq!(calls.len(), 1);
            assert!(calls[0].1.ends_with(":retrieveUserQuotaSummary"));
        }

        // Switching the active account switches usage queries to the newly promoted head only.
        auth::switch_account(&standby_id).await.unwrap();
        server.calls.lock().unwrap().clear();
        let switched = usage(false).await.unwrap();
        assert_eq!(
            switched["accounts"][0]["label"],
            "standby-1@example.invalid"
        );
        {
            let calls = server.calls.lock().unwrap();
            assert!(calls.iter().all(|(tok, _)| tok == "tok-sb1"));
        }

        auth::logout().await.unwrap();
        remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
    }
}
