//! Optional account labels never determine whether an authorization is usable.

use super::{auth, config_root, credential, state};
use serde_json::json;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

const PROFILE_TIMEOUT: Duration = Duration::from_secs(2);
const RETRY_COOLDOWN: Duration = Duration::from_secs(30);

pub(super) async fn hydrate_missing_account() {
    let _ = super::diagnostics::result("profile", async {
        super::diagnostics::event("profile_budget", json!({"timeoutMs":2000}));
        tokio::time::timeout(PROFILE_TIMEOUT, hydrate())
            .await
            .map_err(|_| {
                super::diagnostics::event("deadline", json!({"timeoutMs":2000}));
                "cursor_network_error".to_string()
            })?;
        Ok(())
    })
    .await;
}

async fn hydrate() {
    static LOOKUP: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    super::diagnostics::event("profile_lock_wait", json!({}));
    let _lookup = LOOKUP
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    super::diagnostics::event("profile_lock_acquired", json!({}));
    let (mut stored, generation) = {
        let mut state = state().lock().unwrap_or_else(|p| p.into_inner());
        let Ok(stored) = credential::read(&config_root(&state)) else {
            return;
        };
        if state.rejected
            || stored.user_info.valid().is_err()
            || stored
                .user_info
                .account
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
            || state
                .account_retry_at
                .is_some_and(|retry| retry > Instant::now())
        {
            super::diagnostics::event(
                "profile_skipped",
                json!({"rejected":state.rejected,"hasAccount":stored.user_info.account.is_some(),"cooldown":state.account_retry_at.is_some_and(|retry| retry > Instant::now())}),
            );
            return;
        }
        state.account_retry_at = Some(Instant::now() + RETRY_COOLDOWN);
        (stored, state.generation)
    };
    let lookup = async {
        super::diagnostics::event(
            "request_send",
            json!({"endpoint":"profile","timeoutMs":2000}),
        );
        let response = credential::authenticated(
            credential::client().ok()?.post(format!(
                "{}/aiserver.v1.DashboardService/GetMe",
                stored.user_info.api_origin().ok()?
            )),
            &stored.user_info,
        )
        .header("content-type", "application/json")
        .json(&json!({}))
        .timeout(PROFILE_TIMEOUT)
        .send()
        .await
        .map_err(|error| super::diagnostics::network_error(&error))
        .ok()?;
        let (status, profile) = auth::bounded(response).await.ok()?;
        (status == 200).then_some(())?;
        profile["email"]
            .as_str()
            .map(str::trim)
            .filter(|value| {
                !value.is_empty() && value.len() <= 1024 && !value.chars().any(char::is_control)
            })
            .map(str::to_string)
    };
    let Some(account) = lookup.await else {
        super::diagnostics::event("profile_unavailable", json!({}));
        return;
    };
    let state = state().lock().unwrap_or_else(|p| p.into_inner());
    let root = config_root(&state);
    if state.generation != generation
        || state.rejected
        || credential::read(&root)
            .ok()
            .is_none_or(|current| current.credential_id != stored.credential_id)
    {
        return;
    }
    stored.user_info.account = Some(account);
    let saved = credential::write(&root, &stored).is_ok();
    super::diagnostics::event(
        "profile_commit",
        json!({"saved":saved,"generation":generation}),
    );
}
