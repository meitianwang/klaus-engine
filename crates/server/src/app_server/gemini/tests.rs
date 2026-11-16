//! Unit coverage for the source-owned surfaces: projection rules, the bundled
//! baseline contract, the retagging vocabulary and the usage pool merge.

use super::*;
use klaus_agent_core::provider::gemini::GeminiApiError;
use serde_json::json;

fn entry(display: &str) -> Value {
    json!({
        "displayName": display,
        "maxTokens": 1048576,
        "maxOutputTokens": 65535,
        "supportsImages": true,
        "supportsThinking": true,
        "thinkingLevel": 1,
        "apiProvider": "API_PROVIDER_GOOGLE_GEMINI",
        "modelProvider": "MODEL_PROVIDER_GOOGLE",
        "quotaInfo": {"remainingFraction": 0.5, "resetTime": "2026-11-21T03:13:12Z"},
    })
}

#[test]
fn public_chat_id_pattern_accepts_versioned_lineup_only() {
    for id in [
        "gemini-2.5-pro",
        "gemini-2.5-flash",
        "gemini-2.5-flash-lite",
        "gemini-2.5-flash-thinking",
        "gemini-3-flash",
        "gemini-3.1-flash-lite",
        "gemini-3.5-flash-lite",
        "gemini-4-flash",
        "gemini-10.2-pro-thinking",
    ] {
        assert!(catalog::is_public_chat_id(id), "{id} should be public");
    }
    for id in [
        "gemini-4-argon-high",
        "gemini-3.8-flash-tiered",
        "gemini-3.1-flash-image",
        "gemini-pro-orchestrator",
        "gemini-flash-worker-agy",
        "gemini-2.5-flash-extra-low",
        "claude-sonnet-5-5-high",
        "chat_20706",
        "tab_flash_lite_preview",
        "gemini-",
        "gemini-x-flash",
        "gemini-2.5.1.2-flash",
        "gemini-2.5-flash-lite-thinking-x",
    ] {
        assert!(!catalog::is_public_chat_id(id), "{id} should be excluded");
    }
}

#[test]
fn aliased_display_names_are_dropped() {
    // The backend retires stale ids by re-pointing them at a newer model.
    assert!(!catalog::display_matches_id(
        "gemini-2.5-flash",
        &entry("Gemini 3.5 Flash Lite")
    ));
    assert!(catalog::display_matches_id(
        "gemini-3-flash",
        &entry("Gemini 3 Flash")
    ));
    assert!(catalog::display_matches_id(
        "gemini-3.1-flash-lite",
        &entry("Gemini 3.1 Flash Lite")
    ));
    // No display name or no version in it: keep, the id pattern already gated.
    assert!(catalog::display_matches_id("gemini-3-flash", &json!({})));
    assert!(catalog::display_matches_id(
        "gemini-3-flash",
        &entry("Flash preview")
    ));
}

#[test]
fn projection_keeps_only_unaliased_public_chat_models() {
    let payload = json!({
        "defaultAgentModelId": "gemini-4-carbon-medium",
        "agentModelSorts": [{"displayName": "Recommended", "groups": [{"modelIds": ["gemini-4-carbon-medium"]}]}],
        "imageGenerationModelIds": ["gemini-3.1-flash-image"],
        "webSearchModelIds": ["gemini-3.5-flash-lite"],
        "audioTranscriptionModelIds": ["models/proactive-observer-v10"],
        "tabModelIds": ["chat_20706"],
        "models": {
            "gemini-3-flash": entry("Gemini 3 Flash"),
            "gemini-2.5-pro": entry("Gemini 2.5 Pro"),
            "gemini-2.5-flash": entry("Gemini 3.5 Flash Lite"),
            "gemini-3.5-flash-lite": entry("Gemini 3.5 Flash Lite"),
            "gemini-3.1-flash-image": entry("Gemini 3.1 Flash Image"),
            "gemini-4-argon-high": entry("Gemini 4 Argon (High)"),
            "chat_20706": {"isInternal": true},
            "gpt-oss-120b-medium": entry("GPT OSS 120B"),
        }
    });
    let projected = catalog::project_payload(&payload);
    let models = projected["models"].as_object().unwrap();
    let mut ids: Vec<&String> = models.keys().collect();
    ids.sort();
    assert_eq!(
        ids,
        vec!["gemini-2.5-pro", "gemini-3-flash", "gemini-3.5-flash-lite"]
    );
    assert_eq!(projected["defaultAgentModelId"], json!("gemini-3-flash"));
    // Task lists and sibling-source curation must not ride along.
    assert!(projected.get("agentModelSorts").is_none());
    assert!(projected.get("imageGenerationModelIds").is_none());
    assert!(projected.get("webSearchModelIds").is_none());
    assert!(projected.get("tabModelIds").is_none());
}

#[test]
fn projection_default_falls_back_when_lineup_lacks_it() {
    let payload = json!({
        "models": {
            "gemini-9-flash": entry("Gemini 9 Flash"),
        }
    });
    let projected = catalog::project_payload(&payload);
    assert_eq!(projected["defaultAgentModelId"], json!("gemini-9-flash"));
}

#[test]
fn bundled_baseline_is_sanitized_and_consistent() {
    let payload = catalog::bundled_payload();
    assert_eq!(payload["catalogBaseline"], json!(true));
    let models = payload["models"].as_object().expect("baseline models");
    assert!(!models.is_empty());
    for (id, entry) in models {
        assert!(catalog::is_public_chat_id(id), "{id} in baseline");
        assert!(catalog::display_matches_id(id, entry), "{id} aliased");
        assert!(entry.get("quotaInfo").is_none(), "{id} carries quota");
    }
    assert_eq!(
        payload["defaultAgentModelId"],
        json!(catalog::DEFAULT_MAIN_MODEL)
    );
    assert!(models.contains_key(catalog::DEFAULT_MAIN_MODEL));
}

#[test]
fn baseline_rows_stay_selectable_with_unknown_entitlement() {
    let payload = catalog::bundled_payload();
    let rows = catalog::models_rows(&payload);
    assert!(!rows.is_empty());
    for row in &rows {
        assert!(catalog::is_chat_model(row), "{row} must be chat");
        assert_eq!(row["available"], json!(false));
        assert_eq!(row["entitlementUnknown"], json!(true));
        assert_eq!(row["canDisableThinking"], json!(false));
    }
    // The default sorts first.
    assert_eq!(rows[0]["key"], json!(catalog::DEFAULT_MAIN_MODEL));
}

#[test]
fn retag_rewrites_only_the_source_prefix() {
    assert_eq!(
        cloud::retag("antigravity_rate_limited: slow down"),
        "gemini_rate_limited: slow down"
    );
    assert_eq!(
        cloud::retag("antigravity_transport_unavailable (connect)"),
        "gemini_transport_unavailable (connect)"
    );
    assert_eq!(
        cloud::retag("model_context_window_exceeded"),
        "model_context_window_exceeded"
    );
    assert_eq!(cloud::retag("gemini_not_signed_in"), "gemini_not_signed_in");
}

#[test]
fn classifier_feed_reads_the_gemini_vocabulary() {
    use klaus_agent_core::provider::gemini::GeminiClient;
    let client = cloud::RetaggingClient::default();
    let error = GeminiApiError {
        status: Some(429),
        message: "gemini_quota_exhausted [RESOURCE_EXHAUSTED]: weekly limit".into(),
        headers: None,
    };
    let raw = client.error_details(&error);
    assert_eq!(raw.code.as_deref(), Some("gemini_quota_exhausted"));
    assert_eq!(raw.error_type.as_deref(), Some("RESOURCE_EXHAUSTED"));
    let error = GeminiApiError {
        status: None,
        message: "gemini_stream_incomplete".into(),
        headers: None,
    };
    let raw = client.error_details(&error);
    assert!(raw
        .message
        .contains("network stream ended before its terminal event"));
}

#[test]
fn classify_maps_gemini_tags_like_the_shared_transport() {
    use klaus_agent_core::provider::classify::{classify_error, RawProviderError};
    use klaus_agent_core::types::errors::ProviderErrorKind;
    for (code, expected) in [
        ("gemini_quota_exhausted", ProviderErrorKind::Quota),
        ("gemini_rate_limited", ProviderErrorKind::RateLimit),
        ("gemini_service_unavailable", ProviderErrorKind::Overloaded),
        ("gemini_auth_required", ProviderErrorKind::Auth),
        ("gemini_not_signed_in", ProviderErrorKind::Auth),
        ("gemini_login_expired", ProviderErrorKind::Auth),
        ("gemini_refresh_unavailable", ProviderErrorKind::Timeout),
        (
            "gemini_model_not_offered",
            ProviderErrorKind::InvalidRequest,
        ),
        (
            "gemini_region_unsupported",
            ProviderErrorKind::InvalidRequest,
        ),
        ("gemini_account_disabled", ProviderErrorKind::InvalidRequest),
        (
            "gemini_egress_unavailable",
            ProviderErrorKind::InvalidRequest,
        ),
    ] {
        let classified = classify_error(&RawProviderError {
            code: Some(code.to_string()),
            message: code.to_string(),
            ..Default::default()
        });
        assert_eq!(classified.kind, expected, "{code}");
    }
}

#[test]
fn usage_merge_keeps_one_worst_remaining_pool_row() {
    let payload = json!({
        "signedIn": true,
        "account": "user@example.com",
        "windows": [
            {"id": "quota:a", "label": "gemini-3-flash", "modelId": "gemini-3-flash", "remainingFraction": 0.4, "resetsAt": 2000, "tokenType": "WTUS"},
            {"id": "quota:b", "label": "gemini-2.5-pro", "modelId": "gemini-2.5-pro", "remainingFraction": 0.2, "resetsAt": 1000, "tokenType": "WTUS"},
            {"id": "quota:c", "label": "claude-opus-5-5-high", "modelId": "claude-opus-5-5-high", "remainingFraction": 1.0, "resetsAt": 3000},
            {"id": "quota:d", "label": "gemini-4-argon-high", "modelId": "gemini-4-argon-high", "remainingFraction": 0.9, "resetsAt": 1000},
        ]
    });
    let filtered = filter_usage(payload);
    let windows = filtered["windows"].as_array().unwrap();
    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0]["id"], json!("gemini:shared-pool"));
    assert_eq!(windows[0]["label"], json!("Gemini"));
    assert_eq!(windows[0]["remainingFraction"], json!(0.2));
    assert_eq!(windows[0]["usedPercent"], json!(80.0));
    assert_eq!(windows[0]["resetsAt"], json!(1000));
    assert!(windows[0].get("modelId").is_none());
    assert_eq!(filtered["account"], json!("user@example.com"));
}

#[test]
fn usage_merge_falls_back_to_gemini_labelled_group_windows() {
    let payload = json!({
        "signedIn": true,
        "windows": [
            {"id": "summary:g", "label": "Gemini weekly", "groupLabel": "Gemini", "remainingFraction": 0.75, "resetsAt": 5000},
            {"id": "summary:c", "label": "Claude weekly", "groupLabel": "Claude", "remainingFraction": 1.0, "resetsAt": 5000},
        ]
    });
    let filtered = filter_usage(payload);
    let windows = filtered["windows"].as_array().unwrap();
    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0]["id"], json!("gemini:shared-pool"));
    assert_eq!(windows[0]["remainingFraction"], json!(0.75));
}

#[test]
fn usage_accounts_rows_are_filtered_individually() {
    let payload = json!({
        "signedIn": true,
        "accounts": [
            {"id": "a1", "label": "one", "isCurrent": true, "state": "ok",
             "windows": [{"id": "q1", "modelId": "gemini-3-flash", "remainingFraction": 0.5, "resetsAt": 10}]},
            {"id": "a2", "label": "two", "isCurrent": false, "state": "ok",
             "windows": [{"id": "q2", "modelId": "claude-sonnet-5-5-low", "remainingFraction": 1.0, "resetsAt": 10}]},
        ],
        "windows": [{"id": "q1", "modelId": "gemini-3-flash", "remainingFraction": 0.5, "resetsAt": 10}]
    });
    let filtered = filter_usage(payload);
    let accounts = filtered["accounts"].as_array().unwrap();
    assert_eq!(accounts[0]["windows"].as_array().unwrap().len(), 1);
    assert!(accounts[1].get("windows").is_none());
    assert_eq!(filtered["windows"].as_array().unwrap().len(), 1);
}
