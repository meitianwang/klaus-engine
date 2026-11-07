//! Search backends that spend a model subscription's OWN search service.
//!
//! A named subscription owns its search requests independently of the chat
//! provider. An unavailable credential or rejected request is an explicit error;
//! the selected source never silently falls back to a different backend.

mod bailian;
mod codex;
mod cursor;
mod kimi;
mod kiro;
mod minimax;
mod qoder;
mod qwenwork;
mod subscription;
mod trae;
mod workbuddy;
mod zhipu;

use klaus_agent_core::schemas::config::{WebSearchAccess, WebSearchConfig};
use klaus_agent_core::tools::builtins::web_search::{WebSearchBackend, WebSearchBackendFactory};
use std::sync::Arc;

/// The config root the engine itself resolves (`KLAUS_CONFIG_DIR` or the
/// default home), which is where every subscription store lives. The desktop
/// spawn sets the variable; a bare CLI run falls back to the same home the
/// engine's config store uses.
pub(crate) fn config_root() -> std::path::PathBuf {
    match std::env::var("KLAUS_CONFIG_DIR") {
        Ok(value) if !value.trim().is_empty() => std::path::PathBuf::from(value.trim()),
        _ => std::path::PathBuf::from(std::env::var("HOME").unwrap_or_default()).join(".klaus"),
    }
}

/// The session-level backend picker: `None` for every other source (the
/// ordinary backend stays in place), the named subscription's service
/// otherwise.
pub fn source_backend_factory() -> WebSearchBackendFactory {
    Arc::new(|config: &WebSearchConfig| {
        let source = config.source.as_deref();
        if matches!(
            source,
            Some(
                "qoder"
                    | "qoderwork"
                    | "bailian"
                    | "cursor"
                    | "workbuddy"
                    | "qwenwork"
                    | "antigravity"
                    | "trae"
                    | "kiro"
                    | "kimi"
                    | "minimax"
                    | "zhipu"
                    | "subscription"
            )
        ) && config.access != WebSearchAccess::Live
        {
            let source = source.expect("named source").to_string();
            let access = config.access;
            let backend: WebSearchBackend = Arc::new(move |_, _| {
                let message = format!("The {source} search service does not support {access:?} access. Select live search or a source that supports the requested access.");
                Box::pin(async move { Err(message.into()) })
            });
            return Some(backend);
        }
        match source {
            Some("kiro") => Some(kiro::backend()),
            Some("trae") => Some(trae::backend()),
            Some("antigravity") => Some(super::antigravity::search_backend()),
            Some("codex") => Some(codex::backend(config.access)),
            Some("qoder") => Some(qoder::backend(qoder::Source::Qoder)),
            Some("qoderwork") => Some(qoder::backend(qoder::Source::QoderWork)),
            Some("bailian") => Some(bailian::backend()),
            Some("cursor") => Some(cursor::backend()),
            Some("workbuddy") => Some(workbuddy::backend()),
            Some("qwenwork") => Some(qwenwork::backend()),
            Some("kimi") => Some(kimi::backend()),
            Some("minimax") => Some(minimax::backend()),
            Some("zhipu") => Some(zhipu::backend()),
            Some("subscription") => Some(subscription::backend(config.access)),
            _ => None,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unnamed_sources_keep_the_ordinary_backend() {
        let factory = source_backend_factory();
        for source in [None, Some("builtin"), Some("native"), Some("brave")] {
            let config = WebSearchConfig {
                source: source.map(str::to_string),
                ..Default::default()
            };
            assert!(
                factory(&config).is_none(),
                "{source:?} must not swap the backend"
            );
        }
    }

    #[test]
    fn named_subscriptions_select_their_own_backend() {
        let factory = source_backend_factory();
        for source in [
            "subscription",
            "codex",
            "qoder",
            "qoderwork",
            "bailian",
            "cursor",
            "workbuddy",
            "qwenwork",
            "antigravity",
            "trae",
            "kiro",
            "kimi",
            "minimax",
            "zhipu",
        ] {
            let config = WebSearchConfig {
                source: Some(source.to_string()),
                ..Default::default()
            };
            assert!(
                factory(&config).is_some(),
                "{source} must serve its own search"
            );
        }
    }

    #[tokio::test]
    async fn unsupported_subscription_search_access_fails_before_credentials_or_network() {
        let factory = source_backend_factory();
        for source in [
            "qoder",
            "qoderwork",
            "bailian",
            "cursor",
            "workbuddy",
            "qwenwork",
            "antigravity",
            "kimi",
            "minimax",
            "zhipu",
            "subscription",
        ] {
            for access in [
                WebSearchAccess::Cached,
                WebSearchAccess::Indexed,
                WebSearchAccess::Disabled,
            ] {
                let backend = factory(&WebSearchConfig {
                    source: Some(source.to_string()),
                    access,
                    ..Default::default()
                })
                .unwrap();
                let result = backend(
                    "fixture".into(),
                    klaus_agent_core::types::abort::AbortSignal::new(),
                )
                .await;
                assert!(result.unwrap_err().contains("does not support"));
            }
        }
        let backend = factory(&WebSearchConfig {
            source: Some("codex".to_string()),
            access: WebSearchAccess::Disabled,
            ..Default::default()
        })
        .unwrap();
        assert!(backend(
            "fixture".into(),
            klaus_agent_core::types::abort::AbortSignal::new()
        )
        .await
        .unwrap_err()
        .contains("disabled"));
    }

    // Live probes against the real subscriptions. Each spends one real search
    // request, so they stay ignored and opt in per source:
    //   KLAUS_LIVE_SUBSCRIPTION_SEARCH=1 cargo test -p klaus-agent-server \
    //     --lib native_web_search::tests::live_codex_search -- --exact --ignored
    async fn live_search(source: &str) {
        assert_eq!(
            std::env::var("KLAUS_LIVE_SUBSCRIPTION_SEARCH").as_deref(),
            Ok("1"),
            "live subscription searches must be opted into explicitly"
        );
        let config = WebSearchConfig {
            source: Some(source.to_string()),
            ..Default::default()
        };
        let backend = (source_backend_factory())(&config).expect("backend for the source");
        let signal = klaus_agent_core::types::abort::AbortSignal::new();
        let hits = backend(
            klaus_agent_core::runtime_values::RuntimeString::from("Klaus desktop agent"),
            signal,
        )
        .await
        .unwrap_or_else(|error| {
            panic!(
                "{source} search failed: {}",
                error.try_to_utf8().unwrap_or_default()
            )
        });
        assert!(!hits.is_empty(), "{source} returned no hits");
        for hit in &hits {
            let url = hit.url.try_to_utf8().unwrap_or_default();
            assert!(url.starts_with("http"), "{source} hit without a URL: {url}");
        }
        eprintln!(
            "[live] {source} returned {} hits, first: {}",
            hits.len(),
            hits[0].url.try_to_utf8().unwrap_or_default()
        );
    }

    #[tokio::test]
    #[ignore = "live network call against the real Codex subscription"]
    async fn live_codex_search() {
        live_search("codex").await;
    }

    #[tokio::test]
    #[ignore = "live network call against the real Qoder subscription"]
    async fn live_qoder_search() {
        live_search("qoder").await;
    }

    #[tokio::test]
    #[ignore = "live network call against the real QoderWork subscription"]
    async fn live_qoderwork_search() {
        live_search("qoderwork").await;
    }

    #[tokio::test]
    #[ignore = "live network call against the real Bailian Token Plan subscription"]
    async fn live_bailian_search() {
        live_search("bailian").await;
    }

    #[tokio::test]
    #[ignore = "live network call against the real Cursor subscription"]
    async fn live_cursor_search() {
        live_search("cursor").await;
    }

    #[tokio::test]
    #[ignore = "live network call against the real WorkBuddy subscription"]
    async fn live_workbuddy_search() {
        live_search("workbuddy").await;
    }

    #[tokio::test]
    #[ignore = "live network call against the real QwenWork subscription"]
    async fn live_qwenwork_search() {
        live_search("qwenwork").await;
    }

    #[tokio::test]
    #[ignore = "live network call against the real Antigravity subscription"]
    async fn live_antigravity_search() {
        live_search("antigravity").await;
    }

    #[tokio::test]
    #[ignore = "live network call against the real Kimi membership"]
    async fn live_kimi_search() {
        live_search("kimi").await;
    }

    #[tokio::test]
    #[ignore = "live network call against the real MiniMax M Plan subscription"]
    async fn live_minimax_search() {
        live_search("minimax").await;
    }

    #[tokio::test]
    #[ignore = "live network call against the real Zhipu Coding Plan subscription"]
    async fn live_zhipu_search() {
        live_search("zhipu").await;
    }

    #[tokio::test]
    #[ignore = "live network call against the real Claude subscription"]
    async fn live_subscription_search() {
        live_search("subscription").await;
    }
}
