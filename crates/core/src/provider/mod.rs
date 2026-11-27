//! Model providers: request assembly, streaming, auth, retry and fallback.

pub mod access_policy;
pub mod anthropic;
pub mod approval_classifier_transport;
pub mod auth;
pub mod aws_eventstream;
pub mod behavior;
pub mod classify;
pub mod claude_subscription;
pub mod codex_subscription;
pub mod gemini;
pub mod gemini_code_assist;
pub mod gemini_subscription;
pub mod interceptor;
pub mod model_catalog;
pub mod model_fallback;
pub mod native_compaction;
pub mod native_media;
pub(crate) mod native_tools;
pub mod oauth;
pub mod observability;
pub mod openai;
pub(crate) mod output_reservation;
pub mod prewarm;
pub mod retry;
pub mod review_target;
pub mod rotation;
pub mod sigv4;
pub mod tool_choice;
pub(crate) mod util;

use std::ffi::OsString;
use std::sync::{LazyLock, Mutex};

/// Process-wide native root certificate store so provider and classifier client
/// construction does not repeatedly scan system certificate stores.
pub(crate) fn shared_native_root_store() -> &'static rustls::RootCertStore {
    static NATIVE_ROOTS: LazyLock<rustls::RootCertStore> = LazyLock::new(|| {
        let mut roots = rustls::RootCertStore::empty();
        let native = rustls_native_certs::load_native_certs();
        if !native.errors.is_empty() {
            tracing::warn!(
                native_root_error_count = native.errors.len(),
                "encountered errors while loading system root certificates"
            );
        }
        roots.add_parsable_certificates(native.certs);
        roots
    });
    &NATIVE_ROOTS
}

type TransportEnvSnapshot = [Option<OsString>; 10];

fn current_transport_env() -> TransportEnvSnapshot {
    [
        std::env::var_os("HTTP_PROXY"),
        std::env::var_os("http_proxy"),
        std::env::var_os("HTTPS_PROXY"),
        std::env::var_os("https_proxy"),
        std::env::var_os("ALL_PROXY"),
        std::env::var_os("all_proxy"),
        std::env::var_os("NO_PROXY"),
        std::env::var_os("no_proxy"),
        std::env::var_os("SSL_CERT_FILE"),
        std::env::var_os("SSL_CERT_DIR"),
    ]
}

fn build_shared_clients() -> (reqwest::Client, reqwest::Client) {
    let default_client = reqwest::Client::new();
    let http1_client = reqwest::Client::builder()
        .http1_only()
        .build()
        .unwrap_or_else(|_| default_client.clone());
    (default_client, http1_client)
}

fn shared_client_pair() -> (reqwest::Client, reqwest::Client) {
    static CLIENTS: LazyLock<
        Mutex<Option<(TransportEnvSnapshot, reqwest::Client, reqwest::Client)>>,
    > = LazyLock::new(|| Mutex::new(None));
    let env = current_transport_env();
    let mut guard = CLIENTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some((cached_env, client, http1_client)) = guard.as_ref() {
        if *cached_env == env {
            return (client.clone(), http1_client.clone());
        }
    }
    let (client, http1_client) = build_shared_clients();
    *guard = Some((env, client.clone(), http1_client.clone()));
    (client, http1_client)
}

/// Shared default HTTP client reused across provider targets when transport
/// environment variables have not changed.
pub fn shared_http_client() -> reqwest::Client {
    shared_client_pair().0
}

/// Shared HTTP/1.1 client for WebSocket upgrade paths reused across provider
/// targets when transport environment variables have not changed.
pub fn shared_http1_client() -> reqwest::Client {
    shared_client_pair().1
}

/// Reject controls a transport cannot represent before opening a network request.
pub(crate) fn unsupported_inference_control(
    request: &crate::types::provider::ModelRequest,
    provider: &str,
    verbosity_supported: bool,
    summary_supported: bool,
    cached_search_supported: bool,
) -> Option<crate::types::errors::ProviderError> {
    use crate::schemas::config::WebSearchAccess;
    use crate::types::provider::ReasoningSummary;
    let control = if request.model_verbosity.is_some() && !verbosity_supported {
        "modelVerbosity"
    } else if !matches!(
        request.reasoning_summary,
        ReasoningSummary::Auto | ReasoningSummary::None
    ) && !summary_supported
    {
        "reasoningSummary"
    } else if request.web_search.as_ref().is_some_and(|search| {
        search.access == WebSearchAccess::Disabled
            || (!cached_search_supported && search.access != WebSearchAccess::Live)
    }) {
        "webSearch.access"
    } else {
        return None;
    };
    Some(crate::types::errors::ProviderError {
        kind: crate::types::errors::ProviderErrorKind::InvalidRequest,
        message: format!("{provider} cannot apply the requested {control} setting for this model. Choose a supported setting or model.").into(),
        status: None,
        code: Some("unsupported_inference_setting".into()),
        error_type: None,
        media_origins: None,
        media_repair_complete: false, credential_recovery: None,
        retry_after_ms: None,
        request_id: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_native_roots_and_http_clients_initialize_without_reloading_certificates() {
        let first_roots = shared_native_root_store();
        let second_roots = shared_native_root_store();
        assert!(
            std::ptr::eq(first_roots, second_roots),
            "native root certificate store must be initialized once per process"
        );
        let _c1 = shared_http_client();
        let _c2 = shared_http_client();
        let _h1 = shared_http1_client();
        let _h2 = shared_http1_client();
    }
}
