//! Kimi membership search backend.
//!
//! The membership's own search service is a plain HTTP route beside the model
//! gateway: one POST carrying the query, answered with a ranked result list
//! (`search_results` with site name, title, url and snippet). The bearer is
//! whichever credential the store holds — the plan API key or the device-flow
//! login's access token, renewed first when it is about to expire — the same
//! credential the model connection authenticates with. A missing credential
//! fails the search with the reason; there is no fallback to another backend.

use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::tools::builtins::web_search::{WebSearchBackend, WebSearchResult};
use klaus_agent_core::types::abort::AbortSignal;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

const HTTP_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_HITS: usize = 20;
/// Results per request. The service caps a page at 20; a search source that
/// asks for more than it can rank gains nothing.
const PAGE_LIMIT: u32 = 10;
/// The service-side bound one request asks the search itself to respect.
const SERVICE_TIMEOUT_SECONDS: u32 = 30;

pub(crate) fn backend() -> WebSearchBackend {
    Arc::new(move |query, signal| Box::pin(async move { search(query, signal).await }))
}

fn text(value: &RuntimeString) -> String {
    value.try_to_utf8().unwrap_or_default()
}

fn fail(message: impl Into<String>) -> RuntimeString {
    RuntimeString::from(message.into())
}

async fn search(
    query: RuntimeString,
    signal: AbortSignal,
) -> Result<Vec<WebSearchResult>, RuntimeString> {
    let query = text(&query);
    if query.trim().is_empty() {
        return Err(fail("The search query is empty."));
    }
    let token = crate::app_server::kimi::auth::access_token()
        .await
        .map_err(|error| fail(format!("Kimi search is unavailable: {}", error.message)))?;
    // The search route hangs off the coding root one version segment deep, the
    // same root the model gateway serves `/v1/messages` from.
    let url = format!("{}/v1/search", crate::app_server::kimi::auth::api_base());
    let body = json!({
        "text_query": query,
        "limit": PAGE_LIMIT,
        "enable_page_crawling": false,
        "timeout_seconds": SERVICE_TIMEOUT_SECONDS,
    })
    .to_string();
    if signal.is_cancelled() {
        return Err(fail("The search was cancelled."));
    }
    let client = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .map_err(|error| fail(error.to_string()))?;
    let response = client
        .post(&url)
        .bearer_auth(&token)
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await
        .map_err(|error| fail(format!("Kimi search request failed: {error}")))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|error| fail(format!("Kimi search response could not be read: {error}")))?;
    if std::env::var_os("KLAUS_KIMI_SEARCH_DEBUG").is_some() {
        eprintln!(
            "[kimi-search] status={} body head: {:.400}",
            status.as_u16(),
            body
        );
    }
    if !status.is_success() {
        // The tool-visible failure stays locally constructed: a gateway body
        // can echo request material, and only public wording crosses to the
        // model. The debug log keeps the raw head for diagnosis.
        return Err(fail(format!(
            "Kimi search rejected the request (HTTP {}).",
            status.as_u16()
        )));
    }
    let parsed: Value = serde_json::from_str(&body)
        .map_err(|_| fail("Kimi search returned an unreadable response."))?;
    parse_search_response(&parsed).map_err(|error| fail(format!("Kimi search: {error}")))
}

/// Map the service's `search_results` onto ranked hits. Entries without a
/// usable http URL are dropped; a bare entry keeps whatever title and snippet
/// the service supplied.
fn parse_search_response(value: &Value) -> Result<Vec<WebSearchResult>, String> {
    let items = value
        .get("search_results")
        .and_then(Value::as_array)
        .ok_or_else(|| "the response carries no search_results".to_string())?;
    let mut hits = Vec::new();
    for item in items {
        let Some(url) = item
            .get("url")
            .and_then(Value::as_str)
            .filter(|url| url.starts_with("http"))
            .map(str::to_string)
        else {
            continue;
        };
        let title = item
            .get("title")
            .and_then(Value::as_str)
            .filter(|title| !title.is_empty())
            .unwrap_or_else(|| {
                item.get("site_name")
                    .and_then(Value::as_str)
                    .filter(|name| !name.is_empty())
                    .unwrap_or("(untitled)")
            });
        let snippet = item
            .get("snippet")
            .and_then(Value::as_str)
            .filter(|snippet| !snippet.is_empty())
            .map(str::to_string);
        hits.push(WebSearchResult {
            title: RuntimeString::from(title),
            url: RuntimeString::from(url),
            snippet: snippet.map(RuntimeString::from),
        });
        if hits.len() >= MAX_HITS {
            break;
        }
    }
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn results_map_onto_hits_with_site_name_fallback() {
        let parsed = parse_search_response(&json!({
            "search_results": [
                { "site_name": "rust-lang", "title": "Rust", "url": "https://www.rust-lang.org", "snippet": "A systems language." },
                { "site_name": "bare", "title": "", "url": "https://example.test/bare", "snippet": "" },
                { "site_name": "nope", "title": "Not http", "url": "ftp://example.test/x", "snippet": "" },
            ]
        }))
        .unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].title.try_to_utf8().unwrap(), "Rust");
        assert_eq!(
            parsed[0].snippet.as_ref().unwrap().try_to_utf8().unwrap(),
            "A systems language."
        );
        assert_eq!(parsed[1].title.try_to_utf8().unwrap(), "bare");
        assert!(parsed[1].snippet.is_none());
    }

    #[test]
    fn a_response_without_results_is_a_parse_failure_not_empty_hits() {
        assert!(parse_search_response(&json!({ "code": 1 })).is_err());
        assert!(parse_search_response(&json!({ "search_results": [] }))
            .unwrap()
            .is_empty());
    }
}
