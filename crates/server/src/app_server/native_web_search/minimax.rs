//! MiniMax M Plan search backend.
//!
//! The plan's own web search is one POST on the service root carrying the bare
//! query, answered with an `organic` result list plus a `base_resp` status
//! envelope. The subscription key rides as a bearer, the same key the model
//! gateway and the allowance route spend. A nonzero `base_resp` status is the
//! service's refusal channel and fails the search instead of emptying it; a
//! missing key fails with the reason. No fallback to another backend.

use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::tools::builtins::web_search::{WebSearchBackend, WebSearchResult};
use klaus_agent_core::types::abort::AbortSignal;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

const HTTP_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_HITS: usize = 20;

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
    let key = crate::app_server::minimax::api_key().ok_or_else(|| {
        fail("MiniMax M Plan search is unavailable: the subscription key is not stored.")
    })?;
    let url = crate::app_server::minimax::auth::search_base();
    let body = json!({ "q": query }).to_string();
    if signal.is_cancelled() {
        return Err(fail("The search was cancelled."));
    }
    let client = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .map_err(|error| fail(error.to_string()))?;
    let response = client
        .post(&url)
        .bearer_auth(&key)
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await
        .map_err(|error| fail(format!("MiniMax search request failed: {error}")))?;
    let status = response.status();
    let body = response.text().await.map_err(|error| {
        fail(format!(
            "MiniMax search response could not be read: {error}"
        ))
    })?;
    if std::env::var_os("KLAUS_MINIMAX_SEARCH_DEBUG").is_some() {
        eprintln!(
            "[minimax-search] status={} body head: {:.400}",
            status.as_u16(),
            body
        );
    }
    if !status.is_success() {
        return Err(fail(format!(
            "MiniMax search rejected the request (HTTP {}).",
            status.as_u16()
        )));
    }
    let parsed: Value = serde_json::from_str(&body)
        .map_err(|_| fail("MiniMax search returned an unreadable response."))?;
    parse_search_response(&parsed).map_err(|error| fail(format!("MiniMax search: {error}")))
}

/// Map the `organic` list onto ranked hits. The status envelope is checked
/// first: a nonzero code is a refusal, not an empty answer.
fn parse_search_response(value: &Value) -> Result<Vec<WebSearchResult>, String> {
    if let Some(base) = value.get("base_resp") {
        let code = base.get("status_code").and_then(Value::as_i64).unwrap_or(0);
        if code != 0 {
            return Err(format!("the service refused the search (code {code})"));
        }
    }
    let items = value
        .get("organic")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut hits = Vec::new();
    for item in &items {
        let Some(url) = item
            .get("link")
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
            .unwrap_or("(untitled)");
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
    fn organic_results_map_onto_hits() {
        let parsed = parse_search_response(&json!({
            "organic": [
                { "title": "Tokio", "link": "https://tokio.rs", "snippet": "Async runtime.", "date": "2026-01-01" },
                { "title": "", "link": "https://example.test/bare", "snippet": "" },
            ],
            "base_resp": { "status_code": 0, "status_msg": "success" }
        }))
        .unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].title.try_to_utf8().unwrap(), "Tokio");
        assert_eq!(parsed[1].title.try_to_utf8().unwrap(), "(untitled)");
    }

    #[test]
    fn a_nonzero_status_is_a_refusal_not_empty_hits() {
        let error = parse_search_response(&json!({
            "organic": [],
            "base_resp": { "status_code": 1004, "status_msg": "invalid key" }
        }))
        .unwrap_err();
        assert!(error.contains("1004"));
    }
}
