//! Bailian Token Plan search backend.
//!
//! The plan's built-in web search is reachable only through the OpenAI-compatible
//! Responses API: a request carrying `tools: [{"type": "web_search"}]` makes the
//! gateway run the subscription's own search, and the response's
//! `web_search_call` output items carry the sources the search consumed. The
//! plan key reaches no other search entry point — the DashScope-native
//! `enable_source` route and the WebSearch MCP route both refuse `sk-sp-` keys —
//! so the Responses route is the one that returns hits for this subscription.
//! Sources carry URLs only (the gateway exposes neither titles nor snippets),
//! so a hit's title degrades to the source host, which is the only identifying
//! material the service provides. Domain filters are applied by the tool around
//! this backend, exactly as for every other source. A source that is not signed
//! in fails the search with the reason — never a silent fallback to another
//! backend.

use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::tools::builtins::web_search::{WebSearchBackend, WebSearchResult};
use klaus_agent_core::types::abort::AbortSignal;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

/// The Responses route spends a real search plus one model turn, and the
/// reasoning models on the plan are slow; the bound covers a forced search
/// without letting a hung gateway hold the tool call open.
const HTTP_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_HITS: usize = 20;

/// Which plan model answers the search-only Responses request. The flash tier
/// keeps the turn short (measured ~9s against ~20s for the reasoning tier) and
/// the sources are the gateway's, not the model's, so the tier does not change
/// what the search returns.
fn search_model() -> String {
    match std::env::var("BAILIAN_SEARCH_MODEL") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => "qwen3.8-flash".to_string(),
    }
}

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
    let Some(key) = crate::app_server::bailian::api_key() else {
        return Err(fail(
            "Bailian Token Plan search is unavailable: the subscription key is not stored.",
        ));
    };
    // Same plan key, same host as the catalog route; the Responses API lives
    // beside the chat routes on the OpenAI-compatible base.
    let url = format!(
        "{}/responses",
        crate::app_server::bailian::auth::catalog_base()
    );
    // `forced_search` makes the gateway search for every query instead of
    // letting the model decide; a search source that sometimes skips the
    // search would read as an empty backend.
    let body = json!({
        "model": search_model(),
        "input": query,
        "tools": [{ "type": "web_search", "search_options": { "forced_search": true } }],
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
        .bearer_auth(&key)
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await
        .map_err(|error| fail(format!("Bailian search request failed: {error}")))?;
    let status = response.status();
    let body = response.text().await.map_err(|error| {
        fail(format!(
            "Bailian search response could not be read: {error}"
        ))
    })?;
    if std::env::var_os("KLAUS_BAILIAN_SEARCH_DEBUG").is_some() {
        eprintln!(
            "[bailian-search] status={} body head: {:.400}",
            status.as_u16(),
            body
        );
    }
    if !status.is_success() {
        // The tool-visible failure stays locally constructed: a gateway body
        // can echo request material, and only public wording crosses to the
        // model. The debug log keeps the raw head for diagnosis.
        return Err(fail(format!(
            "Bailian search rejected the request (HTTP {}).",
            status.as_u16()
        )));
    }
    let parsed: Value = serde_json::from_str(&body)
        .map_err(|_| fail("Bailian search returned an unreadable response."))?;
    parse_search_response(&parsed).map_err(|error| fail(format!("Bailian search: {error}")))
}

/// Map every `web_search_call` item's sources onto ranked hits. The gateway
/// answers with URLs only; a source that carries a title or snippet anyway is
/// honored, and a bare URL degrades to its host as the title so the result
/// line still says something a reader can place.
fn parse_search_response(value: &Value) -> Result<Vec<WebSearchResult>, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "the response is not a JSON object".to_string())?;
    if let Some(error) = object.get("error").filter(|error| !error.is_null()) {
        // The gateway's own wording stays out of the tool-visible failure.
        let code = error
            .get("code")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        return Err(format!("the service refused the search (code {code})"));
    }
    let output = object
        .get("output")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut hits = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for item in &output {
        if item.get("type").and_then(Value::as_str) != Some("web_search_call") {
            continue;
        }
        let Some(sources) = item
            .get("action")
            .and_then(|action| action.get("sources"))
            .and_then(Value::as_array)
        else {
            continue;
        };
        for source in sources {
            let Some(url) = source
                .get("url")
                .and_then(Value::as_str)
                .filter(|url| url.starts_with("http"))
                .map(str::to_string)
            else {
                continue;
            };
            if !seen.insert(url.clone()) {
                continue;
            }
            let title = source
                .get("title")
                .and_then(Value::as_str)
                .filter(|title| !title.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| host_title(&url));
            let snippet = source
                .get("snippet")
                .and_then(Value::as_str)
                .filter(|snippet| !snippet.is_empty())
                .map(RuntimeString::from);
            hits.push(WebSearchResult {
                title: RuntimeString::from(title),
                url: RuntimeString::from(url),
                snippet,
            });
            if hits.len() >= MAX_HITS {
                return Ok(hits);
            }
        }
    }
    Ok(hits)
}

/// The readable part of a bare source URL: its host without the usual `www.`
/// prefix, which is what a person recognizes a site by.
fn host_title(url: &str) -> String {
    url.split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(url)
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(url)
        .trim_start_matches("www.")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hits(value: &Value) -> Vec<WebSearchResult> {
        parse_search_response(value).unwrap()
    }

    #[test]
    fn url_only_sources_degrade_to_their_host() {
        let parsed = hits(&json!({
            "output": [
                { "type": "reasoning" },
                { "type": "web_search_call", "action": { "type": "search", "query": "q", "sources": [
                    { "type": "url", "url": "https://www.weather.com.cn/weather40d/101210101.shtml" },
                    { "type": "url", "url": "https://example.test/bare" },
                    { "type": "url", "url": "ftp://example.test/not-http" },
                ] } },
                { "type": "message" },
            ]
        }));
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].title.try_to_utf8().unwrap(), "weather.com.cn");
        assert_eq!(
            parsed[0].url.try_to_utf8().unwrap(),
            "https://www.weather.com.cn/weather40d/101210101.shtml"
        );
        assert!(parsed[0].snippet.is_none());
        assert_eq!(parsed[1].title.try_to_utf8().unwrap(), "example.test");
    }

    #[test]
    fn titles_and_snippets_are_honored_when_present() {
        let parsed = hits(&json!({
            "output": [{ "type": "web_search_call", "action": { "sources": [
                { "type": "url", "url": "https://docs.example.test/guide", "title": "Guide", "snippet": "Useful" },
            ] } }]
        }));
        assert_eq!(parsed[0].title.try_to_utf8().unwrap(), "Guide");
        assert_eq!(
            parsed[0].snippet.as_ref().unwrap().try_to_utf8().unwrap(),
            "Useful"
        );
    }

    #[test]
    fn repeated_urls_across_calls_are_deduplicated() {
        let parsed = hits(&json!({
            "output": [
                { "type": "web_search_call", "action": { "sources": [
                    { "type": "url", "url": "https://example.test/a" },
                ] } },
                { "type": "web_search_call", "action": { "sources": [
                    { "type": "url", "url": "https://example.test/a" },
                    { "type": "url", "url": "https://example.test/b" },
                ] } },
            ]
        }));
        assert_eq!(parsed.len(), 2);
    }

    #[test]
    fn a_gateway_error_code_fails_instead_of_emptying() {
        let error = parse_search_response(
            &json!({ "error": { "code": "invalid_request_error", "message": "secret wording" } }),
        )
        .unwrap_err();
        assert!(error.contains("invalid_request_error"), "{error}");
        assert!(
            !error.contains("secret wording"),
            "the service wording must not cross to the model: {error}"
        );
    }

    #[test]
    fn an_empty_source_list_is_a_valid_empty_search() {
        assert!(hits(&json!({ "output": [] })).is_empty());
    }

    #[test]
    fn hits_stop_at_the_bound() {
        let sources: Vec<Value> = (0..30)
            .map(|index| json!({ "type": "url", "url": format!("https://example.test/{index}") }))
            .collect();
        assert_eq!(
            hits(&json!({ "output": [{ "type": "web_search_call", "action": { "sources": sources } }] })).len(),
            MAX_HITS
        );
    }
}
