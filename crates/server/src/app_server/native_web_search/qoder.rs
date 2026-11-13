//! Qoder search backend.
//!
//! The subscription answers search through the gateway's signed one-shot
//! search route: a JSON envelope whose `payload` is the stringified search
//! request, signed with the same COSY context the inference bridge uses. The
//! answer is plain JSON holding `pageItems` (title / link / snippet). Domain
//! filters are applied by the tool around this backend, exactly as for every
//! other source. A source that is not signed in fails the search with the
//! reason — never a silent fallback to another backend.

use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::tools::builtins::web_search::{WebSearchBackend, WebSearchResult};
use klaus_agent_core::types::abort::AbortSignal;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

/// Which Qoder-family subscription serves the search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Source {
    Qoder,
}

impl Source {
    fn label(self) -> &'static str {
        match self {
            Source::Qoder => "Qoder",
        }
    }
    /// Refresh the stored token before signing, so a lapsed bearer does not
    /// fail a search the subscription could have served.
    async fn refresh(self) -> Result<(), String> {
        match self {
            Source::Qoder => crate::app_server::qoder::auth::ensure_fresh_token().await,
        }
    }
    fn sign(self, body: &str) -> Result<crate::app_server::qoder::sign::SignedRequest, String> {
        match self {
            Source::Qoder => crate::app_server::qoder::catalog::sign_search_post(body)
                .map_err(|error| error.message),
        }
    }
}

const HTTP_TIMEOUT: Duration = Duration::from_secs(25);
const MAX_HITS: usize = 20;

pub(crate) fn backend(source: Source) -> WebSearchBackend {
    Arc::new(move |query, signal| Box::pin(async move { search(source, query, signal).await }))
}

fn text(value: &RuntimeString) -> String {
    value.try_to_utf8().unwrap_or_default()
}

fn fail(message: impl Into<String>) -> RuntimeString {
    RuntimeString::from(message.into())
}

async fn search(
    source: Source,
    query: RuntimeString,
    signal: AbortSignal,
) -> Result<Vec<WebSearchResult>, RuntimeString> {
    let query = text(&query);
    if query.trim().is_empty() {
        return Err(fail("The search query is empty."));
    }
    source
        .refresh()
        .await
        .map_err(|error| fail(format!("{} search is unavailable: {error}", source.label())))?;
    // `payload` is a JSON STRING inside the envelope, and the signature covers
    // the exact envelope bytes that go on the wire.
    let body = json!({
        "payload": json!({
            "query": query,
            "timeRange": "NoLimit",
            "contents": { "mainText": false, "markdownText": false, "summary": false },
        })
        .to_string(),
        "encodeVersion": "1",
    })
    .to_string();
    let signed = source.sign(&body).map_err(|error| {
        fail(format!(
            "{} search could not be signed: {error}",
            source.label()
        ))
    })?;
    if signal.is_cancelled() {
        return Err(fail("The search was cancelled."));
    }
    let client = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .map_err(|error| fail(error.to_string()))?;
    let mut request = client.post(&signed.url);
    for (name, value) in &signed.headers {
        request = request.header(name, value);
    }
    let response = request
        .header("X-Session-ID", crate::app_server::qoder::uuid_v4())
        .body(signed.body.unwrap_or_default())
        .send()
        .await
        .map_err(|error| fail(format!("{} search request failed: {error}", source.label())))?;
    let status = response.status();
    let body = response.text().await.map_err(|error| {
        fail(format!(
            "{} search response could not be read: {error}",
            source.label()
        ))
    })?;
    if std::env::var_os("KLAUS_QODER_SEARCH_DEBUG").is_some() {
        eprintln!(
            "[qoder-search] {} status={} body head: {:.400}",
            source.label(),
            status.as_u16(),
            body
        );
    }
    if !status.is_success() {
        // The tool-visible failure stays locally constructed: a gateway body
        // can echo request material, and only public wording crosses to the
        // model. The debug log keeps the raw head for diagnosis.
        return Err(fail(format!(
            "{} search rejected the request (HTTP {}).",
            source.label(),
            status.as_u16()
        )));
    }
    let parsed: Value = serde_json::from_str(&body)
        .or_else(|_| {
            // Some gateway routes answer with the encoded envelope even when
            // the request asked for plain JSON; decode before giving up.
            crate::app_server::qoder::sign::decode_server_response(&body).and_then(|decoded| {
                serde_json::from_str(&decoded).map_err(|error| error.to_string())
            })
        })
        .map_err(|_| {
            fail(format!(
                "{} search returned an unreadable response.",
                source.label()
            ))
        })?;
    parse_search_response(&parsed)
        .map_err(|error| fail(format!("{} search: {error}", source.label())))
}

/// Map the gateway's `pageItems` onto ranked hits. `errorCode` is the
/// service's own refusal channel and must fail the search, not empty it.
fn parse_search_response(value: &Value) -> Result<Vec<WebSearchResult>, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "the response is not a JSON object".to_string())?;
    if let Some(code) = object.get("errorCode") {
        let nonzero = match code {
            Value::Number(number) => number.as_i64().map(|number| number != 0).unwrap_or(true),
            Value::String(text) => !text.is_empty() && text != "0",
            Value::Null => false,
            _ => true,
        };
        if nonzero {
            // The service's own wording stays out of the tool-visible failure:
            // only the code crosses, so a gateway body cannot reach the model.
            return Err(format!("the service refused the search (code {code})"));
        }
    }
    let items = object
        .get("pageItems")
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

    fn hits(value: &Value) -> Vec<WebSearchResult> {
        parse_search_response(value).unwrap()
    }

    #[test]
    fn page_items_map_to_ranked_hits() {
        let parsed = hits(&json!({
            "errorCode": 0,
            "requestId": "r1",
            "pageItems": [
                { "title": "Guide", "link": "https://docs.example.test/guide", "snippet": "Useful" },
                { "title": "", "link": "https://example.test/bare", "snippet": "" },
                { "title": "Not a URL", "link": "ftp://example.test/x", "snippet": "" },
            ]
        }));
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].title.try_to_utf8().unwrap(), "Guide");
        assert_eq!(
            parsed[0].url.try_to_utf8().unwrap(),
            "https://docs.example.test/guide"
        );
        assert_eq!(
            parsed[0].snippet.as_ref().unwrap().try_to_utf8().unwrap(),
            "Useful"
        );
        assert_eq!(parsed[1].title.try_to_utf8().unwrap(), "(untitled)");
        assert!(parsed[1].snippet.is_none());
    }

    #[test]
    fn a_service_error_code_fails_instead_of_emptying() {
        let error = parse_search_response(
            &json!({ "errorCode": 403, "errorMsg": "forbidden", "pageItems": [] }),
        )
        .unwrap_err();
        assert!(error.contains("403"), "{error}");
        assert!(
            !error.contains("forbidden"),
            "the service wording must not cross to the model: {error}"
        );
    }

    #[test]
    fn an_empty_item_list_is_a_valid_empty_search() {
        assert!(hits(&json!({ "errorCode": 0, "pageItems": [] })).is_empty());
    }

    #[test]
    fn hits_stop_at_the_bound() {
        let items: Vec<Value> = (0..30)
            .map(|index| json!({ "title": format!("t{index}"), "link": format!("https://example.test/{index}") }))
            .collect();
        assert_eq!(
            hits(&json!({ "errorCode": 0, "pageItems": items })).len(),
            MAX_HITS
        );
    }
}
