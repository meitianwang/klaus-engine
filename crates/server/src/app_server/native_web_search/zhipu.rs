//! Zhipu GLM Coding Plan search backend.
//!
//! The plan's own search is a remote MCP server on the site root: one
//! streamable-HTTP endpoint speaking JSON-RPC, authenticated with the plan key
//! as a bearer. The backend runs the short MCP conversation itself —
//! initialize, read the search tool's declared input schema, then call the
//! tool — so the argument names come from the service's own schema instead of
//! a guess. Responses arrive either as plain JSON or as server-sent events;
//! both are decoded. A missing key fails the search with the reason; there is
//! no fallback to another backend.

use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::tools::builtins::web_search::{WebSearchBackend, WebSearchResult};
use klaus_agent_core::types::abort::AbortSignal;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

const HTTP_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_HITS: usize = 20;
const PROTOCOL_VERSION: &str = "2025-06-18";

pub(crate) fn backend() -> WebSearchBackend {
    Arc::new(move |query, signal| Box::pin(async move { search(query, signal).await }))
}

fn text(value: &RuntimeString) -> String {
    value.try_to_utf8().unwrap_or_default()
}

fn fail(message: impl Into<String>) -> RuntimeString {
    RuntimeString::from(message.into())
}

/// One JSON-RPC round trip. The session id, once the server issues one, rides
/// on every later request; a stateless server simply never issues it. The
/// reply is plain JSON or an event stream whose `data:` lines carry the JSON-RPC
/// messages — the last one holding a `result` or `error` wins.
async fn rpc(
    client: &reqwest::Client,
    url: &str,
    key: &str,
    session: &mut Option<String>,
    id: u32,
    method: &str,
    params: Value,
) -> Result<Value, String> {
    let mut request = client
        .post(url)
        .bearer_auth(key)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(
            json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }).to_string(),
        );
    if let Some(session) = session.as_deref() {
        request = request.header("Mcp-Session-Id", session);
    }
    let response = request
        .send()
        .await
        .map_err(|error| format!("the search service request failed: {error}"))?;
    if let Some(value) = response
        .headers()
        .get("mcp-session-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
    {
        *session = Some(value.to_string());
    }
    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|error| format!("the search service response could not be read: {error}"))?;
    if std::env::var_os("KLAUS_ZHIPU_SEARCH_DEBUG").is_some() {
        eprintln!(
            "[zhipu-search] {method} status={} body head: {:.400}",
            status.as_u16(),
            body
        );
    }
    if !status.is_success() {
        return Err(format!(
            "the search service rejected the request (HTTP {})",
            status.as_u16()
        ));
    }
    decode_rpc(&body, id)
}

/// Decode a JSON-RPC reply from either a JSON body or an event stream.
fn decode_rpc(body: &str, id: u32) -> Result<Value, String> {
    let trimmed = body.trim();
    if trimmed.starts_with('{') {
        return serde_json::from_str(trimmed)
            .map_err(|_| "the search service returned an unreadable response".to_string());
    }
    let mut reply: Option<Value> = None;
    for line in trimmed.lines() {
        let Some(payload) = line.trim().strip_prefix("data:") else {
            continue;
        };
        let Ok(parsed) = serde_json::from_str::<Value>(payload.trim()) else {
            continue;
        };
        if parsed.get("id").and_then(Value::as_u64) == Some(id as u64)
            && (parsed.get("result").is_some() || parsed.get("error").is_some())
        {
            reply = Some(parsed);
        }
    }
    reply.ok_or_else(|| "the search service stream carried no reply".to_string())
}

/// The query argument name and the optional page-size argument name, read from
/// the tool's own input schema so the call matches what the service declares.
fn argument_names(schema: &Value) -> Option<(&'static str, Option<&'static str>)> {
    let properties = schema.get("properties")?.as_object()?;
    let query = ["search_query", "query", "q", "text"]
        .into_iter()
        .find(|name| properties.contains_key(*name))?;
    let count = ["count", "size", "limit", "top_k"]
        .into_iter()
        .find(|name| properties.contains_key(*name));
    Some((query, count))
}

async fn search(
    query: RuntimeString,
    signal: AbortSignal,
) -> Result<Vec<WebSearchResult>, RuntimeString> {
    let query = text(&query);
    if query.trim().is_empty() {
        return Err(fail("The search query is empty."));
    }
    let key = crate::app_server::zhipu::api_key().ok_or_else(|| {
        fail("Zhipu Coding Plan search is unavailable: the plan key is not stored.")
    })?;
    let url = crate::app_server::zhipu::auth::search_mcp_url();
    if signal.is_cancelled() {
        return Err(fail("The search was cancelled."));
    }
    let client = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .map_err(|error| fail(error.to_string()))?;
    let mut session: Option<String> = None;
    rpc(
        &client,
        &url,
        &key,
        &mut session,
        1,
        "initialize",
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": { "name": "klaus-desktop", "version": env!("CARGO_PKG_VERSION") },
        }),
    )
    .await
    .map_err(fail)?;
    // The initialized notification completes the handshake. A stateless
    // deployment answers 202 and carries on; one that insists on the
    // notification would refuse the later tool call, so it is always sent and
    // its reply deliberately ignored.
    let mut notify = client
        .post(&url)
        .bearer_auth(&key)
        .header("Content-Type", "application/json")
        .body(json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }).to_string());
    if let Some(session) = session.as_deref() {
        notify = notify.header("Mcp-Session-Id", session);
    }
    let _ = notify.send().await;
    let listed = rpc(
        &client,
        &url,
        &key,
        &mut session,
        2,
        "tools/list",
        json!({}),
    )
    .await
    .map_err(fail)?;
    if let Some(error) = listed.get("error") {
        return Err(fail(format!(
            "the search service refused to list its tools (code {})",
            error.get("code").cloned().unwrap_or(Value::Null)
        )));
    }
    let tool = listed
        .pointer("/result/tools")
        .and_then(Value::as_array)
        .and_then(|tools| {
            tools.iter().find(|tool| {
                matches!(
                    tool.get("name").and_then(Value::as_str),
                    Some("web_search_prime") | Some("webSearchPrime")
                )
            })
        })
        .ok_or_else(|| fail("The search service did not advertise its search tool."))?;
    // The call names the tool exactly as the service advertised it; the two
    // spellings exist in the wild and only the listed one is authoritative.
    let tool_name = tool
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("web_search_prime")
        .to_string();
    let schema = tool.get("inputSchema").cloned().unwrap_or(Value::Null);
    let (query_name, count_name) = argument_names(&schema)
        .ok_or_else(|| fail("The search tool declared no usable query argument."))?;
    let mut arguments = json!({ query_name: query });
    if let Some(count_name) = count_name {
        arguments[count_name] = json!(10);
    }
    if signal.is_cancelled() {
        return Err(fail("The search was cancelled."));
    }
    let called = rpc(
        &client,
        &url,
        &key,
        &mut session,
        3,
        "tools/call",
        json!({ "name": tool_name, "arguments": arguments }),
    )
    .await
    .map_err(fail)?;
    if let Some(error) = called.get("error") {
        return Err(fail(format!(
            "the search service refused the search (code {})",
            error.get("code").cloned().unwrap_or(Value::Null)
        )));
    }
    let joined = called
        .pointer("/result/content")
        .and_then(Value::as_array)
        .map(|content| {
            content
                .iter()
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default();
    parse_search_payload(&joined).map_err(|error| fail(format!("Zhipu search: {error}")))
}

/// The tool's text payload is itself JSON: a result list under one of the
/// service's known keys, or a bare array. A payload that is not JSON falls back
/// to the source lines it carries, so a prose answer still yields its links.
fn parse_search_payload(payload: &str) -> Result<Vec<WebSearchResult>, String> {
    let mut hits = Vec::new();
    if let Ok(parsed) = serde_json::from_str::<Value>(payload.trim()) {
        let candidates: Vec<&Value> = match &parsed {
            Value::Array(items) => items.iter().collect(),
            Value::Object(object) => ["results", "search_results", "organic", "pageItems"]
                .into_iter()
                .filter_map(|key| object.get(key))
                .filter_map(Value::as_array)
                .flatten()
                .collect(),
            _ => Vec::new(),
        };
        for item in candidates {
            let Some(url) = ["url", "link"]
                .into_iter()
                .filter_map(|key| item.get(key))
                .find_map(Value::as_str)
                .filter(|url| url.starts_with("http"))
                .map(str::to_string)
            else {
                continue;
            };
            let title = ["title", "name"]
                .into_iter()
                .filter_map(|key| item.get(key))
                .find_map(Value::as_str)
                .filter(|title| !title.is_empty())
                .unwrap_or("(untitled)");
            let snippet = ["snippet", "summary", "abstract", "content"]
                .into_iter()
                .filter_map(|key| item.get(key))
                .find_map(Value::as_str)
                .filter(|snippet| !snippet.is_empty())
                .map(str::to_string);
            hits.push(WebSearchResult {
                title: RuntimeString::from(title),
                url: RuntimeString::from(url),
                snippet: snippet.map(RuntimeString::from),
            });
            if hits.len() >= MAX_HITS {
                return Ok(hits);
            }
        }
        if !hits.is_empty() {
            return Ok(hits);
        }
    }
    for line in payload.lines() {
        let Some(position) = line.find("http") else {
            continue;
        };
        let url: String = line[position..]
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .trim_end_matches([')', ']', ',', '.', ';', ':', '"', '\''])
            .to_string();
        let title = line[..position]
            .trim()
            .trim_end_matches(['-', '—', ':'])
            .trim();
        if hits.len() >= MAX_HITS {
            break;
        }
        hits.push(WebSearchResult {
            title: RuntimeString::from(if title.is_empty() {
                "(untitled)"
            } else {
                title
            }),
            url: RuntimeString::from(url),
            snippet: None,
        });
    }
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_result_list_under_any_known_key_maps_onto_hits() {
        let hits = parse_search_payload(
            &json!({ "results": [ { "title": "GLM", "url": "https://z.ai", "snippet": "Plan." } ] })
                .to_string(),
        )
        .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].title.try_to_utf8().unwrap(), "GLM");
        let bare = parse_search_payload(
            &json!([ { "name": "Bare", "link": "https://example.test/x" } ]).to_string(),
        )
        .unwrap();
        assert_eq!(bare[0].title.try_to_utf8().unwrap(), "Bare");
    }

    #[test]
    fn a_prose_payload_still_yields_its_links() {
        let hits =
            parse_search_payload("Sources:\n- Docs - https://docs.example.test/guide\n").unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(
            hits[0].url.try_to_utf8().unwrap(),
            "https://docs.example.test/guide"
        );
    }

    #[test]
    fn an_event_stream_reply_is_decoded_by_id() {
        let body = "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"x\":1}}\n\nevent: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":3,\"result\":{\"y\":2}}\n\n";
        let reply = decode_rpc(body, 3).unwrap();
        assert_eq!(reply.pointer("/result/y"), Some(&json!(2)));
        assert!(decode_rpc(body, 9).is_err());
    }

    #[test]
    fn argument_names_follow_the_declared_schema() {
        let schema = json!({ "properties": { "search_query": { "type": "string" }, "count": { "type": "integer" } } });
        assert_eq!(
            argument_names(&schema),
            Some(("search_query", Some("count")))
        );
        assert_eq!(argument_names(&json!({})), None);
    }
}
