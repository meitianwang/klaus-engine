//! Search uses the selected account service independently of the chat provider.
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::tools::builtins::web_search::{WebSearchBackend, WebSearchResult};
use std::sync::Arc;
pub(super) fn backend() -> WebSearchBackend {
    Arc::new(|query, signal| {
        Box::pin(async move {
            let query = query
                .try_to_utf8()
                .map_err(|_| RuntimeString::from("workbuddy_search_query_invalid"))?;
            let result = crate::app_server::workbuddy::search(
                crate::app_server::wire::WorkBuddySearchParams {
                    query,
                    allowed_domains: vec![],
                    blocked_domains: vec![],
                    freshness: None,
                    session_id: None,
                    operation_id: None,
                },
                signal,
            )
            .await
            .map_err(RuntimeString::from)?;
            let rows = result.value["results"]
                .as_array()
                .ok_or_else(|| RuntimeString::from("workbuddy_search_response_invalid"))?;
            Ok(rows
                .iter()
                .map(|row| WebSearchResult {
                    title: RuntimeString::from(row["title"].as_str().unwrap_or("")),
                    url: RuntimeString::from(row["url"].as_str().unwrap_or("")),
                    snippet: row["snippet"].as_str().map(RuntimeString::from),
                })
                .collect())
        })
    })
}
