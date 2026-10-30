//! Account-native search does not depend on the conversation model.
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::tools::builtins::web_search::{WebSearchBackend, WebSearchResult};
use std::sync::Arc;

pub(crate) fn backend() -> WebSearchBackend {
    Arc::new(|query, signal| {
        Box::pin(async move {
            let query = query
                .try_to_utf8()
                .map_err(|_| RuntimeString::from("qwenwork_search_query_invalid"))?;
            let response = crate::app_server::qwenwork::search(
                crate::app_server::wire::QwenWorkSearchParams {
                    query,
                    count: None,
                    time_range: None,
                    session_id: None,
                    operation_id: None,
                },
                signal,
            )
            .await
            .map_err(RuntimeString::from)?;
            let rows = response.value["results"]
                .as_array()
                .ok_or_else(|| RuntimeString::from("qwenwork_search_response_invalid"))?;
            Ok(rows
                .iter()
                .map(|row| WebSearchResult {
                    title: RuntimeString::from(row["title"].as_str().unwrap_or_default()),
                    url: RuntimeString::from(row["url"].as_str().unwrap_or_default()),
                    snippet: row["snippet"].as_str().map(RuntimeString::from),
                })
                .collect())
        })
    })
}
