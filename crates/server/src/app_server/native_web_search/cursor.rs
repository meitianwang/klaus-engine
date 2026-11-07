//! Account-native search stays separate from the conversation's language model.
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::tools::builtins::web_search::{WebSearchBackend, WebSearchResult};
use std::sync::Arc;

pub(crate) fn backend() -> WebSearchBackend {
    Arc::new(|query, signal| {
        Box::pin(async move {
            let query = query
                .try_to_utf8()
                .map_err(|_| RuntimeString::from("cursor_search_query_invalid"))?;
            let result = crate::app_server::cursor::search(
                crate::app_server::wire::CursorSearchParams {
                    query,
                    model_selection: None,
                    operation_id: None,
                },
                signal,
            )
            .await
            .map_err(RuntimeString::from)?;
            let rows = result.value["results"]
                .as_array()
                .ok_or_else(|| RuntimeString::from("cursor_search_response_invalid"))?;
            if rows.is_empty()
                && result.value["answer"]
                    .as_str()
                    .is_some_and(|answer| !answer.is_empty())
            {
                return Err(RuntimeString::from("cursor_search_citations_unavailable"));
            }
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
