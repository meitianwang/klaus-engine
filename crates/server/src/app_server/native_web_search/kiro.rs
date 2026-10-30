//! The explicitly selected search source is independent of the chat model.
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::tools::builtins::web_search::{WebSearchBackend, WebSearchResult};
use std::sync::Arc;
pub(super) fn backend() -> WebSearchBackend {
    Arc::new(|query, signal| {
        Box::pin(async move {
            crate::app_server::kiro::set_config_root(&super::config_root());
            let query = query
                .try_to_utf8()
                .map_err(|_| RuntimeString::from("kiro_search_query_invalid"))?;
            let response = crate::app_server::kiro::search(&query, signal)
                .await
                .map_err(RuntimeString::from)?;
            let rows = response["results"]
                .as_array()
                .ok_or_else(|| RuntimeString::from("kiro_search_response_invalid"))?;
            Ok(rows
                .iter()
                .filter_map(|row| {
                    Some(WebSearchResult {
                        title: row["title"].as_str()?.into(),
                        url: row["url"].as_str()?.into(),
                        snippet: row["snippet"].as_str().map(RuntimeString::from),
                    })
                })
                .collect())
        })
    })
}
