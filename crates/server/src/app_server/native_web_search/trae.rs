//! Search runs under its own selected account, independently of the chat source.
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::tools::builtins::web_search::{WebSearchBackend, WebSearchResult};
use serde_json::json;
use std::sync::Arc;

pub(super) fn backend() -> WebSearchBackend {
    Arc::new(|query, signal| {
        Box::pin(async move {
            crate::app_server::trae::set_config_root(&super::config_root());
            let query = query
                .try_to_utf8()
                .map_err(|_| RuntimeString::from("trae_search_query_invalid"))?;
            let response = crate::app_server::trae::native::search(json!({"query":query}), signal)
                .await
                .map_err(RuntimeString::from)?;
            if !(response.publication_check)() {
                return Err("trae_credential_changed".into());
            }
            let rows = response.value["results"]
                .as_array()
                .ok_or_else(|| RuntimeString::from("trae_search_response_invalid"))?;
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
