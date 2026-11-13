//! Choose native-vs-client web search for a turn. Provider-NEUTRAL: the native
//! wire spec lives entirely inside each adapter; here we only decide which search
//! tool the model sees and whether to set `ModelRequest.web_search`.

use crate::schemas::config::{WebSearchAccess, WebSearchContextSize, WebSearchMode};
use crate::types::provider::{
    UserLocation, WebSearchContextSize as ProviderWebSearchContextSize, WebSearchRequest,
};
use crate::types::tool::Tool;
use std::sync::Arc;

// Host-facing web-search config (mirrors `config.web_search`).
pub use crate::schemas::config::WebSearchConfig;

/// The engine name of the client-side browser or host-provided search tool.
pub const CLIENT_WEB_SEARCH_TOOL: &str = "web_search";

/// Model subscriptions whose own search service backs the client `web_search`
/// tool when named by `WebSearchConfig::source`.
pub const SUBSCRIPTION_SEARCH_SOURCES: [&str; 13] = [
    "trae",
    "kiro",
    "subscription",
    "codex",
    "cursor",
    "qoder",
    "qwenwork",
    "workbuddy",
    "antigravity",
    "kimi",
    "bailian",
    "minimax",
    "zhipu",
];

/// True when the config names a subscription search source, which keeps the
/// client tool (its backend is that subscription's service) and never sets a
/// native search request on the session model.
pub fn subscription_search_source(config: Option<&WebSearchConfig>) -> Option<&str> {
    config
        .and_then(|c| c.source.as_deref())
        .filter(|source| SUBSCRIPTION_SEARCH_SOURCES.contains(source))
}

/// The chosen tool pool + optional native-search request for a turn.
pub struct WebSearchSelection {
    /// The tool pool with the client `web_search` tool dropped iff native search is
    /// in effect (or `off`).
    pub tools: Vec<Arc<dyn Tool>>,
    /// Set when native server search is selected — flows to
    /// `ModelRequest.web_search`.
    pub request: Option<WebSearchRequest>,
}

impl std::fmt::Debug for WebSearchSelection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<_> = self.tools.iter().map(|t| t.name()).collect();
        f.debug_struct("WebSearchSelection")
            .field("tools", &names)
            .field("request", &self.request)
            .finish()
    }
}

fn without_client_tool(tools: Vec<Arc<dyn Tool>>) -> Vec<Arc<dyn Tool>> {
    tools
        .into_iter()
        .filter(|t| t.name() != CLIENT_WEB_SEARCH_TOOL)
        .collect()
}

/// Decide native-vs-client web search for a turn. `capable` is
/// `provider.capabilities(model).web_search`.
///
///  - `off` → drop the client `web_search` tool, no native flag (no search this
///    turn).
///  - native in effect (`native`/`auto` AND `capable`) → drop the client tool (so
///    the model sees ONE search surface) and emit the native request.
///  - otherwise (`client`, or `native`/`auto` on a non-capable model) → keep the
///    client `web_search` tool (graceful fallback — search still works via the
///    browser/injectable backend).
pub fn select_web_search(
    tools: Vec<Arc<dyn Tool>>,
    config: Option<&WebSearchConfig>,
    capable: bool,
) -> WebSearchSelection {
    let mode = config.map(|c| c.mode).unwrap_or(WebSearchMode::Client);
    let access = config.map(|c| c.access).unwrap_or_default();
    if access == WebSearchAccess::Disabled
        || matches!(mode, WebSearchMode::Off | WebSearchMode::Disabled)
    {
        return WebSearchSelection {
            tools: without_client_tool(tools),
            request: None,
        };
    }
    // A named subscription source owns every search: the client tool stays (its
    // injected backend is that subscription's service) and the session model
    // never gets a native search request, even when it would be capable.
    if subscription_search_source(config).is_some() {
        return WebSearchSelection {
            tools,
            request: None,
        };
    }
    let wants_native = matches!(
        mode,
        WebSearchMode::Native
            | WebSearchMode::Auto
            | WebSearchMode::Indexed
            | WebSearchMode::Cached
            | WebSearchMode::Live
    );
    let use_native = wants_native && capable;
    if !use_native {
        return WebSearchSelection {
            tools,
            request: None,
        };
    }
    let request = WebSearchRequest {
        access,
        max_uses: config.and_then(|c| c.max_uses),
        allowed_domains: config.and_then(|c| c.allowed_domains.clone()),
        blocked_domains: config.and_then(|c| c.blocked_domains.clone()),
        user_location: config
            .and_then(|c| c.user_location.as_ref())
            .map(|u| UserLocation {
                city: u.city.clone(),
                region: u.region.clone(),
                country: u.country.clone(),
                timezone: u.timezone.clone(),
            }),
        context_size: config.and_then(|c| {
            c.context_size.map(|size| match size {
                WebSearchContextSize::Low => ProviderWebSearchContextSize::Low,
                WebSearchContextSize::Medium => ProviderWebSearchContextSize::Medium,
                WebSearchContextSize::High => ProviderWebSearchContextSize::High,
            })
        }),
    };
    WebSearchSelection {
        tools: without_client_tool(tools),
        request: Some(request),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::build_tool::text_result;
    use crate::types::errors::ToolError;
    use crate::types::json::{Json, JsonSchema};
    use crate::types::tool::{ToolContext, ToolProgress, ToolResult};
    use async_trait::async_trait;

    struct StubTool {
        name: String,
        schema: JsonSchema,
    }

    #[async_trait]
    impl Tool for StubTool {
        fn static_name(&self) -> &str {
            &self.name
        }
        fn static_description(&self) -> &str {
            "stub"
        }
        fn static_input_schema(&self) -> &JsonSchema {
            &self.schema
        }
        fn is_enabled(&self) -> bool {
            true
        }
        fn is_concurrency_safe(&self, _input: &Json) -> bool {
            true
        }
        fn is_read_only(&self, _input: &Json) -> bool {
            true
        }
        async fn call(
            &self,
            _input: Json,
            _ctx: &ToolContext,
            _on_progress: Option<ToolProgress>,
        ) -> Result<ToolResult, ToolError> {
            Ok(text_result(Json::Null, String::new(), false))
        }
    }

    fn tool(name: &str) -> Arc<dyn Tool> {
        Arc::new(StubTool {
            name: name.into(),
            schema: JsonSchema::new(),
        })
    }

    fn pool() -> Vec<Arc<dyn Tool>> {
        vec![tool("bash"), tool(CLIENT_WEB_SEARCH_TOOL), tool("read")]
    }

    fn names(sel: &WebSearchSelection) -> Vec<crate::runtime_values::RuntimeString> {
        sel.tools.iter().map(|t| t.name()).collect()
    }

    #[test]
    fn off_drops_client_tool_no_request() {
        let cfg = WebSearchConfig {
            mode: WebSearchMode::Off,
            ..Default::default()
        };
        let sel = select_web_search(pool(), Some(&cfg), true);
        assert!(!names(&sel).contains(&CLIENT_WEB_SEARCH_TOOL.into()));
        assert!(sel.request.is_none());
    }

    #[test]
    fn codex_disabled_mode_alias_drops_client_tool_no_request() {
        let cfg = WebSearchConfig {
            mode: WebSearchMode::Disabled,
            ..Default::default()
        };
        let sel = select_web_search(pool(), Some(&cfg), true);
        assert!(!names(&sel).contains(&CLIENT_WEB_SEARCH_TOOL.into()));
        assert!(sel.request.is_none());
    }

    #[test]
    fn legacy_modes_preserve_search_access_on_the_native_surface() {
        for (mode, access) in [
            ("cached", WebSearchAccess::Cached),
            ("indexed", WebSearchAccess::Indexed),
            ("live", WebSearchAccess::Live),
        ] {
            let cfg: WebSearchConfig =
                serde_json::from_value(serde_json::json!({"mode":mode})).unwrap();
            let selected = select_web_search(pool(), Some(&cfg), true);
            assert!(!names(&selected).contains(&CLIENT_WEB_SEARCH_TOOL.into()));
            assert_eq!(selected.request.unwrap().access, access);
        }
    }

    #[test]
    fn native_capable_drops_client_and_emits_request() {
        let cfg = WebSearchConfig {
            mode: WebSearchMode::Native,
            max_uses: Some(3),
            ..Default::default()
        };
        let sel = select_web_search(pool(), Some(&cfg), true);
        assert!(!names(&sel).contains(&CLIENT_WEB_SEARCH_TOOL.into()));
        let req = sel.request.expect("native request");
        assert_eq!(req.max_uses, Some(3));
    }

    #[test]
    fn native_request_preserves_codex_context_size() {
        let cfg = WebSearchConfig {
            mode: WebSearchMode::Native,
            context_size: Some(WebSearchContextSize::High),
            ..Default::default()
        };
        let sel = select_web_search(pool(), Some(&cfg), true);
        assert_eq!(
            sel.request.and_then(|r| r.context_size),
            Some(ProviderWebSearchContextSize::High)
        );
    }

    #[test]
    fn native_non_capable_keeps_client_tool() {
        let cfg = WebSearchConfig {
            mode: WebSearchMode::Native,
            ..Default::default()
        };
        let sel = select_web_search(pool(), Some(&cfg), false);
        assert!(names(&sel).contains(&CLIENT_WEB_SEARCH_TOOL.into()));
        assert!(sel.request.is_none());
    }

    #[test]
    fn a_named_subscription_source_keeps_the_client_tool_on_a_capable_model() {
        // The subscription's own service answers through the client tool's
        // injected backend, so the tool must survive AND the session model
        // must not get a native search request of its own.
        for source in SUBSCRIPTION_SEARCH_SOURCES {
            let cfg = WebSearchConfig {
                mode: WebSearchMode::Native,
                source: Some(source.to_string()),
                ..Default::default()
            };
            let sel = select_web_search(pool(), Some(&cfg), true);
            assert!(
                names(&sel).contains(&CLIENT_WEB_SEARCH_TOOL.into()),
                "{source} must keep the client tool"
            );
            assert!(
                sel.request.is_none(),
                "{source} must not set a native request"
            );
        }
    }

    #[test]
    fn off_still_wins_over_a_named_subscription_source() {
        let cfg = WebSearchConfig {
            mode: WebSearchMode::Off,
            source: Some("codex".to_string()),
            ..Default::default()
        };
        let sel = select_web_search(pool(), Some(&cfg), true);
        assert!(!names(&sel).contains(&CLIENT_WEB_SEARCH_TOOL.into()));
        assert!(sel.request.is_none());
    }

    #[test]
    fn an_unknown_source_value_changes_nothing() {
        let cfg = WebSearchConfig {
            mode: WebSearchMode::Native,
            source: Some("brave".to_string()),
            ..Default::default()
        };
        let sel = select_web_search(pool(), Some(&cfg), true);
        assert!(!names(&sel).contains(&CLIENT_WEB_SEARCH_TOOL.into()));
        assert!(sel.request.is_some());
    }

    #[test]
    fn client_mode_keeps_client_tool_even_if_capable() {
        let cfg = WebSearchConfig {
            mode: WebSearchMode::Client,
            ..Default::default()
        };
        let sel = select_web_search(pool(), Some(&cfg), true);
        assert!(names(&sel).contains(&CLIENT_WEB_SEARCH_TOOL.into()));
        assert!(sel.request.is_none());
    }

    #[test]
    fn none_config_defaults_to_client_even_if_provider_is_capable() {
        let sel = select_web_search(pool(), None, true);
        assert!(names(&sel).contains(&CLIENT_WEB_SEARCH_TOOL.into()));
        assert!(sel.request.is_none());
    }
}
