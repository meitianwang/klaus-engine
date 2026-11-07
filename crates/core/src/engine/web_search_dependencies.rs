//! Keep the browser read needed by a restricted research child callable without
//! granting that child the browser's general tool surface.

use std::sync::Arc;

use async_trait::async_trait;

use crate::browser::{
    BrowserHostToolMapping, BrowserOperation, BrowserPermissionSet, BrowserRuntimeRequest,
    BrowserTarget, BrowserWaitUntil,
};
use crate::runtime_values::{runtime_codec, RuntimeString};
use crate::types::errors::ToolError;
use crate::types::json::Json;
use crate::types::permission::PermissionResult;
use crate::types::tool::{
    BrowserDependencyPersistence, PathTargets, PermissionPathKind, Tool, ToolContext, ToolExposure,
    ToolInputKind, ToolProgress, ToolResult, ValidationResult,
};

use super::web_search_select::CLIENT_WEB_SEARCH_TOOL;

const SEARCH_DEPENDENCY_ERROR: &str =
    "This browser dependency only accepts a bounded Bing search page read for web_search.";

/// Restore only the selected browser's bounded search read after role filtering.
/// Explicitly denied tools remain unavailable, and tools already granted by the
/// role retain their original exposure and behavior.
pub(crate) fn retain_browser_search_dependency(
    tools: &mut Vec<Arc<dyn Tool>>,
    available: impl IntoIterator<Item = Arc<dyn Tool>>,
    mapping: Option<&BrowserHostToolMapping>,
    disallowed: &[RuntimeString],
    disabled: &[RuntimeString],
) {
    let Some(mapping) = mapping else { return };
    let dependency_name = RuntimeString::from(mapping.tabs_content.as_str());
    let search_name = RuntimeString::from(CLIENT_WEB_SEARCH_TOOL);
    if !tools.iter().any(|tool| tool.name() == search_name)
        || tools.iter().any(|tool| tool.name() == dependency_name)
        || disallowed
            .iter()
            .chain(disabled)
            .any(|name| name == &dependency_name || name == &search_name)
    {
        return;
    }
    if let Some(inner) = available
        .into_iter()
        .find(|tool| tool.name() == dependency_name && tool.is_enabled())
    {
        tools.push(Arc::new(BrowserSearchDependency { inner }));
    }
}

pub(crate) fn validate_search_request(input: &Json) -> Result<(), ToolError> {
    let invalid = || ToolError::new(SEARCH_DEPENDENCY_ERROR);
    let request: BrowserRuntimeRequest =
        runtime_codec::decode_value(input).map_err(|_| invalid())?;
    // Decoders accept compatibility fields elsewhere. A hidden dependency must
    // not pass unknown host overrides through to the browser implementation.
    if runtime_codec::encode_value(&request).map_err(|_| invalid())? != *input
        || request.target != BrowserTarget::default()
        || !matches!(request.timeout_ms, Some(1..=20_000))
        || request.metadata.len() != 1
        || request.metadata.get(&RuntimeString::from("toolName"))
            != Some(&Json::String(CLIENT_WEB_SEARCH_TOOL.into()))
        || request.permissions != BrowserPermissionSet::for_operation(&request.operation)
    {
        return Err(invalid());
    }
    let BrowserOperation::TabsContent {
        urls,
        content_type,
        max_chars,
        wait_until,
    } = request.operation
    else {
        return Err(invalid());
    };
    if urls.len() != 1
        || content_type.as_ref().is_none_or(|value| value != "html")
        || !matches!(max_chars, Some(1..=200_000))
        || wait_until != Some(BrowserWaitUntil::DomContentLoaded)
    {
        return Err(invalid());
    }
    let address = urls[0].try_to_utf8().map_err(|_| invalid())?;
    let url = reqwest::Url::parse(&address).map_err(|_| invalid())?;
    if url.scheme() != "https"
        || url.host_str() != Some("www.bing.com")
        || url.port().is_some()
        || url.path() != "/search"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid());
    }
    let mut pairs = url.query_pairs();
    if !matches!(pairs.next(), Some((key, query)) if key == "q" && !query.trim().is_empty() && query.encode_utf16().count() <= 4_096)
        || pairs.next().is_some()
    {
        return Err(invalid());
    }
    Ok(())
}

struct BrowserSearchDependency {
    inner: Arc<dyn Tool>,
}

#[async_trait]
impl Tool for BrowserSearchDependency {
    fn native_mcp_binding(&self) -> Option<crate::mcp::tool_wrapper::NativeMcpToolBinding> {
        self.inner.native_mcp_binding()
    }
    fn rebind_native_mcp(&self, replacement: Arc<dyn Tool>) -> Arc<dyn Tool> {
        Arc::new(Self {
            inner: self.inner.rebind_native_mcp(replacement),
        })
    }
    fn approval_metadata(&self) -> Option<crate::types::permission::ApprovalActionMetadata> {
        self.inner.approval_metadata()
    }

    fn persistence_identity(&self) -> Option<RuntimeString> {
        self.inner
            .persistence_identity()
            .map(|identity| RuntimeString::from("restricted-browser-search:1:").concat(&identity))
    }
    fn browser_dependency_persistence(&self) -> Option<BrowserDependencyPersistence> {
        Some(BrowserDependencyPersistence::Search {
            inner: self.inner.clone(),
        })
    }
    fn browser_dependency_owners(&self) -> Vec<RuntimeString> {
        vec![CLIENT_WEB_SEARCH_TOOL.into()]
    }
    fn browser_dependency_authorization(
        &self,
        caller: Option<&RuntimeString>,
        input: &Json,
    ) -> Option<bool> {
        Some(
            caller.is_some_and(|caller| caller == CLIENT_WEB_SEARCH_TOOL)
                && validate_search_request(input).is_ok(),
        )
    }
    fn name(&self) -> RuntimeString {
        self.inner.name()
    }
    fn description(&self) -> RuntimeString {
        self.inner.description()
    }
    fn input_schema(&self) -> Json {
        self.inner.input_schema()
    }
    fn input_kind(&self) -> ToolInputKind {
        self.inner.input_kind()
    }
    fn output_schema(&self) -> Option<Json> {
        self.inner.output_schema()
    }
    fn exposure(&self) -> ToolExposure {
        ToolExposure::Hidden
    }
    fn max_result_size_chars(&self) -> Option<u64> {
        self.inner.max_result_size_chars()
    }
    fn mcp_server(&self) -> Option<RuntimeString> {
        self.inner.mcp_server()
    }
    fn is_enabled(&self) -> bool {
        self.inner.is_enabled()
    }
    fn is_concurrency_safe(&self, input: &Json) -> bool {
        self.inner.is_concurrency_safe(input)
    }
    fn is_read_only(&self, input: &Json) -> bool {
        self.inner.is_read_only(input)
    }
    fn is_goal_state_control(&self) -> bool {
        self.inner.is_goal_state_control()
    }
    fn planning_capability(&self, input: &Json) -> crate::types::tool::PlanningToolCapability {
        self.inner.planning_capability(input)
    }
    fn is_destructive(&self, input: &Json) -> Option<bool> {
        self.inner.is_destructive(input)
    }
    fn parse_input(&self, input: &Json) -> Result<Json, ToolError> {
        validate_search_request(input)?;
        let parsed = self.inner.parse_input(input)?;
        validate_search_request(&parsed)?;
        Ok(parsed)
    }
    async fn validate_input(&self, input: &Json, ctx: &ToolContext) -> Option<ValidationResult> {
        if validate_search_request(input).is_err() {
            return Some(ValidationResult::Err {
                message: SEARCH_DEPENDENCY_ERROR.to_string(),
                error_code: None,
            });
        }
        self.inner.validate_input(input, ctx).await
    }
    async fn check_permissions(&self, input: &Json, ctx: &ToolContext) -> Option<PermissionResult> {
        self.inner.check_permissions(input, ctx).await
    }
    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        // Hooks may replace an already parsed input, so enforce the boundary at
        // execution as well as before permissions and hook dispatch.
        validate_search_request(&input)?;
        self.inner.call(input, ctx, on_progress).await
    }
    async fn call_with_id(
        &self,
        call_id: &RuntimeString,
        input: Json,
        ctx: &ToolContext,
        on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        validate_search_request(&input)?;
        self.inner
            .call_with_id(call_id, input, ctx, on_progress)
            .await
    }
    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        self.inner.summarize(input)
    }
    fn to_command_string(&self, input: &Json) -> Option<RuntimeString> {
        self.inner.to_command_string(input)
    }
    fn get_path(&self, input: &Json) -> Option<PathTargets> {
        self.inner.get_path(input)
    }
    fn get_raw_path(&self, input: &Json) -> Option<Json> {
        self.inner.get_raw_path(input)
    }
    fn permission_path_kind(&self) -> Option<PermissionPathKind> {
        self.inner.permission_path_kind()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::build_tool::text_result;
    use crate::types::json::JsonSchema;

    struct StubTool {
        name: String,
        enabled: bool,
        schema: JsonSchema,
    }

    #[async_trait]
    impl Tool for StubTool {
        fn static_name(&self) -> &str {
            &self.name
        }
        fn static_description(&self) -> &str {
            "Dependency fixture"
        }
        fn static_input_schema(&self) -> &JsonSchema {
            &self.schema
        }
        fn is_enabled(&self) -> bool {
            self.enabled
        }
        fn is_concurrency_safe(&self, _: &Json) -> bool {
            false
        }
        fn is_read_only(&self, _: &Json) -> bool {
            true
        }
        async fn call(
            &self,
            _: Json,
            _: &ToolContext,
            _: Option<ToolProgress>,
        ) -> Result<ToolResult, ToolError> {
            Ok(text_result(Json::Null, "fixture", false))
        }
    }

    fn tool(name: &str) -> Arc<dyn Tool> {
        Arc::new(StubTool {
            name: name.to_string(),
            enabled: true,
            schema: JsonSchema::new(),
        })
    }

    fn request() -> BrowserRuntimeRequest {
        let mut request = BrowserRuntimeRequest::new(
            BrowserOperation::TabsContent {
                urls: vec!["https://www.bing.com/search?q=rust%20documentation".into()],
                content_type: Some("html".into()),
                max_chars: Some(200_000),
                wait_until: Some(BrowserWaitUntil::DomContentLoaded),
            },
            BrowserTarget::default(),
            Some(20_000),
        );
        request
            .metadata
            .insert("toolName".into(), Json::String("web_search".into()));
        request
    }

    fn assert_rejected(request: BrowserRuntimeRequest) {
        assert!(validate_search_request(&runtime_codec::encode_value(&request).unwrap()).is_err());
    }

    #[test]
    fn retains_only_the_selected_browser_read_as_hidden() {
        let mapping = BrowserHostToolMapping::for_mcp_server("selected");
        let mut tools = vec![tool("web_search")];
        retain_browser_search_dependency(
            &mut tools,
            vec![
                tool("mcp__other__tabs_content"),
                tool(&mapping.eval),
                tool(&mapping.navigate),
                tool(&mapping.tabs_content),
            ],
            Some(&mapping),
            &[],
            &[],
        );
        assert_eq!(tools.len(), 2);
        assert_eq!(tools[1].name(), mapping.tabs_content.as_str());
        assert_eq!(tools[1].exposure(), ToolExposure::Hidden);
        assert!(tools[1].aliases().is_empty());
    }

    #[test]
    fn preserves_explicit_denials_absence_and_existing_grants() {
        let mapping = BrowserHostToolMapping::for_mcp_server("selected");
        for blocked in [mapping.tabs_content.as_str(), CLIENT_WEB_SEARCH_TOOL] {
            for disabled in [false, true] {
                let mut tools = vec![tool("web_search")];
                let denied = vec![RuntimeString::from(blocked)];
                retain_browser_search_dependency(
                    &mut tools,
                    vec![tool(&mapping.tabs_content)],
                    Some(&mapping),
                    if disabled { &[] } else { &denied },
                    if disabled { &denied } else { &[] },
                );
                assert_eq!(tools.len(), 1);
            }
        }
        let dependency = tool(&mapping.tabs_content);
        let mut granted = vec![tool("web_search"), dependency.clone()];
        retain_browser_search_dependency(
            &mut granted,
            vec![dependency.clone()],
            Some(&mapping),
            &[],
            &[],
        );
        assert_eq!(granted.len(), 2);
        assert!(Arc::ptr_eq(&granted[1], &dependency));
        for has_search in [false, true] {
            let mut tools = if has_search {
                vec![tool("web_search")]
            } else {
                vec![]
            };
            let count = tools.len();
            retain_browser_search_dependency(
                &mut tools,
                vec![dependency.clone()],
                if has_search { None } else { Some(&mapping) },
                &[],
                &[],
            );
            assert_eq!(tools.len(), count);
        }
        let mut tools = vec![tool("web_search")];
        retain_browser_search_dependency(
            &mut tools,
            vec![Arc::new(StubTool {
                name: mapping.tabs_content.clone(),
                enabled: false,
                schema: JsonSchema::new(),
            }) as Arc<dyn Tool>],
            Some(&mapping),
            &[],
            &[],
        );
        assert_eq!(tools.len(), 1);
    }

    #[test]
    fn rejects_broader_operations_origins_and_unbounded_reads() {
        assert!(validate_search_request(&runtime_codec::encode_value(&request()).unwrap()).is_ok());
        for address in [
            "https://example.com/search?q=rust",
            "https://bing.com/search?q=rust",
            "http://www.bing.com/search?q=rust",
            "https://www.bing.com:444/search?q=rust",
            "https://user@www.bing.com/search?q=rust",
            "https://www.bing.com/?q=rust",
            "https://www.bing.com/search",
            "https://www.bing.com/search?q=",
            "https://www.bing.com/search?q=%20",
            "https://www.bing.com/search?q=rust&q=other",
            "https://www.bing.com/search?q=rust&url=https://example.com",
            "https://www.bing.com/search?q=rust#fragment",
        ] {
            let mut value = request();
            if let BrowserOperation::TabsContent { urls, .. } = &mut value.operation {
                *urls = vec![address.into()];
            }
            assert_rejected(value);
        }
        for chars in [None, Some(0), Some(200_001)] {
            let mut value = request();
            if let BrowserOperation::TabsContent { max_chars, .. } = &mut value.operation {
                *max_chars = chars;
            }
            assert_rejected(value);
        }
        for query in ["a".repeat(4_097), "\u{1f50e}".repeat(2_049)] {
            let mut address = reqwest::Url::parse("https://www.bing.com/search").unwrap();
            address.query_pairs_mut().append_pair("q", &query);
            let mut value = request();
            if let BrowserOperation::TabsContent { urls, .. } = &mut value.operation {
                *urls = vec![address.as_str().into()];
            }
            assert_rejected(value);
        }
        for timeout in [None, Some(0), Some(20_001)] {
            let mut value = request();
            value.timeout_ms = timeout;
            assert_rejected(value);
        }
        for change in 0..10 {
            let mut value = request();
            match change {
                0 => value.target.tab_id = Some("1".into()),
                1 => value.target.session_id = Some("another-task".into()),
                2 => value.target.browser_id = Some("chrome".into()),
                3 => value.permissions.javascript_eval = true,
                4 => {
                    value.metadata.clear();
                }
                5 => {
                    if let BrowserOperation::TabsContent { urls, .. } = &mut value.operation {
                        urls.push("https://www.bing.com/search?q=other".into());
                    }
                }
                6 => {
                    if let BrowserOperation::TabsContent { content_type, .. } = &mut value.operation
                    {
                        *content_type = Some("text".into());
                    }
                }
                7 => {
                    if let BrowserOperation::TabsContent { wait_until, .. } = &mut value.operation {
                        *wait_until = None;
                    }
                }
                8 => {
                    value
                        .metadata
                        .insert("browserId".into(), Json::from("chrome"));
                }
                _ => {
                    value.operation = BrowserOperation::Eval {
                        script: "document.title".into(),
                        args: None,
                        await_promise: false,
                    };
                    value.permissions = BrowserPermissionSet::for_operation(&value.operation);
                }
            }
            assert_rejected(value);
        }
        let mut value = runtime_codec::encode_value(&request()).unwrap();
        if let Json::Object(object) = &mut value {
            object.insert("browserId", Json::from("chrome"));
        }
        assert!(validate_search_request(&value).is_err());
    }
}
