//! Retain only the selected host operations required by a child's granted browser tools.

use crate::browser::{BrowserHostToolMapping, BrowserPermissionSet, BrowserRuntimeRequest};
use crate::runtime_values::{runtime_codec, RuntimeString};
use crate::types::errors::ToolError;
use crate::types::json::Json;
use crate::types::permission::PermissionResult;
use crate::types::tool::{
    BrowserDependencyPersistence, PathTargets, PermissionPathKind, Tool, ToolContext, ToolExposure,
    ToolInputKind, ToolProgress, ToolResult, ValidationResult,
};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::Arc;

const BROWSER_DEPENDENCY_ERROR: &str =
    "This browser dependency only accepts a typed request from a granted browser tool.";

pub(super) fn bindings(mapping: &BrowserHostToolMapping) -> [(&'static str, &str); 36] {
    [
        ("browser_navigate", mapping.navigate.as_str()),
        ("browser_snapshot", mapping.snapshot.as_str()),
        ("browser_screenshot", mapping.screenshot.as_str()),
        ("browser_click", mapping.click.as_str()),
        ("browser_type", mapping.type_text.as_str()),
        ("browser_eval", mapping.eval.as_str()),
        ("browser_console", mapping.console.as_str()),
        ("browser_network", mapping.network.as_str()),
        ("browser_read_page", mapping.read_page.as_str()),
        ("browser_find", mapping.find.as_str()),
        ("browser_form_input", mapping.form_input.as_str()),
        ("browser_computer", mapping.computer.as_str()),
        ("browser_tabs", mapping.tabs.as_str()),
        ("browser_locator", mapping.locator.as_str()),
        ("browser_page_text", mapping.page_text.as_str()),
        ("browser_batch", mapping.batch.as_str()),
        ("browser_resize", mapping.resize.as_str()),
        ("browser_close", mapping.close.as_str()),
        ("browser_gif_creator", mapping.gif_creator.as_str()),
        ("browser_upload_image", mapping.upload_image.as_str()),
        ("browser_file_upload", mapping.file_upload.as_str()),
        ("browser_shortcuts_list", mapping.shortcuts_list.as_str()),
        (
            "browser_shortcuts_execute",
            mapping.shortcuts_execute.as_str(),
        ),
        ("browser_switch_browser", mapping.switch_browser.as_str()),
        (
            "browser_list_connected_browsers",
            mapping.list_connected_browsers.as_str(),
        ),
        ("browser_select_browser", mapping.select_browser.as_str()),
        ("browser_resolve_tab", mapping.resolve_tab.as_str()),
        ("browser_claim_tab", mapping.claim_tab.as_str()),
        ("browser_history", mapping.history.as_str()),
        ("browser_tabs_content", mapping.tabs_content.as_str()),
        ("browser_content_export", mapping.content_export.as_str()),
        ("browser_download", mapping.download.as_str()),
        ("browser_clipboard", mapping.clipboard.as_str()),
        ("browser_dialog", mapping.dialog.as_str()),
        ("browser_mark_tab", mapping.mark_tab.as_str()),
        ("browser_permission", mapping.permission.as_str()),
    ]
}

pub(crate) fn retain_browser_tool_dependencies(
    tools: &mut Vec<Arc<dyn Tool>>,
    available: impl IntoIterator<Item = Arc<dyn Tool>>,
    mapping: Option<&BrowserHostToolMapping>,
    disallowed: &[RuntimeString],
    disabled: &[RuntimeString],
) {
    let Some(mapping) = mapping else { return };
    let denied = |name: &RuntimeString| {
        disallowed
            .iter()
            .chain(disabled)
            .any(|denied| denied == name)
    };
    let mut needed: BTreeMap<RuntimeString, Vec<RuntimeString>> = BTreeMap::new();
    for (native, dependency) in bindings(mapping) {
        let native = RuntimeString::from(native);
        let dependency = RuntimeString::from(dependency);
        if denied(&native)
            || denied(&dependency)
            || !tools
                .iter()
                .any(|tool| tool.is_enabled() && tool.name() == native)
            || tools
                .iter()
                .any(|tool| tool.name() == dependency && tool.exposure() != ToolExposure::Hidden)
        {
            continue;
        }
        needed.entry(dependency).or_default().push(native);
    }
    let search = RuntimeString::from("web_search");
    let search_dependency = RuntimeString::from(mapping.tabs_content.as_str());
    if !denied(&search)
        && !denied(&search_dependency)
        && tools
            .iter()
            .any(|tool| tool.is_enabled() && tool.name() == search)
        && !tools
            .iter()
            .any(|tool| tool.name() == search_dependency && tool.exposure() != ToolExposure::Hidden)
    {
        needed.entry(search_dependency).or_default().push(search);
    }
    for inner in available {
        if !inner.is_enabled() {
            continue;
        }
        let Some(owners) = needed.remove(&inner.name()) else {
            continue;
        };
        tools.retain(|tool| tool.name() != inner.name() || tool.exposure() != ToolExposure::Hidden);
        tools.push(Arc::new(BrowserToolDependency {
            inner,
            owners,
            mapping: mapping.clone(),
        }));
    }
}

struct BrowserToolDependency {
    inner: Arc<dyn Tool>,
    owners: Vec<RuntimeString>,
    mapping: BrowserHostToolMapping,
}

impl BrowserToolDependency {
    fn validate_request(&self, input: &Json) -> Result<(), ToolError> {
        let invalid = || ToolError::new(BROWSER_DEPENDENCY_ERROR);
        let request: BrowserRuntimeRequest =
            runtime_codec::decode_value(input).map_err(|_| invalid())?;
        let search = RuntimeString::from("web_search");
        if self.owners.contains(&search)
            && self.inner.name() == self.mapping.tabs_content.as_str()
            && request.metadata.get(&RuntimeString::from("toolName")) == Some(&Json::String(search))
        {
            return super::web_search_dependencies::validate_search_request(input);
        }
        let operation = runtime_codec::encode_value(&request.operation).map_err(|_| invalid())?;
        let owner = RuntimeString::from("browser_").concat(
            operation
                .get("type")
                .and_then(Json::as_string)
                .ok_or_else(invalid)?,
        );
        let mapped = bindings(&self.mapping)
            .into_iter()
            .any(|(native, dependency)| owner == native && self.inner.name() == dependency);
        if !mapped
            || !self.owners.contains(&owner)
            || request.metadata.len() != 1
            || request.metadata.get(&RuntimeString::from("toolName")) != Some(&Json::String(owner))
            || request.permissions != BrowserPermissionSet::for_operation(&request.operation)
            || runtime_codec::encode_value(&request).map_err(|_| invalid())? != *input
        {
            return Err(invalid());
        }
        Ok(())
    }
}

#[async_trait]
impl Tool for BrowserToolDependency {
    fn native_mcp_binding(&self) -> Option<crate::mcp::tool_wrapper::NativeMcpToolBinding> {
        self.inner.native_mcp_binding()
    }
    fn rebind_native_mcp(&self, replacement: Arc<dyn Tool>) -> Arc<dyn Tool> {
        Arc::new(Self {
            inner: self.inner.rebind_native_mcp(replacement),
            owners: self.owners.clone(),
            mapping: self.mapping.clone(),
        })
    }
    fn approval_metadata(&self) -> Option<crate::types::permission::ApprovalActionMetadata> {
        self.inner.approval_metadata()
    }

    fn persistence_identity(&self) -> Option<RuntimeString> {
        self.inner.persistence_identity().map(|identity| {
            RuntimeString::from("restricted-browser-operations:1:").concat(&identity)
        })
    }
    fn browser_dependency_persistence(&self) -> Option<BrowserDependencyPersistence> {
        Some(BrowserDependencyPersistence::Operations {
            inner: self.inner.clone(),
            owners: self.owners.clone(),
            mapping: Box::new(self.mapping.clone()),
        })
    }
    fn browser_dependency_owners(&self) -> Vec<RuntimeString> {
        self.owners.clone()
    }
    fn browser_dependency_authorization(
        &self,
        caller: Option<&RuntimeString>,
        input: &Json,
    ) -> Option<bool> {
        Some(caller.is_some_and(|caller| {
            self.owners.contains(caller)
                && input
                    .get("metadata")
                    .and_then(|metadata| metadata.get("toolName"))
                    == Some(&Json::String(caller.clone()))
                && self.validate_request(input).is_ok()
        }))
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
        self.validate_request(input)?;
        let parsed = self.inner.parse_input(input)?;
        self.validate_request(&parsed)?;
        Ok(parsed)
    }
    async fn validate_input(&self, input: &Json, ctx: &ToolContext) -> Option<ValidationResult> {
        if self.validate_request(input).is_err() {
            return Some(ValidationResult::Err {
                message: BROWSER_DEPENDENCY_ERROR.to_string(),
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
        self.validate_request(&input)?;
        self.inner.call(input, ctx, on_progress).await
    }
    async fn call_with_id(
        &self,
        call_id: &RuntimeString,
        input: Json,
        ctx: &ToolContext,
        on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.validate_request(&input)?;
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
    use crate::browser::{BrowserOperation, BrowserTabsAction, BrowserTarget};
    use crate::tools::build_tool::text_result;
    use crate::tools::builtins::browser::create_browser_tools;
    use crate::types::json::JsonSchema;

    struct Host {
        name: String,
        schema: JsonSchema,
    }
    #[async_trait]
    impl Tool for Host {
        fn static_name(&self) -> &str {
            &self.name
        }
        fn static_description(&self) -> &str {
            "Host dependency fixture"
        }
        fn static_input_schema(&self) -> &JsonSchema {
            &self.schema
        }
        fn is_enabled(&self) -> bool {
            true
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
            Ok(text_result(Json::Null, "unused", false))
        }
    }
    fn host(name: &str) -> Arc<dyn Tool> {
        Arc::new(Host {
            name: name.into(),
            schema: Default::default(),
        })
    }
    fn native() -> Arc<dyn Tool> {
        create_browser_tools()
            .into_iter()
            .find(|tool| tool.name() == "browser_tabs")
            .unwrap()
    }

    #[test]
    fn hidden_browser_dependency_is_bound_to_the_granted_owner_and_operation() {
        let mapping = BrowserHostToolMapping::for_mcp_server("selected");
        let mut tools = vec![native()];
        retain_browser_tool_dependencies(
            &mut tools,
            vec![host(&mapping.tabs), host(&mapping.navigate)],
            Some(&mapping),
            &[],
            &[],
        );
        assert_eq!(tools.len(), 2);
        assert_eq!(tools[1].name(), mapping.tabs.as_str());
        assert_eq!(tools[1].exposure(), ToolExposure::Hidden);
        let mut request = BrowserRuntimeRequest::new(
            BrowserOperation::Tabs {
                action: BrowserTabsAction::List,
                url: None,
                name: None,
            },
            BrowserTarget::default(),
            Some(5000),
        );
        request
            .metadata
            .insert("toolName".into(), Json::from("browser_tabs"));
        assert!(tools[1]
            .parse_input(&runtime_codec::encode_value(&request).unwrap())
            .is_ok());
        let canonical = runtime_codec::encode_value(&request).unwrap();
        assert_eq!(
            tools[1].browser_dependency_authorization(None, &canonical),
            Some(false)
        );
        assert_eq!(
            tools[1].browser_dependency_authorization(Some(&"browser_navigate".into()), &canonical),
            Some(false)
        );
        assert_eq!(
            tools[1].browser_dependency_authorization(Some(&"browser_tabs".into()), &canonical),
            Some(true)
        );
        request
            .metadata
            .insert("toolName".into(), Json::from("browser_navigate"));
        assert!(tools[1]
            .parse_input(&runtime_codec::encode_value(&request).unwrap())
            .is_err());
        request
            .metadata
            .insert("toolName".into(), Json::from("browser_tabs"));
        request.operation = BrowserOperation::Navigate {
            url: "https://app.test".into(),
            wait_until: None,
            viewport: None,
        };
        request.permissions = BrowserPermissionSet::for_operation(&request.operation);
        assert!(tools[1]
            .parse_input(&runtime_codec::encode_value(&request).unwrap())
            .is_err());
    }

    #[test]
    fn browser_search_and_native_content_share_one_hidden_host_dependency_without_scope_loss() {
        use crate::browser::BrowserWaitUntil;
        let mapping = BrowserHostToolMapping::for_mcp_server("selected");
        let native = create_browser_tools()
            .into_iter()
            .find(|tool| tool.name() == "browser_tabs_content")
            .unwrap();
        let mut tools = vec![native, host("web_search")];
        retain_browser_tool_dependencies(
            &mut tools,
            vec![host(&mapping.tabs_content)],
            Some(&mapping),
            &[],
            &[],
        );
        assert_eq!(tools.len(), 3);
        let dependency = tools.last().unwrap();
        let mut request = BrowserRuntimeRequest::new(
            BrowserOperation::TabsContent {
                urls: vec!["https://www.bing.com/search?q=rust".into()],
                content_type: Some("html".into()),
                max_chars: Some(1000),
                wait_until: Some(BrowserWaitUntil::DomContentLoaded),
            },
            BrowserTarget::default(),
            Some(5000),
        );
        request
            .metadata
            .insert("toolName".into(), Json::from("web_search"));
        assert!(dependency
            .parse_input(&runtime_codec::encode_value(&request).unwrap())
            .is_ok());
        if let BrowserOperation::TabsContent { urls, .. } = &mut request.operation {
            *urls = vec!["https://app.test/results".into()];
        }
        assert!(dependency
            .parse_input(&runtime_codec::encode_value(&request).unwrap())
            .is_err());
        request
            .metadata
            .insert("toolName".into(), Json::from("browser_tabs_content"));
        assert!(dependency
            .parse_input(&runtime_codec::encode_value(&request).unwrap())
            .is_ok());
        let canonical = runtime_codec::encode_value(&request).unwrap();
        assert_eq!(
            dependency.browser_dependency_authorization(Some(&"web_search".into()), &canonical),
            Some(false),
            "a hook cannot change the request owner on a shared host mapping"
        );
        assert_eq!(
            dependency
                .browser_dependency_authorization(Some(&"browser_tabs_content".into()), &canonical),
            Some(true)
        );
    }

    #[test]
    fn browser_dependency_retention_respects_both_owner_and_host_denials() {
        let mapping = BrowserHostToolMapping::for_mcp_server("selected");
        for blocked in [
            RuntimeString::from("browser_tabs"),
            RuntimeString::from(mapping.tabs.as_str()),
        ] {
            for disabled in [false, true] {
                let mut tools = vec![native()];
                retain_browser_tool_dependencies(
                    &mut tools,
                    vec![host(&mapping.tabs)],
                    Some(&mapping),
                    if disabled {
                        &[]
                    } else {
                        std::slice::from_ref(&blocked)
                    },
                    if disabled {
                        std::slice::from_ref(&blocked)
                    } else {
                        &[]
                    },
                );
                assert_eq!(tools.len(), 1);
            }
        }
        let mut tools = vec![native(), host(&mapping.tabs)];
        retain_browser_tool_dependencies(
            &mut tools,
            vec![host(&mapping.tabs)],
            Some(&mapping),
            &[],
            &[],
        );
        assert_eq!(
            tools.len(),
            2,
            "explicit host grants retain their existing implementation"
        );
    }
}
