//! User-confirmed plugin install request tool.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Map as SchemaMap, Value as SchemaValue};

use crate::extensions::plugins::catalog::{
    default_plugin_catalog, PluginCatalog, PluginInstallFromCatalogOptions,
};
use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimeString};
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{
    ElicitRequest, ElicitResponse, Tool, ToolContext, ToolProgress, ToolResult,
};

#[derive(Default)]
pub struct RequestPluginInstallToolOptions {
    pub backend: Option<Box<dyn PluginCatalog>>,
}

fn build_input_schema() -> JsonSchema {
    let mut slug = SchemaMap::new();
    slug.insert("type".to_string(), json!("string"));
    slug.insert(
        "description".to_string(),
        json!("The exact plugin slug returned by plugin_search"),
    );
    let mut reason = SchemaMap::new();
    reason.insert("type".to_string(), json!("string"));
    reason.insert(
        "description".to_string(),
        json!("One concise, user-facing line on why this plugin helps with the current request"),
    );
    let mut version = SchemaMap::new();
    version.insert("type".to_string(), json!("string"));
    version.insert(
        "description".to_string(),
        json!("Optional plugin version; defaults to catalog latest"),
    );
    let mut properties = SchemaMap::new();
    properties.insert("slug".to_string(), SchemaValue::Object(slug));
    properties.insert("reason".to_string(), SchemaValue::Object(reason));
    properties.insert("version".to_string(), SchemaValue::Object(version));
    let mut schema = SchemaMap::new();
    schema.insert("type".to_string(), json!("object"));
    schema.insert("properties".to_string(), SchemaValue::Object(properties));
    schema.insert("required".to_string(), json!(["slug", "reason"]));
    schema.insert("additionalProperties".to_string(), json!(false));
    schema
}

fn install_output(
    slug: &RuntimeString,
    installed: bool,
    plugin: Option<&str>,
    version: Option<&str>,
) -> RuntimeJson {
    let mut output = RuntimeObject::new();
    output.insert("slug", RuntimeJson::String(slug.clone()));
    output.insert("installed", RuntimeJson::Bool(installed));
    if let Some(plugin) = plugin {
        output.insert("plugin", RuntimeJson::from(plugin));
    }
    if let Some(version) = version {
        output.insert("version", RuntimeJson::from(version));
    } else if plugin.is_some() {
        output.insert("version", RuntimeJson::Null);
    }
    RuntimeJson::Object(output)
}

pub struct RequestPluginInstallTool {
    input_schema: JsonSchema,
    backend: Option<Box<dyn PluginCatalog>>,
}

pub fn create_request_plugin_install_tool(opts: RequestPluginInstallToolOptions) -> Arc<dyn Tool> {
    let backend = opts.backend.or_else(default_plugin_catalog);
    Arc::new(RequestPluginInstallTool {
        input_schema: build_input_schema(),
        backend,
    })
}

#[async_trait]
impl Tool for RequestPluginInstallTool {
    fn static_name(&self) -> &str {
        "request_plugin_install"
    }

    fn static_description(&self) -> &str {
        "Request installation of ONE plugin from an exact plugin_search result. Use the returned slug and explain how the plugin fulfills the user's request or supplies a capability needed for the authorized task. Do not request unrelated plugins or broad recommendations. The user confirms first; only then does the engine ask the host catalog to install it. Discovery or an inferred need does not grant installation approval. If the user declines, continue without it and do not repeat the request unless they bring it up."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let slug = input
            .get("slug")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_default();
        Some(RuntimeString::from("Requesting plugin install: ").concat(&slug))
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let slug = input
            .get("slug")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_default();
        let reason = input
            .get("reason")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_default();
        let version = input
            .get("version")
            .and_then(RuntimeJson::as_string)
            .filter(|v| !v.is_empty())
            .cloned();

        let Some(backend) = self.backend.as_ref() else {
            return Ok(text_result(
                install_output(&slug, false, None, None),
                "No plugin catalog is configured in this build.",
                true,
            ));
        };

        let entry = match backend.get(&slug, Some(ctx.signal.clone())).await {
            Ok(entry) => entry,
            Err(err) => {
                return Ok(text_result(
                    install_output(&slug, false, None, None),
                    format!("Plugin catalog error: {err}"),
                    true,
                ));
            }
        };
        let Some(entry) = entry else {
            return Ok(text_result(
                install_output(&slug, false, None, None),
                RuntimeString::from("No plugin \"")
                    .concat(&slug)
                    .concat(&RuntimeString::from(
                        "\" exists in the catalog. Call plugin_search first and use a slug it returned.",
                    )),
                true,
            ));
        };

        let Some(elicit) = ctx.elicit.as_ref() else {
            return Ok(text_result(
                install_output(&slug, false, None, None),
                RuntimeString::from("Cannot install \"")
                    .concat(&slug)
                    .concat(&RuntimeString::from(
                        "\" without a user to confirm (no interactive session).",
                    )),
                true,
            ));
        };

        let resp = elicit
            .elicit(
                ElicitRequest {
                    questions: None,
                    attachments_supported: None,
                    metadata: None,
                    prompt: RuntimeString::from("Install plugin \"")
                        .concat(&entry.name)
                        .concat(&RuntimeString::from("\" ("))
                        .concat(&entry.slug)
                        .concat(&RuntimeString::from(")?\n"))
                        .concat(&reason)
                        .concat(&RuntimeString::from(
                            "\n\nThis downloads and enables third-party plugin code.",
                        )),
                    options: Some(vec![
                        RuntimeString::from("Install"),
                        RuntimeString::from("Cancel"),
                    ]),
                    multi_select: None,
                },
                ctx.signal.clone(),
            )
            .await;

        let accepted = matches!(
            &resp,
            ElicitResponse::Accept { values } if values.first().is_some_and(|v| v == "Install")
        );
        if !accepted {
            return Ok(text_result(
                install_output(&slug, false, None, None),
                RuntimeString::from("The user declined to install \"")
                    .concat(&slug)
                    .concat(&RuntimeString::from("\". Continue without it.")),
                false,
            ));
        }

        match backend
            .install(
                &slug,
                PluginInstallFromCatalogOptions {
                    version,
                    signal: Some(ctx.signal.clone()),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(Some(plugin)) => Ok(text_result(
                install_output(
                    &slug,
                    true,
                    Some(&plugin.name),
                    plugin.version.as_deref(),
                ),
                format!(
                    "Installed plugin \"{}\". It becomes available after the host refreshes plugin state.",
                    plugin.name
                ),
                false,
            )),
            Ok(None) => Ok(text_result(
                install_output(&slug, false, None, None),
                RuntimeString::from("Install of \"")
                    .concat(&slug)
                    .concat(&RuntimeString::from("\" did not complete.")),
                true,
            )),
            Err(err) => Ok(text_result(
                install_output(&slug, false, None, None),
                format!("Plugin install failed: {err}"),
                true,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extensions::plugins::catalog::{PluginCatalogEntry, PluginCatalogSearchOptions};
    use crate::extensions::plugins::loader::LoadedPlugin;
    use crate::runtime_values::runtime_json;
    use crate::tools::builtins::test_ctx::make_ctx;
    use crate::types::tool::ElicitHandler;
    use std::sync::Mutex;

    type BoxError = Box<dyn std::error::Error + Send + Sync>;

    struct RecordingCatalog {
        installed: Arc<Mutex<Vec<RuntimeString>>>,
    }

    fn catalog_entry(slug: &RuntimeString) -> PluginCatalogEntry {
        PluginCatalogEntry {
            slug: slug.clone(),
            name: RuntimeString::from("Browser"),
            description: RuntimeString::from("Browser runtime plugin"),
            source: Some(RuntimeString::from("OFFICIAL")),
            version: Some(RuntimeString::from("1.0.0")),
            installed: false,
            skills: Vec::new(),
            runtime_capabilities: vec![RuntimeString::from("browser")],
        }
    }

    fn loaded_plugin() -> LoadedPlugin {
        LoadedPlugin {
            name: "browser".to_string(),
            version: Some("1.0.0".to_string()),
            display_name: Some("Browser".to_string()),
            description: Some("Browser runtime plugin".to_string()),
            plugin_namespace: "browser".to_string(),
            keywords: Vec::new(),
            dependencies: Vec::new(),
            user_config: crate::types::json::JsonObject::new(),
            settings: crate::types::json::JsonObject::new(),
            dir: "/plugins/browser".to_string(),
            skills: Vec::new(),
            commands: Vec::new(),
            agents: Vec::new(),
            hooks: Vec::new(),
            output_styles: Vec::new(),
            mcp_servers: Vec::new(),
            lsp_servers: crate::types::json::JsonObject::new(),
            runtime_capabilities: vec!["browser".to_string()],
            interface_capabilities: Vec::new(),
            app_connectors: Vec::new(),
        }
    }

    #[async_trait]
    impl PluginCatalog for RecordingCatalog {
        async fn search(
            &self,
            _query: &RuntimeString,
            _opts: PluginCatalogSearchOptions,
        ) -> Result<Vec<PluginCatalogEntry>, BoxError> {
            Ok(vec![catalog_entry(&RuntimeString::from(
                "browser@official",
            ))])
        }

        async fn get(
            &self,
            slug: &RuntimeString,
            _signal: Option<crate::types::abort::AbortSignal>,
        ) -> Result<Option<PluginCatalogEntry>, BoxError> {
            Ok(Some(catalog_entry(slug)))
        }

        async fn install(
            &self,
            slug: &RuntimeString,
            _opts: PluginInstallFromCatalogOptions,
        ) -> Result<Option<LoadedPlugin>, BoxError> {
            self.installed.lock().unwrap().push(slug.clone());
            Ok(Some(loaded_plugin()))
        }
    }

    fn exact_json(source: &str) -> RuntimeJson {
        runtime_json::parse(&RuntimeString::from(source)).expect("valid exact JSON fixture")
    }

    struct StaticElicit(ElicitResponse);

    #[async_trait]
    impl ElicitHandler for StaticElicit {
        async fn elicit(
            &self,
            _req: ElicitRequest,
            _signal: crate::types::abort::AbortSignal,
        ) -> ElicitResponse {
            self.0.clone()
        }
    }

    #[tokio::test]
    async fn declined_install_does_not_call_catalog_install() {
        let installed = Arc::new(Mutex::new(Vec::new()));
        let tool = create_request_plugin_install_tool(RequestPluginInstallToolOptions {
            backend: Some(Box::new(RecordingCatalog {
                installed: installed.clone(),
            })),
        });
        let mut ctx = make_ctx(".");
        ctx.elicit = Some(Arc::new(StaticElicit(ElicitResponse::Decline {
            reason: Some(RuntimeString::from("no")),
        })));

        let result = tool
            .call(
                exact_json(r#"{"slug":"browser@official","reason":"Need browser automation"}"#),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_ne!(result.serializable.is_error, Some(true));
        assert_eq!(
            result.serializable.output["installed"],
            RuntimeJson::Bool(false)
        );
        assert!(installed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn accepted_install_calls_catalog_install() {
        let installed = Arc::new(Mutex::new(Vec::new()));
        let tool = create_request_plugin_install_tool(RequestPluginInstallToolOptions {
            backend: Some(Box::new(RecordingCatalog {
                installed: installed.clone(),
            })),
        });
        let mut ctx = make_ctx(".");
        ctx.elicit = Some(Arc::new(StaticElicit(ElicitResponse::Accept {
            values: vec![RuntimeString::from("Install")],
        })));

        let result = tool
            .call(
                exact_json(r#"{"slug":"browser@official","reason":"Need browser automation"}"#),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_ne!(result.serializable.is_error, Some(true));
        assert_eq!(
            result.serializable.output["installed"],
            RuntimeJson::Bool(true)
        );
        assert_eq!(
            installed.lock().unwrap().as_slice(),
            [RuntimeString::from("browser@official")]
        );
    }

    #[tokio::test]
    async fn declined_request_preserves_a_lone_surrogate_slug_exactly() {
        let slug = RuntimeString::from_utf16(vec![b'p' as u16, 0xdc00]);
        let installed = Arc::new(Mutex::new(Vec::new()));
        let tool = create_request_plugin_install_tool(RequestPluginInstallToolOptions {
            backend: Some(Box::new(RecordingCatalog {
                installed: installed.clone(),
            })),
        });
        let mut ctx = make_ctx(".");
        ctx.elicit = Some(Arc::new(StaticElicit(ElicitResponse::Decline {
            reason: None,
        })));
        let mut input = RuntimeObject::new();
        input.insert("slug", RuntimeJson::String(slug.clone()));
        input.insert(
            "reason",
            RuntimeJson::String(RuntimeString::from("requested")),
        );
        let result = tool
            .call(RuntimeJson::Object(input), &ctx, None)
            .await
            .unwrap();

        assert_eq!(
            result.serializable.output["slug"],
            RuntimeJson::String(slug)
        );
        match &result.serializable.model_content[0] {
            crate::schemas::content::OutputBlock::Text(text) => {
                assert!(text.text.as_utf16().contains(&0xdc00));
            }
            _ => panic!("expected text output"),
        }
        assert!(installed.lock().unwrap().is_empty());
    }
}
