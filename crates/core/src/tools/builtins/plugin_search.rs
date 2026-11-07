//! Read-only plugin catalog search tool.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Map as SchemaMap, Value as SchemaValue};

use crate::extensions::plugins::catalog::{
    default_plugin_catalog, PluginCatalog, PluginCatalogSearchOptions,
};
use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimeString};
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};

fn search_output(query: &RuntimeString, count: usize, slugs: Vec<RuntimeString>) -> RuntimeJson {
    let mut output = RuntimeObject::new();
    output.insert("query", RuntimeJson::String(query.clone()));
    output.insert("count", RuntimeJson::from(count));
    output.insert(
        "slugs",
        RuntimeJson::Array(slugs.into_iter().map(RuntimeJson::String).collect()),
    );
    RuntimeJson::Object(output)
}

fn runtime_join(values: impl IntoIterator<Item = RuntimeString>, separator: &str) -> RuntimeString {
    let separator = RuntimeString::from(separator);
    let mut values = values.into_iter();
    let Some(mut joined) = values.next() else {
        return RuntimeString::default();
    };
    for value in values {
        joined = joined.concat(&separator).concat(&value);
    }
    joined
}

#[derive(Default)]
pub struct PluginSearchToolOptions {
    pub backend: Option<Box<dyn PluginCatalog>>,
}

fn build_input_schema() -> JsonSchema {
    let mut query = SchemaMap::new();
    query.insert("type".to_string(), json!("string"));
    query.insert(
        "description".to_string(),
        json!("Keywords to search the plugin catalog"),
    );
    let mut source = SchemaMap::new();
    source.insert(
        "description".to_string(),
        json!("Restrict to a single catalog source"),
    );
    source.insert("type".to_string(), json!("string"));
    let mut properties = SchemaMap::new();
    properties.insert("query".to_string(), SchemaValue::Object(query));
    properties.insert("source".to_string(), SchemaValue::Object(source));
    let mut schema = SchemaMap::new();
    schema.insert("type".to_string(), json!("object"));
    schema.insert("properties".to_string(), SchemaValue::Object(properties));
    schema.insert("required".to_string(), json!(["query"]));
    schema.insert("additionalProperties".to_string(), json!(false));
    schema
}

pub struct PluginSearchTool {
    input_schema: JsonSchema,
    backend: Option<Box<dyn PluginCatalog>>,
}

pub fn create_plugin_search_tool(opts: PluginSearchToolOptions) -> Arc<dyn Tool> {
    let backend = opts.backend.or_else(default_plugin_catalog);
    Arc::new(PluginSearchTool {
        input_schema: build_input_schema(),
        backend,
    })
}

#[async_trait]
impl Tool for PluginSearchTool {
    fn static_name(&self) -> &str {
        "plugin_search"
    }

    fn static_description(&self) -> &str {
        "Search the user's plugin catalog for installable plugins when the user asks about plugins or a relevant external capability would materially help fulfill the authorized task. Search by the capability needed for that task and explain useful matches; do not add unrelated recommendations. This tool only discovers options and never installs. Use request_plugin_install with an exact catalog result and a clear reason when installation is needed; its user confirmation remains required."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
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

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let query = input
            .get("query")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_default();
        Some(RuntimeString::from("Searching plugins: ").concat(&query.slice_utf16(..60)))
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let query = input
            .get("query")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_default();

        let Some(backend) = self.backend.as_ref() else {
            return Ok(text_result(
                search_output(&query, 0, Vec::new()),
                "No plugin catalog is configured in this build.",
                true,
            ));
        };

        let sources = input
            .get("source")
            .and_then(RuntimeJson::as_string)
            .filter(|s| !s.is_empty())
            .map(|s| vec![s.clone()]);

        let results = match backend
            .search(
                &query,
                PluginCatalogSearchOptions {
                    sources,
                    signal: Some(ctx.signal.clone()),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(results) => results,
            Err(err) => {
                return Ok(text_result(
                    search_output(&query, 0, Vec::new()),
                    format!("Plugin catalog error: {err}"),
                    true,
                ));
            }
        };

        if results.is_empty() {
            return Ok(text_result(
                search_output(&query, 0, Vec::new()),
                RuntimeString::from("No plugins found for \"")
                    .concat(&query)
                    .concat(&RuntimeString::from("\".")),
                false,
            ));
        }

        let text = results
            .iter()
            .map(|plugin| {
                let mut meta = Vec::new();
                if let Some(source) = &plugin.source {
                    if !source.is_empty() {
                        meta.push(source.clone());
                    }
                }
                if let Some(version) = &plugin.version {
                    if !version.is_empty() {
                        meta.push(RuntimeString::from("v").concat(version));
                    }
                }
                if plugin.installed {
                    meta.push(RuntimeString::from("installed"));
                }
                let meta = if meta.is_empty() {
                    RuntimeString::default()
                } else {
                    RuntimeString::from(" (")
                        .concat(&runtime_join(meta, " "))
                        .concat(&RuntimeString::from(")"))
                };
                RuntimeString::from("- ")
                    .concat(&plugin.slug)
                    .concat(&RuntimeString::from(" — "))
                    .concat(&plugin.name)
                    .concat(&meta)
                    .concat(&RuntimeString::from("\n  "))
                    .concat(&plugin.description)
            })
            .collect::<Vec<_>>();
        let text = runtime_join(text, "\n");
        let slugs = results
            .iter()
            .map(|plugin| plugin.slug.clone())
            .collect::<Vec<_>>();
        Ok(text_result(
            search_output(&query, results.len(), slugs),
            RuntimeString::from(format!("Found {} plugin(s) for \"", results.len()))
                .concat(&query)
                .concat(&RuntimeString::from(
                    "\". Request one with request_plugin_install:\n\n",
                ))
                .concat(&text),
            false,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extensions::plugins::catalog::PluginCatalogEntry;
    use crate::extensions::plugins::loader::LoadedPlugin;
    use crate::runtime_values::runtime_json;
    use crate::tools::builtins::test_ctx::make_ctx;

    type BoxError = Box<dyn std::error::Error + Send + Sync>;

    struct FakeCatalog {
        results: Vec<PluginCatalogEntry>,
    }

    #[async_trait]
    impl PluginCatalog for FakeCatalog {
        async fn search(
            &self,
            _query: &RuntimeString,
            _opts: PluginCatalogSearchOptions,
        ) -> Result<Vec<PluginCatalogEntry>, BoxError> {
            Ok(self.results.clone())
        }

        async fn get(
            &self,
            _slug: &RuntimeString,
            _signal: Option<crate::types::abort::AbortSignal>,
        ) -> Result<Option<PluginCatalogEntry>, BoxError> {
            Ok(None)
        }

        async fn install(
            &self,
            _slug: &RuntimeString,
            _opts: crate::extensions::plugins::catalog::PluginInstallFromCatalogOptions,
        ) -> Result<Option<LoadedPlugin>, BoxError> {
            Ok(None)
        }
    }

    fn exact_json(source: &str) -> RuntimeJson {
        runtime_json::parse(&RuntimeString::from(source)).expect("valid exact test JSON")
    }

    #[tokio::test]
    async fn search_returns_structured_plugin_slugs() {
        let tool = create_plugin_search_tool(PluginSearchToolOptions {
            backend: Some(Box::new(FakeCatalog {
                results: vec![PluginCatalogEntry {
                    slug: RuntimeString::from("browser@official"),
                    name: RuntimeString::from("Browser"),
                    description: RuntimeString::from("Browser runtime plugin"),
                    source: Some(RuntimeString::from("OFFICIAL")),
                    version: Some(RuntimeString::from("1.0.0")),
                    installed: false,
                    skills: Vec::new(),
                    runtime_capabilities: vec![RuntimeString::from("browser")],
                }],
            })),
        });

        let result = tool
            .call(exact_json(r#"{"query":"browser"}"#), &make_ctx("."), None)
            .await
            .unwrap();

        assert_ne!(result.serializable.is_error, Some(true));
        assert_eq!(result.serializable.output["count"], RuntimeJson::from(1));
        assert_eq!(
            result.serializable.output["slugs"],
            RuntimeJson::Array(vec![RuntimeJson::from("browser@official")])
        );
    }

    #[tokio::test]
    async fn preserves_lone_surrogates_in_query_and_slug_output() {
        let slug = RuntimeString::from_utf16(vec![b'p' as u16, 0xd800]);
        let tool = create_plugin_search_tool(PluginSearchToolOptions {
            backend: Some(Box::new(FakeCatalog {
                results: vec![PluginCatalogEntry {
                    slug: slug.clone(),
                    name: RuntimeString::from_utf16(vec![b'n' as u16, 0xdc00]),
                    description: RuntimeString::from("description"),
                    source: None,
                    version: None,
                    installed: false,
                    skills: Vec::new(),
                    runtime_capabilities: Vec::new(),
                }],
            })),
        });
        let query = RuntimeString::from_utf16(vec![b'q' as u16, 0xdc00]);
        let mut input = RuntimeObject::new();
        input.insert("query", RuntimeJson::String(query.clone()));
        let result = tool
            .call(RuntimeJson::Object(input), &make_ctx("."), None)
            .await
            .unwrap();

        assert_eq!(
            result.serializable.output["query"],
            RuntimeJson::String(query)
        );
        assert_eq!(
            result.serializable.output["slugs"],
            RuntimeJson::Array(vec![RuntimeJson::String(slug)])
        );
    }
}
