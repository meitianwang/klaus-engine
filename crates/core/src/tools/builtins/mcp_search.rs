//! The read-only `mcp_search` [`Tool`]: lets the model browse the MCP server
//! store by keyword so it can offer to add one when the user asks for a
//! capability no current tool provides. It NEVER installs — adding a server
//! wires third-party code / credentials into the environment, so that stays a
//! human-gated action (`request_mcp_install`, which the user confirms). Mirrors
//! `skill_search`.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Map as SchemaMap, Value as SchemaValue};

use crate::mcp::mcp_hub::{McpCatalog, McpCatalogSearchOptions};
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

/// Options for constructing the `mcp_search` tool.
#[derive(Default)]
pub struct McpSearchToolOptions {
    /// Catalog backend; defaults to the engine's built-in MCP hub catalog.
    pub backend: Option<Box<dyn McpCatalog>>,
}

/// Build the tool's input JSON Schema: a required `query`, an optional `source` enum.
fn build_input_schema() -> JsonSchema {
    let mut query = SchemaMap::new();
    query.insert("type".to_string(), json!("string"));
    query.insert(
        "description".to_string(),
        json!("Keywords to search the MCP server store (name / description / keyword)"),
    );
    let mut source = SchemaMap::new();
    source.insert(
        "description".to_string(),
        json!("Restrict to a single source: OFFICIAL (curated) or COMMUNITY (user-uploaded)"),
    );
    source.insert("type".to_string(), json!("string"));
    source.insert("enum".to_string(), json!(["OFFICIAL", "COMMUNITY"]));
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

/// Search the MCP server store for an installable server.
pub struct McpSearchTool {
    input_schema: JsonSchema,
    backend: Option<Box<dyn McpCatalog>>,
}

/// Construct the `mcp_search` tool as an erased registry-side [`Tool`].
pub fn create_mcp_search_tool(opts: McpSearchToolOptions) -> Arc<dyn Tool> {
    let backend = opts.backend;
    Arc::new(McpSearchTool {
        input_schema: build_input_schema(),
        backend,
    })
}

#[async_trait]
impl Tool for McpSearchTool {
    fn static_name(&self) -> &str {
        "mcp_search"
    }

    fn static_description(&self) -> &str {
        "Search the MCP server store for an installable server when a capability needed for the user's task is missing from the available tools, or when the user asks about server integrations. Returns matching servers (slug, name, description). Keep discovery relevant to the authorized task and avoid unrelated recommendations. If an exact result would materially help, explain why and use request_mcp_install; the user confirms installation, and search itself changes nothing."
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
        Some(RuntimeString::from("Searching MCP servers: ").concat(&query.slice_utf16(..60)))
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
                "No MCP server store is configured in this build.",
                true,
            ));
        };

        // A single explicit source restricts the search; omit it otherwise.
        let sources = input
            .get("source")
            .and_then(RuntimeJson::as_string)
            .filter(|s| !s.is_empty())
            .map(|s| vec![s.clone()]);

        let results = match backend
            .search(
                &query,
                McpCatalogSearchOptions {
                    sources,
                    signal: Some(ctx.signal.clone()),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(r) => r,
            Err(err) => {
                return Ok(text_result(
                    search_output(&query, 0, Vec::new()),
                    format!("MCP store error: {err}"),
                    true,
                ));
            }
        };

        if results.is_empty() {
            return Ok(text_result(
                search_output(&query, 0, Vec::new()),
                RuntimeString::from("No MCP servers found for \"")
                    .concat(&query)
                    .concat(&RuntimeString::from("\".")),
                false,
            ));
        }

        let list = results
            .iter()
            .map(|r| {
                let meta = match r.source.as_ref() {
                    Some(source) if !source.is_empty() => RuntimeString::from(" (")
                        .concat(source)
                        .concat(&RuntimeString::from(")")),
                    _ => RuntimeString::default(),
                };
                RuntimeString::from("- ")
                    .concat(&r.slug)
                    .concat(&RuntimeString::from(" — "))
                    .concat(&r.name)
                    .concat(&meta)
                    .concat(&RuntimeString::from("\n  "))
                    .concat(&r.description)
            })
            .collect::<Vec<_>>();
        let list = runtime_join(list, "\n");
        let text = RuntimeString::from(format!("Found {} MCP server(s) for \"", results.len()))
            .concat(&query)
            .concat(&RuntimeString::from(
                "\". Add one with `mcp install <slug>` or the request_mcp_install tool:\n\n",
            ))
            .concat(&list);

        let slugs: Vec<RuntimeString> = results.iter().map(|r| r.slug.clone()).collect();
        Ok(text_result(
            search_output(&query, results.len(), slugs),
            text,
            false,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::mcp_hub::{McpCatalogEntry, McpInstallFromCatalogOptions};
    use crate::mcp::types::McpServerConfig;
    use crate::runtime_values::runtime_json;
    use crate::schemas::content::OutputBlock;
    use crate::tools::builtins::test_ctx::make_ctx;
    use crate::types::abort::AbortSignal;

    type BoxError = Box<dyn std::error::Error + Send + Sync>;

    fn exact_json(source: &str) -> RuntimeJson {
        runtime_json::parse(&RuntimeString::from(source)).expect("valid exact test JSON")
    }

    fn entry(slug: &str, name: &str, desc: &str, source: Option<&str>) -> McpCatalogEntry {
        McpCatalogEntry {
            slug: RuntimeString::from(slug),
            name: RuntimeString::from(name),
            description: RuntimeString::from(desc),
            source: source.map(RuntimeString::from),
            server: RuntimeJson::Object(RuntimeObject::new()),
            keywords: None,
        }
    }

    struct FakeCatalog {
        results: Vec<McpCatalogEntry>,
        err: Option<String>,
    }

    #[async_trait]
    impl McpCatalog for FakeCatalog {
        async fn search(
            &self,
            _query: &RuntimeString,
            _opts: McpCatalogSearchOptions,
        ) -> Result<Vec<McpCatalogEntry>, BoxError> {
            if let Some(e) = &self.err {
                return Err(e.clone().into());
            }
            Ok(self.results.clone())
        }
        async fn get(
            &self,
            _slug: &RuntimeString,
            _signal: Option<AbortSignal>,
        ) -> Result<Option<McpCatalogEntry>, BoxError> {
            Ok(None)
        }
        async fn install(
            &self,
            _slug: &RuntimeString,
            _opts: McpInstallFromCatalogOptions,
        ) -> Result<Option<McpServerConfig>, BoxError> {
            Ok(None)
        }
    }

    fn text_of(res: &ToolResult) -> &RuntimeString {
        match &res.serializable.model_content[0] {
            OutputBlock::Text(t) => &t.text,
            _ => panic!("expected a text block"),
        }
    }

    #[test]
    fn exposes_expected_metadata() {
        let tool = create_mcp_search_tool(McpSearchToolOptions::default());
        assert_eq!(tool.name(), "mcp_search");
        let input = exact_json(r#"{"query":"github integration"}"#);
        assert!(tool.is_read_only(&input));
        assert!(tool.is_concurrency_safe(&input));
        assert_eq!(tool.is_destructive(&input), Some(false));
        assert!(tool.is_enabled());
        assert_eq!(
            tool.summarize(&input),
            Some(RuntimeString::from(
                "Searching MCP servers: github integration"
            ))
        );
    }

    #[test]
    fn input_schema_describes_query_and_source() {
        let tool = create_mcp_search_tool(McpSearchToolOptions::default());
        let schema = tool.input_schema();
        assert_eq!(
            schema.get("type").and_then(RuntimeJson::as_string).cloned(),
            Some(RuntimeString::from("object"))
        );
        let query = schema
            .get("properties")
            .and_then(|p| p.get("query"))
            .expect("query property");
        assert_eq!(
            query.get("type").and_then(RuntimeJson::as_string).cloned(),
            Some(RuntimeString::from("string"))
        );
        assert_eq!(
            query
                .get("description")
                .and_then(RuntimeJson::as_string)
                .cloned(),
            Some(RuntimeString::from(
                "Keywords to search the MCP server store (name / description / keyword)"
            ))
        );
        let source = schema
            .get("properties")
            .and_then(|p| p.get("source"))
            .expect("source property");
        assert_eq!(
            source.get("type").and_then(RuntimeJson::as_string).cloned(),
            Some(RuntimeString::from("string"))
        );
        assert_eq!(
            source.get("enum").unwrap(),
            &exact_json(r#"["OFFICIAL","COMMUNITY"]"#)
        );
        assert_eq!(schema.get("required").unwrap(), &exact_json(r#"["query"]"#));
        assert_eq!(
            schema.get("additionalProperties").unwrap(),
            &RuntimeJson::Bool(false)
        );
    }

    #[tokio::test]
    async fn errors_when_no_store_is_configured() {
        let tool = create_mcp_search_tool(McpSearchToolOptions::default());
        let ctx = make_ctx("/tmp");
        let res = tool
            .call(exact_json(r#"{"query":"x"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "No MCP server store is configured in this build."
        );
        assert_eq!(
            res.serializable.output,
            exact_json(r#"{"query":"x","count":0,"slugs":[]}"#)
        );
    }

    #[tokio::test]
    async fn formats_matching_mcp_servers() {
        let cat = FakeCatalog {
            results: vec![entry("gh", "GitHub", "Connect GitHub", Some("OFFICIAL"))],
            err: None,
        };
        let tool = create_mcp_search_tool(McpSearchToolOptions {
            backend: Some(Box::new(cat)),
        });
        let ctx = make_ctx("/tmp");
        let res = tool
            .call(exact_json(r#"{"query":"github"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        let text = text_of(&res);
        assert!(text.starts_with(
            "Found 1 MCP server(s) for \"github\". Add one with `mcp install <slug>` or the request_mcp_install tool:\n\n"
        ));
        assert!(text.contains("- gh — GitHub (OFFICIAL)\n  Connect GitHub"));
        assert_eq!(
            res.serializable.output,
            exact_json(r#"{"query":"github","count":1,"slugs":["gh"]}"#)
        );
    }

    #[tokio::test]
    async fn omits_the_source_suffix_when_absent() {
        let cat = FakeCatalog {
            results: vec![entry("loc", "Local", "A local server", None)],
            err: None,
        };
        let tool = create_mcp_search_tool(McpSearchToolOptions {
            backend: Some(Box::new(cat)),
        });
        let ctx = make_ctx("/tmp");
        let res = tool
            .call(exact_json(r#"{"query":"local"}"#), &ctx, None)
            .await
            .unwrap();
        assert!(text_of(&res).contains("- loc — Local\n  A local server"));
    }

    #[tokio::test]
    async fn reports_no_matches() {
        let cat = FakeCatalog {
            results: vec![],
            err: None,
        };
        let tool = create_mcp_search_tool(McpSearchToolOptions {
            backend: Some(Box::new(cat)),
        });
        let ctx = make_ctx("/tmp");
        let res = tool
            .call(exact_json(r#"{"query":"nope"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(text_of(&res), "No MCP servers found for \"nope\".");
        assert_eq!(
            res.serializable.output,
            exact_json(r#"{"query":"nope","count":0,"slugs":[]}"#)
        );
    }

    #[tokio::test]
    async fn reports_a_store_error() {
        let cat = FakeCatalog {
            results: vec![],
            err: Some("boom".to_string()),
        };
        let tool = create_mcp_search_tool(McpSearchToolOptions {
            backend: Some(Box::new(cat)),
        });
        let ctx = make_ctx("/tmp");
        let res = tool
            .call(exact_json(r#"{"query":"x"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(text_of(&res), "MCP store error: boom");
        assert_eq!(
            res.serializable.output,
            exact_json(r#"{"query":"x","count":0,"slugs":[]}"#)
        );
    }

    #[tokio::test]
    async fn preserves_lone_surrogates_in_query_and_slug_output() {
        let query = RuntimeString::from_utf16(vec![b'q' as u16, 0xd800]);
        let slug = RuntimeString::from_utf16(vec![b's' as u16, 0xdc00]);
        let cat = FakeCatalog {
            results: vec![McpCatalogEntry {
                slug: slug.clone(),
                name: RuntimeString::from_utf16(vec![b'n' as u16, 0xd800]),
                description: RuntimeString::from("description"),
                source: None,
                server: RuntimeJson::Object(RuntimeObject::new()),
                keywords: None,
            }],
            err: None,
        };
        let tool = create_mcp_search_tool(McpSearchToolOptions {
            backend: Some(Box::new(cat)),
        });
        let mut input = RuntimeObject::new();
        input.insert("query", RuntimeJson::String(query.clone()));
        let result = tool
            .call(RuntimeJson::Object(input), &make_ctx("/tmp"), None)
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
        assert!(text_of(&result).as_utf16().contains(&0xd800));
        assert!(text_of(&result).as_utf16().contains(&0xdc00));
    }
}
