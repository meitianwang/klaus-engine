//! The read-only `skill_search` [`Tool`]: lets the model browse the skill store
//! by keyword so it can suggest an installable skill. It NEVER installs —
//! installing downloads and runs third-party code, so it stays a human-gated
//! action (the model suggests, the human confirms).

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Map as SchemaMap, Value as SchemaValue};

use crate::extensions::skills::catalog::{SkillCatalog, SkillSearchOptions};
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

/// Options for constructing the `skill_search` tool.
#[derive(Default)]
pub struct SkillSearchToolOptions {
    /// Catalog backend; defaults to the engine's built-in skill catalog.
    pub backend: Option<Box<dyn SkillCatalog>>,
}

/// Build the tool's input JSON Schema: a required `query`, an optional `source` enum.
fn build_input_schema() -> JsonSchema {
    let mut query = SchemaMap::new();
    query.insert("type".to_string(), json!("string"));
    query.insert(
        "description".to_string(),
        json!("Keywords to search the skill store (name / description / tag)"),
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

/// Search the skill store for an installable skill.
pub struct SkillSearchTool {
    input_schema: JsonSchema,
    backend: Option<Box<dyn SkillCatalog>>,
}

/// Construct the `skill_search` tool as an erased registry-side [`Tool`].
pub fn create_skill_search_tool(opts: SkillSearchToolOptions) -> Arc<dyn Tool> {
    let backend = opts.backend;
    Arc::new(SkillSearchTool {
        input_schema: build_input_schema(),
        backend,
    })
}

#[async_trait]
impl Tool for SkillSearchTool {
    fn static_name(&self) -> &str {
        "skill_search"
    }

    fn static_description(&self) -> &str {
        "Search the skill store for an installable skill when the user asks about skills or when no available tool or installed skill supplies a capability needed for the authorized task. Returns matching skills (slug, name, description). Keep discovery focused on that task; do not add unrelated recommendations. If an exact result would materially help, explain why and use request_skill_install. The user confirms installation; search itself changes nothing."
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
        Some(RuntimeString::from("Searching skills: ").concat(&query.slice_utf16(..60)))
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
                "No skill store is configured in this build.",
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
                SkillSearchOptions {
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
                    format!("Skill store error: {err}"),
                    true,
                ));
            }
        };

        if results.is_empty() {
            return Ok(text_result(
                search_output(&query, 0, Vec::new()),
                RuntimeString::from("No skills found for \"")
                    .concat(&query)
                    .concat(&RuntimeString::from("\".")),
                false,
            ));
        }

        let list = results
            .iter()
            .map(|r| {
                let mut meta_parts: Vec<RuntimeString> = Vec::new();
                if !r.source.is_empty() {
                    meta_parts.push(r.source.clone());
                }
                if let Some(v) = &r.version {
                    if !v.is_empty() {
                        meta_parts.push(RuntimeString::from("v").concat(v));
                    }
                }
                let meta = runtime_join(meta_parts, " ");
                let meta_suffix = if meta.is_empty() {
                    RuntimeString::default()
                } else {
                    RuntimeString::from(" (")
                        .concat(&meta)
                        .concat(&RuntimeString::from(")"))
                };
                RuntimeString::from("- ")
                    .concat(&r.slug)
                    .concat(&RuntimeString::from(" — "))
                    .concat(&r.name)
                    .concat(&meta_suffix)
                    .concat(&RuntimeString::from("\n  "))
                    .concat(&r.description)
            })
            .collect::<Vec<_>>();
        let list = runtime_join(list, "\n");
        let text = RuntimeString::from(format!("Found {} skill(s) for \"", results.len()))
            .concat(&query)
            .concat(&RuntimeString::from(
                "\". Install one with `skill install <slug>`:\n\n",
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
    use crate::extensions::skills::catalog::{SkillCatalogEntry, SkillInstallFromCatalogOptions};
    use crate::extensions::skills::loader::Skill;
    use crate::runtime_values::runtime_json;
    use crate::schemas::content::OutputBlock;
    use crate::tools::builtins::test_ctx::make_ctx;
    use crate::types::abort::AbortSignal;

    type BoxError = Box<dyn std::error::Error + Send + Sync>;

    fn exact_json(source: &str) -> RuntimeJson {
        runtime_json::parse(&RuntimeString::from(source)).expect("valid exact test JSON")
    }

    fn entry(
        slug: &str,
        name: &str,
        desc: &str,
        source: &str,
        version: Option<&str>,
    ) -> SkillCatalogEntry {
        SkillCatalogEntry {
            slug: RuntimeString::from(slug),
            name: RuntimeString::from(name),
            description: RuntimeString::from(desc),
            source: RuntimeString::from(source),
            tags: None,
            downloads: None,
            version: version.map(RuntimeString::from),
        }
    }

    struct FakeCatalog {
        results: Vec<SkillCatalogEntry>,
        err: Option<String>,
    }

    #[async_trait]
    impl SkillCatalog for FakeCatalog {
        async fn search(
            &self,
            _query: &RuntimeString,
            _opts: SkillSearchOptions,
        ) -> Result<Vec<SkillCatalogEntry>, BoxError> {
            if let Some(e) = &self.err {
                return Err(e.clone().into());
            }
            Ok(self.results.clone())
        }
        async fn get(
            &self,
            _slug: &RuntimeString,
            _signal: Option<AbortSignal>,
        ) -> Result<Option<SkillCatalogEntry>, BoxError> {
            Ok(None)
        }
        async fn install(
            &self,
            _process: &crate::runtime_process::RuntimeProcessContext,
            _slug: &RuntimeString,
            _opts: SkillInstallFromCatalogOptions,
        ) -> Result<Option<Skill>, BoxError> {
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
        let tool = create_skill_search_tool(SkillSearchToolOptions::default());
        assert_eq!(tool.name(), "skill_search");
        let input = exact_json(r#"{"query":"hello world"}"#);
        assert!(tool.is_read_only(&input));
        assert!(tool.is_concurrency_safe(&input));
        assert_eq!(tool.is_destructive(&input), Some(false));
        assert!(tool.is_enabled());
        assert_eq!(
            tool.summarize(&input),
            Some(RuntimeString::from("Searching skills: hello world"))
        );
    }

    #[test]
    fn input_schema_describes_query_and_source() {
        let tool = create_skill_search_tool(SkillSearchToolOptions::default());
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
        let tool = create_skill_search_tool(SkillSearchToolOptions::default());
        let ctx = make_ctx("/tmp");
        let res = tool
            .call(exact_json(r#"{"query":"x"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(text_of(&res), "No skill store is configured in this build.");
        assert_eq!(
            res.serializable.output,
            exact_json(r#"{"query":"x","count":0,"slugs":[]}"#)
        );
    }

    #[tokio::test]
    async fn formats_matching_skills() {
        let cat = FakeCatalog {
            results: vec![entry(
                "huangli",
                "Almanac",
                "Chinese almanac",
                "OFFICIAL",
                Some("1.2.0"),
            )],
            err: None,
        };
        let tool = create_skill_search_tool(SkillSearchToolOptions {
            backend: Some(Box::new(cat)),
        });
        let ctx = make_ctx("/tmp");
        let res = tool
            .call(exact_json(r#"{"query":"almanac"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        let text = text_of(&res);
        assert!(text.starts_with(
            "Found 1 skill(s) for \"almanac\". Install one with `skill install <slug>`:\n\n"
        ));
        assert!(text.contains("- huangli — Almanac (OFFICIAL v1.2.0)\n  Chinese almanac"));
        assert_eq!(
            res.serializable.output,
            exact_json(r#"{"query":"almanac","count":1,"slugs":["huangli"]}"#)
        );
    }

    #[tokio::test]
    async fn reports_no_matches() {
        let cat = FakeCatalog {
            results: vec![],
            err: None,
        };
        let tool = create_skill_search_tool(SkillSearchToolOptions {
            backend: Some(Box::new(cat)),
        });
        let ctx = make_ctx("/tmp");
        let res = tool
            .call(exact_json(r#"{"query":"nope"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(text_of(&res), "No skills found for \"nope\".");
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
        let tool = create_skill_search_tool(SkillSearchToolOptions {
            backend: Some(Box::new(cat)),
        });
        let ctx = make_ctx("/tmp");
        let res = tool
            .call(exact_json(r#"{"query":"x"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(text_of(&res), "Skill store error: boom");
    }

    #[tokio::test]
    async fn exact_query_and_catalog_surrogates_reach_output_without_collision() {
        let query = RuntimeString::from_utf16(vec![b'q' as u16, 0xd800]);
        let slug = RuntimeString::from_utf16(vec![b's' as u16, 0xdc00]);
        let tool = create_skill_search_tool(SkillSearchToolOptions {
            backend: Some(Box::new(FakeCatalog {
                results: vec![SkillCatalogEntry {
                    slug: slug.clone(),
                    name: RuntimeString::from_utf16(vec![b'n' as u16, 0xd800]),
                    description: RuntimeString::from_utf16(vec![b'd' as u16, 0xdc00]),
                    source: RuntimeString::from("OFFICIAL"),
                    tags: None,
                    downloads: None,
                    version: None,
                }],
                err: None,
            })),
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
