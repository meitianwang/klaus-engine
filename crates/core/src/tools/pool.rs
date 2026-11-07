//! The tool pool: assemble the active tool set for a turn and project tools into
//! provider-agnostic specs.

use crate::runtime_values::RuntimeString;
use crate::tools::locale_sort::sort_runtime_by_owned_key;
use crate::types::provider::{ToolSpec, ToolSpecFormat, ToolSpecKind};
use crate::types::tool::{Tool, ToolInputKind};
use std::collections::HashSet;
use std::sync::Arc;

/// The three tool sources merged into a pool.
pub struct BuildPoolOptions {
    pub builtins: Vec<Arc<dyn Tool>>,
    pub mcp_tools: Option<Vec<Arc<dyn Tool>>>,
    pub plugin_tools: Option<Vec<Arc<dyn Tool>>>,
}

/// Assemble the active tool set: drop disabled tools, dedupe by name (first
/// wins), and sort by name so the prompt — and thus the prompt cache — is stable.
pub fn build_tool_pool(opts: BuildPoolOptions) -> Vec<Arc<dyn Tool>> {
    let mut all = opts.builtins;
    all.extend(opts.mcp_tools.unwrap_or_default());
    all.extend(opts.plugin_tools.unwrap_or_default());

    let mut seen = HashSet::<RuntimeString>::new();
    let mut out = Vec::<Arc<dyn Tool>>::new();
    for tool in all {
        if !tool.is_enabled() {
            continue;
        }
        let name = tool.name();
        // First registration of a name wins.
        if seen.insert(name) {
            out.push(tool);
        }
    }

    sort_runtime_by_owned_key(&mut out, |tool| tool.name());
    out
}

/// Project a tool into a provider-agnostic spec. The tool already carries its
/// input schema in the normalized, provider-ready form.
pub fn to_tool_spec(tool: &dyn Tool) -> ToolSpec {
    match tool.input_kind() {
        ToolInputKind::JsonSchema => ToolSpec {
            name: tool.name(),
            description: tool.description(),
            input_schema: tool.input_schema(),
            kind: ToolSpecKind::Function,
            format: None,
        },
        ToolInputKind::Freeform { .. } => ToolSpec {
            name: tool.name(),
            description: tool.description(),
            input_schema: tool.input_schema(),
            kind: ToolSpecKind::Freeform,
            format: Some(ToolSpecFormat::Text),
        },
    }
}

pub fn to_tool_specs(tools: &[Arc<dyn Tool>]) -> Vec<ToolSpec> {
    tools.iter().map(|t| to_tool_spec(t.as_ref())).collect()
}

/// Look a tool up by its name or one of its aliases.
pub fn find_tool(tools: &[Arc<dyn Tool>], name: &str) -> Option<Arc<dyn Tool>> {
    tools
        .iter()
        .find(|t| t.name() == name || t.aliases().iter().any(|a| a == name))
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::build_tool::text_result;
    use crate::types::errors::ToolError;
    use crate::types::json::{Json, JsonSchema};
    use crate::types::tool::{ToolContext, ToolProgress, ToolResult};
    use async_trait::async_trait;
    use serde_json::Value as StaticSchemaValue;

    struct MockTool {
        name: String,
        aliases: Vec<String>,
        enabled: bool,
        schema: JsonSchema,
        input_kind: ToolInputKind,
        description: String,
    }

    #[async_trait]
    impl Tool for MockTool {
        fn static_name(&self) -> &str {
            &self.name
        }
        fn static_aliases(&self) -> &[String] {
            &self.aliases
        }
        fn static_description(&self) -> &str {
            &self.description
        }
        fn static_input_schema(&self) -> &JsonSchema {
            &self.schema
        }
        fn input_kind(&self) -> ToolInputKind {
            self.input_kind.clone()
        }
        fn is_enabled(&self) -> bool {
            self.enabled
        }
        fn is_concurrency_safe(&self, _input: &Json) -> bool {
            false
        }
        fn is_read_only(&self, _input: &Json) -> bool {
            false
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

    fn mk_desc(name: &str, enabled: bool, description: &str) -> Arc<dyn Tool> {
        Arc::new(MockTool {
            name: name.into(),
            aliases: vec![],
            enabled,
            schema: JsonSchema::new(),
            input_kind: ToolInputKind::JsonSchema,
            description: description.into(),
        })
    }

    fn mk(name: &str, enabled: bool) -> Arc<dyn Tool> {
        mk_desc(name, enabled, &format!("{name} tool"))
    }

    #[test]
    fn build_pool_sorts_by_name_dedupes_first_wins_and_drops_disabled() {
        let builtins: Vec<Arc<dyn Tool>> = vec![
            mk_desc("read", true, "first-read"),
            mk("bash", true),
            mk("off", false),
            mk_desc("read", true, "second-read"),
        ];
        let pool = build_tool_pool(BuildPoolOptions {
            builtins,
            mcp_tools: None,
            plugin_tools: None,
        });
        let names: Vec<RuntimeString> = pool.iter().map(|t| t.name()).collect();
        assert_eq!(
            names,
            vec![RuntimeString::from("bash"), RuntimeString::from("read")]
        );
        // First registration of the duplicated name wins.
        let read = pool.iter().find(|t| t.name() == "read").unwrap();
        assert_eq!(read.description(), RuntimeString::from("first-read"));
    }

    #[test]
    fn build_pool_merges_mcp_and_plugin_tools() {
        let pool = build_tool_pool(BuildPoolOptions {
            builtins: vec![mk("bash", true)],
            mcp_tools: Some(vec![mk("mcp_x", true)]),
            plugin_tools: Some(vec![mk("plug_y", true)]),
        });
        let names: Vec<RuntimeString> = pool.iter().map(|t| t.name()).collect();
        assert_eq!(
            names,
            vec![
                RuntimeString::from("bash"),
                RuntimeString::from("mcp_x"),
                RuntimeString::from("plug_y"),
            ]
        );
    }

    #[test]
    fn build_pool_orders_mixed_case_names_with_locale_compare() {
        let pool = build_tool_pool(BuildPoolOptions {
            builtins: vec![
                mk("ListPeers", true),
                mk("apply_patch", true),
                mk("ls", true),
                mk("SendMessage", true),
                mk("save_memory", true),
            ],
            mcp_tools: None,
            plugin_tools: None,
        });
        let names: Vec<RuntimeString> = pool.iter().map(|t| t.name()).collect();
        assert_eq!(
            names,
            vec![
                RuntimeString::from("apply_patch"),
                RuntimeString::from("ListPeers"),
                RuntimeString::from("ls"),
                RuntimeString::from("save_memory"),
                RuntimeString::from("SendMessage"),
            ]
        );
    }

    #[test]
    fn locale_compare_matches_node_en_us_unicode_oracle() {
        let pool = build_tool_pool(BuildPoolOptions {
            builtins: vec![
                mk("\u{2010}.rs", true),
                mk("\u{10d6e}.rs", true),
                mk("z.rs", true),
                mk("ä.rs", true),
            ],
            mcp_tools: None,
            plugin_tools: None,
        });
        let names: Vec<RuntimeString> = pool.iter().map(|tool| tool.name()).collect();
        assert_eq!(
            names,
            vec![
                RuntimeString::from("\u{10d6e}.rs"),
                RuntimeString::from("\u{2010}.rs"),
                RuntimeString::from("ä.rs"),
                RuntimeString::from("z.rs"),
            ]
        );
    }

    #[test]
    fn locale_compare_matches_node_en_us_case_oracle() {
        let pool = build_tool_pool(BuildPoolOptions {
            builtins: vec![mk("O", true), mk("o", true), mk("A", true), mk("a", true)],
            mcp_tools: None,
            plugin_tools: None,
        });
        let names: Vec<RuntimeString> = pool.iter().map(|tool| tool.name()).collect();
        assert_eq!(
            names,
            vec![
                RuntimeString::from("a"),
                RuntimeString::from("A"),
                RuntimeString::from("o"),
                RuntimeString::from("O"),
            ]
        );
    }

    #[test]
    fn build_pool_preserves_input_order_for_collation_equal_distinct_names() {
        let decomposed = "e\u{301}";
        let composed = "é";
        for expected in [[decomposed, composed], [composed, decomposed]] {
            let pool = build_tool_pool(BuildPoolOptions {
                builtins: expected.iter().map(|name| mk(name, true)).collect(),
                mcp_tools: None,
                plugin_tools: None,
            });
            assert_eq!(
                pool.iter().map(|tool| tool.name()).collect::<Vec<_>>(),
                expected
                    .iter()
                    .map(|name| RuntimeString::from(*name))
                    .collect::<Vec<_>>(),
                "stable locale ordering must retain the registration order"
            );
        }
    }

    #[test]
    fn to_tool_spec_projects_name_description_and_schema() {
        let mut schema = JsonSchema::new();
        // Native JsonSchema is the explicit scalar-only schema seam; the
        // provider-facing ToolSpec is asserted through its exact carrier.
        schema.insert("type".into(), StaticSchemaValue::String("object".into()));
        let tool = MockTool {
            name: "read".into(),
            aliases: vec![],
            enabled: true,
            schema: schema.clone(),
            input_kind: ToolInputKind::JsonSchema,
            description: "reads".into(),
        };
        let spec = to_tool_spec(&tool);
        assert_eq!(
            spec,
            ToolSpec {
                name: "read".into(),
                description: "reads".into(),
                input_schema: crate::types::json::runtime_schema_from_static(&schema),
                kind: ToolSpecKind::Function,
                format: None,
            }
        );
    }

    #[test]
    fn to_tool_spec_projects_freeform_and_schema_tools_by_input_kind() {
        let freeform = MockTool {
            name: "exec".into(),
            aliases: vec![],
            enabled: true,
            schema: JsonSchema::new(),
            input_kind: ToolInputKind::Freeform {
                format: "javascript".into(),
            },
            description: "run javascript".into(),
        };
        assert_eq!(
            to_tool_spec(&freeform),
            ToolSpec {
                name: "exec".into(),
                description: "run javascript".into(),
                input_schema: crate::types::json::runtime_schema_from_static(&JsonSchema::new()),
                kind: ToolSpecKind::Freeform,
                format: Some(ToolSpecFormat::Text),
            }
        );

        let normal = MockTool {
            name: "wait".into(),
            aliases: vec![],
            enabled: true,
            schema: JsonSchema::new(),
            input_kind: ToolInputKind::JsonSchema,
            description: "wait for background work".into(),
        };
        assert_eq!(to_tool_spec(&normal).kind, ToolSpecKind::Function);
    }

    #[test]
    fn to_tool_specs_maps_each_tool() {
        let tools: Vec<Arc<dyn Tool>> = vec![mk("bash", true), mk("read", true)];
        let specs = to_tool_specs(&tools);
        let names: Vec<RuntimeString> = specs.iter().map(|s| s.name().clone()).collect();
        assert_eq!(
            names,
            vec![RuntimeString::from("bash"), RuntimeString::from("read")]
        );
    }

    #[test]
    fn find_tool_matches_name_or_alias() {
        let tools: Vec<Arc<dyn Tool>> = vec![
            Arc::new(MockTool {
                name: "read".into(),
                aliases: vec!["view".into(), "cat".into()],
                enabled: true,
                schema: JsonSchema::new(),
                input_kind: ToolInputKind::JsonSchema,
                description: String::new(),
            }),
            mk("bash", true),
        ];
        assert_eq!(find_tool(&tools, "read").unwrap().name(), "read");
        assert_eq!(find_tool(&tools, "cat").unwrap().name(), "read");
        assert_eq!(find_tool(&tools, "bash").unwrap().name(), "bash");
        assert!(find_tool(&tools, "nope").is_none());
    }
}
