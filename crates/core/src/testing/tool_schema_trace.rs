use crate::runtime_values::{runtime_codec, RuntimeJson, RuntimeObject, RuntimeString};
use crate::schemas::config::default_config;
use crate::tools::builtins::index::{default_builtins, BuiltinsOptions};
use crate::tools::pool::{build_tool_pool, to_tool_specs, BuildPoolOptions};

const SCENARIO: &str = "default-tool-list-and-model-visible-schemas";

pub fn tool_schema_trace() -> RuntimeJson {
    let tools = build_tool_pool(BuildPoolOptions {
        builtins: default_builtins(
            &default_config(),
            BuiltinsOptions {
                // A zero-config fixture must not inherit a packaged app's
                // KLAUS_PLUGIN_CATALOG_DIR from the test process.
                plugin_catalog: Some(std::sync::Arc::new(|| None)),
                ..BuiltinsOptions::default()
            },
        ),
        mcp_tools: None,
        plugin_tools: None,
    });
    let tool_names = tools
        .iter()
        .map(|tool| RuntimeJson::String(tool.name()))
        .collect();
    let tool_metadata = tools
        .iter()
        .map(|tool| {
            let mut metadata = RuntimeObject::new();
            metadata.insert("name", RuntimeJson::String(tool.name()));
            metadata.insert(
                "exposure",
                runtime_codec::encode_value(&tool.exposure())
                    .expect("tool exposure has an exact runtime encoding"),
            );
            metadata.insert(
                "inputKind",
                runtime_codec::encode_value(&tool.input_kind())
                    .expect("tool input kind has an exact runtime encoding"),
            );
            metadata.insert(
                "searchHint",
                tool.search_hint()
                    .map(RuntimeJson::String)
                    .unwrap_or(RuntimeJson::Null),
            );
            RuntimeJson::Object(metadata)
        })
        .collect();
    let tool_specs = to_tool_specs(&tools)
        .iter()
        .map(|spec| {
            runtime_codec::encode_value(spec).expect("tool spec has an exact runtime encoding")
        })
        .collect();

    let mut trace = RuntimeObject::new();
    trace.insert(
        "scenario",
        RuntimeJson::String(RuntimeString::from(SCENARIO)),
    );
    trace.insert("toolNames", RuntimeJson::Array(tool_names));
    trace.insert("toolMetadata", RuntimeJson::Array(tool_metadata));
    trace.insert("toolSpecs", RuntimeJson::Array(tool_specs));
    RuntimeJson::Object(trace)
}
