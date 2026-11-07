//! `list_mcp_resources` / `read_mcp_resource` — resource access over the
//! session's connected MCP servers (`ctx.mcp_clients`). The host owns the
//! connection lifecycle; these tools only read from the already-connected
//! clients. Both are exposed inline and carry a search hint so a host that wires
//! tool_search can defer them later.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json as schema_json;

use crate::mcp::persist_blob::persist_mcp_blob;
use crate::runtime_values::{runtime_json, RuntimeJson, RuntimeObject, RuntimeString};
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};

/// Shown when the connected server(s) advertise no resources at all.
const EMPTY_RESOURCES_MESSAGE: &str =
    "The connected MCP server(s) expose no resources (they may still expose tools).";

/// The "no such connected server" message shared by both resource tools.
fn runtime_object<K, I>(entries: I) -> RuntimeJson
where
    K: Into<RuntimeString>,
    I: IntoIterator<Item = (K, RuntimeJson)>,
{
    let mut object = RuntimeObject::new();
    for (key, value) in entries {
        object.insert(key, value);
    }
    RuntimeJson::Object(object)
}

fn join_runtime(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let Some(mut output) = values.first().cloned() else {
        return RuntimeString::default();
    };
    let separator = RuntimeString::from(separator);
    for value in &values[1..] {
        output = output.concat(&separator).concat(value);
    }
    output
}

fn unknown_server_message(name: &RuntimeString, available: &[RuntimeString]) -> RuntimeString {
    let list = if available.is_empty() {
        RuntimeString::from("(none)")
    } else {
        join_runtime(available, ", ")
    };
    RuntimeString::from("Unknown MCP server \"")
        .concat(name)
        .concat(&RuntimeString::from("\". Connected servers: "))
        .concat(&list)
}

fn empty_array_output(field: &str) -> RuntimeJson {
    runtime_object([(field, RuntimeJson::Array(Vec::new()))])
}

fn list_resources_input_schema() -> JsonSchema {
    schema_json!({
        "type": "object",
        "properties": {
            "server": {
                "type": "string",
                "description": "Limit the listing to this MCP server (optional)"
            }
        },
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

fn read_resource_input_schema() -> JsonSchema {
    schema_json!({
        "type": "object",
        "properties": {
            "server": {
                "type": "string",
                "description": "Name of the MCP server holding the resource"
            },
            "uri": {
                "type": "string",
                "description": "URI of the resource to fetch"
            }
        },
        "required": ["server", "uri"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

// ---- list_mcp_resources ----------------------------------------------------

/// List resources available from connected MCP servers.
pub struct ListMcpResourcesTool {
    input_schema: JsonSchema,
}

/// Construct the `list_mcp_resources` tool.
pub fn create_list_mcp_resources_tool() -> Arc<dyn Tool> {
    Arc::new(ListMcpResourcesTool {
        input_schema: list_resources_input_schema(),
    })
}

#[async_trait]
impl Tool for ListMcpResourcesTool {
    fn static_name(&self) -> &str {
        "list_mcp_resources"
    }

    fn static_description(&self) -> &str {
        "List resources available from connected MCP servers."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn static_search_hint(&self) -> Option<&str> {
        Some("enumerate resources exposed by connected MCP servers")
    }

    fn max_result_size_chars(&self) -> Option<u64> {
        Some(100_000)
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

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let all = match &ctx.mcp_clients {
            Some(get) => get(),
            None => Vec::new(),
        };
        // An empty-string server is falsy — treat it as "no filter" (list all).
        let target = input
            .get("server")
            .and_then(RuntimeJson::as_str)
            .filter(|s| !s.is_empty());
        let clients = match target {
            Some(t) => all.iter().filter(|c| c.name() == t).collect::<Vec<_>>(),
            None => all.iter().collect::<Vec<_>>(),
        };
        if let Some(t) = target {
            if clients.is_empty() {
                let names: Vec<RuntimeString> = all.iter().map(|c| c.name().clone()).collect();
                return Ok(text_result(
                    empty_array_output("resources"),
                    unknown_server_message(t, &names),
                    true,
                ));
            }
        }

        // One server failing to enumerate shouldn't sink the whole result; the
        // client seam already yields an empty list on failure.
        let mut resources: Vec<RuntimeJson> = Vec::new();
        for client in clients {
            for r in client.list_resources().await {
                let mut object = RuntimeObject::new();
                object.insert("uri", RuntimeJson::String(r.uri));
                if let Some(value) = r.name {
                    object.insert("name", RuntimeJson::String(value));
                }
                if let Some(value) = r.mime_type {
                    object.insert("mimeType", RuntimeJson::String(value));
                }
                if let Some(value) = r.description {
                    object.insert("description", RuntimeJson::String(value));
                }
                object.insert("server", RuntimeJson::String(client.name().clone()));
                resources.push(RuntimeJson::Object(object));
            }
        }

        let text = if resources.is_empty() {
            RuntimeString::from(EMPTY_RESOURCES_MESSAGE)
        } else {
            runtime_json::stringify_pretty(&RuntimeJson::Array(resources.clone()))
        };
        let output = runtime_object([("resources", RuntimeJson::Array(resources))]);
        Ok(text_result(output, text, false))
    }
}

// ---- read_mcp_resource -----------------------------------------------------

/// Read a specific resource from a connected MCP server by URI.
pub struct ReadMcpResourceTool {
    input_schema: JsonSchema,
}

/// Construct the `read_mcp_resource` tool.
pub fn create_read_mcp_resource_tool() -> Arc<dyn Tool> {
    Arc::new(ReadMcpResourceTool {
        input_schema: read_resource_input_schema(),
    })
}

#[async_trait]
impl Tool for ReadMcpResourceTool {
    fn static_name(&self) -> &str {
        "read_mcp_resource"
    }

    fn static_description(&self) -> &str {
        "Read a specific resource from a connected MCP server by URI."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn static_search_hint(&self) -> Option<&str> {
        Some("fetch one MCP resource by server and URI")
    }

    fn max_result_size_chars(&self) -> Option<u64> {
        Some(100_000)
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

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let all = match &ctx.mcp_clients {
            Some(get) => get(),
            None => Vec::new(),
        };
        let server = input
            .get("server")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        let uri = input
            .get("uri")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        let client = match all.iter().find(|c| c.name() == &server) {
            Some(c) => c,
            None => {
                let names: Vec<RuntimeString> = all.iter().map(|c| c.name().clone()).collect();
                return Ok(text_result(
                    empty_array_output("contents"),
                    unknown_server_message(&server, &names),
                    true,
                ));
            }
        };

        let raw = client.read_resource(&uri).await;
        let mut contents: Vec<RuntimeJson> = Vec::with_capacity(raw.len());
        for c in raw {
            // Text passes through unchanged.
            if let Some(text) = c.text {
                let mut object = RuntimeObject::new();
                if let Some(value) = c.uri {
                    object.insert("uri", RuntimeJson::String(value));
                }
                if let Some(value) = c.mime_type {
                    object.insert("mimeType", RuntimeJson::String(value));
                }
                object.insert("text", RuntimeJson::String(text));
                contents.push(RuntimeJson::Object(object));
                continue;
            }
            // No text and no blob: pass through what little we have.
            let blob = match c.blob {
                Some(b) => b,
                None => {
                    let mut object = RuntimeObject::new();
                    if let Some(value) = c.uri {
                        object.insert("uri", RuntimeJson::String(value));
                    }
                    if let Some(value) = c.mime_type {
                        object.insert("mimeType", RuntimeJson::String(value));
                    }
                    contents.push(RuntimeJson::Object(object));
                    continue;
                }
            };
            // A binary blob is decoded and written to disk, with its base64
            // replaced by a path so it never bloats the context.
            let path = persist_mcp_blob(&blob, c.mime_type.as_ref())
                .await
                .map_err(|e| ToolError::new(e.to_string()))?;
            let at = c.uri.clone().unwrap_or_else(|| uri.clone());
            let message = RuntimeString::from("[binary resource from ")
                .concat(&server)
                .concat(&RuntimeString::from(" at "))
                .concat(&at)
                .concat(&RuntimeString::from(" written to "))
                .concat(path.as_runtime_string())
                .concat(&RuntimeString::from("]"));
            let mut object = RuntimeObject::new();
            if let Some(value) = c.uri {
                object.insert("uri", RuntimeJson::String(value));
            }
            if let Some(value) = c.mime_type {
                object.insert("mimeType", RuntimeJson::String(value));
            }
            object.insert(
                "blobSavedTo",
                RuntimeJson::String(path.into_runtime_string()),
            );
            object.insert("text", RuntimeJson::String(message));
            contents.push(RuntimeJson::Object(object));
        }

        let output = runtime_object([("contents", RuntimeJson::Array(contents))]);
        let text = runtime_json::stringify_pretty(&output);
        Ok(text_result(output, text, false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::RuntimePath;
    use crate::schemas::content::OutputBlock;
    use crate::tools::builtins::test_ctx::make_ctx;
    use crate::types::tool::{McpResourceClient, McpResourceContent, McpResourceDescriptor};
    use async_trait::async_trait;

    fn exact_json(source: &str) -> RuntimeJson {
        runtime_json::parse(&RuntimeString::from(source)).expect("valid exact runtime JSON")
    }

    macro_rules! json {
        ($($tokens:tt)+) => {
            exact_json(stringify!($($tokens)+))
        };
    }

    /// A mock connected server returning canned resources / read contents.
    struct MockClient {
        name: RuntimeString,
        resources: Vec<McpResourceDescriptor>,
        contents: Vec<McpResourceContent>,
    }

    #[async_trait]
    impl McpResourceClient for MockClient {
        fn name(&self) -> &RuntimeString {
            &self.name
        }
        async fn list_resources(&self) -> Vec<McpResourceDescriptor> {
            self.resources.clone()
        }
        async fn read_resource(&self, _uri: &RuntimeString) -> Vec<McpResourceContent> {
            self.contents.clone()
        }
    }

    /// A `ToolContext` wired with the given connected clients.
    fn ctx_with(clients: Vec<Arc<dyn McpResourceClient>>) -> ToolContext {
        let mut ctx = make_ctx(".");
        ctx.mcp_clients = Some(Box::new(move || clients.clone()));
        ctx
    }

    fn text_of(res: &ToolResult) -> RuntimeString {
        match &res.serializable.model_content[0] {
            OutputBlock::Text(t) => t.text.clone(),
            _ => panic!("expected a text block"),
        }
    }

    fn assert_inner_json_preserves_lone_surrogate(text: &RuntimeString) {
        assert!(text.contains(r#"\ud800"#), "{text:?}");
        assert!(!text.contains("\u{fdd0}"), "{text:?}");
        let outer = runtime_json::stringify(&runtime_object([(
            "text",
            RuntimeJson::String(text.clone()),
        )]));
        assert!(outer.contains(r#"\\ud800"#), "{outer:?}");
        assert!(!outer.contains("\u{fdd0}"), "{outer:?}");
    }

    #[tokio::test]
    async fn resource_json_text_preserves_lone_surrogate_as_inner_json_escape() {
        let lone = RuntimeString::from_utf16(vec![0xd800]);
        let client: Arc<dyn McpResourceClient> = Arc::new(MockClient {
            name: "srv".into(),
            resources: vec![McpResourceDescriptor {
                uri: "res://lone".into(),
                name: Some(lone.clone()),
                mime_type: None,
                description: None,
            }],
            contents: vec![McpResourceContent {
                uri: Some("res://lone".into()),
                mime_type: Some("text/plain".into()),
                text: Some(lone),
                blob: None,
            }],
        });
        let ctx = ctx_with(vec![client]);

        let listed = create_list_mcp_resources_tool()
            .call(json!({}), &ctx, None)
            .await
            .unwrap();
        assert_inner_json_preserves_lone_surrogate(&text_of(&listed));

        let read = create_read_mcp_resource_tool()
            .call(json!({ "server": "srv", "uri": "res://lone" }), &ctx, None)
            .await
            .unwrap();
        assert_inner_json_preserves_lone_surrogate(&text_of(&read));
    }

    #[test]
    fn resource_tools_expose_expected_metadata() {
        let list = create_list_mcp_resources_tool();
        assert_eq!(list.name(), "list_mcp_resources");
        assert_eq!(
            list.description(),
            "List resources available from connected MCP servers."
        );
        assert_eq!(
            list.search_hint(),
            Some(RuntimeString::from(
                "enumerate resources exposed by connected MCP servers"
            ))
        );
        assert_eq!(list.max_result_size_chars(), Some(100_000));
        assert!(list.is_read_only(&json!({})));
        assert!(list.is_concurrency_safe(&json!({})));
        assert!(list.is_enabled());
        assert_eq!(list.is_destructive(&json!({})), Some(false));

        let read = create_read_mcp_resource_tool();
        assert_eq!(read.name(), "read_mcp_resource");
        assert_eq!(
            read.description(),
            "Read a specific resource from a connected MCP server by URI."
        );
        assert_eq!(
            read.search_hint(),
            Some(RuntimeString::from(
                "fetch one MCP resource by server and URI"
            ))
        );
        assert_eq!(read.max_result_size_chars(), Some(100_000));
        assert!(read.is_read_only(&json!({})));
        assert!(read.is_concurrency_safe(&json!({})));
        assert_eq!(read.is_destructive(&json!({})), Some(false));
    }

    #[test]
    fn resource_schemas_match_exactly() {
        assert_eq!(
            create_list_mcp_resources_tool().input_schema(),
            json!({
                "type": "object",
                "properties": {
                    "server": { "type": "string", "description": "Limit the listing to this MCP server (optional)" }
                },
                "additionalProperties": false
            })
        );
        assert_eq!(
            create_read_mcp_resource_tool().input_schema(),
            json!({
                "type": "object",
                "properties": {
                    "server": { "type": "string", "description": "Name of the MCP server holding the resource" },
                    "uri": { "type": "string", "description": "URI of the resource to fetch" }
                },
                "required": ["server", "uri"],
                "additionalProperties": false
            })
        );
    }

    #[tokio::test]
    async fn list_resources_reports_empty_when_no_servers() {
        let tool = create_list_mcp_resources_tool();
        let ctx = make_ctx(".");
        let res = tool.call(json!({}), &ctx, None).await.unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            text_of(&res),
            "The connected MCP server(s) expose no resources (they may still expose tools)."
        );
        assert_eq!(res.serializable.output, json!({ "resources": [] }));
    }

    #[tokio::test]
    async fn list_resources_lists_across_servers() {
        let client: Arc<dyn McpResourceClient> = Arc::new(MockClient {
            name: "srv".into(),
            resources: vec![McpResourceDescriptor {
                uri: "res://a".into(),
                name: Some("A".into()),
                mime_type: Some("text/plain".into()),
                description: None,
            }],
            contents: vec![],
        });
        let ctx = ctx_with(vec![client]);
        let tool = create_list_mcp_resources_tool();
        let res = tool.call(json!({}), &ctx, None).await.unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output,
            json!({ "resources": [
                { "uri": "res://a", "name": "A", "mimeType": "text/plain", "server": "srv" }
            ] })
        );
        let expected = [
            "[",
            "  {",
            "    \"uri\": \"res://a\",",
            "    \"name\": \"A\",",
            "    \"mimeType\": \"text/plain\",",
            "    \"server\": \"srv\"",
            "  }",
            "]",
        ]
        .join("\n");
        assert_eq!(text_of(&res), expected);
    }

    #[tokio::test]
    async fn list_resources_filters_by_server() {
        let a: Arc<dyn McpResourceClient> = Arc::new(MockClient {
            name: "a".into(),
            resources: vec![McpResourceDescriptor {
                uri: "res://a".into(),
                name: None,
                mime_type: None,
                description: None,
            }],
            contents: vec![],
        });
        let b: Arc<dyn McpResourceClient> = Arc::new(MockClient {
            name: "b".into(),
            resources: vec![McpResourceDescriptor {
                uri: "res://b".into(),
                name: None,
                mime_type: None,
                description: None,
            }],
            contents: vec![],
        });
        let ctx = ctx_with(vec![a, b]);
        let tool = create_list_mcp_resources_tool();
        let res = tool
            .call(json!({ "server": "b" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(
            res.serializable.output,
            json!({ "resources": [ { "uri": "res://b", "server": "b" } ] })
        );
    }

    #[tokio::test]
    async fn list_resources_flags_unknown_server() {
        let client: Arc<dyn McpResourceClient> = Arc::new(MockClient {
            name: "srv".into(),
            resources: vec![],
            contents: vec![],
        });
        let ctx = ctx_with(vec![client]);
        let tool = create_list_mcp_resources_tool();
        let res = tool
            .call(json!({ "server": "other" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "Unknown MCP server \"other\". Connected servers: srv"
        );
        assert_eq!(res.serializable.output, json!({ "resources": [] }));
    }

    #[tokio::test]
    async fn list_resources_unknown_server_with_none_connected() {
        let tool = create_list_mcp_resources_tool();
        let ctx = make_ctx(".");
        let res = tool
            .call(json!({ "server": "x" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "Unknown MCP server \"x\". Connected servers: (none)"
        );
    }

    #[tokio::test]
    async fn read_resource_returns_text_content() {
        let client: Arc<dyn McpResourceClient> = Arc::new(MockClient {
            name: "srv".into(),
            resources: vec![],
            contents: vec![McpResourceContent {
                uri: Some("res://a".into()),
                text: Some("hello".into()),
                mime_type: Some("text/plain".into()),
                blob: None,
            }],
        });
        let ctx = ctx_with(vec![client]);
        let tool = create_read_mcp_resource_tool();
        let res = tool
            .call(json!({ "server": "srv", "uri": "res://a" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output,
            json!({ "contents": [ { "uri": "res://a", "mimeType": "text/plain", "text": "hello" } ] })
        );
        let expected = [
            "{",
            "  \"contents\": [",
            "    {",
            "      \"uri\": \"res://a\",",
            "      \"mimeType\": \"text/plain\",",
            "      \"text\": \"hello\"",
            "    }",
            "  ]",
            "}",
        ]
        .join("\n");
        assert_eq!(text_of(&res), expected);
    }

    #[tokio::test]
    async fn read_resource_persists_a_binary_blob() {
        // base64 of "hi" is "aGk=".
        let client: Arc<dyn McpResourceClient> = Arc::new(MockClient {
            name: "srv".into(),
            resources: vec![],
            contents: vec![McpResourceContent {
                uri: Some("res://img".into()),
                text: None,
                mime_type: Some("image/png".into()),
                blob: Some("aGk=".into()),
            }],
        });
        let ctx = ctx_with(vec![client]);
        let tool = create_read_mcp_resource_tool();
        let res = tool
            .call(json!({ "server": "srv", "uri": "res://img" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        let contents = res
            .serializable
            .output
            .get("contents")
            .and_then(RuntimeJson::as_array)
            .expect("contents array");
        let entry = &contents[0];
        let path = entry
            .get("blobSavedTo")
            .and_then(RuntimeJson::as_str)
            .expect("blobSavedTo path");
        assert!(path.ends_with(".png"));
        assert_eq!(
            entry.get("uri").and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("res://img"))
        );
        assert_eq!(
            entry.get("mimeType").and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("image/png"))
        );
        let expected_text =
            RuntimeString::from("[binary resource from srv at res://img written to ")
                .concat(path)
                .concat(&RuntimeString::from("]"));
        assert_eq!(
            entry.get("text").and_then(RuntimeJson::as_str),
            Some(&expected_text)
        );
        let native_path = crate::fs::node_path::project_runtime_path(
            &RuntimePath::from(path.clone()),
            "blobSavedTo",
        )
        .unwrap();
        assert_eq!(std::fs::read(&native_path).unwrap(), b"hi");
        let _ = std::fs::remove_file(native_path);
    }

    #[tokio::test]
    async fn read_resource_handles_content_without_text_or_blob() {
        let client: Arc<dyn McpResourceClient> = Arc::new(MockClient {
            name: "srv".into(),
            resources: vec![],
            contents: vec![McpResourceContent {
                uri: Some("res://x".into()),
                text: None,
                mime_type: Some("application/octet-stream".into()),
                blob: None,
            }],
        });
        let ctx = ctx_with(vec![client]);
        let tool = create_read_mcp_resource_tool();
        let res = tool
            .call(json!({ "server": "srv", "uri": "res://x" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(
            res.serializable.output,
            json!({ "contents": [ { "uri": "res://x", "mimeType": "application/octet-stream" } ] })
        );
    }

    #[tokio::test]
    async fn read_resource_flags_unknown_server() {
        let tool = create_read_mcp_resource_tool();
        let ctx = make_ctx(".");
        let res = tool
            .call(json!({ "server": "srv", "uri": "res://a" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "Unknown MCP server \"srv\". Connected servers: (none)"
        );
        assert_eq!(res.serializable.output, json!({ "contents": [] }));
    }
}
