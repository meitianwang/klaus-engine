//! `list_mcp_prompts` / `get_mcp_prompt` — prompt access over the session's
//! connected MCP servers (`ctx.mcp_clients`). The headless core has no user
//! slash-command surface, so a server's prompts are exposed as MODEL-invokable
//! tools (mirroring the resource tools): the model lists available prompts, then
//! renders one (with arguments) to fold its content into its reasoning.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json as schema_json;

use crate::runtime_values::{runtime_json, RuntimeJson, RuntimeObject, RuntimeString};
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};

/// Shown when the connected server(s) advertise no prompts at all.
const EMPTY_PROMPTS_MESSAGE: &str =
    "The connected MCP server(s) expose no prompts (they may still expose tools and resources).";

/// The "no such connected server" message shared by both prompt tools.
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

fn empty_output(field: &str, value: RuntimeJson) -> RuntimeJson {
    runtime_object([(field, value)])
}

fn list_prompts_input_schema() -> JsonSchema {
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

fn get_prompt_input_schema() -> JsonSchema {
    schema_json!({
        "type": "object",
        "properties": {
            "server": {
                "type": "string",
                "description": "Name of the MCP server holding the prompt"
            },
            "name": {
                "type": "string",
                "description": "Name of the prompt to render"
            },
            "arguments": {
                "type": "object",
                "description": "Argument values for the prompt, as a string→string map (optional)",
                "propertyNames": { "type": "string" },
                "additionalProperties": { "type": "string" }
            }
        },
        "required": ["server", "name"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

// ---- list_mcp_prompts ------------------------------------------------------

/// List prompts available from connected MCP servers.
pub struct ListMcpPromptsTool {
    input_schema: JsonSchema,
}

/// Construct the `list_mcp_prompts` tool.
pub fn create_list_mcp_prompts_tool() -> Arc<dyn Tool> {
    Arc::new(ListMcpPromptsTool {
        input_schema: list_prompts_input_schema(),
    })
}

#[async_trait]
impl Tool for ListMcpPromptsTool {
    fn static_name(&self) -> &str {
        "list_mcp_prompts"
    }

    fn static_description(&self) -> &str {
        "List prompts available from connected MCP servers (name, description, arguments)."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn static_search_hint(&self) -> Option<&str> {
        Some("enumerate prompts exposed by connected MCP servers")
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
                    empty_output("prompts", RuntimeJson::Array(Vec::new())),
                    unknown_server_message(t, &names),
                    true,
                ));
            }
        }

        // A server that exposes no prompts (or fails to enumerate them) yields
        // `None` from the client seam and contributes nothing.
        let mut prompts: Vec<RuntimeJson> = Vec::new();
        for client in clients {
            if let Some(list) = client.list_prompts().await {
                for p in list {
                    let mut object = RuntimeObject::new();
                    object.insert("name", RuntimeJson::String(p.name));
                    if let Some(value) = p.description {
                        object.insert("description", RuntimeJson::String(value));
                    }
                    if let Some(arguments) = p.arguments {
                        let arguments = arguments
                            .into_iter()
                            .map(|argument| {
                                let mut object = RuntimeObject::new();
                                object.insert("name", RuntimeJson::String(argument.name));
                                if let Some(value) = argument.description {
                                    object.insert("description", RuntimeJson::String(value));
                                }
                                if let Some(value) = argument.required {
                                    object.insert("required", RuntimeJson::Bool(value));
                                }
                                RuntimeJson::Object(object)
                            })
                            .collect();
                        object.insert("arguments", RuntimeJson::Array(arguments));
                    }
                    object.insert("server", RuntimeJson::String(client.name().clone()));
                    prompts.push(RuntimeJson::Object(object));
                }
            }
        }

        let text = if prompts.is_empty() {
            RuntimeString::from(EMPTY_PROMPTS_MESSAGE)
        } else {
            runtime_json::stringify_pretty(&RuntimeJson::Array(prompts.clone()))
        };
        let output = runtime_object([("prompts", RuntimeJson::Array(prompts))]);
        Ok(text_result(output, text, false))
    }
}

// ---- get_mcp_prompt --------------------------------------------------------

/// Render a specific prompt from a connected MCP server.
pub struct GetMcpPromptTool {
    input_schema: JsonSchema,
}

/// Construct the `get_mcp_prompt` tool.
pub fn create_get_mcp_prompt_tool() -> Arc<dyn Tool> {
    Arc::new(GetMcpPromptTool {
        input_schema: get_prompt_input_schema(),
    })
}

#[async_trait]
impl Tool for GetMcpPromptTool {
    fn static_name(&self) -> &str {
        "get_mcp_prompt"
    }

    fn static_description(&self) -> &str {
        "Render a specific prompt from a connected MCP server, with optional arguments."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn static_search_hint(&self) -> Option<&str> {
        Some("fetch and render one MCP prompt by server and name")
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
        let client = match all.iter().find(|c| c.name() == &server) {
            Some(c) => c,
            None => {
                let names: Vec<RuntimeString> = all.iter().map(|c| c.name().clone()).collect();
                return Ok(text_result(
                    empty_output("prompt", RuntimeJson::String(RuntimeString::default())),
                    unknown_server_message(&server, &names),
                    true,
                ));
            }
        };

        let name = input
            .get("name")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        let args: Option<RuntimeObject> = input
            .get("arguments")
            .and_then(RuntimeJson::as_object)
            .map(|obj| {
                let mut arguments = RuntimeObject::new();
                obj.entries()
                    .filter_map(|(key, value)| {
                        value
                            .as_str()
                            .map(|value| (key.clone(), RuntimeJson::String(value.clone())))
                    })
                    .for_each(|(key, value)| {
                        arguments.insert(key, value);
                    });
                arguments
            });

        // The client seam renders to `Some(text)`; a server without prompt
        // support yields `None`.
        match client.get_prompt(&name, args).await {
            Some(prompt) => {
                let text = if prompt.is_empty() {
                    RuntimeString::from("(the prompt rendered to empty content)")
                } else {
                    prompt.clone()
                };
                Ok(text_result(
                    empty_output("prompt", RuntimeJson::String(prompt)),
                    text,
                    false,
                ))
            }
            None => Ok(text_result(
                empty_output("prompt", RuntimeJson::String(RuntimeString::default())),
                RuntimeString::from("MCP server \"")
                    .concat(&server)
                    .concat(&RuntimeString::from("\" does not expose prompts.")),
                true,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::content::OutputBlock;
    use crate::tools::builtins::test_ctx::make_ctx;
    use crate::types::tool::{
        McpPromptArgument, McpPromptDescriptor, McpResourceClient, McpResourceContent,
        McpResourceDescriptor,
    };
    use async_trait::async_trait;
    use std::sync::Mutex;

    fn exact_json(source: &str) -> RuntimeJson {
        runtime_json::parse(&RuntimeString::from(source)).expect("valid exact runtime JSON")
    }

    macro_rules! json {
        ($($tokens:tt)+) => {
            exact_json(stringify!($($tokens)+))
        };
    }

    /// A mock connected server with canned prompts / rendered content.
    struct MockPromptClient {
        name: RuntimeString,
        prompts: Option<Vec<McpPromptDescriptor>>,
        rendered: Option<RuntimeString>,
    }

    #[async_trait]
    impl McpResourceClient for MockPromptClient {
        fn name(&self) -> &RuntimeString {
            &self.name
        }
        async fn list_resources(&self) -> Vec<McpResourceDescriptor> {
            Vec::new()
        }
        async fn read_resource(&self, _uri: &RuntimeString) -> Vec<McpResourceContent> {
            Vec::new()
        }
        async fn list_prompts(&self) -> Option<Vec<McpPromptDescriptor>> {
            self.prompts.clone()
        }
        async fn get_prompt(
            &self,
            _name: &RuntimeString,
            _args: Option<RuntimeObject>,
        ) -> Option<RuntimeString> {
            self.rendered.clone()
        }
    }

    /// A mock that records the arguments it was rendered with.
    struct CapturingClient {
        name: RuntimeString,
        seen: Mutex<Option<Option<RuntimeObject>>>,
    }

    #[async_trait]
    impl McpResourceClient for CapturingClient {
        fn name(&self) -> &RuntimeString {
            &self.name
        }
        async fn list_resources(&self) -> Vec<McpResourceDescriptor> {
            Vec::new()
        }
        async fn read_resource(&self, _uri: &RuntimeString) -> Vec<McpResourceContent> {
            Vec::new()
        }
        async fn get_prompt(
            &self,
            _name: &RuntimeString,
            args: Option<RuntimeObject>,
        ) -> Option<RuntimeString> {
            *self.seen.lock().unwrap() = Some(args.clone());
            Some("ok".into())
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

    #[tokio::test]
    async fn prompt_json_text_preserves_lone_surrogate_as_inner_json_escape() {
        let lone = RuntimeString::from_utf16(vec![0xd800]);
        let client: Arc<dyn McpResourceClient> = Arc::new(MockPromptClient {
            name: "srv".into(),
            prompts: Some(vec![McpPromptDescriptor {
                name: lone,
                description: None,
                arguments: None,
            }]),
            rendered: None,
        });
        let ctx = ctx_with(vec![client]);

        let result = create_list_mcp_prompts_tool()
            .call(json!({}), &ctx, None)
            .await
            .unwrap();
        let inner = text_of(&result);
        assert!(inner.contains(r#"\ud800"#), "{inner:?}");
        assert!(!inner.contains("\u{fdd0}"), "{inner:?}");
        let outer =
            runtime_json::stringify(&runtime_object([("text", RuntimeJson::String(inner))]));
        assert!(outer.contains(r#"\\ud800"#), "{outer:?}");
        assert!(!outer.contains("\u{fdd0}"), "{outer:?}");
    }

    #[test]
    fn prompt_tools_expose_expected_metadata() {
        let list = create_list_mcp_prompts_tool();
        assert_eq!(list.name(), "list_mcp_prompts");
        assert_eq!(
            list.description(),
            "List prompts available from connected MCP servers (name, description, arguments)."
        );
        assert_eq!(
            list.search_hint(),
            Some(RuntimeString::from(
                "enumerate prompts exposed by connected MCP servers"
            ))
        );
        assert_eq!(list.max_result_size_chars(), Some(100_000));
        assert!(list.is_read_only(&json!({})));
        assert!(list.is_concurrency_safe(&json!({})));
        assert!(list.is_enabled());
        assert_eq!(list.is_destructive(&json!({})), Some(false));

        let get = create_get_mcp_prompt_tool();
        assert_eq!(get.name(), "get_mcp_prompt");
        assert_eq!(
            get.description(),
            "Render a specific prompt from a connected MCP server, with optional arguments."
        );
        assert_eq!(
            get.search_hint(),
            Some(RuntimeString::from(
                "fetch and render one MCP prompt by server and name"
            ))
        );
        assert_eq!(get.max_result_size_chars(), Some(100_000));
        assert!(get.is_read_only(&json!({})));
        assert!(get.is_concurrency_safe(&json!({})));
        assert_eq!(get.is_destructive(&json!({})), Some(false));
    }

    #[test]
    fn prompt_schemas_match_exactly() {
        assert_eq!(
            create_list_mcp_prompts_tool().input_schema(),
            json!({
                "type": "object",
                "properties": {
                    "server": { "type": "string", "description": "Limit the listing to this MCP server (optional)" }
                },
                "additionalProperties": false
            })
        );
        assert_eq!(
            create_get_mcp_prompt_tool().input_schema(),
            json!({
                "type": "object",
                "properties": {
                    "server": { "type": "string", "description": "Name of the MCP server holding the prompt" },
                    "name": { "type": "string", "description": "Name of the prompt to render" },
                    "arguments": {
                        "type": "object",
                        "description": "Argument values for the prompt, as a string→string map (optional)",
                        "propertyNames": { "type": "string" },
                        "additionalProperties": { "type": "string" }
                    }
                },
                "required": ["server", "name"],
                "additionalProperties": false
            })
        );
    }

    #[tokio::test]
    async fn list_prompts_reports_empty_when_no_servers() {
        let tool = create_list_mcp_prompts_tool();
        let ctx = make_ctx(".");
        let res = tool.call(json!({}), &ctx, None).await.unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            text_of(&res),
            "The connected MCP server(s) expose no prompts (they may still expose tools and resources)."
        );
        assert_eq!(res.serializable.output, json!({ "prompts": [] }));
    }

    #[tokio::test]
    async fn list_prompts_lists_with_arguments() {
        let client: Arc<dyn McpResourceClient> = Arc::new(MockPromptClient {
            name: "srv".into(),
            prompts: Some(vec![McpPromptDescriptor {
                name: "p".into(),
                description: Some("d".into()),
                arguments: Some(vec![McpPromptArgument {
                    name: "a".into(),
                    description: None,
                    required: Some(true),
                }]),
            }]),
            rendered: None,
        });
        let ctx = ctx_with(vec![client]);
        let tool = create_list_mcp_prompts_tool();
        let res = tool.call(json!({}), &ctx, None).await.unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output,
            json!({ "prompts": [ {
                "name": "p",
                "description": "d",
                "arguments": [ { "name": "a", "required": true } ],
                "server": "srv"
            } ] })
        );
        let expected = [
            "[",
            "  {",
            "    \"name\": \"p\",",
            "    \"description\": \"d\",",
            "    \"arguments\": [",
            "      {",
            "        \"name\": \"a\",",
            "        \"required\": true",
            "      }",
            "    ],",
            "    \"server\": \"srv\"",
            "  }",
            "]",
        ]
        .join("\n");
        assert_eq!(text_of(&res), expected);
    }

    #[tokio::test]
    async fn list_prompts_skips_servers_without_prompt_support() {
        let client: Arc<dyn McpResourceClient> = Arc::new(MockPromptClient {
            name: "srv".into(),
            prompts: None,
            rendered: None,
        });
        let ctx = ctx_with(vec![client]);
        let tool = create_list_mcp_prompts_tool();
        let res = tool.call(json!({}), &ctx, None).await.unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            text_of(&res),
            "The connected MCP server(s) expose no prompts (they may still expose tools and resources)."
        );
        assert_eq!(res.serializable.output, json!({ "prompts": [] }));
    }

    #[tokio::test]
    async fn list_prompts_flags_unknown_server() {
        let client: Arc<dyn McpResourceClient> = Arc::new(MockPromptClient {
            name: "srv".into(),
            prompts: None,
            rendered: None,
        });
        let ctx = ctx_with(vec![client]);
        let tool = create_list_mcp_prompts_tool();
        let res = tool
            .call(json!({ "server": "other" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "Unknown MCP server \"other\". Connected servers: srv"
        );
        assert_eq!(res.serializable.output, json!({ "prompts": [] }));
    }

    #[tokio::test]
    async fn get_prompt_renders_content() {
        let client: Arc<dyn McpResourceClient> = Arc::new(MockPromptClient {
            name: "srv".into(),
            prompts: None,
            rendered: Some("You are a helpful assistant.".into()),
        });
        let ctx = ctx_with(vec![client]);
        let tool = create_get_mcp_prompt_tool();
        let res = tool
            .call(json!({ "server": "srv", "name": "p" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(text_of(&res), "You are a helpful assistant.");
        assert_eq!(
            res.serializable.output,
            json!({ "prompt": "You are a helpful assistant." })
        );
    }

    #[tokio::test]
    async fn get_prompt_renders_empty_content() {
        let client: Arc<dyn McpResourceClient> = Arc::new(MockPromptClient {
            name: "srv".into(),
            prompts: None,
            rendered: Some(RuntimeString::default()),
        });
        let ctx = ctx_with(vec![client]);
        let tool = create_get_mcp_prompt_tool();
        let res = tool
            .call(json!({ "server": "srv", "name": "p" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(text_of(&res), "(the prompt rendered to empty content)");
        assert_eq!(res.serializable.output, json!({ "prompt": "" }));
    }

    #[tokio::test]
    async fn get_prompt_reports_missing_prompt_support() {
        let client: Arc<dyn McpResourceClient> = Arc::new(MockPromptClient {
            name: "srv".into(),
            prompts: None,
            rendered: None,
        });
        let ctx = ctx_with(vec![client]);
        let tool = create_get_mcp_prompt_tool();
        let res = tool
            .call(json!({ "server": "srv", "name": "p" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(text_of(&res), "MCP server \"srv\" does not expose prompts.");
        assert_eq!(res.serializable.output, json!({ "prompt": "" }));
    }

    #[tokio::test]
    async fn get_prompt_flags_unknown_server() {
        let tool = create_get_mcp_prompt_tool();
        let ctx = make_ctx(".");
        let res = tool
            .call(json!({ "server": "srv", "name": "p" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "Unknown MCP server \"srv\". Connected servers: (none)"
        );
        assert_eq!(res.serializable.output, json!({ "prompt": "" }));
    }

    #[tokio::test]
    async fn get_prompt_forwards_arguments() {
        let client = Arc::new(CapturingClient {
            name: "srv".into(),
            seen: Mutex::new(None),
        });
        let dyn_client: Arc<dyn McpResourceClient> = client.clone();
        let ctx = ctx_with(vec![dyn_client]);
        let tool = create_get_mcp_prompt_tool();
        let res = tool
            .call(
                json!({ "server": "srv", "name": "p", "arguments": { "k": "v" } }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(text_of(&res), "ok");
        let mut expected = RuntimeObject::new();
        expected.insert("k", RuntimeJson::String("v".into()));
        assert_eq!(client.seen.lock().unwrap().clone(), Some(Some(expected)));
    }

    #[tokio::test]
    async fn get_prompt_forwards_no_arguments_as_none() {
        let client = Arc::new(CapturingClient {
            name: "srv".into(),
            seen: Mutex::new(None),
        });
        let dyn_client: Arc<dyn McpResourceClient> = client.clone();
        let ctx = ctx_with(vec![dyn_client]);
        let tool = create_get_mcp_prompt_tool();
        let _ = tool
            .call(json!({ "server": "srv", "name": "p" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(client.seen.lock().unwrap().clone(), Some(None));
    }
}
