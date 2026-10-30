//! The `request_mcp_install` [`Tool`]: the model ASKS to add ONE store MCP
//! server the user explicitly wants; the user confirms via an elicitation
//! prompt, and only then does the engine write it into the local MCP config. The
//! model never adds a server on its own — it requests, the human confirms, the
//! engine performs the write. The server is added to the private `local` scope
//! and connects on the NEXT session (adding a live connection mid-session is a
//! host concern). Mirrors `request_skill_install`.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Map as SchemaMap, Value as SchemaValue};

use crate::mcp::config_file::McpScope;
use crate::mcp::mcp_hub::{McpCatalog, McpInstallFromCatalogOptions};
use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimeString};
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{
    ElicitRequest, ElicitResponse, Tool, ToolContext, ToolProgress, ToolResult,
};

/// Options for constructing the `request_mcp_install` tool.
#[derive(Default)]
pub struct RequestMcpInstallToolOptions {
    /// Catalog backend; defaults to the engine's built-in MCP hub catalog.
    pub backend: Option<Box<dyn McpCatalog>>,
}

/// Build the tool's input JSON Schema: required `slug` + `reason`.
fn build_input_schema() -> JsonSchema {
    let mut slug = SchemaMap::new();
    slug.insert("type".to_string(), json!("string"));
    slug.insert(
        "description".to_string(),
        json!("The exact server slug returned by mcp_search"),
    );
    let mut reason = SchemaMap::new();
    reason.insert("type".to_string(), json!("string"));
    reason.insert(
        "description".to_string(),
        json!("One concise, user-facing line on why this server helps with the current request"),
    );
    let mut properties = SchemaMap::new();
    properties.insert("slug".to_string(), SchemaValue::Object(slug));
    properties.insert("reason".to_string(), SchemaValue::Object(reason));
    let mut schema = SchemaMap::new();
    schema.insert("type".to_string(), json!("object"));
    schema.insert("properties".to_string(), SchemaValue::Object(properties));
    schema.insert("required".to_string(), json!(["slug", "reason"]));
    schema.insert("additionalProperties".to_string(), json!(false));
    schema
}

fn install_output(slug: &RuntimeString, installed: bool) -> RuntimeJson {
    let mut output = RuntimeObject::new();
    output.insert("slug", RuntimeJson::String(slug.clone()));
    output.insert("installed", RuntimeJson::Bool(installed));
    RuntimeJson::Object(output)
}

/// Ask the user to add one MCP server from the store.
pub struct RequestMcpInstallTool {
    input_schema: JsonSchema,
    backend: Option<Box<dyn McpCatalog>>,
}

/// Construct the `request_mcp_install` tool as an erased registry-side [`Tool`].
pub fn create_request_mcp_install_tool(opts: RequestMcpInstallToolOptions) -> Arc<dyn Tool> {
    let backend = opts.backend;
    Arc::new(RequestMcpInstallTool {
        input_schema: build_input_schema(),
        backend,
    })
}

#[async_trait]
impl Tool for RequestMcpInstallTool {
    fn static_name(&self) -> &str {
        "request_mcp_install"
    }

    fn static_description(&self) -> &str {
        "Request installation of ONE MCP server from a prior mcp_search result, using its exact slug. Explain how its capability fulfills the user's request or supplies missing access needed for the authorized task. Do not request unrelated servers or broad recommendations. The user is shown a confirmation, and only after approval does the engine write the server into the local config; it connects next session. Discovery or an inferred need does not grant installation approval. If the user declines, continue without it and do not ask again unless they bring it up."
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
        Some(RuntimeString::from("Requesting MCP server: ").concat(&slug))
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

        let Some(backend) = self.backend.as_ref() else {
            return Ok(text_result(
                install_output(&slug, false),
                "No MCP server store is configured in this build.",
                true,
            ));
        };

        // Validate the slug is a real store entry before bothering the user.
        let entry = match backend.get(&slug, Some(ctx.signal.clone())).await {
            Ok(e) => e,
            Err(err) => {
                return Ok(text_result(
                    install_output(&slug, false),
                    format!("MCP store error: {err}"),
                    true,
                ));
            }
        };
        let Some(entry) = entry else {
            return Ok(text_result(
                install_output(&slug, false),
                RuntimeString::from("No MCP server \"")
                    .concat(&slug)
                    .concat(&RuntimeString::from(
                        "\" exists in the store. Call mcp_search first and use a slug it returned.",
                    )),
                true,
            ));
        };

        // Human gate: adding a server is the user's decision, asked via the host's elicitation surface.
        let Some(elicit) = ctx.elicit.as_ref() else {
            return Ok(text_result(
                install_output(&slug, false),
                RuntimeString::from("Cannot add \"")
                    .concat(&slug)
                    .concat(&RuntimeString::from(
                        "\" without a user to confirm (no interactive session). Tell the user to run `mcp install ",
                    ))
                    .concat(&slug)
                    .concat(&RuntimeString::from("`.")),
                true,
            ));
        };

        let resp = elicit
            .elicit(
                ElicitRequest {
                    questions: None,
                    attachments_supported: None,
                    metadata: None,
                    prompt: RuntimeString::from("Add MCP server \"")
                        .concat(&entry.name)
                        .concat(&RuntimeString::from("\" ("))
                        .concat(&entry.slug)
                        .concat(&RuntimeString::from(")?\n"))
                        .concat(&reason)
                        .concat(&RuntimeString::from(
                            "\n\nThis wires a third-party MCP server (its code / credentials) into your environment.",
                        )),
                    options: Some(vec![RuntimeString::from("Add"), RuntimeString::from("Cancel")]),
                    multi_select: None,
                },
                ctx.signal.clone(),
            )
            .await;

        let accepted = matches!(
            &resp,
            ElicitResponse::Accept { values } if values.first().is_some_and(|v| v == "Add")
        );
        if !accepted {
            return Ok(text_result(
                install_output(&slug, false),
                RuntimeString::from("The user declined to add \"")
                    .concat(&slug)
                    .concat(&RuntimeString::from("\". Continue without it.")),
                false,
            ));
        }

        // Confirmed → write into the private local scope for this project.
        match backend
            .install(
                &slug,
                McpInstallFromCatalogOptions {
                    cwd: ctx.cwd.clone(),
                    scope: Some(McpScope::Local),
                    user_config_dir: None,
                    signal: Some(ctx.signal.clone()),
                },
            )
            .await
        {
            Ok(Some(_cfg)) => Ok(text_result(
                install_output(&slug, true),
                RuntimeString::from("✅ Added MCP server \"")
                    .concat(&slug)
                    .concat(&RuntimeString::from(
                        "\" to your local config. It connects in a new session.",
                    )),
                false,
            )),
            Ok(None) => Ok(text_result(
                install_output(&slug, false),
                RuntimeString::from("Adding \"")
                    .concat(&slug)
                    .concat(&RuntimeString::from(
                        "\" did not complete (the store entry has no usable command/url).",
                    )),
                true,
            )),
            Err(err) => Ok(text_result(
                install_output(&slug, false),
                format!("Adding failed: {err}"),
                true,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::mcp_hub::{McpCatalogEntry, McpCatalogSearchOptions};
    use crate::mcp::types::McpServerConfig;
    use crate::mcp::types::McpServerOptions;
    use crate::runtime_values::runtime_json;
    use crate::schemas::content::OutputBlock;
    use crate::tools::builtins::test_ctx::make_ctx;
    use crate::types::abort::AbortSignal;
    use crate::types::tool::ElicitHandler;
    use std::collections::HashSet;
    use std::sync::Mutex;

    type BoxError = Box<dyn std::error::Error + Send + Sync>;

    /// A catalog stub recording installs. `get` returns `None` for slugs in
    /// `missing`; `install` returns `None` for slugs in `incomplete`.
    struct RecordingCatalog {
        missing: HashSet<RuntimeString>,
        incomplete: HashSet<RuntimeString>,
        installed: Arc<Mutex<Vec<RuntimeString>>>,
    }

    #[async_trait]
    impl McpCatalog for RecordingCatalog {
        async fn search(
            &self,
            _query: &RuntimeString,
            _opts: McpCatalogSearchOptions,
        ) -> Result<Vec<McpCatalogEntry>, BoxError> {
            Ok(Vec::new())
        }
        async fn get(
            &self,
            slug: &RuntimeString,
            _signal: Option<AbortSignal>,
        ) -> Result<Option<McpCatalogEntry>, BoxError> {
            if self.missing.contains(slug) {
                return Ok(None);
            }
            Ok(Some(McpCatalogEntry {
                slug: slug.clone(),
                name: RuntimeString::from("Name ").concat(slug),
                description: RuntimeString::from("d"),
                source: Some(RuntimeString::from("OFFICIAL")),
                server: exact_json(r#"{"command":"x"}"#),
                keywords: None,
            }))
        }
        async fn install(
            &self,
            slug: &RuntimeString,
            _opts: McpInstallFromCatalogOptions,
        ) -> Result<Option<McpServerConfig>, BoxError> {
            self.installed.lock().unwrap().push(slug.clone());
            if self.incomplete.contains(slug) {
                return Ok(None);
            }
            Ok(Some(McpServerConfig::Stdio {
                name: "recording".to_string(),
                command: "x".to_string(),
                args: None,
                env: None,
                options: McpServerOptions::default(),
            }))
        }
    }

    struct FakeElicit {
        resp: ElicitResponse,
    }

    #[async_trait]
    impl ElicitHandler for FakeElicit {
        async fn elicit(&self, _req: ElicitRequest, _signal: AbortSignal) -> ElicitResponse {
            self.resp.clone()
        }
    }

    fn accept_ctx() -> ToolContext {
        let mut ctx = make_ctx("/tmp");
        ctx.elicit = Some(Arc::new(FakeElicit {
            resp: ElicitResponse::Accept {
                values: vec![RuntimeString::from("Add")],
            },
        }));
        ctx
    }

    fn decline_ctx() -> ToolContext {
        let mut ctx = make_ctx("/tmp");
        ctx.elicit = Some(Arc::new(FakeElicit {
            resp: ElicitResponse::Decline { reason: None },
        }));
        ctx
    }

    fn recording(
        missing: &[&str],
        incomplete: &[&str],
    ) -> (RequestMcpInstallToolOptions, Arc<Mutex<Vec<RuntimeString>>>) {
        let installed = Arc::new(Mutex::new(Vec::new()));
        let cat = RecordingCatalog {
            missing: missing.iter().map(|s| RuntimeString::from(*s)).collect(),
            incomplete: incomplete.iter().map(|s| RuntimeString::from(*s)).collect(),
            installed: installed.clone(),
        };
        (
            RequestMcpInstallToolOptions {
                backend: Some(Box::new(cat)),
            },
            installed,
        )
    }

    fn exact_json(source: &str) -> RuntimeJson {
        runtime_json::parse(&RuntimeString::from(source)).expect("valid exact JSON fixture")
    }

    fn text_of(res: &ToolResult) -> &RuntimeString {
        match &res.serializable.model_content[0] {
            OutputBlock::Text(t) => &t.text,
            _ => panic!("expected a text block"),
        }
    }

    #[test]
    fn exposes_expected_metadata() {
        let tool = create_request_mcp_install_tool(RequestMcpInstallToolOptions::default());
        assert_eq!(tool.name(), "request_mcp_install");
        let input = exact_json(r#"{"slug":"gh","reason":"x"}"#);
        assert!(!tool.is_read_only(&input));
        assert!(!tool.is_concurrency_safe(&input));
        assert_eq!(tool.is_destructive(&input), Some(false));
        assert!(tool.is_enabled());
        assert_eq!(
            tool.summarize(&input),
            Some(RuntimeString::from("Requesting MCP server: gh"))
        );
    }

    #[test]
    fn input_schema_describes_the_fields() {
        let tool = create_request_mcp_install_tool(RequestMcpInstallToolOptions::default());
        let schema = tool.input_schema();
        assert_eq!(
            schema.get("type").and_then(RuntimeJson::as_string).cloned(),
            Some(RuntimeString::from("object"))
        );
        let props = schema.get("properties").expect("properties");
        assert_eq!(
            props
                .get("slug")
                .and_then(|s| s.get("type"))
                .and_then(RuntimeJson::as_string)
                .cloned(),
            Some(RuntimeString::from("string"))
        );
        assert_eq!(
            props
                .get("slug")
                .and_then(|s| s.get("description"))
                .and_then(RuntimeJson::as_string)
                .cloned(),
            Some(RuntimeString::from(
                "The exact server slug returned by mcp_search"
            ))
        );
        assert_eq!(
            props
                .get("reason")
                .and_then(|s| s.get("type"))
                .and_then(RuntimeJson::as_string)
                .cloned(),
            Some(RuntimeString::from("string"))
        );
        assert_eq!(
            schema.get("required").unwrap(),
            &exact_json(r#"["slug","reason"]"#)
        );
        assert_eq!(
            schema.get("additionalProperties").unwrap(),
            &RuntimeJson::Bool(false)
        );
    }

    #[tokio::test]
    async fn adds_only_after_the_user_accepts() {
        let (opts, installed) = recording(&[], &[]);
        let tool = create_request_mcp_install_tool(opts);
        let ctx = accept_ctx();
        let res = tool
            .call(
                exact_json(r#"{"slug":"gh","reason":"user asked for github"}"#),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output,
            exact_json(r#"{"slug":"gh","installed":true}"#)
        );
        assert_eq!(
            text_of(&res),
            "✅ Added MCP server \"gh\" to your local config. It connects in a new session."
        );
        assert_eq!(*installed.lock().unwrap(), vec![RuntimeString::from("gh")]);
    }

    #[tokio::test]
    async fn does_not_add_when_the_user_declines() {
        let (opts, installed) = recording(&[], &[]);
        let tool = create_request_mcp_install_tool(opts);
        let ctx = decline_ctx();
        let res = tool
            .call(exact_json(r#"{"slug":"gh","reason":"x"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output,
            exact_json(r#"{"slug":"gh","installed":false}"#)
        );
        assert_eq!(
            text_of(&res),
            "The user declined to add \"gh\". Continue without it."
        );
        assert!(installed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn errors_for_a_slug_not_in_the_store() {
        let (opts, installed) = recording(&["ghost"], &[]);
        let tool = create_request_mcp_install_tool(opts);
        let ctx = accept_ctx();
        let res = tool
            .call(exact_json(r#"{"slug":"ghost","reason":"x"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "No MCP server \"ghost\" exists in the store. Call mcp_search first and use a slug it returned."
        );
        assert!(installed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn errors_when_there_is_no_user_to_confirm() {
        let (opts, installed) = recording(&[], &[]);
        let tool = create_request_mcp_install_tool(opts);
        let ctx = make_ctx("/tmp");
        let res = tool
            .call(exact_json(r#"{"slug":"gh","reason":"x"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "Cannot add \"gh\" without a user to confirm (no interactive session). Tell the user to run `mcp install gh`."
        );
        assert!(installed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn reports_when_the_install_does_not_complete() {
        let (opts, installed) = recording(&[], &["bad"]);
        let tool = create_request_mcp_install_tool(opts);
        let ctx = accept_ctx();
        let res = tool
            .call(exact_json(r#"{"slug":"bad","reason":"x"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            res.serializable.output,
            exact_json(r#"{"slug":"bad","installed":false}"#)
        );
        assert_eq!(
            text_of(&res),
            "Adding \"bad\" did not complete (the store entry has no usable command/url)."
        );
        assert_eq!(*installed.lock().unwrap(), vec![RuntimeString::from("bad")]);
    }

    #[tokio::test]
    async fn errors_when_no_catalog_is_configured() {
        let tool = create_request_mcp_install_tool(RequestMcpInstallToolOptions::default());
        let ctx = accept_ctx();
        let res = tool
            .call(exact_json(r#"{"slug":"gh","reason":"x"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "No MCP server store is configured in this build."
        );
    }

    #[tokio::test]
    async fn declined_request_preserves_a_lone_surrogate_slug_exactly() {
        let slug = RuntimeString::from_utf16(vec![b'm' as u16, 0xd800]);
        let (opts, installed) = recording(&[], &[]);
        let tool = create_request_mcp_install_tool(opts);
        let mut input = RuntimeObject::new();
        input.insert("slug", RuntimeJson::String(slug.clone()));
        input.insert(
            "reason",
            RuntimeJson::String(RuntimeString::from("requested")),
        );
        let result = tool
            .call(RuntimeJson::Object(input), &decline_ctx(), None)
            .await
            .unwrap();

        assert_eq!(
            result.serializable.output["slug"],
            RuntimeJson::String(slug)
        );
        assert!(text_of(&result).as_utf16().contains(&0xd800));
        assert!(installed.lock().unwrap().is_empty());
    }
}
