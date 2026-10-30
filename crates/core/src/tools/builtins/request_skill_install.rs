//! The `request_skill_install` [`Tool`]: the model ASKS to install a store skill
//! the user explicitly wants; the user confirms via an elicitation prompt, and
//! only then does the engine download + install it. The model never installs on
//! its own — it requests, the human confirms, the engine performs the install.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Map as SchemaMap, Value as SchemaValue};

use crate::extensions::skills::catalog::{SkillCatalog, SkillInstallFromCatalogOptions};
use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimeString};
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{
    ElicitRequest, ElicitResponse, Tool, ToolContext, ToolProgress, ToolResult,
};

/// Options for constructing the `request_skill_install` tool.
#[derive(Default)]
pub struct RequestSkillInstallToolOptions {
    /// Catalog backend; defaults to the engine's built-in skill catalog.
    pub backend: Option<Box<dyn SkillCatalog>>,
}

/// Build the tool's input JSON Schema: required `slug` + `reason`, optional `version`.
fn build_input_schema() -> JsonSchema {
    let mut slug = SchemaMap::new();
    slug.insert("type".to_string(), json!("string"));
    slug.insert(
        "description".to_string(),
        json!("The exact skill slug returned by skill_search"),
    );
    let mut reason = SchemaMap::new();
    reason.insert("type".to_string(), json!("string"));
    reason.insert(
        "description".to_string(),
        json!("One concise, user-facing line on why this skill helps with the current request"),
    );
    let mut version = SchemaMap::new();
    version.insert(
        "description".to_string(),
        json!("Optional version; defaults to the latest"),
    );
    version.insert("type".to_string(), json!("string"));
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

fn install_output(slug: &RuntimeString, installed: bool) -> RuntimeJson {
    let mut output = RuntimeObject::new();
    output.insert("slug", RuntimeJson::String(slug.clone()));
    output.insert("installed", RuntimeJson::Bool(installed));
    RuntimeJson::Object(output)
}

/// Ask the user to install one skill from the store.
pub struct RequestSkillInstallTool {
    input_schema: JsonSchema,
    backend: Option<Box<dyn SkillCatalog>>,
}

/// Construct the `request_skill_install` tool as an erased registry-side [`Tool`].
pub fn create_request_skill_install_tool(opts: RequestSkillInstallToolOptions) -> Arc<dyn Tool> {
    let backend = opts.backend;
    Arc::new(RequestSkillInstallTool {
        input_schema: build_input_schema(),
        backend,
    })
}

#[async_trait]
impl Tool for RequestSkillInstallTool {
    fn static_name(&self) -> &str {
        "request_skill_install"
    }

    fn static_description(&self) -> &str {
        "Request installation of ONE skill from a prior skill_search result, using its exact slug. Explain how it fulfills the user's request or supplies a capability needed for the authorized task. Do not request unrelated skills or broad recommendations. The user is shown a confirmation, and only after approval does the engine download and install it. Discovery or an inferred need does not grant installation approval. If the user declines, continue without it and do not ask again unless they bring it up."
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
        Some(RuntimeString::from("Requesting install: ").concat(&slug))
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
        // Set version only when the model supplied a non-empty one.
        let version = input
            .get("version")
            .and_then(RuntimeJson::as_string)
            .filter(|v| !v.is_empty())
            .cloned();

        let Some(backend) = self.backend.as_ref() else {
            return Ok(text_result(
                install_output(&slug, false),
                "No skill store is configured in this build.",
                true,
            ));
        };

        // Validate the slug is a real store entry before bothering the user.
        let entry = match backend.get(&slug, Some(ctx.signal.clone())).await {
            Ok(e) => e,
            Err(err) => {
                return Ok(text_result(
                    install_output(&slug, false),
                    format!("Skill store error: {err}"),
                    true,
                ));
            }
        };
        let Some(entry) = entry else {
            return Ok(text_result(
                install_output(&slug, false),
                RuntimeString::from("No skill \"")
                    .concat(&slug)
                    .concat(&RuntimeString::from(
                    "\" exists in the store. Call skill_search first and use a slug it returned.",
                )),
                true,
            ));
        };

        // Human gate: install is the user's decision, asked via the host's elicitation surface.
        let Some(elicit) = ctx.elicit.as_ref() else {
            return Ok(text_result(
                install_output(&slug, false),
                RuntimeString::from("Cannot install \"")
                    .concat(&slug)
                    .concat(&RuntimeString::from(
                        "\" without a user to confirm (no interactive session). Tell the user to run `skill install ",
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
                    prompt: RuntimeString::from("Install skill \"")
                        .concat(&entry.name)
                        .concat(&RuntimeString::from("\" ("))
                        .concat(&entry.slug)
                        .concat(&RuntimeString::from(")?\n"))
                        .concat(&reason)
                        .concat(&RuntimeString::from(
                            "\n\nThis downloads and enables third-party code from the skill store.",
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
                install_output(&slug, false),
                RuntimeString::from("The user declined to install \"")
                    .concat(&slug)
                    .concat(&RuntimeString::from("\". Continue without it.")),
                false,
            ));
        }

        // Confirmed → the engine performs the install.
        match backend
            .install(
                &ctx.process_context,
                &slug,
                SkillInstallFromCatalogOptions {
                    version,
                    signal: Some(ctx.signal.clone()),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(Some(skill)) => Ok(text_result(
                install_output(&slug, true),
                RuntimeString::from("✅ Installed \"")
                    .concat(&skill.name)
                    .concat(&RuntimeString::from(
                        "\". It becomes available to skills in a new session.",
                    )),
                false,
            )),
            Ok(None) => Ok(text_result(
                install_output(&slug, false),
                RuntimeString::from("Install of \"")
                    .concat(&slug)
                    .concat(&RuntimeString::from(
                        "\" did not complete (not found, invalid, or already installed).",
                    )),
                true,
            )),
            Err(err) => Ok(text_result(
                install_output(&slug, false),
                format!("Install failed: {err}"),
                true,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extensions::skills::catalog::{SkillCatalogEntry, SkillSearchOptions};
    use crate::extensions::skills::loader::Skill;
    use crate::runtime_values::runtime_json;
    use crate::schemas::content::OutputBlock;
    use crate::tools::builtins::test_ctx::make_ctx as base_make_ctx;
    use crate::types::abort::AbortSignal;
    use crate::types::tool::ElicitHandler;
    use std::collections::HashSet;
    use std::sync::Mutex;

    type BoxError = Box<dyn std::error::Error + Send + Sync>;

    /// A catalog stub recording installs; `get` returns `None` for slugs in `missing`.
    struct RecordingCatalog {
        missing: HashSet<RuntimeString>,
        installed: Arc<Mutex<Vec<RuntimeString>>>,
    }

    #[async_trait]
    impl SkillCatalog for RecordingCatalog {
        async fn search(
            &self,
            _query: &RuntimeString,
            _opts: SkillSearchOptions,
        ) -> Result<Vec<SkillCatalogEntry>, BoxError> {
            Ok(Vec::new())
        }
        async fn get(
            &self,
            slug: &RuntimeString,
            _signal: Option<AbortSignal>,
        ) -> Result<Option<SkillCatalogEntry>, BoxError> {
            if self.missing.contains(slug) {
                return Ok(None);
            }
            Ok(Some(SkillCatalogEntry {
                slug: slug.clone(),
                name: RuntimeString::from("Name ").concat(slug),
                description: RuntimeString::from("d"),
                source: RuntimeString::from("OFFICIAL"),
                tags: None,
                downloads: None,
                version: None,
            }))
        }
        async fn install(
            &self,
            _process: &crate::runtime_process::RuntimeProcessContext,
            slug: &RuntimeString,
            _opts: SkillInstallFromCatalogOptions,
        ) -> Result<Option<Skill>, BoxError> {
            self.installed.lock().unwrap().push(slug.clone());
            Ok(Some(Skill {
                name: "Name huangli".into(),
                description: "d".into(),
                body: RuntimeString::default(),
                path: "/x/huangli/SKILL.md".into(),
                ..Default::default()
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

    fn make_ctx(elicit: Option<Arc<dyn ElicitHandler>>) -> ToolContext {
        let mut ctx = base_make_ctx("/tmp");
        ctx.elicit = elicit;
        ctx
    }

    fn accept_ctx() -> ToolContext {
        make_ctx(Some(Arc::new(FakeElicit {
            resp: ElicitResponse::Accept {
                values: vec![RuntimeString::from("Install")],
            },
        })))
    }

    fn decline_ctx() -> ToolContext {
        make_ctx(Some(Arc::new(FakeElicit {
            resp: ElicitResponse::Decline { reason: None },
        })))
    }

    fn recording(
        missing: &[&str],
    ) -> (
        RequestSkillInstallToolOptions,
        Arc<Mutex<Vec<RuntimeString>>>,
    ) {
        let installed = Arc::new(Mutex::new(Vec::new()));
        let cat = RecordingCatalog {
            missing: missing.iter().map(|s| RuntimeString::from(*s)).collect(),
            installed: installed.clone(),
        };
        (
            RequestSkillInstallToolOptions {
                backend: Some(Box::new(cat)),
            },
            installed,
        )
    }

    fn exact_json(source: &str) -> RuntimeJson {
        runtime_json::parse(&RuntimeString::from(source)).expect("valid exact JSON fixture")
    }

    #[test]
    fn exposes_expected_metadata() {
        let tool = create_request_skill_install_tool(RequestSkillInstallToolOptions::default());
        assert_eq!(tool.name(), "request_skill_install");
        let input = exact_json(r#"{"slug":"huangli","reason":"x"}"#);
        assert!(!tool.is_read_only(&input));
        assert!(!tool.is_concurrency_safe(&input));
        assert_eq!(tool.is_destructive(&input), Some(false));
        assert!(tool.is_enabled());
        assert_eq!(
            tool.summarize(&input),
            Some(RuntimeString::from("Requesting install: huangli"))
        );
    }

    #[test]
    fn input_schema_describes_the_fields() {
        let tool = create_request_skill_install_tool(RequestSkillInstallToolOptions::default());
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
                .get("reason")
                .and_then(|s| s.get("type"))
                .and_then(RuntimeJson::as_string)
                .cloned(),
            Some(RuntimeString::from("string"))
        );
        assert_eq!(
            props
                .get("version")
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

    fn text_of(res: &ToolResult) -> &RuntimeString {
        match &res.serializable.model_content[0] {
            OutputBlock::Text(t) => &t.text,
            _ => panic!("expected a text block"),
        }
    }

    #[tokio::test]
    async fn installs_only_after_the_user_accepts() {
        let (opts, installed) = recording(&[]);
        let tool = create_request_skill_install_tool(opts);
        let ctx = accept_ctx();
        let res = tool
            .call(
                exact_json(r#"{"slug":"huangli","reason":"user asked for almanac"}"#),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output,
            exact_json(r#"{"slug":"huangli","installed":true}"#)
        );
        assert!(text_of(&res).contains("Installed \"Name huangli\""));
        assert_eq!(
            *installed.lock().unwrap(),
            vec![RuntimeString::from("huangli")]
        );
    }

    #[tokio::test]
    async fn does_not_install_when_the_user_declines() {
        let (opts, installed) = recording(&[]);
        let tool = create_request_skill_install_tool(opts);
        let ctx = decline_ctx();
        let res = tool
            .call(exact_json(r#"{"slug":"huangli","reason":"x"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(
            res.serializable.output,
            exact_json(r#"{"slug":"huangli","installed":false}"#)
        );
        assert!(installed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn errors_for_a_slug_not_in_the_store() {
        let (opts, installed) = recording(&["ghost"]);
        let tool = create_request_skill_install_tool(opts);
        let ctx = accept_ctx();
        let res = tool
            .call(exact_json(r#"{"slug":"ghost","reason":"x"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert!(installed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn errors_when_there_is_no_user_to_confirm() {
        let (opts, installed) = recording(&[]);
        let tool = create_request_skill_install_tool(opts);
        let ctx = make_ctx(None);
        let res = tool
            .call(exact_json(r#"{"slug":"huangli","reason":"x"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert!(installed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn errors_when_no_catalog_is_configured() {
        let tool = create_request_skill_install_tool(RequestSkillInstallToolOptions::default());
        let ctx = accept_ctx();
        let res = tool
            .call(exact_json(r#"{"slug":"huangli","reason":"x"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
    }

    #[tokio::test]
    async fn declined_request_preserves_a_lone_surrogate_slug_exactly() {
        let slug = RuntimeString::from_utf16(vec![b's' as u16, 0xd800]);
        let (opts, installed) = recording(&[]);
        let tool = create_request_skill_install_tool(opts);
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
