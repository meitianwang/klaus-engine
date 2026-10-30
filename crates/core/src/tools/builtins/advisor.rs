//! Independent text-only consultation using the calling query's effective context.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::runtime_values::{RuntimeJson, RuntimeString};
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};

pub struct AdvisorTool {
    schema: JsonSchema,
}

pub fn create_advisor_tool() -> Arc<dyn Tool> {
    Arc::new(AdvisorTool {
        schema: json!({
            "type": "object",
            "properties": {"question": {"type": "string", "minLength": 1,
                "description": "The focused question to ask the independently configured advisor. The effective context is supplied automatically; the current main model summarizes it if needed to fit the advisor."}},
            "required": ["question"],
            "additionalProperties": false
        }).as_object().cloned().unwrap(),
    })
}

#[async_trait]
impl Tool for AdvisorTool {
    fn static_name(&self) -> &str {
        "advisor"
    }

    fn static_description(&self) -> &str {
        "Consult the independently configured advisor when focused advice would help resolve a difficult decision or consequential uncertainty. Supply only the focused question: the effective context is included automatically, with a summary by the current main model when required by the advisor's capacity. Returns text advice, never actions or tool calls. Consultation is optional; do not call for trivial replies or repeatedly retry a disabled or unavailable advisor."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }

    fn is_enabled(&self) -> bool {
        true
    }

    // The snapshot must include completed earlier tools, not race those tools.
    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let question = input
            .get("question")
            .and_then(Json::as_str)
            .filter(|question| !crate::engine::session::runtime_trim(question).is_empty())
            .ok_or_else(|| ToolError::new("advisor question must be nonempty"))?;
        let advisor = ctx
            .advisor
            .as_ref()
            .ok_or_else(|| ToolError::new("advisor is unavailable in this execution context"))?;
        let request = advisor
            .request
            .clone()
            .ok_or_else(|| ToolError::new("advisor requires a current query context snapshot"))?;
        let text =
            (advisor.query)(RuntimeString::from(question), request, ctx.signal.clone()).await?;
        Ok(text_result(RuntimeJson::String(text.clone()), text, false))
    }
}
