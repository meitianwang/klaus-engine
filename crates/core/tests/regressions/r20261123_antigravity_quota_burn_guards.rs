//! Defect: Antigravity sessions drained the shared subscription quota far
//! faster than their visible work warranted. Three engine paths re-sent large
//! uncached prefixes at full price:
//! (1) sub-agents spawned without `fork_turns` inherited the parent's entire
//!     conversation even though a child request can never reuse the parent's
//!     implicit cache entry;
//! (2) oversized tool results were never budgeted and aged tool outputs were
//!     only reclaimed after a 60-minute idle gap, while the implicit Gemini
//!     cache expires within minutes;
//! (3) composer suggestions ran on the main model with its full reasoning
//!     depth, and still fired after the implicit cache had gone cold, paying a
//!     full long-history prefix for a single short line.
//! Root cause: the isolated-context default, the tool-result budget and the
//! cold-cache window were keyed to the Claude subscription source only, and
//! the suggestion request copied the main turn's thinking settings verbatim.
//! Work item: 20261123 Antigravity quota burn guards.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::config::EngineConfig;
use klaus_agent_core::tools::build_tool::{
    text_result, DEFAULT_IS_CONCURRENCY_SAFE, DEFAULT_IS_DESTRUCTIVE,
};
use klaus_agent_core::tools::builtins::read::create_read_tool;
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ToolError;
use klaus_agent_core::types::json::Json;
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use serde_json::{json, Value};

use super::r20261117_antigravity_gemini_prompt_cache::{
    start_antigravity_gemini_session_with_overrides, CapturedGeminiCall, GeminiFixtureOverrides,
    ScriptedGeminiClient, ScriptedReply,
};

const START_MS: i64 = 1_800_000_000_000;
const MINUTE_MS: i64 = 60 * 1_000;
const PAYLOAD_TAIL: &str = "QUOTA_PAYLOAD_TAIL_MARKER";

fn contents_text(call: &CapturedGeminiCall) -> String {
    call.wire["contents"].to_string()
}

fn controllable_clock() -> (Arc<AtomicI64>, klaus_agent_core::engine::deps::NowFn) {
    let clock = Arc::new(AtomicI64::new(START_MS));
    let reader = clock.clone();
    (clock, Arc::new(move || reader.load(Ordering::SeqCst)))
}

/// A non-`read` tool whose single result is large enough to be reclaimed.
struct LargeReportTool;

#[async_trait]
impl Tool for LargeReportTool {
    fn name(&self) -> RuntimeString {
        RuntimeString::from("large_report")
    }

    fn description(&self) -> RuntimeString {
        RuntimeString::from("Return a large diagnostic report")
    }

    fn input_schema(&self) -> Json {
        runtime_json::parse(&RuntimeString::from(
            r#"{"type":"object","properties":{},"additionalProperties":false}"#,
        ))
        .expect("valid schema")
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        DEFAULT_IS_CONCURRENCY_SAFE
    }

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        DEFAULT_IS_DESTRUCTIVE
    }

    async fn call(
        &self,
        _input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let body = format!("{}{PAYLOAD_TAIL}", "report line\n".repeat(600));
        Ok(text_result(RuntimeJson::Null, body, false))
    }
}

#[test]
fn antigravity_source_enables_isolated_children_and_tool_result_budget() {
    let antigravity: EngineConfig = serde_json::from_value(json!({
        "modelSource": { "source": "antigravity" }
    }))
    .unwrap();
    assert!(antigravity.is_antigravity_source());
    assert!(antigravity.subagents_default_isolated_context());
    assert!(antigravity.effective_aggregate_tool_results());

    let codex: EngineConfig = serde_json::from_value(json!({
        "modelSource": { "source": "codex" }
    }))
    .unwrap();
    assert!(!codex.is_antigravity_source());
    assert!(!codex.subagents_default_isolated_context());
    assert!(!codex.effective_aggregate_tool_results());
}

#[tokio::test]
async fn antigravity_subagents_start_isolated_unless_inheritance_is_explicit() {
    let client = ScriptedGeminiClient::new(vec![
        ScriptedReply::ToolCall {
            id: "call-default-child",
            name: "task",
            args: json!({
                "description": "default child",
                "prompt": "CHILD_DEFAULT_PROMPT",
                "model": "low",
                "run_in_background": false
            }),
        },
        ScriptedReply::Text("default child done"),
        ScriptedReply::ToolCall {
            id: "call-fork-child",
            name: "task",
            args: json!({
                "description": "fork child",
                "prompt": "CHILD_FORK_PROMPT",
                "model": "low",
                "fork_turns": "all",
                "run_in_background": false
            }),
        },
        ScriptedReply::Text("fork child done"),
        ScriptedReply::Text("parent done"),
    ]);
    let (_dir, session) = start_antigravity_gemini_session_with_overrides(
        client.clone(),
        vec![
            create_read_tool(),
            create_task_tool(TaskToolOptions::default()),
        ],
        200_000,
        None,
        None,
        GeminiFixtureOverrides::default(),
    );

    let result = session
        .run(
            Input::Text("PARENT_ANTIGRAVITY_SENTINEL_9137 coordinate children".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(result.final_text, "parent done");

    let calls = client.taken();
    let default_child = calls
        .iter()
        .find(|call| contents_text(call).contains("CHILD_DEFAULT_PROMPT"))
        .expect("default child request");
    assert!(
        !contents_text(default_child).contains("PARENT_ANTIGRAVITY_SENTINEL_9137"),
        "a child spawned without fork_turns must not carry the parent conversation"
    );
    let fork_child = calls
        .iter()
        .find(|call| contents_text(call).contains("CHILD_FORK_PROMPT"))
        .expect("fork child request");
    assert!(
        contents_text(fork_child).contains("PARENT_ANTIGRAVITY_SENTINEL_9137"),
        "an explicit fork_turns=all child must keep inheriting the parent conversation"
    );

    session.dispose().await;
}

#[tokio::test]
async fn antigravity_reclaims_aged_tool_output_after_ten_idle_minutes() {
    let (clock, now) = controllable_clock();
    let client = ScriptedGeminiClient::new(vec![
        ScriptedReply::ToolCall {
            id: "call-report",
            name: "large_report",
            args: json!({}),
        },
        ScriptedReply::Text("report read"),
        ScriptedReply::Text("warm follow-up"),
        ScriptedReply::Text("cold follow-up"),
    ]);
    let (_dir, session) = start_antigravity_gemini_session_with_overrides(
        client.clone(),
        vec![Arc::new(LargeReportTool) as Arc<dyn Tool>],
        200_000,
        None,
        None,
        GeminiFixtureOverrides {
            config: Some(json!({ "reclaimToolResultsKeepRecent": 0 })),
            now: Some(now),
            thinking_level: false,
        },
    );

    for (advance, prompt) in [
        (0, "Fetch the report."),
        (5 * MINUTE_MS, "Warm follow-up."),
        (11 * MINUTE_MS, "Cold follow-up."),
    ] {
        clock.fetch_add(advance, Ordering::SeqCst);
        let result = session
            .run(Input::Text(prompt.into()), TurnOptions::default())
            .await;
        assert!(result.error.is_none(), "{prompt}: {:?}", result.error);
    }

    let calls = client.taken();
    assert_eq!(calls.len(), 4);
    assert!(
        contents_text(&calls[2]).contains(PAYLOAD_TAIL),
        "within the implicit cache window the full tool output must stay in place"
    );
    assert!(
        !contents_text(&calls[3]).contains(PAYLOAD_TAIL),
        "after ten idle minutes the aged tool output must be reclaimed to its preview"
    );

    session.dispose().await;
}

#[tokio::test]
async fn antigravity_suggestions_skip_reasoning_and_stay_silent_once_the_cache_is_cold() {
    let (clock, now) = controllable_clock();
    let client = ScriptedGeminiClient::new(vec![
        ScriptedReply::Text("parent reply"),
        ScriptedReply::Text(r#"{"suggestion":"Run the tests"}"#),
    ]);
    let (_dir, session) = start_antigravity_gemini_session_with_overrides(
        client.clone(),
        Vec::new(),
        200_000,
        None,
        None,
        GeminiFixtureOverrides {
            config: Some(json!({ "thinking": true, "effort": "high" })),
            now: Some(now),
            thinking_level: true,
        },
    );

    let long_context = format!("{} end", "project context paragraph ".repeat(2_500));
    let result = session
        .run(Input::Text(long_context.into()), TurnOptions::default())
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    let source = session
        .prediction_context()
        .await
        .unwrap()
        .turn_id
        .expect("completed source turn")
        .try_to_utf8()
        .unwrap();

    // Warm: the suggestion runs on the main prefix with reasoning disabled and
    // a small output ceiling.
    clock.fetch_add(MINUTE_MS, Ordering::SeqCst);
    let warm = session
        .predict_message_with_signal(
            "Suggest the next message.".into(),
            source.clone(),
            None,
            AbortSignal::new(),
        )
        .await
        .unwrap();
    assert_eq!(warm, Some(RuntimeString::from("Run the tests")));
    let calls = client.taken();
    assert_eq!(calls.len(), 2);
    let generation: &Value = &calls[1].wire["generationConfig"];
    assert_eq!(
        generation["thinkingConfig"]["includeThoughts"],
        json!(false),
        "suggestions must not request reasoning: {generation}"
    );
    assert!(
        generation["maxOutputTokens"]
            .as_u64()
            .is_some_and(|tokens| tokens <= 512),
        "suggestions must cap their output: {generation}"
    );
    assert_eq!(
        calls[0].wire["generationConfig"]["thinkingConfig"]["includeThoughts"],
        json!(true),
        "the main turn keeps its own reasoning settings"
    );

    // Cold: past the implicit cache window a long session skips the request.
    clock.fetch_add(10 * MINUTE_MS, Ordering::SeqCst);
    let cold = session
        .predict_message_with_signal(
            "Suggest the next message.".into(),
            source,
            None,
            AbortSignal::new(),
        )
        .await
        .unwrap();
    assert_eq!(cold, None);
    assert_eq!(
        client.taken().len(),
        2,
        "a cold long-history suggestion must not reach the provider"
    );

    session.dispose().await;
}
