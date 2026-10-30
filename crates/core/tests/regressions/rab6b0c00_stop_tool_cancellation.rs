//! Defect: Stop could remain blocked by a tool that ignored cancellation.
//! Root cause: foreground tool execution waited for the tool future indefinitely.
//! Fix: bound cancellation while preserving interrupted tool results and turn state.
//! Fixing commit: ab6b0c00713bc6845cae822ad44743c52e5a842c.

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};
use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::content::OutputBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{TerminalReason, ToolError};
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::json::Json;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

const WAIT: Duration = Duration::from_secs(10);
const CANCEL_BUDGET: Duration = Duration::from_secs(2);

#[derive(Default)]
struct HeldExecution {
    entered: Notify,
    release: Notify,
    dropped: AtomicUsize,
    returned: AtomicUsize,
}

struct DropExecution(Arc<HeldExecution>);

impl Drop for DropExecution {
    fn drop(&mut self) {
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

struct HeldTool(Arc<HeldExecution>);

#[async_trait]
impl Tool for HeldTool {
    fn static_name(&self) -> &str {
        "held_tool"
    }

    fn static_description(&self) -> &str {
        "Wait for external cleanup without responding to cancellation."
    }

    fn input_schema(&self) -> Json {
        Json::Object(Default::default())
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_read_only(&self, _: &Json) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _: &Json) -> bool {
        true
    }

    async fn call(
        &self,
        _: Json,
        _ctx: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let _drop = DropExecution(self.0.clone());
        self.0.entered.notify_one();
        // Only the test can open this barrier. Ignoring the signal models an
        // external operation whose cleanup cannot cooperate with cancellation.
        self.0.release.notified().await;
        self.0.returned.fetch_add(1, Ordering::SeqCst);
        Ok(text_result(
            RuntimeJson::Null,
            "natural tool completion",
            false,
        ))
    }
}

struct ToolProvider {
    info: ProviderInfo,
    calls: AtomicUsize,
}

impl ModelProvider for ToolProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 8_192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, _: ModelRequest, _: AbortSignal) -> ModelStream {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(futures::stream::iter([
            ModelStreamEvent::MessageStart {
                model: "held-tool-test".into(),
            },
            ModelStreamEvent::ToolUseStart {
                id: "held-call".into(),
                name: "held_tool".into(),
                thought_signature: None,
                tool_kind: None,
                input_format: None,
            },
            ModelStreamEvent::ToolUseInputDelta {
                id: "held-call".into(),
                partial_json: "{}".into(),
            },
            ModelStreamEvent::ToolUseStop {
                id: "held-call".into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::ToolUse,
                usage: Usage {
                    input_tokens: 10,
                    output_tokens: 4,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            },
        ]))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_tool_cancellation_retires_uncooperative_execution_with_interrupted_history() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let root = tempfile::tempdir().unwrap();
    let execution = Arc::new(HeldExecution::default());
    let provider = Arc::new(ToolProvider {
        info: ProviderInfo {
            name: "held-tool-test".into(),
            default_model: "held-tool-test".into(),
            base_url: None,
            env_key: None,
        },
        calls: AtomicUsize::new(0),
    });
    let mut options = agent_options(process.clone());
    options.provider = provider.clone();
    options.tools = Some(vec![Arc::new(HeldTool(execution.clone()))]);
    options.config = Some(json!({
        "model":"held-tool-test", "prewarm":false, "goal":false,
        "autoMemory":false, "autoDream":false,
        "approval_policy":"never", "sandbox_mode":"danger-full-access"
    }));
    let directory = root.path().to_string_lossy().into_owned();
    let session = Agent::new(options).start_session(StartSessionOptions {
        cwd: Some(directory.clone().into()),
        user_config_dir: Some(directory.into()),
        ..Default::default()
    });
    let signal = AbortSignal::new();
    let turn = session.run_streamed(
        Input::Text("Run the held tool once".into()),
        TurnOptions {
            signal: Some(signal.clone()),
            ..Default::default()
        },
    );
    let mut task = tokio::spawn(async move { turn.events.collect::<Vec<_>>().await });
    tokio::time::timeout(WAIT, execution.entered.notified())
        .await
        .expect("the provider must enter the tool before cancellation");

    let started = tokio::time::Instant::now();
    signal.cancel();
    let outcome = tokio::time::timeout(CANCEL_BUDGET, &mut task).await;
    let elapsed = started.elapsed();
    let dropped_while_blocked = execution.dropped.load(Ordering::SeqCst);
    let returned_while_blocked = execution.returned.load(Ordering::SeqCst);
    let (finished_while_blocked, events) = match outcome {
        Ok(result) => (true, result.unwrap()),
        Err(_) => {
            // Release and join even on the pre-fix failure path before asserting.
            execution.release.notify_one();
            let events = tokio::time::timeout(WAIT, &mut task)
                .await
                .expect("cleanup release must finish the baseline turn")
                .unwrap();
            (false, events)
        }
    };
    let history = session.history();
    let results: Vec<_> = history
        .iter()
        .filter_map(|message| match message {
            Message::ToolResult(result) if result.tool_use_id == "held-call" => Some(result),
            _ => None,
        })
        .collect();
    let interrupted_results = results
        .iter()
        .filter(|result| {
            result.is_error
                && result.content.iter().any(|block| {
                    matches!(block,
                        OutputBlock::Text(text) if text.text.contains("interrupted")
                    )
                })
        })
        .count();
    let aborted = events.iter().any(|event| {
        matches!(
            event,
            EngineEvent::TurnCompleted {
                stop_reason: TerminalReason::Aborted,
                ..
            }
        )
    });
    let failed = events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnFailed { .. }));
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();

    eprintln!("finished_while_blocked={finished_while_blocked}; elapsed={elapsed:?}; dropped_while_blocked={dropped_while_blocked}; returned_while_blocked={returned_while_blocked}; tool_results={}; interrupted_results={interrupted_results}; aborted={aborted}; failed={failed}; model_calls={}", results.len(), provider.calls.load(Ordering::SeqCst));
    assert!(
        finished_while_blocked,
        "cancellation must finish while external tool cleanup remains blocked"
    );
    assert!(
        elapsed < CANCEL_BUDGET,
        "cancellation exceeded its bounded budget: {elapsed:?}"
    );
    assert_eq!(
        dropped_while_blocked, 1,
        "the uncooperative execution future must be retired"
    );
    assert_eq!(
        returned_while_blocked, 0,
        "the tool must not complete naturally before cancellation settles"
    );
    assert_eq!(
        results.len(),
        1,
        "retain one result for the interrupted tool call"
    );
    assert_eq!(
        interrupted_results, 1,
        "retain an interrupted error tool_result in public history"
    );
    assert!(
        aborted && !failed,
        "the turn must end as interrupted rather than failed: {events:?}"
    );
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        1,
        "cancellation must not start another inference"
    );
}
