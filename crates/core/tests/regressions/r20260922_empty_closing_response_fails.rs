//! Defect: a closing response that came back EMPTY with a NORMAL stop reason
//! (not the output ceiling) after the turn had already produced visible work
//! settled as a normal completion. On screen the agent went silent mid-task:
//! the last tool round's results landed, the model was handed them, answered
//! with no content and no tool call, and the turn closed without a closing
//! message and without an error. Observed on a 53-minute turn whose final test
//! run was all green — the user saw the loop "stop" with no explanation.
//! Root cause: the settlement guard in `query/turn.rs` failed only two shapes
//! (closing cut off at the output ceiling; nothing produced all turn) and
//! deliberately exempted this third one, on the theory that a model may
//! legitimately finish with tool calls and no closing prose. That theory
//! cannot tell "finished with tools" from "went silent", and silence on an
//! unfinished task reads as a broken loop.
//! Fixed by: a one-shot retry (step 4.8) that asks the model to close or
//! continue, then settlement shape (c): an empty closing after visible work
//! that survived the retry fails the turn instead of completing silently.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::schemas::message::Usage;
use klaus_agent_core::tools::builtins::read::create_read_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::EngineErrorCode;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{
    ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
};
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

fn usage(output_tokens: u64) -> Usage {
    Usage {
        input_tokens: 10,
        output_tokens,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: None,
    }
}

/// Response 0 is a tool call (real output on screen). Every later response is
/// EMPTY with a NORMAL stop — the gateway-hiccup / forgot-to-close shape.
/// `recover_on_retry` makes the post-nudge response carry closing prose, which
/// is the shape the retry ladder exists to rescue.
fn agent_with_empty_closing(recover_on_retry: bool) -> (Agent, Arc<AtomicUsize>) {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut options = agent_options(process);
    options.tools = Some(vec![create_read_tool()]);
    let calls = Arc::new(AtomicUsize::new(0));
    options.deps.call_model = Some({
        let calls = Arc::clone(&calls);
        Box::new(
            move |_request: ModelRequest, _signal: AbortSignal| -> ModelStream {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    let tool_id = "read-once";
                    return Box::pin(futures::stream::iter(vec![
                        ModelStreamEvent::MessageStart {
                            model: "regression-noop".into(),
                        },
                        ModelStreamEvent::ToolUseStart {
                            id: tool_id.into(),
                            name: "read".into(),
                            thought_signature: None,
                            tool_kind: None,
                            input_format: None,
                        },
                        ModelStreamEvent::ToolUseInputDelta {
                            id: tool_id.into(),
                            partial_json: json!({ "path": "does-not-exist.txt" })
                                .to_string()
                                .into(),
                        },
                        ModelStreamEvent::ToolUseStop { id: tool_id.into() },
                        ModelStreamEvent::MessageStop {
                            stop_reason: ModelStopReason::ToolUse,
                            usage: usage(8),
                        },
                    ]));
                }
                // n == 1 is the first empty closing; n == 2 is the answer to the
                // retry nudge.
                if n == 2 && recover_on_retry {
                    return Box::pin(futures::stream::iter(vec![
                        ModelStreamEvent::MessageStart {
                            model: "regression-noop".into(),
                        },
                        ModelStreamEvent::TextDelta {
                            text: "closing summary after the retry nudge".into(),
                        },
                        ModelStreamEvent::MessageStop {
                            stop_reason: ModelStopReason::EndTurn,
                            usage: usage(12),
                        },
                    ]));
                }
                Box::pin(futures::stream::iter(vec![
                    ModelStreamEvent::MessageStart {
                        model: "regression-noop".into(),
                    },
                    ModelStreamEvent::MessageStop {
                        stop_reason: ModelStopReason::EndTurn,
                        usage: usage(2),
                    },
                ]))
            },
        )
    });
    (Agent::new(options), calls)
}

async fn run(agent: Agent) -> (Option<String>, bool) {
    let session = agent.start_session(StartSessionOptions {
        config: Some(json!({ "prewarm": false, "goal": false })),
        ..Default::default()
    });
    let mut events = session
        .run_streamed(Input::Text("do the work".into()), TurnOptions::default())
        .events;
    let mut failure: Option<String> = None;
    let mut saw_completion = false;
    while let Some(event) = events.next().await {
        match event {
            EngineEvent::TurnFailed { error, .. } => {
                assert_eq!(error.code, EngineErrorCode::ProviderError);
                failure = Some(error.message.try_to_utf8().unwrap_or_default());
            }
            EngineEvent::TurnCompleted { .. } => saw_completion = true,
            _ => {}
        }
    }
    (failure, saw_completion)
}

#[tokio::test]
async fn an_empty_closing_that_survives_the_retry_fails_the_turn() {
    let (agent, calls) = agent_with_empty_closing(false);
    let (failure, saw_completion) = run(agent).await;

    assert!(
        !saw_completion,
        "an empty closing response after visible work must NOT settle as a normal completion"
    );
    let message = failure.expect("the turn fails instead of completing silently");
    assert!(
        message.contains("empty closing response"),
        "the failure should name the empty closing, got: {message}"
    );
    // First response (tool round) + empty closing + one retry = three calls.
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn an_empty_closing_that_recovers_on_retry_completes() {
    let (agent, calls) = agent_with_empty_closing(true);
    let (failure, saw_completion) = run(agent).await;

    assert!(
        failure.is_none(),
        "a closing recovered by the retry must not fail the turn, got: {failure:?}"
    );
    assert!(saw_completion, "the recovered turn completes normally");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}
