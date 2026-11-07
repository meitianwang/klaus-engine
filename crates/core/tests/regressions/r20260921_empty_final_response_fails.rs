//! Defect: a turn whose FINAL provider response came back empty (no answer text,
//! no tool call) settled as a normal completion whenever an EARLIER response in
//! the same turn had produced output. On screen the agent stopped mid-work with
//! no closing message and no error — a 62-minute turn ended on a 16384-token
//! response that carried only invisible reasoning looked like the loop had simply
//! broken.
//! Root cause: the empty-response guard in `query/turn.rs` keyed on a turn-level
//! `produced_output` flag that was set by any earlier tool round and never reset,
//! so the closing response inherited it and skipped the failure path.
//! Fixed by: settlement now checks `response_produced_output` (this response only);
//! the turn-level flag is retained solely to word the failure. An empty closing
//! response after prior work is a `TurnFailed`, distinct from "nothing all turn".

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

/// One scripted model: response 0 is a tool call (real output on screen), every
/// response after it is EMPTY and stops at the output-token ceiling — the exact
/// shape that the turn-level flag used to mask.
fn agent_with_empty_closing_response() -> Agent {
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
                            // A missing path: the tool still returns a (error) result,
                            // which is real output the user can see — the point is that
                            // the FIRST response produced something.
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
                // The closing response: reasoning burned the whole budget, nothing
                // visible came back.
                Box::pin(futures::stream::iter(vec![
                    ModelStreamEvent::MessageStart {
                        model: "regression-noop".into(),
                    },
                    ModelStreamEvent::ThinkingDelta {
                        text: "thinking only, never surfaced as an answer".into(),
                    },
                    ModelStreamEvent::MessageStop {
                        stop_reason: ModelStopReason::MaxTokens,
                        usage: usage(16_384),
                    },
                ]))
            },
        )
    });
    Agent::new(options)
}

#[tokio::test]
async fn an_empty_closing_response_after_prior_work_fails_the_turn() {
    let agent = agent_with_empty_closing_response();
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
                failure = Some(error.message.try_to_utf8().unwrap_or_default());
                assert_eq!(error.code, EngineErrorCode::ProviderError);
            }
            EngineEvent::TurnCompleted { .. } => saw_completion = true,
            _ => {}
        }
    }

    assert!(
        !saw_completion,
        "an empty closing response must NOT settle the turn as a normal completion"
    );
    let message = failure.expect("the turn fails instead of completing silently");
    // It names the truncation specifically, so the user learns the closing answer
    // was cut off by the output ceiling rather than lost for an unknown reason.
    assert!(
        message.contains("output limit") && message.contains("closing"),
        "the failure should say the closing response hit the output limit, got: {message}"
    );
}
