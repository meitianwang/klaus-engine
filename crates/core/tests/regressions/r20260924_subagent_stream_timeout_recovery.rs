//! Defect: a background sub-agent failed after one model response reached the stream total timeout.
//! Root cause: the turn's stream retry policy excluded timeout errors.
//! Fixed by: 2026-09-24 sub-agent stream timeout recovery.

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

struct TimeoutChildProvider {
    info: ProviderInfo,
    parent_calls: AtomicUsize,
    child_calls: Arc<AtomicUsize>,
    recover: bool,
}

fn stopped() -> ModelStreamEvent {
    ModelStreamEvent::MessageStop {
        stop_reason: ModelStopReason::EndTurn,
        usage: Usage {
            input_tokens: 10,
            output_tokens: 4,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: None,
        },
    }
}

impl ModelProvider for TimeoutChildProvider {
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

    fn stream(&self, request: ModelRequest, _signal: AbortSignal) -> ModelStream {
        let is_child = request.messages.iter().any(|message| match message {
            Message::User(message) => message.content.iter().any(|block| {
                matches!(block, ContentBlock::Text(text) if text.text.contains("timeout-child-directive"))
            }),
            _ => false,
        });
        if is_child {
            let attempt = self.child_calls.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 || !self.recover {
                return Box::pin(
                    futures::stream::iter(vec![
                        ModelStreamEvent::MessageStart {
                            model: "timeout-test".into(),
                        },
                        ModelStreamEvent::TextDelta {
                            text: "partial".into(),
                        },
                    ])
                    .chain(futures::stream::pending()),
                );
            }
            return Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::MessageStart {
                    model: "timeout-test".into(),
                },
                ModelStreamEvent::TextDelta {
                    text: "Recovered child".into(),
                },
                stopped(),
            ]));
        }
        if self.parent_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            let input = json!({"model": "low",
                "description": "timeout child",
                "prompt": "timeout-child-directive",
                "run_in_background": true
            });
            return Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::MessageStart {
                    model: "timeout-test".into(),
                },
                ModelStreamEvent::ToolUseStart {
                    id: "spawn-child".into(),
                    name: "task".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "spawn-child".into(),
                    partial_json: input.to_string().into(),
                },
                ModelStreamEvent::ToolUseStop {
                    id: "spawn-child".into(),
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
            ]));
        }
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::MessageStart {
                model: "timeout-test".into(),
            },
            ModelStreamEvent::TextDelta {
                text: "Parent finished".into(),
            },
            stopped(),
        ]))
    }
}

async fn run_child(recover: bool, max_retries: u32) -> (String, usize) {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let child_calls = Arc::new(AtomicUsize::new(0));
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.config.as_mut().unwrap()["model"] = json!("timeout-test");
    options.config.as_mut().unwrap()["streamIdleTimeoutMs"] = json!(100);
    options.config.as_mut().unwrap()["streamTotalTimeoutMs"] = json!(40);
    options.config.as_mut().unwrap()["maxRetries"] = json!(max_retries);
    options.config.as_mut().unwrap()["agentMemory"] = json!(false);
    options.config.as_mut().unwrap()["autoMemory"] = json!(false);
    options.tools = Some(vec![create_task_tool(TaskToolOptions::default())]);
    options.provider = Arc::new(TimeoutChildProvider {
        info: ProviderInfo {
            name: "timeout-test".into(),
            base_url: None,
            env_key: None,
            default_model: "timeout-test".into(),
        },
        parent_calls: AtomicUsize::new(0),
        child_calls: child_calls.clone(),
        recover,
    });
    let session = Agent::new(options).start_session(StartSessionOptions::default());
    let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
    session.set_agent_notification_sink(Some(Arc::new(move |receipt| {
        let _ = send.send(receipt.clone());
    })));
    let parent = tokio::time::timeout(
        Duration::from_secs(5),
        session.run(
            Input::Text("Delegate a child".into()),
            TurnOptions::default(),
        ),
    )
    .await
    .expect("the parent finishes independently");
    assert!(parent.error.is_none(), "{:?}", parent.error);

    let receipt = tokio::time::timeout(Duration::from_secs(5), receive.recv())
        .await
        .expect("the background child settles")
        .expect("the child emits a completion notification");
    let calls = child_calls.load(Ordering::SeqCst);
    let status = receipt.status.try_to_utf8().unwrap().to_owned();
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    if let Some(agent_id) = receipt.agent_id {
        let output = std::env::temp_dir().join(format!(
            "klaus-agent-bg-{}.txt",
            agent_id.try_to_utf8().unwrap()
        ));
        if output.exists() {
            std::fs::remove_file(output).unwrap();
        }
    }
    (status, calls)
}

#[tokio::test]
async fn background_child_recovers_after_one_total_timeout() {
    let (status, calls) = run_child(true, 5).await;
    assert_eq!(status, "completed");
    assert_eq!(calls, 2);
}

#[tokio::test]
async fn background_child_stops_after_bounded_total_timeout_retry() {
    let (status, calls) = run_child(false, 5).await;
    assert_eq!(status, "failed");
    assert_eq!(calls, 2);
}

#[tokio::test]
async fn background_child_respects_disabled_retries() {
    let (status, calls) = run_child(true, 0).await;
    assert_eq!(status, "failed");
    assert_eq!(calls, 1);
}
