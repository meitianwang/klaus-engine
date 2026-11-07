//! Defect: root interruption cancelled accepted background work instead of leaving explicit child stop to release its inference lease.
//! Root cause: root and accepted child cancellation crossed independent task lifetimes.
//! Work item: 2026-09-28 stop and continue after background delegation.

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, ToolResultMessage, Usage};
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};
use klaus_agent_core::tools::builtins::task_control::create_task_stop_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::TerminalReason;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::BackgroundTaskStatus;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct DelegatingProvider {
    info: ProviderInfo,
    parent_calls: AtomicUsize,
    child_entered: tokio::sync::mpsc::UnboundedSender<AbortSignal>,
    parent_waiting: tokio::sync::mpsc::UnboundedSender<RuntimeString>,
    accepted_child: Mutex<Option<RuntimeString>>,
}

fn tool_result<'a>(request: &'a ModelRequest, id: &str) -> &'a ToolResultMessage {
    request
        .messages
        .iter()
        .rev()
        .find_map(|message| match message {
            Message::ToolResult(result) if result.tool_use_id == id => Some(result),
            _ => None,
        })
        .expect("the provider must receive the actual tool result")
}

fn stopped(reason: ModelStopReason) -> ModelStreamEvent {
    ModelStreamEvent::MessageStop {
        stop_reason: reason,
        usage: Usage {
            input_tokens: 10,
            output_tokens: 4,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: None,
        },
    }
}

fn tool_call(id: &str, name: &str, input: serde_json::Value) -> ModelStream {
    Box::pin(futures::stream::iter(vec![
        ModelStreamEvent::MessageStart {
            model: "stop-test".into(),
        },
        ModelStreamEvent::ToolUseStart {
            id: id.into(),
            name: name.into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: id.into(),
            partial_json: input.to_string().into(),
        },
        ModelStreamEvent::ToolUseStop { id: id.into() },
        stopped(ModelStopReason::ToolUse),
    ]))
}

fn finish(model: RuntimeString) -> ModelStream {
    Box::pin(futures::stream::iter(vec![
        ModelStreamEvent::MessageStart { model },
        ModelStreamEvent::TextDelta {
            text: "Targeted stop checks completed.".into(),
        },
        stopped(ModelStopReason::EndTurn),
    ]))
}

impl ModelProvider for DelegatingProvider {
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

    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        let is_child = request.messages.iter().any(|message| match message {
            Message::User(message) => message.content.iter().any(|block| {
                matches!(block, ContentBlock::Text(text) if text.text.contains("stop-child-directive"))
            }),
            _ => false,
        });
        if is_child {
            self.child_entered.send(signal.clone()).unwrap();
            return Box::pin(async_stream::stream! {
                yield ModelStreamEvent::MessageStart { model: "stop-test".into() };
                signal.cancelled().await;
            });
        }
        match self.parent_calls.fetch_add(1, Ordering::SeqCst) {
            0 => {
                return tool_call(
                    "spawn-child",
                    "task",
                    json!({"model": "low",
                        "description": "stop child",
                        "prompt": "stop-child-directive",
                        "run_in_background": true
                    }),
                );
            }
            1 => {
                let result = tool_result(&request, "spawn-child");
                assert!(!result.is_error, "{:?}", result.content);
                let child = result
                    .output
                    .as_ref()
                    .unwrap()
                    .get("agentId")
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .clone();
                *self.accepted_child.lock().unwrap() = Some(child.clone());
                self.parent_waiting.send(child).unwrap();
            }
            2 => {
                let child = self.accepted_child.lock().unwrap().clone().unwrap();
                return tool_call(
                    "stop-accepted-child",
                    "task_stop",
                    json!({"task_id": child.try_to_utf8().unwrap()}),
                );
            }
            3 => {
                let result = tool_result(&request, "stop-accepted-child");
                assert!(!result.is_error, "{:?}", result.content);
                let child = self.accepted_child.lock().unwrap().clone().unwrap();
                let output = result.output.as_ref().unwrap();
                assert_eq!(
                    output.get("task_id").and_then(|id| id.as_str()),
                    Some(&child)
                );
                assert_eq!(
                    output.get("task_type").and_then(|kind| kind.as_str()),
                    Some(&RuntimeString::from("agent"))
                );
                return finish(request.model);
            }
            4 => {
                assert_eq!(request.model, "replacement");
                return finish(request.model);
            }
            _ => panic!("unexpected parent continuation"),
        }
        Box::pin(async_stream::stream! {
            yield ModelStreamEvent::MessageStart { model: "stop-test".into() };
            signal.cancelled().await;
        })
    }
}

#[tokio::test]
async fn root_interrupt_preserves_accepted_child_until_targeted_stop_releases_inference() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.config.as_mut().unwrap()["model"] = json!("stop-test");
    options.config.as_mut().unwrap()["agentMemory"] = json!(false);
    options.config.as_mut().unwrap()["autoMemory"] = json!(false);
    options.tools = Some(vec![
        create_task_tool(TaskToolOptions::default()),
        create_task_stop_tool(),
    ]);
    let (child_tx, mut child_rx) = tokio::sync::mpsc::unbounded_channel();
    let (parent_tx, mut parent_rx) = tokio::sync::mpsc::unbounded_channel();
    let provider = Arc::new(DelegatingProvider {
        info: ProviderInfo {
            name: "stop-test".into(),
            base_url: None,
            env_key: None,
            default_model: "stop-test".into(),
        },
        parent_calls: AtomicUsize::new(0),
        child_entered: child_tx,
        parent_waiting: parent_tx,
        accepted_child: Mutex::new(None),
    });
    options.provider = provider.clone();
    let session = Agent::new(options).start_session(StartSessionOptions::default());
    let (notice_tx, mut notices) = tokio::sync::mpsc::unbounded_channel();
    session.set_agent_notification_sink(Some(Arc::new(move |receipt| {
        let _ = notice_tx.send(receipt.clone());
    })));
    let running = tokio::spawn({
        let session = session.clone();
        async move {
            session
                .run(
                    Input::Text("Delegate a child".into()),
                    TurnOptions::default(),
                )
                .await
        }
    });
    let child_signal = tokio::time::timeout(Duration::from_secs(5), child_rx.recv())
        .await
        .expect("the background child starts")
        .expect("the child start channel stays open");
    let child_id = tokio::time::timeout(Duration::from_secs(5), parent_rx.recv())
        .await
        .expect("the parent receives the committed child creation result")
        .expect("the parent continuation channel stays open")
        .try_to_utf8()
        .unwrap();
    session.interrupt();
    let parent = tokio::time::timeout(Duration::from_secs(5), running)
        .await
        .expect("the interrupted parent settles")
        .unwrap();
    assert!(parent.error.is_none(), "{:?}", parent.error);
    assert_eq!(parent.stop_reason, TerminalReason::Aborted);
    let accepted = session
        .background_agent_summaries()
        .into_iter()
        .find(|child| child.id == child_id)
        .expect("the accepted background child remains addressable");
    assert_eq!(accepted.status, BackgroundTaskStatus::Running);
    assert!(
        !child_signal.is_cancelled(),
        "root interruption must leave the accepted child token live"
    );
    assert!(
        notices.try_recv().is_err(),
        "root interruption must not synthesize a child completion notice"
    );
    let stopped = tokio::time::timeout(
        Duration::from_secs(5),
        session.run(
            Input::Text("Stop only the accepted child".into()),
            TurnOptions::default(),
        ),
    )
    .await
    .expect("the real task_stop tool settles");
    assert!(stopped.error.is_none(), "{:?}", stopped.error);
    assert_eq!(stopped.stop_reason, TerminalReason::Completed);
    tokio::time::timeout(Duration::from_secs(5), child_signal.cancelled())
        .await
        .expect("targeted stop cancels the child's actual provider token");
    let background = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(child) = session
                .background_agent_summaries()
                .into_iter()
                .find(|child| {
                    child.id == child_id && child.status == BackgroundTaskStatus::Interrupted
                })
            {
                break child;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the background child is interrupted");
    assert!(
        notices.try_recv().is_err(),
        "interruption is not a completion notice"
    );

    let mut config = session.model_runtime().1;
    config.model = "replacement".into();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match session.reconfigure_inference(provider.clone(), config.clone(), None, None, 0) {
                Ok(revision) => break assert_eq!(revision, 1),
                Err(error) if error.contains("inference is running") => {
                    tokio::task::yield_now().await
                }
                Err(error) => panic!("unexpected replacement error: {error}"),
            }
        }
    })
    .await
    .expect("the old inference lease is released");
    let replacement = tokio::time::timeout(
        Duration::from_secs(5),
        session.run(
            Input::Text("Check the replacement model".into()),
            TurnOptions::default(),
        ),
    )
    .await
    .expect("the next turn uses the replacement model");
    assert!(replacement.error.is_none(), "{:?}", replacement.error);
    assert_eq!(replacement.stop_reason, TerminalReason::Completed);
    assert_eq!(replacement.final_text, "Targeted stop checks completed.");
    assert_eq!(provider.parent_calls.load(Ordering::SeqCst), 5);
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    let output = std::path::Path::new(&background.output_file);
    if output.exists() {
        std::fs::remove_file(output).unwrap();
    }
}
