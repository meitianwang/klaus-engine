//! Ordinary background task follow-ups must not depend on team membership.
//! Explicit task resume retains the original child identity and conversation.

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
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct MessagingProvider {
    info: ProviderInfo,
    role: Option<&'static str>,
    parent_calls: AtomicUsize,
    child_calls: AtomicUsize,
    child_requests: Mutex<Vec<ModelRequest>>,
    resume_failed: Mutex<Option<bool>>,
    completed_id: Arc<Mutex<Option<RuntimeString>>>,
    completed: Arc<tokio::sync::Notify>,
}

fn contains(request: &ModelRequest, text: &str) -> bool {
    request.messages.iter().any(|message| match message {
        Message::User(message) => message
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::Text(block) if block.text.contains(text))),
        _ => false,
    })
}

fn usage() -> Usage {
    Usage {
        input_tokens: 10,
        output_tokens: 5,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: None,
    }
}

fn tool_events(id: &str, name: &str, input: Value) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::MessageStart {
            model: "messaging-test".into(),
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
        ModelStreamEvent::MessageStop {
            stop_reason: ModelStopReason::ToolUse,
            usage: usage(),
        },
    ]
}

impl ModelProvider for MessagingProvider {
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
        let child_history = request.messages.iter().any(|message| match message {
            Message::User(message) => message.content.iter().any(|block| {
                matches!(block, ContentBlock::Text(block) if block.text == "original-child-direction")
            }),
            _ => false,
        });
        if child_history {
            self.child_calls.fetch_add(1, Ordering::SeqCst);
            self.child_requests.lock().unwrap().push(request);
            return Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::MessageStart {
                    model: "messaging-test".into(),
                },
                ModelStreamEvent::TextDelta {
                    text: "Child work complete.".into(),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::EndTurn,
                    usage: usage(),
                },
            ]));
        }
        match self.parent_calls.fetch_add(1, Ordering::SeqCst) {
            0 => {
                let mut input = json!({"model": "low", "description":"named-investigation", "prompt":"original-child-direction", "fork_turns":"none", "run_in_background":true});
                if let Some(role) = self.role {
                    input["subagent_type"] = json!(role);
                    input["model"] = json!("inherit");
                }
                Box::pin(futures::stream::iter(tool_events(
                    "spawn-child",
                    "task",
                    input,
                )))
            }
            1 => {
                let completed_id = self.completed_id.clone();
                let completed = self.completed.clone();
                Box::pin(async_stream::stream! {
                    let id = loop {
                        let notified = completed.notified();
                        if let Some(id) = completed_id.lock().unwrap().clone() { break id; }
                        tokio::select! {
                            _ = notified => {},
                            _ = signal.cancelled() => return,
                        }
                    };
                    for event in tool_events("resume-child", "task", json!({
                        "resume": id.try_to_utf8().unwrap(),
                        "description": "Continue the investigation",
                        "prompt": "followup-child-direction"
                    })) { yield event; }
                })
            }
            _ => {
                let failed = request.messages.iter().find_map(|message| match message {
                    Message::ToolResult(result) if result.tool_use_id == "resume-child" => {
                        Some(result.is_error)
                    }
                    Message::User(message) => {
                        message.content.iter().find_map(|block| match block {
                            ContentBlock::ToolResult(result)
                                if result.tool_use_id == "resume-child" =>
                            {
                                Some(result.is_error == Some(true))
                            }
                            _ => None,
                        })
                    }
                    _ => None,
                });
                if let Some(failed) = failed {
                    *self.resume_failed.lock().unwrap() = Some(failed);
                }
                Box::pin(futures::stream::iter(vec![
                    ModelStreamEvent::MessageStart {
                        model: "messaging-test".into(),
                    },
                    ModelStreamEvent::TextDelta {
                        text: "Parent work complete.".into(),
                    },
                    ModelStreamEvent::MessageStop {
                        stop_reason: ModelStopReason::EndTurn,
                        usage: usage(),
                    },
                ]))
            }
        }
    }
}

#[tokio::test]
async fn an_ordinary_child_resumes_after_completion_without_a_team() {
    completed_child_resumes(None).await;
}

pub(super) async fn completed_child_resumes(role: Option<&'static str>) {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.config.as_mut().unwrap()["model"] = json!("messaging-test");
    options.config.as_mut().unwrap()["agentMemory"] = json!(false);
    options.config.as_mut().unwrap()["autoMemory"] = json!(false);
    options.config.as_mut().unwrap()["swarm"] = json!(false);
    options.tools = if role.is_some() {
        None
    } else {
        Some(vec![create_task_tool(TaskToolOptions::default())])
    };
    let completed_id = Arc::new(Mutex::new(None));
    let completed = Arc::new(tokio::sync::Notify::new());
    let provider = Arc::new(MessagingProvider {
        role,
        info: ProviderInfo {
            name: "messaging-test".into(),
            base_url: None,
            env_key: None,
            default_model: "messaging-test".into(),
        },
        parent_calls: AtomicUsize::new(0),
        child_calls: AtomicUsize::new(0),
        child_requests: Mutex::new(Vec::new()),
        resume_failed: Mutex::new(None),
        completed_id: completed_id.clone(),
        completed: completed.clone(),
    });
    options.provider = provider.clone();
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions::default());
    let (notice_tx, mut notices) = tokio::sync::mpsc::unbounded_channel();
    session.set_agent_notification_sink(Some(Arc::new(move |receipt| {
        if let Some(id) = &receipt.agent_id {
            *completed_id.lock().unwrap() = Some(id.clone());
            completed.notify_one();
            let _ = notice_tx.send(id.clone());
        }
    })));
    let parent = tokio::time::timeout(
        Duration::from_secs(10),
        session.run(
            Input::Text("Delegate and continue the child".into()),
            TurnOptions::default(),
        ),
    )
    .await
    .unwrap_or_else(|error| {
        panic!("background task resume settles without waiting for child completion: {error:?}; parent_calls={}, child_calls={}, completed_id={:?}, resume_failed={:?}",
            provider.parent_calls.load(Ordering::SeqCst),
            provider.child_calls.load(Ordering::SeqCst),
            provider.completed_id.lock().unwrap(), provider.resume_failed.lock().unwrap());
    });
    assert!(parent.error.is_none(), "{:?}", parent.error);
    assert_eq!(*provider.resume_failed.lock().unwrap(), Some(false));
    let first_id = tokio::time::timeout(Duration::from_secs(5), notices.recv())
        .await
        .unwrap()
        .unwrap();
    let second_id = tokio::time::timeout(Duration::from_secs(5), notices.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first_id, second_id, "resuming preserves the child identity");
    assert_eq!(provider.child_calls.load(Ordering::SeqCst), 2);
    {
        let requests = provider.child_requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].model, requests[1].model);
        assert_eq!(requests[0].tools, requests[1].tools);
        if role == Some("Explore") {
            assert!(requests[1].tools.iter().any(|tool| tool.name == "read"));
            assert!(!requests[1]
                .tools
                .iter()
                .any(|tool| tool.name == "write" || tool.name == "task"));
            let role_parts = |request: &ModelRequest| {
                request
                    .system
                    .iter()
                    .filter(|part| part.text.contains("read-only exploration sub-agent"))
                    .cloned()
                    .collect::<Vec<_>>()
            };
            assert_eq!(role_parts(&requests[0]).len(), 1);
            assert_eq!(role_parts(&requests[0]), role_parts(&requests[1]));
        }
        assert!(contains(&requests[1], "original-child-direction"));
        assert!(contains(&requests[1], "followup-child-direction"));
        assert!(!contains(&requests[1], "Delegate and continue the child"));
    }
    assert_eq!(session.subagent_lifecycle_snapshot().agents.len(), 1);
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    let output = std::env::temp_dir().join(format!(
        "klaus-agent-bg-{}.txt",
        first_id.try_to_utf8().unwrap()
    ));
    if output.exists() {
        std::fs::remove_file(output).unwrap();
    }
}
