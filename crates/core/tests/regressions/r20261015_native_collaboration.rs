//! Defect: child agents advertised detached delegation without a detached spawn
//! capability, and ordinary messaging required unrelated tool discovery.
//! Root cause: child contexts wired only the foreground spawner.
//! Work item: 2026-10-15 native collaboration contracts.

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};
use futures::FutureExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::{RuntimeEnv, RuntimeProcessContext};
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::{ContentBlock, OutputBlock};
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

const MODEL: &str = "native-collaboration-test";

fn directive(request: &ModelRequest, expected: &str) -> bool {
    request.messages.iter().any(|message| match message {
        Message::User(message) => message
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::Text(text) if text.text == expected)),
        _ => false,
    })
}

fn receipt(request: &ModelRequest, id: &str) -> (bool, String) {
    let text = |content: &[OutputBlock]| {
        content
            .iter()
            .filter_map(|block| match block {
                OutputBlock::Text(text) => Some(text.text.try_to_utf8().unwrap()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    request
        .messages
        .iter()
        .rev()
        .find_map(|message| match message {
            Message::ToolResult(result) if result.tool_use_id == id => {
                Some((result.is_error, text(&result.content)))
            }
            Message::User(message) => message.content.iter().find_map(|block| match block {
                ContentBlock::ToolResult(result) if result.tool_use_id == id => {
                    Some((result.is_error == Some(true), text(&result.content)))
                }
                _ => None,
            }),
            _ => None,
        })
        .unwrap_or_else(|| panic!("missing native tool result {id}"))
}

fn spawned_id(request: &ModelRequest, call: &str) -> String {
    let (failed, text) = receipt(request, call);
    assert!(!failed, "{call}: {text}");
    text.split("Started background sub-agent ")
        .nth(1)
        .and_then(|text| text.split('.').next())
        .map(str::to_owned)
        .unwrap_or_else(|| panic!("spawn did not return a detached identity: {text}"))
}

fn stop(reason: ModelStopReason) -> ModelStreamEvent {
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

fn tool(id: &str, name: &str, input: Value) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::MessageStart {
            model: MODEL.into(),
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
        stop(ModelStopReason::ToolUse),
    ]
}

fn answer(text: &str) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::MessageStart {
            model: MODEL.into(),
        },
        ModelStreamEvent::TextDelta { text: text.into() },
        stop(ModelStopReason::EndTurn),
    ]
}

fn assert_controls(request: &ModelRequest, can_spawn: bool) {
    for name in ["task_output", "task_stop", "SendMessage"] {
        assert!(
            request.tools.iter().any(|tool| tool.name == name),
            "native control {name} must be visible without discovery"
        );
    }
    assert_eq!(
        request.tools.iter().any(|tool| tool.name == "task"),
        can_spawn
    );
    assert_eq!(
        request.model, MODEL,
        "an omitted model inherits the selected model"
    );
}

struct NativeProvider {
    info: ProviderInfo,
    root_calls: AtomicUsize,
    worker_calls: AtomicUsize,
    worker_turn_calls: AtomicUsize,
    grandchild_calls: AtomicUsize,
    worker_id: Mutex<String>,
    grandchild_id: Mutex<String>,
    root_blocker_id: Mutex<String>,
    worker_blocker_id: Mutex<String>,
    root_blocker_started: Arc<Semaphore>,
    worker_blocker_started: Arc<Semaphore>,
}

struct NodeEnvironmentRestore(Option<std::ffi::OsString>);

impl Drop for NodeEnvironmentRestore {
    fn drop(&mut self) {
        match &self.0 {
            Some(previous) => klaus_agent_core::runtime_process::set_process_environment_var(
                "KLAUS_NODE",
                previous,
            ),
            None => klaus_agent_core::runtime_process::remove_process_environment_var("KLAUS_NODE"),
        }
    }
}

impl NativeProvider {
    fn root(&self, request: &ModelRequest) -> Vec<ModelStreamEvent> {
        match self.root_calls.fetch_add(1, Ordering::SeqCst) {
            0 => tool(
                "root-spawn",
                "task",
                json!({"model": "low",
                    "description":"nested worker", "prompt":"native-worker",
                    "name":"worker", "fork_turns":"none", "run_in_background":true
                }),
            ),
            1 => {
                let id = spawned_id(request, "root-spawn");
                *self.worker_id.lock().unwrap() = id.clone();
                tool(
                    "root-wait",
                    "task_output",
                    json!({"task_id":id,"block":true,"timeout":10000}),
                )
            }
            2 => {
                let (failed, text) = receipt(request, "root-wait");
                assert!(!failed && text.contains("worker complete"), "{text}");
                tool(
                    "root-message",
                    "SendMessage",
                    json!({
                        "to":"worker", "summary":"Continue existing worker", "message":"native-worker-followup"
                    }),
                )
            }
            3 => {
                let (failed, text) = receipt(request, "root-message");
                assert!(!failed && text.contains("queued"), "{text}");
                assert_eq!(
                    self.worker_turn_calls.load(Ordering::SeqCst),
                    8,
                    "passive mail must not start another worker turn"
                );
                tool(
                    "root-activate",
                    "SendMessage",
                    json!({
                        "to":"worker", "summary":"Execute queued direction",
                        "message":"Execute the queued worker task", "trigger_turn":true
                    }),
                )
            }
            4 => {
                let (failed, text) = receipt(request, "root-activate");
                assert!(!failed && text.contains("resumed"), "{text}");
                tool(
                    "root-followup-wait",
                    "task_output",
                    json!({
                        "task_id":self.worker_id.lock().unwrap().clone(), "block":true,"timeout":10000
                    }),
                )
            }
            5 => {
                let (failed, text) = receipt(request, "root-followup-wait");
                assert!(
                    !failed && text.contains("worker followup complete"),
                    "{text}"
                );
                tool(
                    "root-blocker",
                    "task",
                    json!({"model": "low",
                        "description":"main interrupt target", "prompt":"native-root-blocker",
                        "fork_turns":"none", "run_in_background":true
                    }),
                )
            }
            6 => {
                let id = spawned_id(request, "root-blocker");
                *self.root_blocker_id.lock().unwrap() = id.clone();
                tool("root-stop", "task_stop", json!({"task_id":id}))
            }
            7 => {
                let (failed, text) = receipt(request, "root-stop");
                assert!(!failed, "{text}");
                answer("root complete")
            }
            call => panic!("unexpected root provider call {call}"),
        }
    }

    fn worker(&self, request: &ModelRequest) -> Vec<ModelStreamEvent> {
        if directive(request, "native-worker-followup") {
            assert_eq!(
                request.cache_key.as_ref().unwrap(),
                &*self.worker_id.lock().unwrap()
            );
            return answer("worker followup complete");
        }
        match self.worker_calls.fetch_add(1, Ordering::SeqCst) {
            0 => tool(
                "child-spawn",
                "task",
                json!({"model": "low",
                    "description":"nested background child", "prompt":"native-grandchild",
                    "name":"grandchild", "fork_turns":"none"
                }),
            ),
            1 => {
                let id = spawned_id(request, "child-spawn");
                *self.grandchild_id.lock().unwrap() = id.clone();
                tool(
                    "child-wait",
                    "task_output",
                    json!({"task_id":id,"block":true,"timeout":10000}),
                )
            }
            2 => {
                let (failed, text) = receipt(request, "child-wait");
                assert!(!failed && text.contains("grandchild complete"), "{text}");
                tool(
                    "child-message",
                    "SendMessage",
                    json!({
                        "to":"grandchild", "summary":"Continue existing grandchild", "message":"native-grandchild-followup"
                    }),
                )
            }
            3 => {
                let (failed, text) = receipt(request, "child-message");
                assert!(!failed && text.contains("queued"), "{text}");
                assert_eq!(
                    self.grandchild_calls.load(Ordering::SeqCst),
                    1,
                    "passive mail must not start another grandchild turn"
                );
                tool(
                    "child-activate",
                    "SendMessage",
                    json!({
                        "to":"grandchild", "summary":"Execute queued direction",
                        "message":"Execute the queued grandchild task", "trigger_turn":true
                    }),
                )
            }
            4 => {
                let (failed, text) = receipt(request, "child-activate");
                assert!(!failed && text.contains("resumed"), "{text}");
                tool(
                    "child-followup-wait",
                    "task_output",
                    json!({
                        "task_id":self.grandchild_id.lock().unwrap().clone(), "block":true,"timeout":10000
                    }),
                )
            }
            5 => {
                let (failed, text) = receipt(request, "child-followup-wait");
                assert!(
                    !failed && text.contains("grandchild followup complete"),
                    "{text}"
                );
                tool(
                    "child-blocker",
                    "task",
                    json!({"model": "low",
                        "description":"child interrupt target", "prompt":"native-worker-blocker",
                        "fork_turns":"none", "run_in_background":true
                    }),
                )
            }
            6 => {
                let id = spawned_id(request, "child-blocker");
                *self.worker_blocker_id.lock().unwrap() = id.clone();
                tool("child-stop", "task_stop", json!({"task_id":id}))
            }
            7 => {
                let (failed, text) = receipt(request, "child-stop");
                assert!(!failed, "{text}");
                answer("worker complete")
            }
            call => panic!("unexpected worker provider call {call}"),
        }
    }
}

impl ModelProvider for NativeProvider {
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
            context_window: 200000,
            max_output_tokens: 8192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }
    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        let worker = directive(&request, "native-worker");
        let grandchild = directive(&request, "native-grandchild");
        let root_blocker = directive(&request, "native-root-blocker");
        let worker_blocker = directive(&request, "native-worker-blocker");
        // Legacy depth does not remove the spawner from these unrestricted roles.
        assert_controls(&request, true);
        if root_blocker || worker_blocker {
            let started = if root_blocker {
                &self.root_blocker_started
            } else {
                &self.worker_blocker_started
            };
            started.add_permits(1);
            return Box::pin(async_stream::stream! {
                yield ModelStreamEvent::MessageStart { model: MODEL.into() };
                signal.cancelled().await;
            });
        }
        // Stop only after the targeted runtime has actually entered its model.
        let wait = if worker && self.worker_calls.load(Ordering::SeqCst) == 6 {
            Some(self.worker_blocker_started.clone())
        } else if !worker && !grandchild && self.root_calls.load(Ordering::SeqCst) == 6 {
            Some(self.root_blocker_started.clone())
        } else {
            None
        };
        let events = if worker {
            self.worker_turn_calls.fetch_add(1, Ordering::SeqCst);
            self.worker(&request)
        } else if grandchild {
            self.grandchild_calls.fetch_add(1, Ordering::SeqCst);
            if directive(&request, "native-grandchild-followup") {
                assert_eq!(
                    request.cache_key.as_ref().unwrap(),
                    &*self.grandchild_id.lock().unwrap()
                );
                answer("grandchild followup complete")
            } else {
                answer("grandchild complete")
            }
        } else {
            self.root(&request)
        };
        Box::pin(async_stream::stream! {
            if let Some(wait) = wait { wait.acquire_owned().await.unwrap().forget(); }
            for event in events { yield event; }
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_main_and_child_collaboration_needs_no_workflow_or_node() {
    let _restore = NodeEnvironmentRestore(std::env::var_os("KLAUS_NODE"));
    let missing_node = std::env::temp_dir().join(format!("klaus-no-node-{}", uuid::Uuid::new_v4()));
    assert!(!missing_node.exists());
    klaus_agent_core::runtime_process::set_process_environment_var("KLAUS_NODE", &missing_node);
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let provider = Arc::new(NativeProvider {
        info: ProviderInfo {
            name: MODEL.into(),
            default_model: MODEL.into(),
            base_url: None,
            env_key: None,
        },
        root_calls: AtomicUsize::new(0),
        worker_calls: AtomicUsize::new(0),
        worker_turn_calls: AtomicUsize::new(0),
        grandchild_calls: AtomicUsize::new(0),
        worker_id: Mutex::new(String::new()),
        grandchild_id: Mutex::new(String::new()),
        root_blocker_id: Mutex::new(String::new()),
        worker_blocker_id: Mutex::new(String::new()),
        root_blocker_started: Arc::new(Semaphore::new(0)),
        worker_blocker_started: Arc::new(Semaphore::new(0)),
    });
    let mut options = agent_options(process.clone());
    options.provider = provider.clone();
    options.tools = None;
    let config = options.config.as_mut().unwrap();
    config["model"] = json!(MODEL);
    config["workflow"] = json!(false);
    config["swarm"] = json!(false);
    config["agentMemory"] = json!(false);
    config["autoMemory"] = json!(false);
    config["maxSubAgentDepth"] = json!(2);
    // Keep all four child runtimes resident, including the stopped target.
    config["maxConcurrentThreadsPerSession"] = json!(5);
    // A deliberately absent interpreter in the session environment ensures
    // delegation does not borrow the optional program-execution runtime.
    let mut environment = RuntimeEnv::new();
    environment.insert(
        "PATH".into(),
        "/native-collaboration-no-interpreters".into(),
    );
    environment.insert(
        "KLAUS_NODE".into(),
        "/native-collaboration-no-interpreters/node".into(),
    );
    let session = Agent::new(options).start_session(StartSessionOptions {
        shell_env: Some(environment),
        ..Default::default()
    });
    let outcome = std::panic::AssertUnwindSafe(async {
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            session.run(
                Input::Text("Use native collaboration tools to complete the fixture".into()),
                TurnOptions::default(),
            ),
        )
        .await
        .expect("the native collaboration chain must settle");
        assert!(result.error.is_none(), "{:?}", result.error);
        assert_eq!(provider.root_calls.load(Ordering::SeqCst), 8);
        assert_eq!(provider.worker_calls.load(Ordering::SeqCst), 8);
        assert_eq!(provider.worker_turn_calls.load(Ordering::SeqCst), 9);
        assert_eq!(provider.grandchild_calls.load(Ordering::SeqCst), 2);
        let records = session.subagent_lifecycle_snapshot().agents;
        let worker_id = provider.worker_id.lock().unwrap().clone();
        let grandchild_id = provider.grandchild_id.lock().unwrap().clone();
        let grandchild = records
            .iter()
            .find(|record| record.agent_id == grandchild_id)
            .unwrap();
        assert_eq!(grandchild.context.depth, 2);
        assert_eq!(
            grandchild.context.parent_agent_id.as_ref(),
            Some(&RuntimeString::from(worker_id))
        );
        for id in [
            &*provider.root_blocker_id.lock().unwrap(),
            &*provider.worker_blocker_id.lock().unwrap(),
        ] {
            let record = records
                .iter()
                .find(|record| record.agent_id == *id)
                .unwrap();
            assert_ne!(
                record.status,
                klaus_agent_core::schemas::subagent_lifecycle::SubagentStatus::Running
            );
        }
    })
    .catch_unwind()
    .await;
    let files = session
        .background_agent_summaries()
        .into_iter()
        .map(|task| task.output_file)
        .collect::<Vec<_>>();
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    for file in files {
        if std::path::Path::new(&file).exists() {
            std::fs::remove_file(file).unwrap();
        }
    }
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
