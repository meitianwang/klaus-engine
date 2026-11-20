//! Defect: concurrent streaming pre-execution, wake-reason injection, caller ownership,
//! and OpenAI control facades had lifecycle and isolation gaps across background shells
//! and subagents.
//! Root cause: `Yield` shared tool-result envelopes and lacked strict serial barriers,
//! `TaskOutput`/`TaskStop` lacked full caller ownership enforcement, and OpenAI
//! `wait_agent` did not wait on running targets or format shell results as command output.

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, Session, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(25);

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

fn answer(model: &str, text: &str) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::MessageStart {
            model: model.into(),
        },
        ModelStreamEvent::TextDelta { text: text.into() },
        stop(ModelStopReason::EndTurn),
    ]
}

fn calls_stream(model: &str, items: Vec<(&str, &str, Value)>) -> Vec<ModelStreamEvent> {
    let mut events = vec![ModelStreamEvent::MessageStart {
        model: model.into(),
    }];
    for (id, name, input) in items {
        events.push(ModelStreamEvent::ToolUseStart {
            id: id.into(),
            name: name.into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        });
        events.push(ModelStreamEvent::ToolUseInputDelta {
            id: id.into(),
            partial_json: input.to_string().into(),
        });
        events.push(ModelStreamEvent::ToolUseStop { id: id.into() });
    }
    events.push(stop(ModelStopReason::ToolUse));
    events
}

fn user_text(message: &Message) -> String {
    match message {
        Message::User(user) => user
            .content
            .iter()
            .filter_map(|block| match block {
                ContentBlock::Text(text) => Some(text.text.try_to_utf8().unwrap_or_default()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

fn tool_result_text(message: &Message) -> String {
    match message {
        Message::ToolResult(result) => result
            .content
            .iter()
            .filter_map(|block| match block {
                klaus_agent_core::schemas::content::OutputBlock::Text(text) => {
                    Some(text.text.try_to_utf8().unwrap_or_default())
                }
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

fn has_user_line(request: &ModelRequest, needle: &str) -> bool {
    request
        .messages
        .iter()
        .any(|message| user_text(message).contains(needle))
}

fn tool_result_for<'a>(
    request: &'a ModelRequest,
    id: &str,
) -> Option<&'a klaus_agent_core::schemas::message::ToolResultMessage> {
    request.messages.iter().find_map(|message| match message {
        Message::ToolResult(result) if result.tool_use_id == id => Some(result),
        _ => None,
    })
}

fn shell_id_from_result(result: &klaus_agent_core::schemas::message::ToolResultMessage) -> String {
    let text = tool_result_text(&Message::ToolResult(result.clone()));
    text.split("Started background shell ")
        .nth(1)
        .and_then(|rest| rest.split('.').next())
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| panic!("bash background result carries shellId in text: {text}"))
}

async fn run_text(session: &Arc<Session>, prompt: &str) {
    let mut events = session
        .run_streamed(Input::Text(prompt.into()), TurnOptions::default())
        .events;
    tokio::time::timeout(TIMEOUT, async {
        while let Some(event) = events.next().await {
            assert!(
                !matches!(event, EngineEvent::TurnFailed { .. }),
                "unexpected TurnFailed: {event:?}"
            );
        }
    })
    .await
    .expect("session turn timed out");
}

async fn open_test_session(
    provider: Arc<dyn ModelProvider>,
    dir: &std::path::Path,
) -> (Arc<Session>, RuntimeProcessContext) {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        ..Default::default()
    }));
    let mut options = agent_options(process.clone());
    options.provider = provider.clone();
    options.tools = None;
    let config = options.config.as_mut().unwrap();
    config["model"] = json!(provider.info().default_model.try_to_utf8().unwrap());
    config["agentMemory"] = json!(false);
    config["autoMemory"] = json!(false);
    config["fork"] = json!(false);
    config["maxSubAgentDepth"] = json!(3);
    let session = Agent::new(options)
        .open_session(
            store,
            StartSessionOptions {
                id: Some(format!("bg-lifecycle-{}", uuid::Uuid::new_v4()).into()),
                cwd: Some(dir.to_string_lossy().into_owned().into()),
                user_config_dir: Some(dir.to_string_lossy().into_owned().into()),
                ..Default::default()
            },
        )
        .await
        .expect("open session");
    (session, process)
}

#[tokio::test]
async fn same_turn_background_bash_and_yield_orders_tool_result_before_wake_and_task_notices() {
    struct SameTurnYieldProvider {
        info: ProviderInfo,
        requests: Mutex<Vec<ModelRequest>>,
    }
    impl ModelProvider for SameTurnYieldProvider {
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
        fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
            self.requests.lock().unwrap().push(request.clone());
            let model = "claude-sonnet-5";
            let events = if tool_result_for(&request, "call-yield-1").is_none() {
                calls_stream(
                    model,
                    vec![
                        (
                            "call-bash-1",
                            "Bash",
                            json!({
                                "command": "sleep 0.6 && printf bg-shell-ok",
                                "description": "background shell before yield",
                                "run_in_background": true,
                                "yieldMs": 250
                            }),
                        ),
                        ("call-yield-1", "Yield", json!({})),
                    ],
                )
            } else if let Some(bash_res) = tool_result_for(&request, "call-bash-1") {
                if tool_result_for(&request, "call-read-1").is_none() {
                    let shell_id = shell_id_from_result(bash_res);
                    calls_stream(
                        model,
                        vec![("call-read-1", "TaskOutput", json!({ "task_id": shell_id }))],
                    )
                } else {
                    answer(model, "ordered-yield-ok")
                }
            } else {
                answer(model, "ordered-yield-ok")
            };
            Box::pin(futures::stream::iter(events))
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(SameTurnYieldProvider {
        info: ProviderInfo {
            name: "anthropic".into(),
            base_url: None,
            env_key: None,
            default_model: "claude-sonnet-5".into(),
        },
        requests: Mutex::new(Vec::new()),
    });
    let (session, process) = open_test_session(provider.clone(), dir.path()).await;
    run_text(&session, "run bg bash and yield").await;

    let requests = provider.requests.lock().unwrap().clone();
    let wake_req = requests
        .iter()
        .find(|req| tool_result_for(req, "call-yield-1").is_some())
        .expect("provider saw post-yield request");

    let yield_pos = wake_req
        .messages
        .iter()
        .position(|m| matches!(m, Message::ToolResult(r) if r.tool_use_id == "call-yield-1"))
        .unwrap();
    let wake_pos = wake_req
        .messages
        .iter()
        .position(|m| {
            matches!(m, Message::User(u) if u.is_meta == Some(true)
                && user_text(m).contains("something arrived; it follows this line."))
        })
        .expect("wake notice placed as meta user message after Yield result");
    let finish_pos = wake_req
        .messages
        .iter()
        .position(|m| {
            matches!(m, Message::User(u) if u.is_meta == Some(true)
                && user_text(m).contains("has finished (exit code 0)"))
        })
        .expect("background shell notice placed as meta user message after wake notice");

    assert!(
        yield_pos < wake_pos,
        "yield_pos={yield_pos} wake_pos={wake_pos}"
    );
    assert!(
        wake_pos < finish_pos,
        "wake_pos={wake_pos} finish_pos={finish_pos}"
    );

    // The Yield ToolResult keeps only the fixed header and does not swallow the
    // background shell completion notice.
    let yield_text = tool_result_text(&wake_req.messages[yield_pos]);
    assert!(yield_text.contains("The wait has ended. The line after this result says why."));
    assert!(!yield_text.contains("has finished"));

    let read_req = requests
        .iter()
        .find(|req| tool_result_for(req, "call-read-1").is_some())
        .expect("TaskOutput read recorded");
    let read_res = tool_result_for(read_req, "call-read-1").unwrap();
    let read_body = tool_result_text(&Message::ToolResult(read_res.clone()));
    assert!(
        read_body.contains("bg-shell-ok"),
        "expected bg-shell-ok in output: {read_body}"
    );

    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn subagent_cannot_access_root_background_shell_and_owns_its_own_shell_notices() {
    struct OwnershipProvider {
        info: ProviderInfo,
        root_shell_id: Mutex<Option<String>>,
        requests: Mutex<Vec<ModelRequest>>,
    }
    impl ModelProvider for OwnershipProvider {
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
        fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
            self.requests.lock().unwrap().push(request.clone());
            let model = "claude-sonnet-5";
            if has_user_line(&request, "child-ownership-probe") {
                let root_shell_id = self
                    .root_shell_id
                    .lock()
                    .unwrap()
                    .clone()
                    .expect("root shell started first");
                if tool_result_for(&request, "child-read-root").is_none() {
                    return Box::pin(futures::stream::iter(calls_stream(
                        model,
                        vec![
                            (
                                "child-read-root",
                                "TaskOutput",
                                json!({ "task_id": root_shell_id }),
                            ),
                            (
                                "child-stop-root",
                                "TaskStop",
                                json!({ "task_id": root_shell_id }),
                            ),
                        ],
                    )));
                }
                if tool_result_for(&request, "child-yield").is_none() {
                    return Box::pin(futures::stream::iter(calls_stream(
                        model,
                        vec![
                            (
                                "child-bash",
                                "Bash",
                                json!({
                                    "command": "sleep 0.6 && printf child-shell-ok",
                                    "description": "child own shell",
                                    "run_in_background": true,
                                    "yieldMs": 250
                                }),
                            ),
                            ("child-yield", "Yield", json!({})),
                        ],
                    )));
                }
                return Box::pin(futures::stream::iter(answer(model, "child-done")));
            }

            if tool_result_for(&request, "root-bash").is_none() {
                return Box::pin(futures::stream::iter(calls_stream(
                    model,
                    vec![(
                        "root-bash",
                        "Bash",
                        json!({
                            "command": "sleep 15 && printf root-secret",
                            "description": "root owned long shell",
                            "run_in_background": true,
                            "yieldMs": 250
                        }),
                    )],
                )));
            }
            if let Some(root_res) = tool_result_for(&request, "root-bash") {
                let id = shell_id_from_result(root_res);
                *self.root_shell_id.lock().unwrap() = Some(id.clone());
                if tool_result_for(&request, "spawn-child").is_none() {
                    return Box::pin(futures::stream::iter(calls_stream(
                        model,
                        vec![(
                            "spawn-child",
                            "Task",
                            json!({
                                "description": "child ownership check",
                                "prompt": "child-ownership-probe",
                                "name": "child-worker",
                                "model": "low",
                                "fork_turns": "none",
                                "run_in_background": false
                            }),
                        )],
                    )));
                }
                if tool_result_for(&request, "root-stop").is_none() {
                    return Box::pin(futures::stream::iter(calls_stream(
                        model,
                        vec![("root-stop", "TaskStop", json!({ "task_id": id }))],
                    )));
                }
            }
            Box::pin(futures::stream::iter(answer(model, "root-ownership-ok")))
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(OwnershipProvider {
        info: ProviderInfo {
            name: "anthropic".into(),
            base_url: None,
            env_key: None,
            default_model: "claude-sonnet-5".into(),
        },
        root_shell_id: Mutex::new(None),
        requests: Mutex::new(Vec::new()),
    });
    let (session, process) = open_test_session(provider.clone(), dir.path()).await;
    run_text(&session, "verify subagent isolation").await;

    let requests = provider.requests.lock().unwrap().clone();
    let child_post_probe = requests
        .iter()
        .find(|req| {
            has_user_line(req, "child-ownership-probe")
                && tool_result_for(req, "child-read-root").is_some()
        })
        .expect("child read/stop attempt recorded");
    let child_read = tool_result_for(child_post_probe, "child-read-root").unwrap();
    assert!(child_read.is_error);
    assert!(tool_result_text(&Message::ToolResult(child_read.clone()))
        .contains("belongs to another agent"));
    let child_stop = tool_result_for(child_post_probe, "child-stop-root").unwrap();
    assert!(child_stop.is_error);
    assert!(tool_result_text(&Message::ToolResult(child_stop.clone()))
        .contains("belongs to another agent"));

    let child_post_yield = requests
        .iter()
        .find(|req| {
            has_user_line(req, "child-ownership-probe")
                && tool_result_for(req, "child-yield").is_some()
        })
        .expect("child received its own shell completion");
    let child_shell_id =
        shell_id_from_result(tool_result_for(child_post_yield, "child-bash").unwrap());
    assert!(has_user_line(
        child_post_yield,
        &format!("Background shell {child_shell_id} has finished")
    ));

    // Root's own background shell must still be alive for root to stop cleanly.
    let root_post_stop = requests
        .iter()
        .find(|req| {
            !has_user_line(req, "child-ownership-probe")
                && tool_result_for(req, "root-stop").is_some()
        })
        .expect("root stop recorded");
    let root_stop = tool_result_for(root_post_stop, "root-stop").unwrap();
    assert!(!root_stop.is_error);

    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn openai_wait_agent_waits_for_running_shell_and_uses_command_output_envelope() {
    struct OpenAiShellWaitProvider {
        info: ProviderInfo,
        requests: Mutex<Vec<ModelRequest>>,
    }
    impl ModelProvider for OpenAiShellWaitProvider {
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
        fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
            self.requests.lock().unwrap().push(request.clone());
            let model = "gpt-5.4-0305-global";
            let events = if tool_result_for(&request, "oa-bash").is_none() {
                calls_stream(
                    model,
                    vec![(
                        "oa-bash",
                        "bash",
                        json!({
                            "command": "sleep 0.6 && printf openai-shell-done",
                            "description": "slow openai shell",
                            "runInBackground": true,
                            "yieldMs": 250
                        }),
                    )],
                )
            } else if tool_result_for(&request, "oa-wait").is_none() {
                let shell_id = shell_id_from_result(tool_result_for(&request, "oa-bash").unwrap());
                calls_stream(
                    model,
                    vec![(
                        "oa-wait",
                        "wait_agent",
                        json!({
                            "target": shell_id,
                            "timeout_ms": 5000
                        }),
                    )],
                )
            } else {
                answer(model, "openai-wait-ok")
            };
            Box::pin(futures::stream::iter(events))
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(OpenAiShellWaitProvider {
        info: ProviderInfo {
            name: "openai".into(),
            base_url: None,
            env_key: None,
            default_model: "gpt-5.4-0305-global".into(),
        },
        requests: Mutex::new(Vec::new()),
    });
    let (session, process) = open_test_session(provider.clone(), dir.path()).await;
    run_text(&session, "wait for openai shell").await;

    let requests = provider.requests.lock().unwrap().clone();
    let wait_spec = requests[0]
        .tools
        .iter()
        .find(|spec| spec.name == "wait_agent")
        .expect("wait_agent exposed on OpenAI profile");
    let wait_desc = wait_spec.description.try_to_utf8().unwrap();
    assert!(!wait_desc.contains("never to wait"), "{wait_desc}");
    assert!(wait_spec
        .input_schema
        .get("properties")
        .and_then(RuntimeJson::as_object)
        .is_some_and(|props| props.contains_key("filter")));

    let post_wait = requests
        .iter()
        .find(|req| tool_result_for(req, "oa-wait").is_some())
        .expect("post-wait request recorded");
    let wait_res = tool_result_for(post_wait, "oa-wait").unwrap();
    assert!(!wait_res.is_error);
    let body = tool_result_text(&Message::ToolResult(wait_res.clone()));
    assert!(body.contains("openai-shell-done"), "{body}");
    assert!(body.contains("# Command Output"), "{body}");
    assert!(!body.contains("# Subagent Report"), "{body}");

    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}
