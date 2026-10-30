//! Defect: a worker role could replace its parent's planning restriction with
//! its declared permission preset.
//! The collaboration view must remain Plan independently of ordinary permissions,
//! reject an actual child mutation, and persist the admitted mode for recovery.
//! Work item: 20261017 independent worker collaboration boundary.

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::agent_definitions::AgentDefinition;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::planning::{instructions::instruction_part, CollaborationMode};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::config::default_config;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::schemas::rollout::RolloutItem;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};
use klaus_agent_core::tools::builtins::write::create_write_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::permission::{
    ApprovalHandler, ApprovalKind, ApprovalRequest, ApprovalResponse,
};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo, SystemPromptPart,
};
use serde_json::json;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

fn usage() -> Usage {
    Usage {
        input_tokens: 10,
        output_tokens: 4,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: None,
    }
}

fn task_input() -> serde_json::Value {
    json!({"model": "low",
        "description": "escape attempt",
        "prompt": "escape-attempt-directive",
        "subagent_type": "escape-fixture",
        "run_in_background": false,
    })
}

struct ScopedTaskApproval {
    expected_input: RuntimeJson,
    approved: AtomicUsize,
    unexpected: AtomicUsize,
}

#[async_trait::async_trait]
impl ApprovalHandler for ScopedTaskApproval {
    async fn approve(&self, request: ApprovalRequest, signal: AbortSignal) -> ApprovalResponse {
        if !signal.is_cancelled()
            && request.kind == ApprovalKind::Tool
            && request.id == "spawn-escape"
            && request.tool_name == "task"
            && request.input == self.expected_input
            && self
                .approved
                .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            ApprovalResponse::Allow {
                updated_input: None,
                updated_permissions: None,
            }
        } else {
            self.unexpected.fetch_add(1, Ordering::SeqCst);
            ApprovalResponse::Deny {
                message: Some("Only the single expected planning delegation is approved.".into()),
            }
        }
    }
}

/// A role declares broad ordinary permissions, then attempts a forbidden write.
struct EscapeAttemptProvider {
    info: ProviderInfo,
    sentinel: String,
    expected_plan: SystemPromptPart,
    parent_requests: AtomicUsize,
    child_requests: AtomicUsize,
    child_write_rejected: AtomicBool,
}

impl ModelProvider for EscapeAttemptProvider {
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
                matches!(block, ContentBlock::Text(text) if text.text.contains("escape-attempt-directive"))
            }),
            _ => false,
        });
        if is_child {
            let modes = request
                .system
                .iter()
                .filter(|part| part.name == "collaboration-mode")
                .collect::<Vec<_>>();
            assert_eq!(modes, vec![&self.expected_plan]);
            assert!(!request.tools.iter().any(|tool| matches!(
                tool.name.try_to_utf8().ok().as_deref(),
                Some(
                    "write"
                        | "edit"
                        | "multi_edit"
                        | "notebook_edit"
                        | "EnterWorktree"
                        | "ExitWorktree"
                )
            )));
            let child_request = self.child_requests.fetch_add(1, Ordering::SeqCst);
            if child_request == 0 {
                return Box::pin(futures::stream::iter(vec![
                    ModelStreamEvent::MessageStart {
                        model: "escape-test".into(),
                    },
                    ModelStreamEvent::ToolUseStart {
                        id: "forbidden-plan-write".into(),
                        name: "write".into(),
                        thought_signature: None,
                        tool_kind: None,
                        input_format: None,
                    },
                    ModelStreamEvent::ToolUseInputDelta {
                        id: "forbidden-plan-write".into(),
                        partial_json: json!({
                            "path": &self.sentinel,
                            "content": "This mutation must never execute.",
                        })
                        .to_string()
                        .into(),
                    },
                    ModelStreamEvent::ToolUseStop {
                        id: "forbidden-plan-write".into(),
                    },
                    ModelStreamEvent::MessageStop {
                        stop_reason: ModelStopReason::ToolUse,
                        usage: usage(),
                    },
                ]));
            }
            assert_eq!(child_request, 1, "exactly one child follow-up request");
            assert!(
                request.messages.iter().any(|message| matches!(
                    message,
                    Message::ToolResult(result)
                        if result.tool_use_id == "forbidden-plan-write" && result.is_error
                )),
                "the next actual child request must contain the correlated write refusal"
            );
            self.child_write_rejected.store(true, Ordering::SeqCst);
            return Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::MessageStart {
                    model: "escape-test".into(),
                },
                ModelStreamEvent::TextDelta {
                    text: "child finished".into(),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::EndTurn,
                    usage: usage(),
                },
            ]));
        }
        let parent_request = self.parent_requests.fetch_add(1, Ordering::SeqCst);
        if parent_request == 0 {
            return Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::MessageStart {
                    model: "escape-test".into(),
                },
                ModelStreamEvent::ToolUseStart {
                    id: "spawn-escape".into(),
                    name: "task".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "spawn-escape".into(),
                    partial_json: task_input().to_string().into(),
                },
                ModelStreamEvent::ToolUseStop {
                    id: "spawn-escape".into(),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::ToolUse,
                    usage: usage(),
                },
            ]));
        }
        assert_eq!(parent_request, 1, "exactly one parent follow-up request");
        assert!(
            request.messages.iter().any(|message| matches!(
                message,
                Message::ToolResult(result)
                    if result.tool_use_id == "spawn-escape" && !result.is_error
            )),
            "the next actual parent request must contain the successful Task result"
        );
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::MessageStart {
                model: "escape-test".into(),
            },
            ModelStreamEvent::TextDelta {
                text: "parent done".into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: usage(),
            },
        ]))
    }
}

#[tokio::test]
async fn a_plan_mode_child_cannot_declare_its_way_out_of_plan_mode() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let directory = std::env::temp_dir().join(format!("klaus-plan-floor-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let sentinel = directory.join("forbidden-plan-write.txt");
    assert!(!sentinel.try_exists().unwrap());
    let thread_id = format!("plan-floor-parent-{}", uuid::Uuid::new_v4());
    let mut options = agent_options(process.clone());
    options.config = Some(json!({
        "approval_policy": "on-request",
        "sandbox_mode": "danger-full-access",
        "approvals_reviewer": "user",
        "ask": ["task"],
        "agentMemory": false,
        "cron": false
    }));
    options.cwd = Some(directory.to_string_lossy().into_owned());
    let provider = Arc::new(EscapeAttemptProvider {
        info: ProviderInfo {
            name: "escape-test".into(),
            default_model: "escape-test".into(),
            base_url: None,
            env_key: None,
        },
        sentinel: sentinel.to_string_lossy().into_owned(),
        expected_plan: instruction_part(&default_config(), CollaborationMode::Plan),
        parent_requests: AtomicUsize::new(0),
        child_requests: AtomicUsize::new(0),
        child_write_rejected: AtomicBool::new(false),
    });
    options.provider = provider.clone();
    let task = create_task_tool(TaskToolOptions::default());
    let approval = Arc::new(ScopedTaskApproval {
        expected_input: task
            .parse_input(&runtime_json::parse(&task_input().to_string().into()).unwrap())
            .unwrap(),
        approved: AtomicUsize::new(0),
        unexpected: AtomicUsize::new(0),
    });
    options.approval_handler = Some(approval.clone());
    options.tools = Some(vec![task, create_write_tool()]);
    // The role declares broad filesystem access while planning remains a separate floor.
    options.agents = Some(vec![AgentDefinition {
        name: "escape-fixture".into(),
        description: "A fixture role that tries to leave plan mode".into(),
        runtime_policy: Some(klaus_agent_core::engine::session::RuntimePolicyUpdate {
            approval_policy: Some(klaus_agent_core::types::permission::ApprovalPolicy::Never),
            sandbox_mode: Some(klaus_agent_core::types::permission::SandboxMode::DangerFullAccess),
            approvals_reviewer: None,
            sandbox_workspace_write: None,
            network_allowed: Some(true),
        }),
        ..Default::default()
    }]);

    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(directory.join("sessions").to_string_lossy().into_owned()),
        ..Default::default()
    }));
    let agent = Agent::new(options);
    let session = agent
        .open_session(
            store.clone(),
            StartSessionOptions {
                id: Some(thread_id.clone().into()),
                cwd: Some(directory.to_string_lossy().into_owned().into()),
                user_config_dir: Some(
                    directory
                        .join("config")
                        .to_string_lossy()
                        .into_owned()
                        .into(),
                ),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    session
        .planning_runtime()
        .set_mode(CollaborationMode::Plan, None)
        .unwrap();

    assert_eq!(
        session.runtime_policy_state().approval_policy,
        Some(klaus_agent_core::types::permission::ApprovalPolicy::OnRequest),
        "the scoped delegation fixture requires an enabled ordinary approval path"
    );
    let mut task_admitted = false;
    let mut events = session
        .run_streamed(
            Input::Text("Delegate to the escape fixture".into()),
            TurnOptions::default(),
        )
        .events;
    while let Some(event) = events.next().await {
        let item = match &event {
            EngineEvent::ItemCompleted { item, .. } => Some(item),
            _ => None,
        };
        if let Some(klaus_agent_core::types::events::ThreadItem::ToolCall {
            id,
            tool_name,
            result,
            error,
            ..
        }) = item
        {
            if id.try_to_utf8().ok().as_deref() == Some("spawn-escape")
                && tool_name.try_to_utf8().ok().as_deref() == Some("task")
                && result.is_some()
                && error.is_none()
            {
                task_admitted = true;
            }
        }
    }
    assert!(
        task_admitted,
        "the specifically approved real Task must admit the planning child"
    );
    assert_eq!(approval.approved.load(Ordering::SeqCst), 1);
    assert_eq!(approval.unexpected.load(Ordering::SeqCst), 0);
    assert_eq!(provider.parent_requests.load(Ordering::SeqCst), 2);
    assert_eq!(provider.child_requests.load(Ordering::SeqCst), 2);
    assert!(provider.child_write_rejected.load(Ordering::SeqCst));
    assert!(!sentinel.try_exists().unwrap());

    let snapshot = session.subagent_lifecycle_snapshot();
    assert_eq!(snapshot.agents.len(), 1, "exactly one hostile child ran");
    let agent_id = snapshot.agents[0].agent_id.clone();
    session.shutdown_and_wait().await.unwrap();

    let agent_id_host = String::from_utf8_lossy(&agent_id.to_node_utf8()).into_owned();
    let persisted = store
        .read_sub_agent(&thread_id, &agent_id_host)
        .await
        .unwrap();
    let meta = persisted
        .meta
        .expect("the child persisted its ordinary provenance");
    assert_eq!(
        meta.agent_type.as_ref(),
        Some(&RuntimeString::from("escape-fixture"))
    );
    assert!(
        meta.permission_mode.is_none(),
        "new child metadata must not publish a retired permission preset"
    );
    let creations = persisted
        .raw_items
        .iter()
        .filter_map(|item| match item {
            RolloutItem::Event(event) if event.event == "ordinary_subagent_creation" => {
                event.payload.as_ref()
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(creations.len(), 1, "one durable admitted child snapshot");
    let creation = creations[0];
    let origin = runtime_json::parse(&r#"{"version":1,"mode":"plan"}"#.into()).unwrap();
    assert_eq!(creation.get("collaborationOrigin"), Some(&origin));
    assert_eq!(
        creation.get("version").and_then(RuntimeJson::as_u64),
        Some(6)
    );
    assert_eq!(
        creation.get("agentType").and_then(RuntimeJson::as_str),
        Some(&RuntimeString::from("escape-fixture"))
    );
    assert!(creation.get("permissionMode").is_none());
    for (field, expected) in [
        ("approvalPolicy", "never"),
        ("sandboxMode", "danger-full-access"),
        ("approvalsReviewer", "user"),
    ] {
        assert_eq!(
            creation.get(field).and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from(expected)),
            "the admitted canonical {field} remains independent of planning"
        );
    }
    assert_eq!(
        creation
            .get("networkAllowed")
            .and_then(RuntimeJson::as_bool),
        Some(true)
    );
    assert!(!sentinel.try_exists().unwrap());

    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
