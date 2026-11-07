//! Defect: workspace instructions gained system authority and live policy was absent.
//! Root cause: request assembly mixed instruction origins and omitted tool policy facts.
//! Work item: 20261016 request-local instruction authority and permission guidance.

use std::sync::{Arc, Mutex};

use klaus_agent_core::context::instruction_policy::project_workspace_instructions;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, RuntimePolicyUpdate, TurnOptions};
use klaus_agent_core::planning::CollaborationMode;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, OutputBlock, TextBlock, ToolUseBlock};
use klaus_agent_core::schemas::message::{
    AssistantMessage, Message, ToolResultMessage, Usage, UserMessage,
};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    CacheScope, ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo, SystemPromptPart,
};
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

fn text(value: RuntimeString) -> TextBlock {
    TextBlock {
        text: value,
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    }
}

fn part(name: &str, value: RuntimeString, cacheable: bool) -> SystemPromptPart {
    SystemPromptPart {
        name: name.into(),
        text: value,
        cacheable: Some(cacheable),
        cache_scope: None,
    }
}

fn human(value: &str) -> Message {
    Message::User(UserMessage {
        question_answer: None,
        uuid: value.into(),
        content: vec![ContentBlock::Text(text(value.into()))],
        is_meta: None,
    })
}

fn history() -> Vec<Message> {
    vec![
        human("Keep this earlier human request unchanged."),
        Message::Assistant(AssistantMessage {
            uuid: "earlier-call".into(),
            model: "instruction-policy-test".into(),
            usage: None,
            content: vec![ContentBlock::ToolUse(ToolUseBlock {
                id: "read-call".into(),
                name: "read".into(),
                input: runtime_json::parse(&RuntimeString::from("{}")).unwrap(),
                tool_kind: None,
                thought_signature: None,
            })],
        }),
        Message::ToolResult(ToolResultMessage {
            uuid: "earlier-result".into(),
            tool_use_id: "read-call".into(),
            content: vec![OutputBlock::Text(text("original result".into()))],
            is_error: false,
            tool_kind: None,
            native_tool_output: None,
            output: None,
            extra_user_blocks: None,
        }),
    ]
}

#[test]
fn workspace_projection_preserves_utf16_history_and_other_instruction_sources() {
    let mut global = part("base", "stable global policy".into(), true);
    global.cache_scope = Some(CacheScope::Global);
    let workspace = RuntimeString::from_utf16(vec![0x0061, 0xd800, 0x0062]);
    let parts = vec![
        global.clone(),
        part("memory", workspace.clone(), true),
        part("memory", "dynamic agent memory".into(), false),
        part("skills", "installed skills catalog".into(), false),
        part("agent-role", "root role constraint".into(), false),
    ];
    let original = history();
    let (system, messages) = project_workspace_instructions(&parts, &original);
    assert_eq!(
        system,
        vec![global, parts[2].clone(), parts[3].clone(), parts[4].clone()]
    );
    assert_eq!(&messages[1..], original.as_slice());
    let Message::User(instructions) = &messages[0] else {
        panic!("workspace instructions must use a user message");
    };
    assert_eq!(instructions.is_meta, Some(true));
    let ContentBlock::Text(body) = &instructions.content[0] else {
        panic!("expected original instruction text");
    };
    assert_eq!(body.text.as_utf16(), workspace.as_utf16());
    assert_eq!(body.harness_envelope, None);
    assert_eq!(parts[1].text.as_utf16(), workspace.as_utf16());
}

struct RecordingProvider {
    info: ProviderInfo,
    requests: Mutex<Vec<ModelRequest>>,
}

impl ModelProvider for RecordingProvider {
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
            prompt_cache_control: true,
            context_window: 200_000,
            max_output_tokens: 8_192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }
    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        self.requests.lock().unwrap().push(request);
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::MessageStart {
                model: "instruction-policy-test".into(),
            },
            ModelStreamEvent::TextDelta {
                text: "Completed.".into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 10,
                    output_tokens: 2,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: Some(12),
                },
            },
        ]))
    }
}

#[tokio::test]
async fn public_session_requests_keep_permission_facts_live_and_workspace_text_at_user_authority() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let provider = Arc::new(RecordingProvider {
        info: ProviderInfo {
            name: "instruction-policy-test".into(),
            base_url: None,
            env_key: None,
            default_model: "instruction-policy-test".into(),
        },
        requests: Mutex::new(Vec::new()),
    });
    let mut global = part("base", "stable global policy".into(), true);
    global.cache_scope = Some(CacheScope::Global);
    let source = vec![
        global.clone(),
        part(
            "memory",
            "User and project instructions: preserve this exact text.".into(),
            true,
        ),
        part("memory", "dynamic agent memory".into(), false),
        part("skills", "installed skills catalog".into(), false),
        part("agent-role", "root role constraint".into(), false),
    ];
    let mut options = agent_options(process);
    options.provider = provider.clone();
    options.include_default_agents = Some(false);
    let config = options.config.as_mut().unwrap();
    config["model"] = json!("instruction-policy-test");
    config["approval_policy"] = json!("never");
    config["sandbox_mode"] = json!("read-only");
    config["approvals_reviewer"] = json!("user");
    config["agentMemory"] = json!(false);
    config["autoMemory"] = json!(false);
    config["prewarm"] = json!(false);
    config["goal"] = json!(false);
    config["memory"] = json!({"enabled":false});
    let agent = Agent::new(options);
    let prompt_source = source.clone();
    let original = history();
    let session = agent.start_session(StartSessionOptions {
        initial_history: Some(original.clone()),
        system_prompt_provider: Some(Arc::new(move || {
            let parts = prompt_source.clone();
            Box::pin(async move { parts })
        })),
        ..Default::default()
    });
    for (approval, label, network, collaboration_mode) in [
        (
            klaus_agent_core::types::permission::ApprovalPolicy::OnRequest,
            "read-only",
            false,
            CollaborationMode::Default,
        ),
        (
            klaus_agent_core::types::permission::ApprovalPolicy::OnRequest,
            "planning",
            true,
            CollaborationMode::Plan,
        ),
        (
            klaus_agent_core::types::permission::ApprovalPolicy::Never,
            "no-prompts",
            false,
            CollaborationMode::Plan,
        ),
    ] {
        session
            .planning_runtime()
            .set_mode(collaboration_mode, None)
            .unwrap();
        let commit = session
            .set_runtime_policy(RuntimePolicyUpdate {
                approval_policy: Some(approval),
                sandbox_mode: Some(klaus_agent_core::types::permission::SandboxMode::ReadOnly),
                approvals_reviewer: None,

                network_allowed: Some(network),

                sandbox_workspace_write: None,
            })
            .unwrap();
        assert_eq!(commit.effective.approval_policy, Some(approval));
        assert_eq!(
            session.runtime_policy_state().sandbox_mode,
            Some(klaus_agent_core::types::permission::SandboxMode::ReadOnly)
        );
        assert_eq!(session.collaboration_mode(), collaboration_mode);
        let human_text = format!("Report only this request: {label}.");
        let result = session
            .run(
                Input::Text(human_text.clone().into()),
                TurnOptions::default(),
            )
            .await;
        assert!(result.error.is_none(), "{:?}", result.error);
        let requests = provider.requests.lock().unwrap();
        let request = requests.last().unwrap();
        assert_eq!(request.system[0], global);
        assert!(request.system.iter().any(|p| p == &source[2]));
        assert!(request.system.iter().any(|p| p == &source[3]));
        assert!(request.system.iter().any(|p| p == &source[4]));
        assert!(!request.system.iter().any(|p| p == &source[1]));
        let permissions = request
            .system
            .iter()
            .find(|p| p.name == "permissions")
            .unwrap();
        assert_eq!(permissions.cacheable, Some(false));
        assert_eq!(permissions.cache_scope, None);
        assert!(permissions.text.contains("Filesystem policy: read-only."));
        assert!(!permissions.text.contains("Permission mode:"));
        let collaboration = request
            .system
            .iter()
            .filter(|part| part.name == "collaboration-mode")
            .collect::<Vec<_>>();
        assert_eq!(collaboration.len(), 1);
        let (mode_label, template) = match collaboration_mode {
            CollaborationMode::Default => (
                "Default",
                include_str!("../../src/planning/prompts/default.md"),
            ),
            CollaborationMode::Plan => ("Plan", include_str!("../../src/planning/prompts/plan.md")),
        };
        let collaboration_text = collaboration[0].text.try_to_utf8().unwrap();
        assert!(collaboration_text.starts_with(&format!(
            "<collaboration_mode>\n# Collaboration Mode: {mode_label}\n"
        )));
        assert!(collaboration_text.ends_with(&format!("{template}\n</collaboration_mode>")));
        assert!(permissions
            .text
            .contains(if collaboration_mode == CollaborationMode::Plan {
                "Planning is active."
            } else {
                "The filesystem sandbox is read-only."
            }));
        assert!(permissions.text.contains(if network {
            "allowed by the shell policy"
        } else {
            "denied by the shell policy"
        }));
        assert!(permissions
            .text
            .contains("No interactive approval flow is available"));
        assert!(permissions
            .text
            .contains("Sandbox-denial retries outside the sandbox are disabled"));
        assert_eq!(
            &request.messages[1..1 + original.len()],
            original.as_slice()
        );
        let Message::User(workspace) = &request.messages[0] else {
            panic!("expected user instruction prefix");
        };
        assert_eq!(workspace.is_meta, Some(true));
        let ContentBlock::Text(body) = &workspace.content[0] else {
            panic!("expected instruction text");
        };
        assert_eq!(body.text, source[1].text);
        assert_eq!(body.harness_envelope, None);
        let occurrences = request
            .messages
            .iter()
            .filter(|m| matches!(m, Message::User(u) if u.uuid == workspace.uuid))
            .count();
        assert_eq!(
            occurrences, 1,
            "request-local instructions must not accumulate in history"
        );
        let latest_user = request
            .messages
            .iter()
            .rev()
            .find_map(|m| match m {
                Message::User(u) if u.is_meta != Some(true) => Some(u),
                _ => None,
            })
            .unwrap();
        assert!(
            matches!(&latest_user.content[0], ContentBlock::Text(body) if body.text == human_text.as_str())
        );
    }
}
