//! Defect: a human follow-up steered into a running turn was persisted glued to
//! the last tool result's text, so once the turn settled and history took over
//! the transcript had no user message for it — the bubble the user saw while the
//! turn ran vanished, and the steer read as part of a tool's output.
//! Root cause: the drain site gave every steer the delegation shape (tool-result
//! append), which is right for a cross-conversation envelope but wrong for a
//! human message that must survive as its own transcript row.
//! Fixed by: `SteerDelivery::User`, which persists a drained user steer as its
//! own user message after the turn's tool results; delegations keep the legacy
//! tool-result shape.

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};
use async_trait::async_trait;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::message_queue::{QueuePriority, SteerDelivery};
use klaus_agent_core::engine::session::{EnqueueOptions, Input, Session, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::schemas::content::{ContentBlock, OutputBlock};
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ToolError;
use klaus_agent_core::types::json::{Json, JsonSchema};
use klaus_agent_core::types::permission::ExternalPermissionMode;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const USER_STEER: &str = "mid-turn human follow-up";
const DELEGATION_STEER: &str = "mid-turn delegation envelope";

struct WaitTool {
    schema: JsonSchema,
}
#[async_trait]
impl Tool for WaitTool {
    fn static_name(&self) -> &str {
        "wait"
    }
    fn static_description(&self) -> &str {
        "Wait, then report."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        true
    }
    fn is_read_only(&self, _: &Json) -> bool {
        true
    }
    fn max_result_size_chars(&self) -> Option<u64> {
        Some(1_000)
    }
    async fn call(
        &self,
        input: Json,
        _: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        Ok(text_result(input, "tool output", false))
    }
}

/// One tool call, then a closing text turn. The first model call also enqueues
/// the steers under test: the drain site runs after this call's tool result, so
/// enqueuing here is deterministic without sleeping on wall-clock timing. The
/// session slot is filled right after `start_session`, before any model call.
struct SteerProvider {
    info: ProviderInfo,
    calls: AtomicUsize,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    session: Arc<Mutex<Option<Arc<Session>>>>,
    delivery: Option<SteerDelivery>,
    display_metadata: Option<klaus_agent_core::runtime_values::RuntimeJson>,
}
impl ModelProvider for SteerProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn capabilities(
        &self,
        _: &klaus_agent_core::runtime_values::RuntimeString,
    ) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 16_000,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }
    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        self.requests.lock().unwrap().push(request);
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let mut events = vec![ModelStreamEvent::MessageStart {
            model: "steer-model".into(),
        }];
        if call == 0 {
            if let Some(session) = self.session.lock().unwrap().clone() {
                session
                    .enqueue(
                        Input::Text(USER_STEER.into()),
                        EnqueueOptions {
                            priority: Some(QueuePriority::Now),
                            correlation_id: Some("steer-user".into()),
                            delivery: self.delivery,
                            display_metadata: self.display_metadata.clone(),
                            ..Default::default()
                        },
                    )
                    .unwrap();
                session
                    .enqueue(
                        Input::Text(DELEGATION_STEER.into()),
                        EnqueueOptions {
                            priority: Some(QueuePriority::Now),
                            correlation_id: Some("steer-delegation".into()),
                            delivery: Some(SteerDelivery::Delegation),
                            ..Default::default()
                        },
                    )
                    .unwrap();
            }
            events.extend([
                ModelStreamEvent::ToolUseStart {
                    id: "call-0".into(),
                    name: "wait".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "call-0".into(),
                    partial_json: "{}".into(),
                },
                ModelStreamEvent::ToolUseStop {
                    id: "call-0".into(),
                },
            ]);
            events.push(ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::ToolUse,
                usage: Usage {
                    input_tokens: 1,
                    output_tokens: 1,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            });
        } else {
            events.push(ModelStreamEvent::TextDelta {
                text: "done".into(),
            });
            events.push(ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 1,
                    output_tokens: 1,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            });
        }
        Box::pin(futures::stream::iter(events))
    }
}

fn user_text(message: &Message) -> Option<String> {
    let Message::User(user) = message else {
        return None;
    };
    let mut out = String::new();
    for block in &user.content {
        if let ContentBlock::Text(text) = block {
            out.push_str(&String::from_utf8(text.text.to_node_utf8()).unwrap());
        }
    }
    Some(out)
}

fn message_uuid(message: &Message) -> klaus_agent_core::runtime_values::RuntimeString {
    match message {
        Message::User(user) => user.uuid.clone(),
        Message::Assistant(assistant) => assistant.uuid.clone(),
        Message::ToolResult(result) => result.uuid.clone(),
        Message::System(system) => system.uuid.clone(),
        Message::CompactBoundary(boundary) => boundary.uuid.clone(),
    }
}

fn tool_text(message: &Message) -> Option<String> {
    let Message::ToolResult(result) = message else {
        return None;
    };
    let mut out = String::new();
    for block in &result.content {
        if let OutputBlock::Text(text) = block {
            out.push_str(&String::from_utf8(text.text.to_node_utf8()).unwrap());
        }
    }
    Some(out)
}

fn session_with(delivery: Option<SteerDelivery>) -> (Arc<Session>, Arc<Mutex<Vec<ModelRequest>>>) {
    session_with_metadata(delivery, None)
}

pub(super) fn session_with_metadata(
    delivery: Option<SteerDelivery>,
    display_metadata: Option<klaus_agent_core::runtime_values::RuntimeJson>,
) -> (Arc<Session>, Arc<Mutex<Vec<ModelRequest>>>) {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let slot: Arc<Mutex<Option<Arc<Session>>>> = Arc::new(Mutex::new(None));
    let provider = Arc::new(SteerProvider {
        info: ProviderInfo {
            name: "steer-provider".into(),
            base_url: None,
            env_key: None,
            default_model: "steer-model".into(),
        },
        calls: AtomicUsize::new(0),
        requests: requests.clone(),
        session: slot.clone(),
        delivery,
        display_metadata,
    });
    let mut options = agent_options(process);
    options.provider = provider;
    options.tools = Some(vec![Arc::new(WaitTool {
        schema: serde_json::json!({"type": "object", "properties": {}})
            .as_object()
            .unwrap()
            .clone(),
    })]);
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions {
        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
        config: Some(serde_json::json!({ "prewarm": false, "goal": false })),
        ..Default::default()
    });
    *slot.lock().unwrap() = Some(session.clone());
    (session, requests)
}

#[tokio::test]
async fn a_user_steer_persists_as_its_own_user_message_after_the_tool_results() {
    let (session, requests) = session_with(Some(SteerDelivery::User));
    let result = session
        .run(Input::Text("work".into()), TurnOptions::default())
        .await;
    assert!(result.error.is_none(), "turn error: {:?}", result.error);
    let history = session.history();

    let user_index = history
        .iter()
        .position(|message| user_text(message).as_deref() == Some(USER_STEER))
        .expect("the user steer must persist as its own user message");
    let tool_index = history
        .iter()
        .position(|message| tool_text(message).is_some())
        .expect("the turn's tool result must persist");
    assert!(
        user_index > tool_index,
        "the steer answers the turn's last tool call, so it persists after it"
    );
    // The steer must join the turn it steered, not open a new one: the renderer
    // cuts a fold boundary at every user message whose turn id differs from the
    // group it would join, so a steer attributed to a fresh turn would render as
    // its own turn. The turn's identity is carried by the messages the turn loop
    // persists through on_message (assistant and tool results); the opening user
    // message is not in the in-memory index, which is why the comparison uses
    // the tool result rather than it.
    let turns = session.turn_index_snapshot().message_turn;
    let tool_turn = turns.get(&message_uuid(&history[tool_index])).cloned();
    let steer_turn = turns.get(&message_uuid(&history[user_index])).cloned();
    assert!(
        tool_turn.is_some() && tool_turn == steer_turn,
        "the steer must share its turn's id, got {tool_turn:?} vs {steer_turn:?}"
    );
    let tool = tool_text(&history[tool_index]).unwrap();
    assert!(
        !tool.contains(USER_STEER),
        "a user steer must not ride the tool result: {tool}"
    );
    assert!(
        tool.contains(DELEGATION_STEER),
        "a delegation steer keeps the tool-result shape: {tool}"
    );
    assert!(
        !history
            .iter()
            .any(|message| user_text(message).as_deref() == Some(DELEGATION_STEER)),
        "a delegation steer must not become a user message"
    );

    // The provider must see the steer as a user turn of its own, not as tool
    // output: that is what keeps the model's view of who said what honest.
    // Copied out before the await: a std mutex guard must not span one.
    let second_messages = requests.lock().unwrap()[1].messages.clone();
    assert!(
        second_messages
            .iter()
            .any(|message| user_text(message).as_deref() == Some(USER_STEER)),
        "the continuation request must carry the steer as a user message"
    );
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn an_unmarked_steer_keeps_the_legacy_tool_result_shape() {
    let (session, _requests) = session_with(None);
    let result = session
        .run(Input::Text("work".into()), TurnOptions::default())
        .await;
    assert!(result.error.is_none(), "turn error: {:?}", result.error);
    let history = session.history();
    assert!(
        !history
            .iter()
            .any(|message| user_text(message).as_deref() == Some(USER_STEER)),
        "a steer without a delivery marker keeps the pre-existing shape"
    );
    let tool = history
        .iter()
        .find_map(tool_text)
        .expect("the turn's tool result must persist");
    assert!(
        tool.contains(USER_STEER),
        "the unmarked steer rides the tool result: {tool}"
    );
    session.shutdown_and_wait().await.unwrap();
}
