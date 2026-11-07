//! Creation must reject a full shared execution pool before returning an ID.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ToolError;
use klaus_agent_core::types::json::Json;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{
    SpawnSubAgentOptions, Tool, ToolContext, ToolProgress, ToolResult,
};
use tokio::sync::Semaphore;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

struct State {
    entered: Semaphore,
    release: Semaphore,
    accepted: Mutex<Vec<RuntimeString>>,
    refusal: Mutex<Option<RuntimeString>>,
}

struct Provider {
    info: ProviderInfo,
    first_root: AtomicBool,
    state: Arc<State>,
}

fn stop(reason: ModelStopReason) -> ModelStreamEvent {
    ModelStreamEvent::MessageStop {
        stop_reason: reason,
        usage: Usage {
            input_tokens: 1,
            output_tokens: 1,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: None,
        },
    }
}

impl ModelProvider for Provider {
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
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
            context_window: 200_000,
            max_output_tokens: 8_192,
        }
    }
    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        let child = request.messages.iter().any(|message| {
            matches!(message,
            Message::User(user) if user.content.iter().any(|block| matches!(block,
                ContentBlock::Text(text) if text.text.starts_with("admitted child ")
                    || text.text.contains("\nDirective: admitted child "))))
        });
        let invoke = !child && !self.first_root.swap(true, Ordering::AcqRel);
        let state = self.state.clone();
        Box::pin(async_stream::stream! {
            if child {
                state.entered.add_permits(1);
                tokio::select! {
                    permit = state.release.acquire() => permit.expect("release semaphore remains open").forget(),
                    _ = signal.cancelled() => return,
                }
            }
            if invoke {
                yield ModelStreamEvent::ToolUseStart { id: "pool-call".into(), name: "exercise_pool".into(), thought_signature: None, tool_kind: None, input_format: None };
                yield ModelStreamEvent::ToolUseInputDelta { id: "pool-call".into(), partial_json: "{}".into() };
                yield ModelStreamEvent::ToolUseStop { id: "pool-call".into() };
                yield stop(ModelStopReason::ToolUse);
            } else {
                yield ModelStreamEvent::TextDelta { text: "Work completed.".into() };
                yield stop(ModelStopReason::EndTurn);
            }
        })
    }
}

struct ExercisePool(Arc<State>);

#[async_trait]
impl Tool for ExercisePool {
    fn static_name(&self) -> &str {
        "exercise_pool"
    }
    fn static_description(&self) -> &str {
        "Exercise shared child creation admission"
    }
    fn persistence_identity(&self) -> Option<RuntimeString> {
        Some("fixture:shared-admission:1".into())
    }
    fn input_schema(&self) -> Json {
        runtime_json::parse(&"{\"type\":\"object\"}".into()).unwrap()
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_read_only(&self, _: &Json) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        true
    }
    async fn call(
        &self,
        _: Json,
        context: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let spawn = context
            .spawn_background_agent
            .as_ref()
            .expect("root exposes child creation");
        for index in 0..3 {
            let result = spawn(SpawnSubAgentOptions {
                prompt: format!("admitted child {index}").into(),
                ..Default::default()
            })
            .await
            .map_err(ToolError::new)?;
            self.0.accepted.lock().unwrap().push(result.agent_id);
            tokio::time::timeout(Duration::from_secs(10), self.0.entered.acquire())
                .await
                .expect("accepted child must start")
                .expect("entered semaphore remains open")
                .forget();
        }
        let refused = spawn(SpawnSubAgentOptions {
            prompt: "fourth child".into(),
            ..Default::default()
        })
        .await;
        *self.0.refusal.lock().unwrap() = refused.err();
        self.0.release.add_permits(3);
        Ok(text_result(
            Json::Null,
            "Child creation admission was exercised.",
            false,
        ))
    }
}

#[tokio::test]
async fn root_execution_does_not_consume_child_capacity_and_full_creation_returns_a_refusal() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let state = Arc::new(State {
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
        accepted: Mutex::new(Vec::new()),
        refusal: Mutex::new(None),
    });
    let mut options = agent_options(process);
    options.provider = Arc::new(Provider {
        info: ProviderInfo {
            name: "shared-pool".into(),
            default_model: "shared-pool".into(),
            base_url: None,
            env_key: None,
        },
        first_root: AtomicBool::new(false),
        state: state.clone(),
    });
    options.config = Some(serde_json::json!({
        "model":"shared-pool", "prewarm":false, "fork":false,
        "maxConcurrentSubAgents":3, "approval_policy":"never", "sandbox_mode":"danger-full-access",
        "memory":{"enabled":false}, "autoMemory":false, "autoDream":false, "memoryRecall":false,
        "speculativeCompaction":false, "earlyMicrocompact":false, "idleMicrocompact":false,
        "compactionSelfCheck":false, "loopDetection":false
    }));
    options.include_default_agents = Some(false);
    options.tools = Some(vec![Arc::new(ExercisePool(state.clone()))]);
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions {
        cwd: Some(dir.path().to_string_lossy().into_owned().into()),
        user_config_dir: Some(dir.path().to_string_lossy().into_owned().into()),
        ..Default::default()
    });
    let turn = session.run_streamed(
        Input::Text("Exercise shared admission".into()),
        TurnOptions::default(),
    );
    let _events = tokio::time::timeout(Duration::from_secs(30), turn.events.collect::<Vec<_>>())
        .await
        .expect("root and child creation must settle");
    assert_eq!(state.accepted.lock().unwrap().len(), 3);
    assert!(state
        .refusal
        .lock()
        .unwrap()
        .as_ref()
        .expect("fourth creation must refuse")
        .contains("execution capacity"));
    assert_eq!(session.subagent_lifecycle_snapshot().agents.len(), 3);
    session.shutdown_and_wait().await.unwrap();
}
