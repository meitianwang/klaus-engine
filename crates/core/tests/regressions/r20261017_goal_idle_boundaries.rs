//! Defect: goal continuation bypassed idle admission and lost context boundaries.
//! Root cause: an unconditional turn loop neither counted real activity nor
//! captured the complete instructions and actual tool inventory together.
//! Work item: 20261017 persistent goal lifecycle.

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::NoopCompactor;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::goal::types::ThreadGoalStatus;
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var, RuntimeProcessContext,
};
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const OBJECTIVE: &str = "Preserve </objective> {{ tokens_used }} & the complete requested outcome.";
struct Home {
    path: PathBuf,
    previous: Option<String>,
}
impl Home {
    fn new() -> Self {
        let previous = std::env::var("KLAUS_CONFIG_DIR").ok();
        let path = std::env::temp_dir().join(format!("klaus-goal-idle-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        set_process_environment_var("KLAUS_CONFIG_DIR", &*path.to_string_lossy());
        Self { path, previous }
    }
}
impl Drop for Home {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => set_process_environment_var("KLAUS_CONFIG_DIR", value),
            None => remove_process_environment_var("KLAUS_CONFIG_DIR"),
        };
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
#[derive(Clone, Copy)]
enum Response {
    Text,
    Empty,
    Reasoning,
}
struct Provider {
    info: ProviderInfo,
    tools: bool,
    responses: Vec<Response>,
    calls: AtomicUsize,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
}
impl ModelProvider for Provider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn capabilities(&self, _model: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: self.tools,
            thinking: true,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 8192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }
    fn stream(&self, request: ModelRequest, _signal: AbortSignal) -> ModelStream {
        let index = self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request);
        let response = *self
            .responses
            .get(index)
            .expect("goal must stop before another provider request");
        Box::pin(
            async_stream::stream! {yield ModelStreamEvent::MessageStart{model:"goal-fixture".into()};match response{Response::Text=>yield ModelStreamEvent::TextDelta{text:"Measured progress.".into()},Response::Reasoning=>yield ModelStreamEvent::ThinkingBlock{text:"Checking current evidence.".into(),signature:None,encrypted_content:None,is_summary:true},Response::Empty=>{}}yield ModelStreamEvent::MessageStop{stop_reason:ModelStopReason::EndTurn,usage:Usage{input_tokens:9,cache_read_input_tokens:3,output_tokens:2,cache_creation_input_tokens:0,total_tokens:Some(999)}};},
        )
    }
}
async fn fixture(
    home: &Home,
    tools: bool,
    responses: Vec<Response>,
) -> (
    RuntimeProcessContext,
    Arc<klaus_agent_core::engine::session::Session>,
    Arc<Mutex<Vec<ModelRequest>>>,
) {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let provider = Arc::new(Provider {
        info: ProviderInfo {
            name: "goal-fixture".into(),
            base_url: None,
            env_key: None,
            default_model: "goal-fixture".into(),
        },
        tools,
        responses,
        calls: AtomicUsize::new(0),
        requests: requests.clone(),
    });
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.provider = provider;
    options.tools = Some(klaus_agent_core::goal::create_goal_tools());
    options.compactor = Some(Arc::new(NoopCompactor));
    let agent = Agent::new(options);
    let store: Arc<dyn klaus_agent_core::store::session_store::SessionStore> = Arc::new(
        klaus_agent_core::store::jsonl_store::JsonlSessionStore::new(
            klaus_agent_core::store::jsonl_store::JsonlSessionStoreOptions {
                dir: Some(
                    home.path
                        .join(format!("sessions-{tools}"))
                        .to_string_lossy()
                        .into_owned(),
                ),
                archive_dir: None,
            },
        ),
    );
    let session=agent.open_session(store,StartSessionOptions{cwd:Some(home.path.to_string_lossy().to_string().into()),user_config_dir:Some(home.path.to_string_lossy().to_string().into()),config:Some(json!({"model":"goal-fixture","goal":true,"prewarm":false,"autoMemory":false,"agentMemory":false})),..Default::default()}).await.unwrap();
    (process, session, requests)
}
fn contract(path: &Path, actual: &str) {
    if std::env::var_os("UPDATE_GOAL_CONTRACTS").is_some() {
        std::fs::write(path, actual).unwrap();
    }
    assert_eq!(
        std::fs::read_to_string(path).unwrap(),
        actual,
        "complete request-boundary snapshot changed: {}",
        path.display()
    );
}

#[tokio::test]
async fn actual_continuation_preserves_escaped_objective_and_complete_inventory() {
    let home = Home::new();
    for enabled in [true, false] {
        let (process, session, requests) =
            fixture(&home, enabled, vec![Response::Text, Response::Text]).await;
        session
            .set_thread_goal(Some(OBJECTIVE.into()), None, None)
            .await
            .unwrap();
        let mut events: Vec<_> = session
            .run_streamed(
                Input::Text("Pursue the approved objective.".into()),
                TurnOptions::default(),
            )
            .events
            .collect()
            .await;
        let idle = session
            .run_goal_idle_streamed(TurnOptions::default())
            .await
            .unwrap()
            .expect("goal admits a distinct idle turn");
        events.extend(idle.events.collect::<Vec<_>>().await);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, EngineEvent::TurnStarted { .. }))
                .count(),
            2
        );
        let captured = requests.lock().unwrap().clone();
        assert_eq!(captured.len(), 2);
        let second = &captured[1];
        let contextual = second
            .messages
            .iter()
            .rev()
            .find_map(|message| match message {
                Message::User(user) if user.is_meta == Some(true) => {
                    user.content.iter().find_map(|block| match block {
                        ContentBlock::Text(text)
                            if text
                                .text
                                .contains("Continue working toward the active thread goal.") =>
                        {
                            Some(text)
                        }
                        _ => None,
                    })
                }
                _ => None,
            })
            .unwrap();
        assert!(
            contextual.harness_envelope.is_none(),
            "goal input must stay contextual user data"
        );
        let mode = if enabled { "enabled" } else { "disabled" };
        let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        contract(
            &fixtures.join(format!("goal-continuation-{mode}.md")),
            &contextual.text.try_to_utf8().unwrap(),
        );
        let tools = runtime_codec::encode_value(&second.tools).unwrap();
        let tools = runtime_json::stringify_pretty(&tools)
            .try_to_utf8()
            .unwrap();
        contract(
            &fixtures.join(format!("goal-inventory-{mode}.json")),
            &tools,
        );
        let artifact = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/goal-mode-logs");
        std::fs::create_dir_all(&artifact).unwrap();
        let request = runtime_codec::encode_value(second).unwrap();
        std::fs::write(
            artifact.join(format!("actual-goal-request-{mode}.json")),
            runtime_json::stringify_pretty(&request).to_node_utf8(),
        )
        .unwrap();
        let goal = session.thread_goal().await.unwrap().unwrap();
        assert_eq!(goal.status, ThreadGoalStatus::Active);
        assert_eq!(
            goal.tokens_used, 22,
            "uncached input plus output, with no total-token double charge"
        );
        session
            .set_thread_goal(None, Some("paused".into()), None)
            .await
            .unwrap();
        session.dispose().await;
        process.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn three_empty_automatic_turns_block_and_reasoning_resets_the_audit() {
    let home = Home::new();
    let (process, session, requests) = fixture(
        &home,
        true,
        vec![
            Response::Text,
            Response::Empty,
            Response::Reasoning,
            Response::Empty,
            Response::Empty,
            Response::Empty,
        ],
    )
    .await;
    session
        .set_thread_goal(Some("Finish the full objective.".into()), None, None)
        .await
        .unwrap();
    let mut events: Vec<_> = session
        .run_streamed(Input::Text("Start.".into()), TurnOptions::default())
        .events
        .collect()
        .await;
    for _ in 0..5 {
        let idle = session
            .run_goal_idle_streamed(TurnOptions::default())
            .await
            .unwrap()
            .expect("audit permits this automatic turn");
        events.extend(idle.events.collect::<Vec<_>>().await);
    }
    assert!(session
        .run_goal_idle_streamed(TurnOptions::default())
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, EngineEvent::TurnStarted { .. }))
            .count(),
        6
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "empty automatic turns have a separate three-turn safety audit"
    );
    assert_eq!(requests.lock().unwrap().len(), 6);
    let goal = session.thread_goal().await.unwrap().unwrap();
    assert_eq!(goal.status, ThreadGoalStatus::Blocked);
    assert_eq!(goal.tokens_used, 66);
    session.dispose().await;
    process.shutdown().await.unwrap();
}
