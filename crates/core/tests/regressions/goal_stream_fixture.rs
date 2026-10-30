//! Persistent public-session fixture for subscriber-lifetime regressions.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::NoopCompactor;
use klaus_agent_core::engine::session::{Session, SystemPromptProvider, Turn, TurnOptions};
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var, RuntimeProcessContext,
};
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::message::Usage;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::Tool;
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

pub(super) const WAIT: Duration = Duration::from_secs(20);
pub(super) type SessionSlot = Arc<Mutex<Weak<Session>>>;
type RequestHandler = Arc<dyn Fn(ModelRequest, AbortSignal, usize) -> ModelStream + Send + Sync>;

struct Home {
    path: PathBuf,
    previous: Option<std::ffi::OsString>,
}
impl Home {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("klaus-goal-stream-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        let previous = std::env::var_os("KLAUS_CONFIG_DIR");
        set_process_environment_var("KLAUS_CONFIG_DIR", &path);
        Self { path, previous }
    }
}
impl Drop for Home {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(previous) => set_process_environment_var("KLAUS_CONFIG_DIR", previous),
            None => remove_process_environment_var("KLAUS_CONFIG_DIR"),
        }
        if std::thread::panicking() {
            eprintln!("Retained stream-lifetime fixture: {}", self.path.display());
        } else {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

struct Provider {
    info: ProviderInfo,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    calls: AtomicUsize,
    handler: RequestHandler,
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
            context_window: 200_000,
            max_output_tokens: 8192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }
    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(
            call < 8,
            "unexpected provider loop in subscriber-lifetime fixture"
        );
        self.requests.lock().unwrap().push(request.clone());
        (self.handler)(request, signal, call)
    }
}

pub(super) struct Fixture {
    pub session: Arc<Session>,
    pub requests: Arc<Mutex<Vec<ModelRequest>>>,
    process: RuntimeProcessContext,
    home: Home,
}
impl Fixture {
    pub async fn new(
        slot: &SessionSlot,
        handler: impl Fn(ModelRequest, AbortSignal, usize) -> ModelStream + Send + Sync + 'static,
    ) -> Self {
        Self::with_system_prompt(slot, None, handler).await
    }
    pub async fn with_system_prompt(
        slot: &SessionSlot,
        system_prompt: Option<SystemPromptProvider>,
        handler: impl Fn(ModelRequest, AbortSignal, usize) -> ModelStream + Send + Sync + 'static,
    ) -> Self {
        Self::with_options(slot, system_prompt, Vec::new(), handler).await
    }
    pub async fn with_tools(
        slot: &SessionSlot,
        tools: Vec<Arc<dyn Tool>>,
        handler: impl Fn(ModelRequest, AbortSignal, usize) -> ModelStream + Send + Sync + 'static,
    ) -> Self {
        Self::with_options(slot, None, tools, handler).await
    }
    async fn with_options(
        slot: &SessionSlot,
        system_prompt: Option<SystemPromptProvider>,
        tools: Vec<Arc<dyn Tool>>,
        handler: impl Fn(ModelRequest, AbortSignal, usize) -> ModelStream + Send + Sync + 'static,
    ) -> Self {
        let home = Home::new();
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut options = agent_options(process.clone());
        options.provider = Arc::new(Provider {
            info: ProviderInfo {
                name: "goal-stream-lifetime".into(),
                default_model: "goal-stream-lifetime".into(),
                base_url: None,
                env_key: None,
            },
            calls: AtomicUsize::new(0),
            requests: requests.clone(),
            handler: Arc::new(handler),
        });
        options.tools = Some(tools);
        options.include_default_agents = Some(false);
        options.system_prompt_provider = system_prompt;
        options.compactor = Some(Arc::new(NoopCompactor));
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(home.path.join("sessions").to_string_lossy().into_owned()),
            archive_dir: None,
        }));
        let session = Agent::new(options)
            .open_session(
                store,
                StartSessionOptions {
                    cwd: Some(home.path.to_string_lossy().into_owned().into()),
                    user_config_dir: Some(home.path.to_string_lossy().into_owned().into()),
                    config: Some(
                        json!({"model":"goal-stream-lifetime","prewarm":false,"goal":true,
                "autoMemory":false,"autoDream":false,"memoryRecall":false,"agentMemory":false,
                "memory":{"enabled":false},"idleMicrocompact":false,"earlyMicrocompact":false,
                "speculativeCompaction":false,"loopDetection":false,"maxRetries":0}),
                    ),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        *slot.lock().unwrap() = Arc::downgrade(&session);
        Self {
            session,
            requests,
            process,
            home,
        }
    }
    pub fn config_root(&self) -> &Path {
        &self.home.path
    }
    pub async fn finish(self) {
        self.session.shutdown_and_wait().await.unwrap();
        self.process.shutdown().await.unwrap();
        drop(self.home);
    }
}

pub(super) fn session_slot() -> SessionSlot {
    Arc::new(Mutex::new(Weak::new()))
}
pub(super) fn session(slot: &SessionSlot) -> Arc<Session> {
    slot.lock()
        .unwrap()
        .upgrade()
        .expect("fixture session remains alive")
}
pub(super) fn usage(input_tokens: u64, output_tokens: u64) -> Usage {
    Usage {
        input_tokens,
        output_tokens,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: None,
    }
}
pub(super) fn text_events(text: &str, usage: Usage) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::MessageStart {
            model: "goal-stream-lifetime".into(),
        },
        ModelStreamEvent::TextDelta { text: text.into() },
        ModelStreamEvent::MessageStop {
            stop_reason: ModelStopReason::EndTurn,
            usage,
        },
    ]
}
pub(super) async fn idle(session: &Arc<Session>) -> Turn {
    tokio::time::timeout(WAIT, session.run_goal_idle_streamed(TurnOptions::default()))
        .await
        .expect("idle admission must settle")
        .unwrap()
        .expect("the active goal admits one turn")
}
pub(super) async fn wait_for_idle(session: &Session) {
    let runtime = session.goal_runtime().unwrap();
    tokio::time::timeout(WAIT, runtime.wait_for_idle_cleanup())
        .await
        .expect("subscriber loss must settle cleanup")
        .unwrap();
    assert!(!runtime.turn_in_flight());
}
