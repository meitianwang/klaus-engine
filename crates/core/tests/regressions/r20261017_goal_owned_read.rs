//! Defect: a durable goal's prepared files could be unreadable under confinement.
//! Root cause: objective text named files without a typed, revocable capability.
//! Work item 20261017: exercise the persistent session and production Read handler.

use std::collections::VecDeque;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::NoopCompactor;
use klaus_agent_core::engine::session::{Input, Session, TurnOptions};
use klaus_agent_core::goal::owned_resources::{
    GoalOwnedResourceFile, GoalOwnedResources, GoalOwnedResourcesInput,
};
use klaus_agent_core::goal::service::HostGoalMutation;
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var, RuntimeProcessContext,
};
use klaus_agent_core::runtime_values::{RuntimePath, RuntimeString};
use klaus_agent_core::schemas::content::OutputBlock;
use klaus_agent_core::schemas::message::{Message, ToolResultMessage, Usage};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::tools::builtins::read::create_read_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::permission::{ApprovalHandler, ApprovalRequest, ApprovalResponse};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::{Notify, Semaphore};

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const DEADLINE: Duration = Duration::from_secs(20);
const PASTE: &str = "Complete pasted evidence, including its final sentinel: PASTE-END-341.";

struct Home {
    root: PathBuf,
    config: PathBuf,
    workspace: PathBuf,
    previous: Option<std::ffi::OsString>,
}
impl Home {
    fn new() -> Self {
        let root =
            std::env::temp_dir().join(format!("klaus-goal-read-session-{}", uuid::Uuid::new_v4()));
        let config = root.join("config");
        let workspace = root.join("workspace");
        std::fs::create_dir_all(&config).unwrap();
        std::fs::create_dir_all(&workspace).unwrap();
        let previous = std::env::var_os("KLAUS_CONFIG_DIR");
        set_process_environment_var("KLAUS_CONFIG_DIR", &config);
        Self {
            root,
            config,
            workspace,
            previous,
        }
    }
}
impl Drop for Home {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(previous) => set_process_environment_var("KLAUS_CONFIG_DIR", previous),
            None => remove_process_environment_var("KLAUS_CONFIG_DIR"),
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

enum Reply {
    Read(PathBuf),
    Text,
}
struct Provider {
    info: ProviderInfo,
    replies: Mutex<VecDeque<Reply>>,
    requests: Mutex<Vec<ModelRequest>>,
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
            vision: true,
            image_limits: None,
            max_media_blocks: None,
        }
    }
    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        let index = {
            let mut requests = self.requests.lock().unwrap();
            assert!(requests.len() < 32, "provider fixture must remain bounded");
            let index = requests.len();
            requests.push(request);
            index
        };
        let reply = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("an explicit provider reply is required");
        Box::pin(async_stream::stream! {
            yield ModelStreamEvent::MessageStart { model:"owned-goal-read".into() };
            let stop_reason = match reply {
                Reply::Read(path) => {
                    let id = RuntimeString::from(format!("owned-goal-read-{index}"));
                    yield ModelStreamEvent::ToolUseStart { id:id.clone(), name:"read".into(),
                        thought_signature:None, tool_kind:None, input_format:None };
                    yield ModelStreamEvent::ToolUseInputDelta { id:id.clone(),
                        partial_json:json!({"path":path}).to_string().into() };
                    yield ModelStreamEvent::ToolUseStop { id };
                    ModelStopReason::ToolUse
                }
                Reply::Text => {
                    yield ModelStreamEvent::TextDelta { text:"The requested read has settled.".into() };
                    ModelStopReason::EndTurn
                }
            };
            yield ModelStreamEvent::MessageStop { stop_reason,
                usage:Usage { input_tokens:4, output_tokens:2, cache_read_input_tokens:0,
                    cache_creation_input_tokens:0, total_tokens:None } };
        })
    }
}

#[derive(Default)]
struct Approval {
    requests: Mutex<Vec<ApprovalRequest>>,
    allow: bool,
    barrier: Option<(Arc<Notify>, Arc<Semaphore>)>,
}
#[async_trait::async_trait]
impl ApprovalHandler for Approval {
    async fn approve(&self, request: ApprovalRequest, _: AbortSignal) -> ApprovalResponse {
        self.requests.lock().unwrap().push(request);
        if let Some((entered, release)) = &self.barrier {
            entered.notify_one();
            tokio::time::timeout(DEADLINE, release.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
        }
        if self.allow {
            ApprovalResponse::Allow {
                updated_input: None,
                updated_permissions: None,
            }
        } else {
            ApprovalResponse::Deny {
                message: Some("The test owner rejected this read.".into()),
            }
        }
    }
}

struct Prepared {
    directory: PathBuf,
    input: GoalOwnedResourcesInput,
    body: String,
    image: Vec<u8>,
}
impl Prepared {
    fn new(home: &Home, thread: &str) -> Self {
        let preparation_id = uuid::Uuid::new_v4().to_string();
        let directory = home
            .config
            .join("goal-objectives")
            .join(format!("{:x}", Sha256::digest(thread.as_bytes())))
            .join(&preparation_id);
        std::fs::create_dir_all(&directory).unwrap();
        let body = (1..=160)
            .map(|line| {
                format!(
                    "Requirement {line:03}: 完整保留目标正文与全部验收条件 GOAL-LINE-{line:03}\n"
                )
            })
            .collect::<String>();
        assert!(body.chars().count() > 4000);
        let mut image = Cursor::new(Vec::new());
        image::DynamicImage::new_rgb8(2, 2)
            .write_to(&mut image, image::ImageFormat::Png)
            .unwrap();
        let image = image.into_inner();
        let mut files = Vec::new();
        for (name, bytes) in [
            ("objective.md", body.as_bytes()),
            ("paste-1.txt", PASTE.as_bytes()),
            ("image-1.png", image.as_slice()),
        ] {
            std::fs::write(directory.join(name), bytes).unwrap();
            files.push(GoalOwnedResourceFile {
                name: name.into(),
                size_bytes: bytes.len() as u64,
                sha256: format!("{:x}", Sha256::digest(bytes)),
            });
        }
        // Private preparation/admission metadata is not part of a Read grant.
        std::fs::write(
            directory.join("preparation.json"),
            b"PRIVATE-PREPARATION-SECRET",
        )
        .unwrap();
        Self {
            directory,
            input: GoalOwnedResourcesInput {
                preparation_id,
                files,
            },
            body,
            image,
        }
    }
    fn objective(&self) -> RuntimeString {
        format!(
            "Read the complete goal objective from this Klaus-owned file: {}",
            self.directory.join("objective.md").display()
        )
        .into()
    }
}

struct Fixture {
    home: Home,
    process: RuntimeProcessContext,
    agent: Agent,
    store: Arc<dyn SessionStore>,
    session: Arc<Session>,
    provider: Arc<Provider>,
    approval: Arc<Approval>,
    config: Value,
}
impl Fixture {
    async fn new(mut config: Value, approval: Arc<Approval>) -> Self {
        let home = Home::new();
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let provider = Arc::new(Provider {
            info: ProviderInfo {
                name: "owned-goal-read".into(),
                base_url: None,
                env_key: None,
                default_model: "owned-goal-read".into(),
            },
            replies: Mutex::new(VecDeque::new()),
            requests: Mutex::new(Vec::new()),
        });
        if config["taskResourceScope"].is_object() {
            config["taskResourceScope"]["readRoots"] =
                json!([std::fs::canonicalize(&home.workspace).unwrap()]);
            config["taskResourceScope"]["writeRoots"] = json!([]);
        }
        config["permissionMode"] = json!("default");
        config["model"] = json!("owned-goal-read");
        config["approval_policy"] = json!("on-request");
        config["sandbox_mode"] = json!("read-only");
        config["approvals_reviewer"] = json!("user");
        config["goal"] = json!(true);
        config["prewarm"] = json!(false);
        config["autoMemory"] = json!(false);
        config["agentMemory"] = json!(false);
        config["memory"] = json!({"enabled":false});
        config["loopDetection"] = json!(false);
        let mut options = agent_options(process.clone());
        options.provider = provider.clone();
        options.tools = Some(vec![create_read_tool()]);
        options.approval_handler = Some(approval.clone());
        options.include_default_agents = Some(false);
        options.compactor = Some(Arc::new(NoopCompactor));
        let agent = Agent::new(options);
        let store: Arc<dyn SessionStore> =
            Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
                dir: Some(home.config.join("sessions").to_string_lossy().into_owned()),
                archive_dir: None,
            }));
        let session = agent
            .open_session(
                store.clone(),
                StartSessionOptions {
                    cwd: Some(home.workspace.to_string_lossy().into_owned().into()),
                    user_config_dir: Some(home.config.to_string_lossy().into_owned().into()),
                    config: Some(config.clone()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        Self {
            home,
            process,
            agent,
            store,
            session,
            provider,
            approval,
            config,
        }
    }
    async fn prepare(&self, typed: bool) -> Prepared {
        let prepared = Prepared::new(&self.home, &self.session.id.try_to_utf8().unwrap());
        self.session
            .set_thread_goal_checked(HostGoalMutation {
                objective: Some(prepared.objective()),
                owned_resources: typed.then(|| prepared.input.clone()),
                ..Default::default()
            })
            .await
            .unwrap();
        prepared
    }
    async fn owned_resources(&self) -> Option<GoalOwnedResources> {
        let goal = self.session.thread_goal().await.unwrap().unwrap();
        let thread_id = self.session.id.try_to_utf8().unwrap();
        let manifest =
            klaus_agent_core::goal::store::get_goal_owned_resources(&thread_id, &goal.goal_id)
                .await
                .unwrap()?;
        Some(
            GoalOwnedResources::restore(&manifest, &self.home.config)
                .await
                .unwrap(),
        )
    }
    fn script(&self, replies: impl IntoIterator<Item = Reply>) {
        let mut queue = self.provider.replies.lock().unwrap();
        assert!(queue.is_empty());
        queue.extend(replies);
    }
    async fn run(&self, replies: impl IntoIterator<Item = Reply>) -> Vec<EngineEvent> {
        self.script(replies);
        tokio::time::timeout(
            DEADLINE,
            self.session
                .run_streamed(
                    Input::Text("Read only the specified prepared resource.".into()),
                    TurnOptions::default(),
                )
                .events
                .collect(),
        )
        .await
        .expect("the actual Read turn must settle")
    }
    async fn restart(&mut self) {
        let thread = self.session.id.try_to_utf8().unwrap();
        self.session.shutdown_and_wait().await.unwrap();
        self.session = self
            .agent
            .resume_session(
                self.store.clone(),
                &thread,
                StartSessionOptions {
                    user_config_dir: Some(self.home.config.to_string_lossy().into_owned().into()),
                    config: Some(self.config.clone()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
    async fn finish(self) {
        self.session.shutdown_and_wait().await.unwrap();
        self.process.shutdown().await.unwrap();
    }
}

fn results(session: &Session) -> Vec<ToolResultMessage> {
    session
        .history()
        .into_iter()
        .filter_map(|message| match message {
            Message::ToolResult(result) => Some(result),
            _ => None,
        })
        .collect()
}
fn text(result: &ToolResultMessage) -> String {
    result
        .content
        .iter()
        .filter_map(|block| match block {
            OutputBlock::Text(text) => Some(text.text.try_to_utf8().unwrap()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}
fn assert_completed(events: &[EngineEvent]) {
    assert!(
        events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })),
        "{events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
}
fn assert_no_resource_content(session: &Session) {
    for result in results(session) {
        let value = text(&result);
        assert!(
            !value.contains("GOAL-LINE-001")
                && !value.contains("PASTE-END-341")
                && !value.contains("PRIVATE-PREPARATION-SECRET"),
            "{value}"
        );
        assert!(!result
            .content
            .iter()
            .any(|block| matches!(block, OutputBlock::Image(_))));
    }
}

#[tokio::test]
async fn persistent_confined_session_reads_complete_text_paste_and_image_after_restart() {
    let mut fixture = Fixture::new(json!({}), Arc::new(Approval::default())).await;
    let prepared = fixture.prepare(true).await;
    fixture.restart().await;
    let resources = fixture.owned_resources().await.unwrap();
    for name in ["objective.md", "paste-1.txt", "image-1.png"] {
        let path = RuntimePath::from(prepared.directory.join(name).to_str().unwrap());
        assert!(resources.allows_read(&path).await);
        assert!(resources.snapshot_read(&path).await.unwrap().is_some());
    }
    let events = fixture
        .run([
            Reply::Read(prepared.directory.join("objective.md")),
            Reply::Read(prepared.directory.join("paste-1.txt")),
            Reply::Read(prepared.directory.join("image-1.png")),
            Reply::Text,
        ])
        .await;
    assert_completed(&events);
    let actual = results(&fixture.session);
    assert_eq!(actual.len(), 3);
    assert!(actual.iter().all(|result| !result.is_error), "{actual:?}");
    let objective = text(&actual[0]);
    for line in prepared.body.lines() {
        assert!(objective.contains(line), "missing complete line {line}");
    }
    assert!(text(&actual[1]).contains(PASTE));
    let image = actual[2]
        .content
        .iter()
        .find_map(|block| match block {
            OutputBlock::Image(image) => Some(image),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(image.base64.try_to_utf8().unwrap())
            .unwrap(),
        prepared.image
    );
    assert!(fixture.approval.requests.lock().unwrap().is_empty());
    fixture.finish().await;
}

#[tokio::test]
async fn task_resource_roots_block_existing_owned_goal_files_and_disable_pursuit() {
    let mut fixture = Fixture::new(json!({}), Arc::new(Approval::default())).await;
    let prepared = fixture.prepare(true).await;
    let original = fixture.session.thread_goal().await.unwrap().unwrap();
    fixture.config["taskResourceScope"] = json!({
        "taskId":"owned-read-task", "mode":"scoped-auto", "networkAllowed":false,
        "readRoots":[std::fs::canonicalize(&fixture.home.workspace).unwrap()], "writeRoots":[]
    });
    fixture.restart().await;
    assert!(!fixture.session.goal_pursuit_eligible());
    assert!(!fixture.session.goal_enabled());
    assert!(fixture.session.goal_runtime().is_none());
    assert!(fixture.session.set_goal_enabled(true).await.is_err());
    assert!(
        fixture
            .session
            .set_thread_goal_checked(HostGoalMutation {
                objective: Some(prepared.objective()),
                owned_resources: Some(prepared.input.clone()),
                ..Default::default()
            })
            .await
            .is_err(),
        "a bounded session cannot create, edit, or regrant goal resources"
    );
    assert!(fixture
        .session
        .set_thread_goal(Some("Unauthorized replacement".into()), None, None)
        .await
        .is_err());
    let retained = fixture.session.thread_goal().await.unwrap().unwrap();
    assert_eq!(retained.goal_id, original.goal_id);
    assert_eq!(retained.objective, original.objective);
    assert!(fixture
        .session
        .run_goal_idle_streamed(TurnOptions::default())
        .await
        .unwrap()
        .is_none());
    assert!(fixture.provider.requests.lock().unwrap().is_empty());
    assert_completed(
        &fixture
            .run([
                Reply::Read(prepared.directory.join("objective.md")),
                Reply::Read(prepared.directory.join("image-1.png")),
                Reply::Read(prepared.directory.join("preparation.json")),
                Reply::Text,
            ])
            .await,
    );
    let actual = results(&fixture.session);
    assert_eq!(actual.len(), 3);
    assert!(
        actual.iter().all(|result| result.is_error),
        "all prepared files remain outside the hard task roots: {actual:?}"
    );
    assert_no_resource_content(&fixture.session);
    assert!(
        fixture.approval.requests.lock().unwrap().is_empty(),
        "task root denials cannot become approval prompts"
    );
    assert_eq!(
        fixture
            .session
            .thread_goal()
            .await
            .unwrap()
            .unwrap()
            .tokens_used,
        original.tokens_used
    );
    fixture
        .session
        .set_thread_goal(None, Some("paused".into()), None)
        .await
        .unwrap();
    assert!(fixture.session.clear_thread_goal().await.unwrap());
    assert!(fixture.session.thread_goal().await.unwrap().is_none());
    fixture.finish().await;
}

#[tokio::test]
async fn explicit_deny_and_explicit_ask_precede_the_typed_read_capability() {
    for (key, expected_approvals) in [("deny", 0), ("ask", 1)] {
        let mut config = json!({});
        config[key] = json!(["Read"]);
        let approval = Arc::new(Approval::default());
        let fixture = Fixture::new(config, approval.clone()).await;
        let prepared = fixture.prepare(true).await;
        fixture
            .run([
                Reply::Read(prepared.directory.join("objective.md")),
                Reply::Text,
            ])
            .await;
        assert_eq!(approval.requests.lock().unwrap().len(), expected_approvals);
        assert!(results(&fixture.session)
            .iter()
            .all(|result| result.is_error));
        assert_no_resource_content(&fixture.session);
        fixture.finish().await;
    }
}

#[tokio::test]
async fn reference_text_and_unlisted_metadata_do_not_mint_resources_or_override_rules() {
    let fixture = Fixture::new(json!({"ask":["Read"]}), Arc::new(Approval::default())).await;
    let prepared = fixture.prepare(false).await;
    assert!(fixture.owned_resources().await.is_none());
    fixture
        .run([
            Reply::Read(prepared.directory.join("objective.md")),
            Reply::Text,
        ])
        .await;
    assert_eq!(fixture.approval.requests.lock().unwrap().len(), 1);
    assert_no_resource_content(&fixture.session);
    fixture.finish().await;
    let fixture = Fixture::new(json!({"ask":["Read"]}), Arc::new(Approval::default())).await;
    let prepared = fixture.prepare(true).await;
    let resources = fixture.owned_resources().await.unwrap();
    let metadata = RuntimePath::from(
        prepared
            .directory
            .join("preparation.json")
            .to_str()
            .unwrap(),
    );
    assert!(!resources.allows_read(&metadata).await);
    assert!(resources.snapshot_read(&metadata).await.unwrap().is_none());
    fixture
        .run([
            Reply::Read(prepared.directory.join("preparation.json")),
            Reply::Text,
        ])
        .await;
    assert_eq!(fixture.approval.requests.lock().unwrap().len(), 1);
    assert_no_resource_content(&fixture.session);
    fixture.finish().await;
}

#[cfg(unix)]
#[tokio::test]
async fn an_alias_to_a_granted_file_does_not_receive_the_capability() {
    let fixture = Fixture::new(json!({"ask":["Read"]}), Arc::new(Approval::default())).await;
    let prepared = fixture.prepare(true).await;
    let alias = fixture.home.config.join("alias.md");
    std::os::unix::fs::symlink(prepared.directory.join("objective.md"), &alias).unwrap();
    let resources = fixture.owned_resources().await.unwrap();
    let path = RuntimePath::from(alias.to_str().unwrap());
    assert!(!resources.allows_read(&path).await);
    assert!(resources.snapshot_read(&path).await.unwrap().is_none());
    fixture.run([Reply::Read(alias), Reply::Text]).await;
    assert_eq!(fixture.approval.requests.lock().unwrap().len(), 1);
    assert_no_resource_content(&fixture.session);
    fixture.finish().await;
}

#[tokio::test]
async fn captured_read_is_revoked_by_clear_or_same_generation_objective_edit_during_approval() {
    for clear in [true, false] {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Semaphore::new(0));
        let approval = Arc::new(Approval {
            allow: true,
            barrier: Some((entered.clone(), release.clone())),
            ..Default::default()
        });
        let fixture = Fixture::new(json!({"ask":["Read"]}), approval).await;
        let prepared = fixture.prepare(true).await;
        fixture.script([
            Reply::Read(prepared.directory.join("objective.md")),
            Reply::Text,
        ]);
        let events = fixture
            .session
            .run_streamed(
                Input::Text("Read the original prepared objective.".into()),
                TurnOptions::default(),
            )
            .events;
        let consumer = tokio::spawn(async move { events.collect::<Vec<_>>().await });
        tokio::time::timeout(DEADLINE, entered.notified())
            .await
            .expect("the actual Read approval must be reached");
        if clear {
            fixture.session.clear_thread_goal().await.unwrap();
        } else {
            fixture
                .session
                .set_thread_goal(
                    Some("A new objective with no prepared resources.".into()),
                    None,
                    None,
                )
                .await
                .unwrap();
        }
        release.add_permits(1);
        let events = tokio::time::timeout(DEADLINE, consumer)
            .await
            .unwrap()
            .unwrap();
        assert_no_resource_content(&fixture.session);
        assert!(
            events
                .iter()
                .any(|event| matches!(event, EngineEvent::TurnFailed { .. }))
                || results(&fixture.session)
                    .iter()
                    .any(|result| result.is_error),
            "{events:?}"
        );
        fixture.finish().await;
    }
}

#[tokio::test]
async fn a_changed_file_after_permission_does_not_fall_back_to_an_original_path_read() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let approval = Arc::new(Approval {
        allow: true,
        barrier: Some((entered.clone(), release.clone())),
        ..Default::default()
    });
    let fixture = Fixture::new(json!({"ask":["Read"]}), approval).await;
    let prepared = fixture.prepare(true).await;
    fixture.script([
        Reply::Read(prepared.directory.join("objective.md")),
        Reply::Text,
    ]);
    let events = fixture
        .session
        .run_streamed(
            Input::Text("Read the prepared objective.".into()),
            TurnOptions::default(),
        )
        .events;
    let consumer = tokio::spawn(async move { events.collect::<Vec<_>>().await });
    tokio::time::timeout(DEADLINE, entered.notified())
        .await
        .expect("the actual Read approval must be reached");
    std::fs::write(
        prepared.directory.join("objective.md"),
        b"UNTRUSTED-REPLACEMENT-MUST-NOT-REACH-MODEL",
    )
    .unwrap();
    release.add_permits(1);
    tokio::time::timeout(DEADLINE, consumer)
        .await
        .unwrap()
        .unwrap();
    let actual = results(&fixture.session);
    assert!(!actual.is_empty());
    assert!(actual.iter().all(|result| result.is_error));
    assert!(actual
        .iter()
        .all(|result| !text(result).contains("UNTRUSTED-REPLACEMENT-MUST-NOT-REACH-MODEL")));
    assert_no_resource_content(&fixture.session);
    fixture.finish().await;
}
