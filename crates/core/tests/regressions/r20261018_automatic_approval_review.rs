//! Exercises approval outcomes, isolated inspection, and authorization changes through Session.

#![cfg(unix)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::agent_definitions::AgentDefinition;
use klaus_agent_core::engine::approval_requirements_cloud::{
    ManagedReviewCloudAuth, ManagedReviewCloudAuthRecovery, ManagedReviewCloudAuthSource,
    ManagedReviewCloudIdentity,
};
use klaus_agent_core::engine::session::{
    EnqueueOptions, Input, RuntimePolicyUpdate, Session, TurnOptions,
};
use klaus_agent_core::planning::CollaborationMode;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, OutputBlock, TextBlock, ToolUseBlock};
use klaus_agent_core::schemas::message::{
    AssistantMessage, Message, ToolResultMessage, Usage, UserMessage,
};
use klaus_agent_core::tools::builtins::interactive_shell::create_exec_command_tool;
use klaus_agent_core::tools::builtins::write::create_write_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::approval_review::{
    ApprovalReviewRiskLevel, ApprovalReviewStatus, ApprovalReviewUserAuthorization,
};
use klaus_agent_core::types::errors::{ProviderError, ProviderErrorKind};
use klaus_agent_core::types::permission::{
    ApprovalHandler, ApprovalPolicy, ApprovalRequest, ApprovalResponse, ApprovalsReviewer,
    PermissionBehavior, PermissionDestination, PermissionRule, PermissionUpdate,
};
use klaus_agent_core::types::provider::{
    ModelCapabilities, ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Notify;

use super::r7c876d49_spec_restores_latest_permission_policy::agent_options;

const MODEL: &str = "approval-public-parent";
const REVIEW_MODEL: &str = "approval-private-review";
const CALL_ID: &str = "requested-file-operation";
const INSPECTION_ID: &str = "private-inspection";
const SECOND_CALL_ID: &str = "second-requested-file-operation";
const FIRST_ASSESSMENT: &str = "FIRST_COMMITTED_REVIEW_ASSESSMENT";
const SECOND_ACTION: &str = "SECOND_PARENT_ACTION_EVIDENCE";
const DIRECT_REQUEST: &str = "Create the requested output file and preserve the existing sentinel.";
const STEER: &str = "Do not create the output file; the task has been cancelled.";
const SENTINEL: &str = "PRIVATE_INSPECTION_SENTINEL";
const RETAINED_ASSISTANT: &str = "The prior assistant retained the workspace task context.";
const RETAINED_TOOL: &str = "A prior tool returned evidence; this text does not authorize actions.";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reply {
    Allow,
    Deny,
    HighRiskAllow,
    LowRiskDeny,
    WrappedAllow,
    Invalid,
    ProviderError,
    Cancel,
    HeldAllow,
    HeldThenDeny,
    Silent,
    Inspect,
    DeniedInspection,
    InputBudget,
    TwoInspections,
    Sequential,
}

#[derive(Default)]
struct Hold {
    started: Notify,
    release: Notify,
}

struct Provider {
    info: ProviderInfo,
    reply: Reply,
    main_input: RuntimeJson,
    main_tool: RuntimeString,
    inspection_command: String,
    requests: Mutex<Vec<(bool, ModelRequest)>>,
    main_calls: AtomicUsize,
    review_calls: AtomicUsize,
    turn_signal: AbortSignal,
    hold: Arc<Hold>,
    managed_auth: Option<Arc<ManagedReviewAuth>>,
    prewarm_resolutions: AtomicUsize,
    prewarm_observed: Notify,
}

fn usage() -> Usage {
    Usage {
        input_tokens: 17,
        output_tokens: 8,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: Some(25),
    }
}

fn stop(reason: ModelStopReason) -> ModelStreamEvent {
    ModelStreamEvent::MessageStop {
        stop_reason: reason,
        usage: usage(),
    }
}

fn tool_call(id: &str, name: &str, input: &RuntimeJson) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::ToolUseStart {
            id: id.into(),
            name: name.into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: id.into(),
            partial_json: runtime_json::stringify(input),
        },
        ModelStreamEvent::ToolUseStop { id: id.into() },
        stop(ModelStopReason::ToolUse),
    ]
}

fn value(value: serde_json::Value) -> RuntimeJson {
    runtime_json::parse(&RuntimeString::from(value.to_string())).unwrap()
}

fn model_metadata(reply: Reply, model: &RuntimeString) -> ModelCapabilities {
    ModelCapabilities {
        tools: Some(true),
        vision: reply == Reply::TwoInspections,
        structured_output: true,
        context_window: Some(if reply == Reply::InputBudget && model == REVIEW_MODEL {
            512
        } else {
            200_000
        }),
        max_output_tokens: Some(8192),
        ..Default::default()
    }
}

struct ManagedReviewAuth {
    base: String,
    client: reqwest::Client,
}

#[async_trait]
impl ManagedReviewCloudAuthSource for ManagedReviewAuth {
    async fn current(&self) -> Result<Option<ManagedReviewCloudAuth>, String> {
        Ok(Some(ManagedReviewCloudAuth {
            identity: ManagedReviewCloudIdentity {
                chatgpt_user_id: Some("approval-test-user".into()),
                account_id: Some("approval-test-account".into()),
                plan_type: Some("business".into()),
                backend_base_url: self.base.clone(),
                owner_generation: 1,
            },
            headers: std::collections::HashMap::from([(
                "Authorization".into(),
                "Bearer managed-review-fixture".into(),
            )]),
            client: self.client.clone(),
        }))
    }

    async fn recover_unauthorized(
        &self,
        _step: usize,
        _owner: &ManagedReviewCloudIdentity,
    ) -> ManagedReviewCloudAuthRecovery {
        ManagedReviewCloudAuthRecovery::Unavailable
    }
}

struct ManagedReviewPolicy {
    auth: Arc<ManagedReviewAuth>,
    served: tokio::task::JoinHandle<()>,
}

impl ManagedReviewPolicy {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let served = tokio::spawn(async move {
            let (mut stream, _) = tokio::time::timeout(Duration::from_secs(10), listener.accept())
                .await
                .expect("managed requirements must use the authenticated provider route")
                .unwrap();
            let mut request = Vec::new();
            loop {
                let mut bytes = [0; 1024];
                let count = stream.read(&mut bytes).await.unwrap();
                assert!(count > 0);
                assert!(
                    request.len() + count <= 8192,
                    "the managed request must fit its header budget"
                );
                request.extend_from_slice(&bytes[..count]);
                if request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                    break;
                }
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("GET /api/codex/config/bundle HTTP/1.1"));
            assert!(request
                .to_ascii_lowercase()
                .contains("authorization: bearer managed-review-fixture"));
            let body = json!({
                "requirements_toml": {
                    "enterprise_managed": [{
                        "id": "approval-model-policy",
                        "name": "Administrator",
                        "contents": format!("[auto_review]\nrequired_on_models = ['{MODEL}']"),
                    }],
                },
            })
            .to_string();
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(head.as_bytes()).await.unwrap();
            stream.write_all(body.as_bytes()).await.unwrap();
        });
        Self {
            auth: Arc::new(ManagedReviewAuth {
                base,
                client: reqwest::Client::builder().no_proxy().build().unwrap(),
            }),
            served,
        }
    }
}

impl ModelProvider for Provider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: true,
            web_search: false,
            prompt_cache_control: false,
            context_window: model_metadata(self.reply, model).context_window.unwrap(),
            max_output_tokens: 8192,
            native_pdf: false,
            vision: self.reply == Reply::TwoInspections,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn model_metadata(&self, model: &RuntimeString) -> ModelCapabilities {
        model_metadata(self.reply, model)
    }

    fn managed_review_cloud_auth_source(&self) -> Option<Arc<dyn ManagedReviewCloudAuthSource>> {
        self.managed_auth
            .as_ref()
            .map(|auth| auth.clone() as Arc<dyn ManagedReviewCloudAuthSource>)
    }

    fn approval_review_preferred_model(&self) -> RuntimeString {
        if self.reply == Reply::Sequential
            && self.prewarm_resolutions.fetch_add(1, Ordering::SeqCst) >= 1
        {
            self.prewarm_observed.notify_one();
        }
        REVIEW_MODEL.into()
    }

    fn offline_model_catalog(&self) -> Vec<(RuntimeString, ModelCapabilities)> {
        vec![(
            REVIEW_MODEL.into(),
            self.model_metadata(&REVIEW_MODEL.into()),
        )]
    }

    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        let review = request
            .system
            .iter()
            .any(|part| part.name == "approval-review-policy");
        {
            let mut requests = self.requests.lock().unwrap();
            assert!(
                requests.len() < 32,
                "the fixture inference script is finite"
            );
            requests.push((review, request.clone()));
        }
        let mut events = vec![ModelStreamEvent::MessageStart {
            model: request.model.clone(),
        }];
        if !review {
            let index = self.main_calls.fetch_add(1, Ordering::SeqCst);
            if self.reply == Reply::Sequential && index == 0 {
                let hold = self.hold.clone();
                let action = tool_call(
                    CALL_ID,
                    &self.main_tool.try_to_utf8().unwrap(),
                    &self.main_input,
                );
                return Box::pin(async_stream::stream! {
                    yield ModelStreamEvent::MessageStart { model: MODEL.into() };
                    hold.started.notify_one();
                    hold.release.notified().await;
                    for event in action {
                        yield event;
                    }
                });
            }
            if index == 0 {
                events.extend(tool_call(
                    CALL_ID,
                    &self.main_tool.try_to_utf8().unwrap(),
                    &self.main_input,
                ));
            } else if self.reply == Reply::Sequential && index == 1 {
                let mut input = self.main_input.clone();
                let fields = input.as_object_mut().unwrap();
                let path = fields
                    .get("path")
                    .and_then(RuntimeJson::as_string)
                    .unwrap()
                    .concat(&RuntimeString::from(".second"));
                fields.insert("path", RuntimeJson::String(path));
                fields.insert("content", RuntimeJson::from(SECOND_ACTION));
                events.extend(tool_call(SECOND_CALL_ID, "write", &input));
            } else {
                events.extend([
                    ModelStreamEvent::TextDelta {
                        text: "The actual operation result has been received.".into(),
                    },
                    stop(ModelStopReason::EndTurn),
                ]);
            }
            return Box::pin(futures::stream::iter(events));
        }

        assert_eq!(request.model, REVIEW_MODEL);
        let index = self.review_calls.fetch_add(1, Ordering::SeqCst);
        if matches!(self.reply, Reply::HeldAllow | Reply::HeldThenDeny) && index == 0 {
            let hold = self.hold.clone();
            return Box::pin(async_stream::stream! {
                yield ModelStreamEvent::MessageStart { model: REVIEW_MODEL.into() };
                hold.started.notify_one();
                tokio::select! {
                    () = hold.release.notified() => {
                        yield ModelStreamEvent::TextDelta { text: r#"{"outcome":"allow"}"#.into() };
                        yield stop(ModelStopReason::EndTurn);
                    }
                    () = signal.cancelled() => {}
                }
            });
        }
        match self.reply {
            Reply::Cancel => {
                self.turn_signal.cancel();
                return Box::pin(futures::stream::pending());
            }
            Reply::Silent => {
                let hold = self.hold.clone();
                return Box::pin(async_stream::stream! {
                    yield ModelStreamEvent::MessageStart { model: REVIEW_MODEL.into() };
                    hold.started.notify_one();
                    signal.cancelled().await;
                });
            }
            Reply::ProviderError => events.push(ModelStreamEvent::Error {
                error: ProviderError {
                    kind: ProviderErrorKind::InvalidRequest,
                    message: "The review transport rejected this request.".into(),
                    status: Some(400),
                    code: None,
                    error_type: None,
                    media_origins: None,
                    media_repair_complete: false,
                    credential_recovery: None,
                    retry_after_ms: None,
                    request_id: None,
                },
            }),
            Reply::TwoInspections if index < 2 => {
                events.extend(tool_call(
                    &format!("private-inspection-{index}"),
                    "exec_command",
                    &value(json!({"command": self.inspection_command,"yieldMs":1000})),
                ));
            }
            Reply::Inspect | Reply::DeniedInspection | Reply::Sequential if index == 0 => {
                events.extend(tool_call(
                    INSPECTION_ID,
                    "exec_command",
                    &value(json!({
                        "command": self.inspection_command,
                        "yieldMs": 1000,
                    })),
                ));
            }
            reply => {
                let output = match reply {
                    Reply::Sequential if index == 1 => {
                        r#"{"outcome":"allow","rationale":"FIRST_COMMITTED_REVIEW_ASSESSMENT"}"#
                    }
                    Reply::Deny | Reply::HeldThenDeny => r#"{"outcome":"deny"}"#,
                    Reply::HighRiskAllow => r#"{"outcome":"allow","risk_level":"critical","user_authorization":"low","rationale":"The exact requested action may proceed."}"#,
                    Reply::LowRiskDeny => r#"{"outcome":"deny","risk_level":"low","user_authorization":"high","rationale":"Do not execute the proposed action."}"#,
                    Reply::WrappedAllow => "Assessment follows.\n{\"outcome\":\"allow\",\"extra_field\":true}\nEnd of assessment.",
                    Reply::Invalid => "This response has no assessment object.",
                    _ => r#"{"outcome":"allow"}"#,
                };
                events.push(ModelStreamEvent::TextDelta {
                    text: output.into(),
                });
                events.push(stop(ModelStopReason::EndTurn));
            }
        }
        Box::pin(futures::stream::iter(events))
    }
}

#[derive(Default)]
struct Human {
    calls: AtomicUsize,
}

#[async_trait]
impl ApprovalHandler for Human {
    async fn approve(&self, _: ApprovalRequest, signal: AbortSignal) -> ApprovalResponse {
        assert!(!signal.is_cancelled());
        self.calls.fetch_add(1, Ordering::SeqCst);
        ApprovalResponse::Allow {
            updated_input: None,
            updated_permissions: None,
        }
    }
}

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("klaus-approval-session-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(path.join("workspace")).unwrap();
        std::fs::create_dir_all(path.join("outside")).unwrap();
        Self(path.canonicalize().unwrap())
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn text(value: &str) -> TextBlock {
    TextBlock {
        text: value.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    }
}

fn initial_history(many_users: bool) -> Vec<Message> {
    let mut history = vec![
        Message::Assistant(AssistantMessage {
            uuid: "prior-assistant".into(),
            model: MODEL.into(),
            usage: None,
            content: vec![
                ContentBlock::Text(text(RETAINED_ASSISTANT)),
                ContentBlock::ToolUse(ToolUseBlock {
                    id: "prior-tool".into(),
                    name: "exec_command".into(),
                    input: value(json!({"command":"printf prior-evidence"})),
                    thought_signature: None,
                    tool_kind: None,
                }),
            ],
        }),
        Message::ToolResult(ToolResultMessage {
            uuid: "prior-tool-result".into(),
            tool_use_id: "prior-tool".into(),
            content: vec![OutputBlock::Text(text(RETAINED_TOOL))],
            is_error: false,
            tool_kind: None,
            native_tool_output: None,
            output: None,
            extra_user_blocks: None,
        }),
    ];
    if many_users {
        for index in 0..20 {
            history.push(Message::User(UserMessage {
                uuid: format!("prior-user-{index}").into(),
                content: vec![ContentBlock::Text(text(&format!(
                    "USER_CONTEXT_{index:02}: preserve the sentinel."
                )))],
                is_meta: None,
                question_answer: None,
            }));
        }
    }
    history
}

struct RecordingBrowser(Arc<AtomicUsize>);

#[async_trait]
unsafe impl klaus_agent_core::browser::BrowserExecutor for RecordingBrowser {
    async fn execute(
        &self,
        _request: klaus_agent_core::browser::BrowserRuntimeRequest,
        _signal: AbortSignal,
        _emit: klaus_agent_core::browser::BrowserEventSink,
    ) -> Result<
        klaus_agent_core::browser::BrowserRuntimeResponse,
        klaus_agent_core::browser::BrowserRuntimeError,
    > {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Default::default())
    }
}

struct Rig {
    directory: Directory,
    browser_calls: Arc<AtomicUsize>,
    process: RuntimeProcessContext,
    provider: Arc<Provider>,
    human: Arc<Human>,
    session: Arc<Session>,
    signal: AbortSignal,
    content: String,
}

impl Rig {
    async fn new(reply: Reply) -> Self {
        Self::with_options(reply, false, false, false).await
    }

    async fn with_options(
        reply: Reply,
        shell: bool,
        many_users: bool,
        required_review: bool,
    ) -> Self {
        Self::with_browser_options(reply, shell, many_users, required_review, None).await
    }

    async fn with_browser_options(
        reply: Reply,
        shell: bool,
        many_users: bool,
        required_review: bool,
        browser: Option<(&str, bool)>,
    ) -> Self {
        let directory = Directory::new();
        std::fs::write(directory.0.join("workspace/sentinel.txt"), SENTINEL).unwrap();
        let process = RuntimeProcessContext::initialize(
            klaus_agent_core::testing::shared_test_process_supervisor(),
        )
        .unwrap();
        let signal = AbortSignal::new();
        let content = if many_users {
            "x".repeat(40_000)
        } else {
            "approved".to_string()
        };
        let output = directory.0.join("outside/result.txt");
        let restricted_parent = reply == Reply::TwoInspections;
        let main_input = if let Some((tool, _)) = browser {
            if tool == "browser_select_browser" {
                value(json!({"deviceId":"chrome:fixture"}))
            } else {
                value(json!({"browserId":"chrome:fixture","tabId":1}))
            }
        } else if shell {
            value(json!({
                "command":format!("printf approved > '{}'", output.display()),
                "yieldMs":1000,
                "sandboxPermissions":"require_escalated",
                "justification":"Create the output file explicitly requested by the user."
            }))
        } else {
            value(json!({"path": output.to_string_lossy(), "content":content}))
        };
        let inspection_command = if restricted_parent {
            format!(
                "cat '{}'",
                directory.0.join("workspace/sentinel.txt").display()
            )
        } else {
            format!(
                "cat '{}'; printf forbidden > '{}'",
                directory.0.join("workspace/sentinel.txt").display(),
                directory.0.join("workspace/private-write.txt").display(),
            )
        };
        let managed_policy = if required_review {
            Some(ManagedReviewPolicy::start().await)
        } else {
            None
        };
        let provider = Arc::new(Provider {
            info: ProviderInfo {
                name: MODEL.into(),
                base_url: None,
                env_key: None,
                default_model: MODEL.into(),
            },
            reply,
            main_input,
            main_tool: browser
                .map(|(tool, _)| tool)
                .unwrap_or(if shell { "exec_command" } else { "write" })
                .into(),
            inspection_command,
            requests: Mutex::new(Vec::new()),
            main_calls: AtomicUsize::new(0),
            review_calls: AtomicUsize::new(0),
            turn_signal: signal.clone(),
            hold: Arc::new(Hold::default()),
            managed_auth: managed_policy.as_ref().map(|policy| policy.auth.clone()),
            prewarm_resolutions: AtomicUsize::new(0),
            prewarm_observed: Notify::new(),
        });
        let human = Arc::new(Human::default());
        let mut options = agent_options(process.clone());
        options.provider = provider.clone();
        options.tools = Some(if browser.is_some() {
            klaus_agent_core::tools::builtins::browser::create_browser_tools()
        } else {
            vec![create_write_tool(), create_exec_command_tool()]
        });
        let browser_calls = Arc::new(AtomicUsize::new(0));
        if browser.is_some() {
            // SAFETY: the fixture only records browser calls and cannot spawn processes.
            options.browser_runtime = Some(unsafe {
                klaus_agent_core::browser::attest_spawn_free(Arc::new(RecordingBrowser(
                    browser_calls.clone(),
                )))
            });
        }
        options.approval_handler = Some(human.clone());
        options.include_default_agents = Some(false);
        options.cwd = Some(directory.0.join("workspace").to_string_lossy().into_owned());
        options.config = Some(json!({
            "model":MODEL,"permissionMode":"default", "approvalPolicy":if browser.is_some() { "never" } else { "on-request" },
            "sandboxMode":if browser.is_some_and(|(_, full)| full) { "danger-full-access" } else { "workspace-write" },
            "approvalsReviewer":if required_review { "user" } else { "automatic" },
            "sandbox_workspace_write":{"exclude_tmpdir_env_var":true,"exclude_slash_tmp":true},
            "sandbox":"auto","requireRealSandbox":true,"sandboxNetworkAllowed":false,"sandboxRetryOnDenial":true,
            "maxOutputTokens":4096,"useStoredPermissionRules":false,
            "prewarm":false,"goal":false,"autoMemory":false,"agentMemory":false,"autoDream":false,
            "memoryRecall":false,"memory":{"enabled":false},"loopDetection":false
        }));
        let session = Agent::new(options)
            .try_start_session_async(StartSessionOptions {
                agent_definition_name: restricted_parent.then(|| "restricted-writer".into()),
                agents: restricted_parent.then(|| {
                    vec![AgentDefinition {
                        name: "restricted-writer".into(),
                        tools: Some(vec!["write".into()]),
                        max_turns: Some(1),
                        ..Default::default()
                    }]
                }),
                initial_history: Some(initial_history(many_users)),
                user_config_dir: Some(
                    directory
                        .0
                        .join("config")
                        .to_string_lossy()
                        .into_owned()
                        .into(),
                ),
                ..Default::default()
            })
            .await
            .expect("the authenticated requirements must be admitted before session startup");
        if let Some(policy) = managed_policy {
            tokio::time::timeout(Duration::from_secs(5), policy.served)
                .await
                .expect("the managed requirements response must finish")
                .unwrap();
        }
        assert_eq!(
            session.runtime_policy_state().approvals_reviewer,
            Some(ApprovalsReviewer::Automatic),
            "the model requirement must replace the user reviewer at startup"
        );
        Self {
            directory,
            browser_calls,
            process,
            provider,
            human,
            session,
            signal,
            content,
        }
    }

    async fn run(&self) {
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            self.session.run(
                Input::Text(DIRECT_REQUEST.into()),
                TurnOptions {
                    signal: Some(self.signal.clone()),
                    ..Default::default()
                },
            ),
        )
        .await
        .expect("the scripted public turn settles");
        if result.error.is_some() || self.provider.review_calls.load(Ordering::SeqCst) == 0 {
            let diagnostic = format!(
                "turn_error={:?}; policy={:?}; main_calls={}; history={:?}; reviews={:?}",
                result.error,
                self.session.runtime_policy_state(),
                self.provider.main_calls.load(Ordering::SeqCst),
                self.session.history(),
                self.session.approval_review_history()
            );
            eprintln!("{}", diagnostic.chars().take(8192).collect::<String>());
        }
    }

    fn spawn_turn(&self) -> tokio::task::JoinHandle<()> {
        let session = self.session.clone();
        let signal = self.signal.clone();
        tokio::spawn(async move {
            session
                .run(
                    Input::Text(DIRECT_REQUEST.into()),
                    TurnOptions {
                        signal: Some(signal),
                        ..Default::default()
                    },
                )
                .await;
        })
    }

    async fn wait_for_review(&self) {
        tokio::time::timeout(
            Duration::from_secs(10),
            self.provider.hold.started.notified(),
        )
        .await
        .expect("the actual private review has started");
    }

    fn assert_output(&self, expected: bool) {
        let path = self.directory.0.join("outside/result.txt");
        if expected {
            assert_eq!(std::fs::read_to_string(path).unwrap(), self.content);
        } else {
            assert!(
                !path.exists(),
                "the pending parent operation must not execute"
            );
        }
        assert_eq!(
            std::fs::read_to_string(self.directory.0.join("workspace/sentinel.txt")).unwrap(),
            SENTINEL
        );
        assert!(
            !self
                .directory
                .0
                .join("workspace/private-write.txt")
                .exists(),
            "the reviewer must remain read-only"
        );
    }

    fn assert_review(&self, count: usize, status: ApprovalReviewStatus, humans: usize) {
        assert_eq!(self.provider.review_calls.load(Ordering::SeqCst), count);
        assert_eq!(self.human.calls.load(Ordering::SeqCst), humans);
        let events = self.session.approval_review_history();
        assert_eq!(events.last().map(|event| event.status), Some(status));
        assert!(events.last().unwrap().completed_at_ms.is_some());
    }

    fn reviews(&self) -> Vec<ModelRequest> {
        self.provider
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(review, _)| *review)
            .map(|(_, request)| request.clone())
            .collect()
    }

    async fn close(self) {
        self.session.shutdown_and_wait().await.unwrap();
        self.process.shutdown().await.unwrap();
    }
}

pub(super) struct ManagedBrowserOutcome {
    pub policy: klaus_agent_core::engine::session::RuntimePolicyState,
    pub executions: usize,
    pub human_prompts: usize,
    pub reviews: usize,
    pub status: Option<ApprovalReviewStatus>,
    pub tool_error: bool,
}

pub(super) async fn managed_browser_outcome(
    tool: &str,
    full_access: bool,
    deny: bool,
) -> ManagedBrowserOutcome {
    let rig = Rig::with_browser_options(
        if deny { Reply::Deny } else { Reply::Allow },
        false,
        false,
        true,
        Some((tool, full_access)),
    )
    .await;
    let result = rig
        .session
        .run(
            Input::Text(
                "Select my connected browser and claim its tab for the requested draft.".into(),
            ),
            TurnOptions::default(),
        )
        .await;
    assert!(
        result.error.is_none(),
        "the scripted browser turn must finish: {:?}",
        result.error
    );
    let tool_error = rig
        .session
        .history()
        .iter()
        .find_map(|message| match message {
            Message::ToolResult(result) if result.tool_use_id == CALL_ID => Some(result.is_error),
            _ => None,
        })
        .expect("the browser call must produce a result");
    if tool_error {
        eprintln!(
            "browser policy: {:?}; tool history: {:?}; reviews: {:?}",
            rig.session.runtime_policy_state(),
            rig.session.history(),
            rig.session.approval_review_history(),
        );
    }
    let outcome = ManagedBrowserOutcome {
        policy: rig.session.runtime_policy_state(),
        executions: rig.browser_calls.load(Ordering::SeqCst),
        human_prompts: rig.human.calls.load(Ordering::SeqCst),
        reviews: rig.provider.review_calls.load(Ordering::SeqCst),
        status: rig
            .session
            .approval_review_history()
            .last()
            .map(|event| event.status),
        tool_error,
    };
    rig.close().await;
    outcome
}

#[tokio::test]
async fn outcome_only_allow_executes_the_native_file_action_and_retains_full_context() {
    let rig = Rig::new(Reply::Allow).await;
    rig.run().await;
    rig.assert_review(1, ApprovalReviewStatus::Approved, 0);
    rig.assert_output(true);
    let review = rig.reviews().remove(0);
    assert_eq!(review.model, REVIEW_MODEL);
    assert!(review.max_output_tokens > 1024);
    let tool_names = review
        .tools
        .iter()
        .map(|tool| tool.name.clone())
        .collect::<Vec<_>>();
    assert!(tool_names.iter().any(|name| name == "exec_command"));
    assert!(tool_names.iter().all(|name| [
        "exec_command",
        "write_stdin",
        "view_image",
        "exec",
        "wait"
    ]
    .iter()
    .any(|allowed| name == *allowed)));
    let schema = review
        .output_schema
        .as_ref()
        .expect("the actual query carries the assessment schema");
    assert_eq!(schema.get("required"), Some(&value(json!(["outcome"]))));
    let serialized = runtime_codec::encode_text(&review).unwrap();
    for evidence in [DIRECT_REQUEST, RETAINED_ASSISTANT, RETAINED_TOOL] {
        assert!(
            serialized.contains(evidence),
            "missing retained evidence: {evidence}"
        );
    }
    rig.close().await;
}

#[tokio::test]
async fn completed_outcome_is_authoritative_for_both_risk_extremes() {
    for (reply, status, executes) in [
        (Reply::HighRiskAllow, ApprovalReviewStatus::Approved, true),
        (Reply::LowRiskDeny, ApprovalReviewStatus::Denied, false),
    ] {
        let rig = Rig::new(reply).await;
        rig.run().await;
        rig.assert_review(1, status, 0);
        rig.assert_output(executes);
        let event = rig.session.approval_review_history().pop().unwrap();
        assert_eq!(
            event.risk_level,
            Some(if executes {
                ApprovalReviewRiskLevel::Critical
            } else {
                ApprovalReviewRiskLevel::Low
            })
        );
        assert_eq!(
            event.user_authorization,
            Some(if executes {
                ApprovalReviewUserAuthorization::Low
            } else {
                ApprovalReviewUserAuthorization::High
            })
        );
        rig.close().await;
    }
}

#[tokio::test]
async fn outcome_only_deny_blocks_execution_and_prose_wrapped_allow_is_accepted() {
    for (reply, status, executes) in [
        (Reply::Deny, ApprovalReviewStatus::Denied, false),
        (Reply::WrappedAllow, ApprovalReviewStatus::Approved, true),
    ] {
        let rig = Rig::new(reply).await;
        rig.run().await;
        rig.assert_review(1, status, 0);
        rig.assert_output(executes);
        rig.close().await;
    }
}

#[tokio::test]
async fn malformed_assessments_retry_three_times_and_nonretryable_errors_fail_closed() {
    for (reply, attempts) in [(Reply::Invalid, 3), (Reply::ProviderError, 1)] {
        let rig = Rig::new(reply).await;
        rig.run().await;
        rig.assert_review(attempts, ApprovalReviewStatus::Denied, 0);
        rig.assert_output(false);
        rig.close().await;
    }
}

#[tokio::test]
async fn cancellation_aborts_without_execution_or_a_human_fallback() {
    let rig = Rig::new(Reply::Cancel).await;
    rig.run().await;
    rig.assert_review(1, ApprovalReviewStatus::Aborted, 0);
    rig.assert_output(false);
    rig.close().await;
}

#[tokio::test]
async fn one_shared_ninety_second_deadline_times_out_without_a_human_fallback() {
    let rig = Rig::new(Reply::Silent).await;
    let running = rig.spawn_turn();
    rig.wait_for_review().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(90)).await;
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("deadline cancellation drains the private query")
        .unwrap();
    tokio::time::resume();
    rig.assert_review(1, ApprovalReviewStatus::TimedOut, 0);
    rig.assert_output(false);
    rig.close().await;
}

#[tokio::test]
async fn private_tool_loop_can_inspect_but_cannot_write_before_parent_shell_execution() {
    let rig = Rig::with_options(Reply::Inspect, true, false, false).await;
    rig.run().await;
    rig.assert_review(2, ApprovalReviewStatus::Approved, 0);
    rig.assert_output(true);
    let requests = rig.reviews();
    let inspection = requests[1]
        .messages
        .iter()
        .find_map(|message| match message {
            Message::ToolResult(result) if result.tool_use_id == INSPECTION_ID => Some(result),
            _ => None,
        })
        .expect("the second private model request sees its real tool result");
    assert!(inspection.is_error);
    let output = runtime_codec::encode_text(inspection).unwrap();
    assert!(
        output.contains(SENTINEL),
        "read-only inspection must execute the real read"
    );
    rig.close().await;
}

#[tokio::test]
async fn parent_deny_prefix_is_preserved_inside_private_inspection() {
    let rig = Rig::new(Reply::DeniedInspection).await;
    rig.session
        .apply_permission_update(PermissionUpdate::AddRules {
            rules: vec![PermissionRule {
                tool_name: "exec_command".into(),
                argument_prefix: Some("cat".into()),
                tool_name_prefix: None,
            }],
            behavior: PermissionBehavior::Deny,
            destination: PermissionDestination::Session,
        });
    rig.run().await;
    rig.assert_review(2, ApprovalReviewStatus::Approved, 0);
    rig.assert_output(true);
    let requests = rig.reviews();
    let inspection = requests[1]
        .messages
        .iter()
        .find_map(|message| match message {
            Message::ToolResult(result) if result.tool_use_id == INSPECTION_ID => Some(result),
            _ => None,
        })
        .expect("a denied private tool returns its actual error to the private model");
    assert!(inspection.is_error);
    assert!(!runtime_codec::encode_text(inspection)
        .unwrap()
        .contains(SENTINEL));
    rig.close().await;
}

#[tokio::test]
async fn twenty_user_messages_and_large_complete_arguments_have_no_invented_small_cap() {
    let rig = Rig::with_options(Reply::Allow, false, true, false).await;
    rig.run().await;
    rig.assert_review(1, ApprovalReviewStatus::Approved, 0);
    rig.assert_output(true);
    let serialized = runtime_codec::encode_text(&rig.reviews()[0]).unwrap();
    let missing: Vec<_> = (0..20)
        .filter(|index| !serialized.contains(&format!("USER_CONTEXT_{index:02}")))
        .collect();
    let public_history = runtime_codec::encode_text(&rig.session.history()).unwrap();
    let public_present: Vec<_> = (0..20)
        .filter(|index| public_history.contains(&format!("USER_CONTEXT_{index:02}")))
        .collect();
    assert!(
        missing.is_empty(),
        "missing private originals: {missing:?}; public originals: {public_present:?}; private messages: {}",
        rig.reviews()[0].messages.len()
    );
    let planned_action = rig.reviews()[0]
        .messages
        .iter()
        .filter_map(|message| match message {
            Message::User(user) => Some(
                user.content
                    .iter()
                    .filter_map(|block| match block {
                        ContentBlock::Text(text) => Some(text.text.try_to_utf8().unwrap()),
                        _ => None,
                    })
                    .collect::<String>(),
            ),
            _ => None,
        })
        .find_map(|text| {
            let (_, action) = text.split_once("Planned action JSON:\n")?;
            let (action, _) = action.split_once(">>> APPROVAL REQUEST END")?;
            serde_json::from_str::<serde_json::Value>(action.trim()).ok()
        })
        .expect("adjacent text parts contain complete planned action JSON");
    assert_eq!(planned_action["content"], rig.content);
    assert_eq!(
        planned_action["path"],
        rig.directory
            .0
            .join("outside/result.txt")
            .to_string_lossy()
            .as_ref()
    );
    rig.close().await;
}

#[tokio::test]
async fn input_budget_is_the_only_optional_review_failure_that_prompts_a_human() {
    for required in [false, true] {
        let rig = Rig::with_options(Reply::InputBudget, false, false, required).await;
        rig.run().await;
        rig.assert_review(
            0,
            if required {
                ApprovalReviewStatus::Denied
            } else {
                ApprovalReviewStatus::Aborted
            },
            usize::from(!required),
        );
        rig.assert_output(!required);
        rig.close().await;
    }
}

#[tokio::test]
async fn new_user_authorization_retries_a_held_review_and_never_executes_the_old_allow() {
    let rig = Rig::new(Reply::HeldThenDeny).await;
    let running = rig.spawn_turn();
    rig.wait_for_review().await;
    rig.session
        .enqueue(Input::Text(STEER.into()), EnqueueOptions::default())
        .unwrap();
    rig.provider.hold.release.notify_one();
    tokio::time::timeout(Duration::from_secs(20), running)
        .await
        .unwrap()
        .unwrap();
    rig.assert_review(2, ApprovalReviewStatus::Denied, 0);
    rig.assert_output(false);
    assert!(runtime_codec::encode_text(&rig.reviews()[1])
        .unwrap()
        .contains(STEER));
    rig.close().await;
}

#[tokio::test]
async fn policy_reset_aborts_a_held_review_without_running_the_old_action() {
    let rig = Rig::new(Reply::HeldAllow).await;
    let running = rig.spawn_turn();
    rig.wait_for_review().await;
    let policy = rig.session.runtime_policy_state();
    rig.session
        .set_runtime_policy(RuntimePolicyUpdate {
            approval_policy: Some(ApprovalPolicy::Never),
            sandbox_mode: policy.sandbox_mode,
            approvals_reviewer: policy.approvals_reviewer,

            network_allowed: Some(policy.network_allowed),

            sandbox_workspace_write: None,
        })
        .unwrap();
    rig.provider.hold.release.notify_one();
    tokio::time::timeout(Duration::from_secs(20), running)
        .await
        .unwrap()
        .unwrap();
    rig.assert_review(1, ApprovalReviewStatus::Aborted, 0);
    rig.assert_output(false);
    rig.close().await;
}

#[tokio::test]
async fn required_review_cannot_override_a_parent_deny_rule() {
    for required in [false, true] {
        let rig = Rig::with_options(Reply::Allow, false, false, required).await;
        rig.session
            .apply_permission_update(PermissionUpdate::AddRules {
                rules: vec![PermissionRule {
                    tool_name: "write".into(),
                    argument_prefix: None,
                    tool_name_prefix: None,
                }],
                behavior: PermissionBehavior::Deny,
                destination: PermissionDestination::Session,
            });
        rig.run().await;
        rig.assert_output(false);
        assert_eq!(rig.provider.review_calls.load(Ordering::SeqCst), 0);
        assert_eq!(rig.human.calls.load(Ordering::SeqCst), 0);
        assert!(rig.session.approval_review_history().is_empty());
        rig.close().await;
    }
}

#[tokio::test]
async fn planning_ceiling_cannot_be_overridden_by_required_review() {
    let rig = Rig::with_options(Reply::Allow, false, false, true).await;
    let policy = rig.session.runtime_policy_state();
    rig.session
        .planning_runtime()
        .set_mode(CollaborationMode::Plan, None)
        .unwrap();
    assert_eq!(rig.session.collaboration_mode(), CollaborationMode::Plan);
    assert_eq!(rig.session.runtime_policy_state(), policy);
    rig.run().await;
    rig.assert_output(false);
    assert_eq!(rig.provider.review_calls.load(Ordering::SeqCst), 0);
    assert_eq!(rig.human.calls.load(Ordering::SeqCst), 0);
    assert!(rig.session.approval_review_history().is_empty());
    rig.close().await;
}

#[tokio::test]
async fn private_review_has_fixed_tools_and_independent_turns_from_a_restricted_parent() {
    let rig = Rig::with_options(Reply::TwoInspections, false, false, false).await;
    rig.run().await;
    rig.assert_review(3, ApprovalReviewStatus::Approved, 0);
    rig.assert_output(true);
    let requests = rig.reviews();
    for request in &requests {
        let names = request
            .tools
            .iter()
            .map(|tool| tool.name.clone())
            .collect::<Vec<_>>();
        for name in ["exec_command", "write_stdin", "view_image", "exec", "wait"] {
            assert!(
                names.iter().any(|actual| actual == name),
                "the independent review omitted {name}"
            );
        }
        assert!(!names.iter().any(|name| name == "read"));
    }
    for index in 0..2 {
        let id = RuntimeString::from(format!("private-inspection-{index}"));
        let result = requests[index + 1]
            .messages
            .iter()
            .find_map(|message| match message {
                Message::ToolResult(result) if result.tool_use_id == id => Some(result),
                _ => None,
            })
            .expect("each private inspection completed before the next model request");
        assert!(!result.is_error);
        assert!(runtime_codec::encode_text(result)
            .unwrap()
            .contains(SENTINEL));
    }
    let parent_requests = rig
        .provider
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|(review, _)| !*review)
        .map(|(_, request)| request.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        parent_requests.len(),
        1,
        "the parent's one-turn limit remains effective"
    );
    assert_eq!(parent_requests[0].tools.len(), 1);
    assert!(parent_requests[0]
        .tools
        .iter()
        .all(|tool| tool.name == "write"));
    let history = rig.session.history();
    let results = history
        .iter()
        .filter_map(|message| match message {
            Message::ToolResult(result) if result.tool_use_id == CALL_ID => Some(result),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        results.len(),
        1,
        "the pending native parent write executes exactly once"
    );
    assert!(!results[0].is_error);
    assert_eq!(
        std::fs::read(rig.directory.0.join("outside/result.txt")).unwrap(),
        rig.content.as_bytes()
    );
    rig.close().await;
}

#[tokio::test]
async fn sequential_parent_actions_reuse_committed_private_history_with_a_delta_prompt() {
    let rig = Rig::new(Reply::Sequential).await;
    let running = rig.spawn_turn();
    rig.wait_for_review().await;
    tokio::time::timeout(
        Duration::from_secs(10),
        rig.provider.prewarm_observed.notified(),
    )
    .await
    .expect("context preparation resolves the prewarmed review target before any action");
    assert!(rig.provider.prewarm_resolutions.load(Ordering::SeqCst) >= 2);
    assert_eq!(rig.provider.main_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        rig.provider.review_calls.load(Ordering::SeqCst),
        0,
        "preparing the private runtime must not issue a model request"
    );
    assert_eq!(rig.provider.requests.lock().unwrap().len(), 1);
    rig.provider.hold.release.notify_one();
    tokio::time::timeout(Duration::from_secs(30), running)
        .await
        .unwrap()
        .unwrap();
    rig.assert_review(3, ApprovalReviewStatus::Approved, 0);
    rig.assert_output(true);
    assert_eq!(
        std::fs::read_to_string(rig.directory.0.join("outside/result.txt.second")).unwrap(),
        SECOND_ACTION
    );
    let requests = rig.reviews();
    let first_followup = &requests[1];
    let second_review = &requests[2];
    assert!(first_followup
        .messages
        .iter()
        .any(|message| matches!(message,
        Message::ToolResult(result) if result.tool_use_id == INSPECTION_ID)));
    let committed_prefix_length = first_followup.messages.len();
    assert!(second_review.messages.len() > committed_prefix_length + 1);
    assert_eq!(
        &second_review.messages[..committed_prefix_length],
        first_followup.messages.as_slice(),
        "the next assessment must retain the exact prior private inspection history"
    );
    let assessment = &second_review.messages[committed_prefix_length];
    assert!(matches!(assessment, Message::Assistant(_)));
    assert!(
        runtime_codec::encode_text(assessment)
            .unwrap()
            .contains(FIRST_ASSESSMENT),
        "the last completed assessment must be part of the committed private history"
    );
    let delta =
        runtime_codec::encode_text(&second_review.messages[committed_prefix_length + 1..].to_vec())
            .unwrap();
    assert!(delta.contains(SECOND_ACTION));
    for retained in [DIRECT_REQUEST, RETAINED_ASSISTANT, RETAINED_TOOL] {
        assert!(
            !delta.contains(retained),
            "the cursor must not add old parent evidence again: {retained}"
        );
    }
    let history = rig.session.history();
    for id in [CALL_ID, SECOND_CALL_ID] {
        assert_eq!(
            history
                .iter()
                .filter(|message| matches!(message,
            Message::ToolResult(result) if result.tool_use_id == id && !result.is_error))
                .count(),
            1,
            "each pending native operation must execute exactly once"
        );
    }
    assert_eq!(rig.session.approval_review_history().len(), 2);
    rig.close().await;
}
