//! Defect: Spec research prompted for every uncached public web page.
//! Root cause: network approval treated Plan like an interactive execution mode.
//! Fixed by: defect session 70130183; Plan web_fetch skips only network approval,
//! retaining explicit tool rules, domain policy, and private-network protection.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::tools::builtins::web_fetch::{create_web_fetch_tool, WebFetchToolOptions};
use klaus_agent_core::tools::builtins::web_fetch_redirect::{FetchError, FetchResponse, Fetcher};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::permission::{
    ApprovalHandler, ApprovalKind, ApprovalRequest, ApprovalResponse, ExternalPermissionMode,
    PermissionBehavior, PermissionDestination, PermissionRule, PermissionUpdate,
};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

struct FetchProvider {
    info: ProviderInfo,
    urls: Vec<String>,
    step: AtomicUsize,
    results: Arc<Mutex<Vec<bool>>>,
}

fn usage() -> Usage {
    Usage {
        input_tokens: 10,
        output_tokens: 4,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: None,
    }
}

impl ModelProvider for FetchProvider {
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
        let step = self.step.fetch_add(1, Ordering::SeqCst);
        if step > 0 {
            let id = format!("fetch-{}", step - 1);
            let result = request
                .messages
                .iter()
                .find_map(|message| match message {
                    Message::ToolResult(result) if result.tool_use_id == id.as_str() => {
                        Some(result)
                    }
                    _ => None,
                })
                .expect("the public session must return the preceding fetch result");
            self.results.lock().unwrap().push(result.is_error);
        }
        let mut events = vec![ModelStreamEvent::MessageStart {
            model: "plan-fetch-fixture".into(),
        }];
        let stop_reason = if let Some(url) = self.urls.get(step) {
            let id = RuntimeString::from(format!("fetch-{step}"));
            events.extend([
                ModelStreamEvent::ToolUseStart {
                    id: id.clone(),
                    name: "web_fetch".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: id.clone(),
                    partial_json: json!({"url": url}).to_string().into(),
                },
                ModelStreamEvent::ToolUseStop { id },
            ]);
            ModelStopReason::ToolUse
        } else {
            events.push(ModelStreamEvent::TextDelta {
                text: "Research completed.".into(),
            });
            ModelStopReason::EndTurn
        };
        events.push(ModelStreamEvent::MessageStop {
            stop_reason,
            usage: usage(),
        });
        Box::pin(futures::stream::iter(events))
    }
}

struct PublicPageFetcher {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Fetcher for PublicPageFetcher {
    async fn fetch(
        &self,
        _: &str,
        _: &[(String, String)],
        _: bool,
        _: &AbortSignal,
    ) -> Result<FetchResponse, FetchError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(FetchResponse::ready(
            200,
            "OK",
            vec![("content-type".into(), "text/plain".into())],
            "Public research evidence.",
        ))
    }
}

struct DenyAndRecordApproval {
    kinds: Arc<Mutex<Vec<ApprovalKind>>>,
}

#[async_trait]
impl ApprovalHandler for DenyAndRecordApproval {
    async fn approve(&self, request: ApprovalRequest, _: AbortSignal) -> ApprovalResponse {
        self.kinds.lock().unwrap().push(request.kind);
        ApprovalResponse::Deny {
            message: Some("Unexpected approval in research fixture.".into()),
        }
    }
}

struct FixtureDirectory(std::path::PathBuf);

impl FixtureDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("plan-web-fetch-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for FixtureDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Outcome {
    results: Vec<bool>,
    fetches: usize,
    approvals: Vec<ApprovalKind>,
    approval_events: usize,
}

async fn fetch_in_plan(
    urls: &[&str],
    rule: Option<PermissionBehavior>,
    blocked_domains: Vec<RuntimeString>,
    with_approval_handler: bool,
) -> Outcome {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let results = Arc::new(Mutex::new(Vec::new()));
    let fetches = Arc::new(AtomicUsize::new(0));
    let approvals = Arc::new(Mutex::new(Vec::new()));
    let mut options = agent_options(process.clone());
    options.provider = Arc::new(FetchProvider {
        info: ProviderInfo {
            name: "plan-fetch-fixture".into(),
            default_model: "plan-fetch-fixture".into(),
            base_url: None,
            env_key: None,
        },
        urls: urls.iter().map(|url| (*url).to_owned()).collect(),
        step: AtomicUsize::new(0),
        results: results.clone(),
    });
    options.tools = Some(vec![create_web_fetch_tool(WebFetchToolOptions {
        fetch_impl: Some(Arc::new(PublicPageFetcher {
            calls: fetches.clone(),
        })),
        blocked_domains: Some(blocked_domains),
        ..Default::default()
    })]);
    if with_approval_handler {
        options.approval_handler = Some(Arc::new(DenyAndRecordApproval {
            kinds: approvals.clone(),
        }));
    }
    let directory = FixtureDirectory::new();
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions {
        user_config_dir: Some(directory.0.to_string_lossy().into_owned().into()),
        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
        network_allowed: Some(false),
        config: Some(json!({
            "sandbox_mode": "read-only",
            "approval_policy": "on-request",
            "approvals_reviewer": "user",
            "prewarm": false,
            "goal": false,
            "agentMemory": false,
            "cron": false,
        })),
        ..Default::default()
    });
    session.apply_permission_update(PermissionUpdate::SetMode {
        mode: ExternalPermissionMode::Plan,
        destination: PermissionDestination::Session,
    });
    if let Some(behavior) = rule {
        session.apply_permission_update(PermissionUpdate::AddRules {
            rules: vec![PermissionRule {
                tool_name: "web_fetch".into(),
                argument_prefix: None,
                tool_name_prefix: None,
            }],
            behavior,
            destination: PermissionDestination::Session,
        });
    }
    let events: Vec<EngineEvent> = session
        .run_streamed(
            Input::Text("Read public research pages.".into()),
            TurnOptions::default(),
        )
        .events
        .collect()
        .await;
    let policy = session.runtime_policy_state();
    assert_eq!(
        policy.sandbox_mode,
        Some(klaus_agent_core::types::permission::SandboxMode::ReadOnly)
    );
    assert_eq!(
        policy.approval_policy,
        Some(klaus_agent_core::types::permission::ApprovalPolicy::OnRequest)
    );
    assert_eq!(
        policy.approvals_reviewer,
        Some(klaus_agent_core::types::permission::ApprovalsReviewer::User)
    );
    assert!(!policy.network_allowed);
    assert_eq!(
        session.collaboration_mode(),
        klaus_agent_core::planning::CollaborationMode::Plan
    );
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    let outcome = Outcome {
        results: results.lock().unwrap().clone(),
        fetches: fetches.load(Ordering::SeqCst),
        approvals: approvals.lock().unwrap().clone(),
        approval_events: events
            .iter()
            .filter(|event| matches!(event, EngineEvent::ApprovalRequested { .. }))
            .count(),
    };
    assert_eq!(outcome.results.len(), urls.len());
    outcome
}

#[tokio::test]
async fn plan_web_fetch_reads_uncached_pages_without_network_approval() {
    let outcome = fetch_in_plan(
        &[
            "https://example.com/first",
            "https://example.com/second",
            "https://example.org/research",
        ],
        None,
        vec![],
        true,
    )
    .await;
    assert_eq!(outcome.results, vec![false; 3]);
    assert_eq!(outcome.fetches, 3);
    assert!(outcome.approvals.is_empty());
    assert_eq!(outcome.approval_events, 0);
}

#[tokio::test]
async fn plan_web_fetch_needs_no_approval_handler() {
    let outcome = fetch_in_plan(&["https://example.com/"], None, vec![], false).await;
    assert_eq!(outcome.results, vec![false]);
    assert_eq!(outcome.fetches, 1);
    assert_eq!(outcome.approval_events, 0);
}

#[tokio::test]
async fn plan_web_fetch_preserves_explicit_deny_rules() {
    let outcome = fetch_in_plan(
        &["https://example.com/"],
        Some(PermissionBehavior::Deny),
        vec![],
        true,
    )
    .await;
    assert_eq!(outcome.results, vec![true]);
    assert_eq!(outcome.fetches, 0);
    assert!(outcome.approvals.is_empty());
    assert_eq!(outcome.approval_events, 0);
}

#[tokio::test]
async fn plan_web_fetch_preserves_explicit_ask_rules() {
    let outcome = fetch_in_plan(
        &["https://example.com/"],
        Some(PermissionBehavior::Ask),
        vec![],
        true,
    )
    .await;
    assert_eq!(outcome.results, vec![true]);
    assert_eq!(outcome.fetches, 0);
    assert_eq!(outcome.approvals, vec![ApprovalKind::Tool]);
    assert_eq!(outcome.approval_events, 1);
}

#[tokio::test]
async fn plan_web_fetch_preserves_domain_and_private_network_protection() {
    let outcome = fetch_in_plan(
        &[
            "https://blocked.example/",
            "https://sub.blocked.example/",
            "https://127.0.0.1/",
            "https://169.254.169.254/",
        ],
        None,
        vec!["blocked.example".into()],
        true,
    )
    .await;
    assert_eq!(outcome.results, vec![true; 4]);
    assert_eq!(outcome.fetches, 0);
    assert!(outcome.approvals.is_empty());
    assert_eq!(outcome.approval_events, 0);
}
