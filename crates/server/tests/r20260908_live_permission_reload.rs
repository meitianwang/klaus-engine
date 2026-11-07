//! Defect: editing durable rules did not update live sessions, including ephemeral side tasks.
//! Root cause: startup flattened disk grants into immutable config and approvals into session-only state.
//! Fixed for the 2026-09-08 Electron live permission-rule migration work item.

use super::r20260908_live_side_task_fork::request;
use super::*;
use async_trait::async_trait;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeJson, RuntimeString};
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::types::errors::ToolError;
use klaus_agent_core::types::json::{Json, JsonSchema};
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

struct FixtureWrite {
    schema: JsonSchema,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Tool for FixtureWrite {
    fn static_name(&self) -> &str {
        "fixture_write"
    }
    fn static_description(&self) -> &str {
        "Exercise the permission boundary without external effects"
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        false
    }
    fn is_read_only(&self, _: &Json) -> bool {
        false
    }
    async fn call(
        &self,
        _: Json,
        _: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(text_result(
            RuntimeJson::Null,
            RuntimeString::from("fixture completed"),
            false,
        ))
    }
}

struct Fixture {
    client: TestClient,
    root: tempfile::TempDir,
    scripts: Arc<Mutex<VecDeque<Vec<ModelStreamEvent>>>>,
    created: Arc<Mutex<Option<Arc<Session>>>>,
    calls: Arc<AtomicUsize>,
    id: u64,
}

impl Fixture {
    async fn new(project_allow: bool, stored_allow: bool) -> Self {
        let root = tempfile::tempdir().unwrap();
        let rules = root.path().join("rules.json");
        if project_allow {
            let project = root.path().join(".klaus");
            std::fs::create_dir(&project).unwrap();
            std::fs::write(project.join("config.toml"), "allow = [\"fixture_write\"]\n").unwrap();
        }
        if stored_allow {
            std::fs::write(
                &rules,
                json!({"allow":["fixture_write"],"deny":[],"ask":[]}).to_string(),
            )
            .unwrap();
        }
        let scripts = Arc::new(Mutex::new(VecDeque::<Vec<ModelStreamEvent>>::new()));
        let source = scripts.clone();
        let agent = make_agent_with_call_model_and_config(
            Box::new(move |_, _| {
                Box::pin(futures::stream::iter(
                    source.lock().unwrap().pop_front().expect("scripted turn"),
                ))
            }),
            Some(
                json!({"autoMemory":false,"autoDream":false,"memoryRecall":false,"memory":{"enabled":false},"approval_policy":"on-request","sandbox_mode":"read-only","approvals_reviewer":"user"}),
            ),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let tool: Arc<dyn Tool> = Arc::new(FixtureWrite {
            schema: json!({"type":"object","properties":{},"additionalProperties":false})
                .as_object()
                .unwrap()
                .clone(),
            calls: calls.clone(),
        });
        let created = Arc::new(Mutex::new(None));
        let captured = created.clone();
        let home = root.path().to_string_lossy().into_owned();
        let create: CreateSession = Arc::new(move |ctx| {
            let (agent, tool, captured, home) =
                (agent.clone(), tool.clone(), captured.clone(), home.clone());
            Box::pin(async move {
                let config = ctx.init.config.as_ref().map(|value| {
                    serde_json::from_str(
                        &runtime_codec::encode_text(value)
                            .unwrap()
                            .try_to_utf8()
                            .unwrap(),
                    )
                    .unwrap()
                });
                let session = agent.try_start_session(StartSessionOptions {
                    id: ctx.init.thread_id,
                    cwd: Some(home.clone().into()),
                    user_config_dir: Some(home.into()),
                    tools: Some(vec![tool]),
                    config,
                    permission_mode: Some(ExternalPermissionMode::Default),
                    durable_permissions: Some(ctx.durable_permissions),
                    approval_handler: Some(ctx.approval_handler),
                    ..Default::default()
                })?;
                let policy = session.runtime_policy_state();
                assert_eq!(
                    policy.approval_policy,
                    Some(klaus_agent_core::types::permission::ApprovalPolicy::OnRequest),
                    "the fixture requires ordinary approval prompts"
                );
                assert_eq!(
                    policy.sandbox_mode,
                    Some(klaus_agent_core::types::permission::SandboxMode::ReadOnly),
                    "the fixture must not inherit unrestricted filesystem access"
                );
                assert_eq!(
                    policy.approvals_reviewer,
                    Some(klaus_agent_core::types::permission::ApprovalsReviewer::User)
                );
                *captured.lock().unwrap() = Some(session.clone());
                Ok::<_, BoxError>(session)
            })
        });
        let mut client = TestClient::start_full(
            create,
            None,
            Some(rules),
            Some(root.path().join("config.toml")),
        );
        client.initialize().await;
        let start = request(
            &mut client,
            2,
            "session/start",
            json!({"init":{"threadId":"live","cwd":root.path()}}),
        )
        .await;
        assert_eq!(start["result"]["sessionId"], "live", "{start}");
        Self {
            client,
            root,
            scripts,
            created,
            calls,
            id: 3,
        }
    }

    async fn rpc(&mut self, method: &str, params: Value) -> Value {
        self.id += 1;
        request(&mut self.client, self.id, method, params).await
    }

    async fn reload(&mut self) -> Value {
        self.rpc("session/reloadPermissionRules", json!({"sessionId":"live"}))
            .await
    }

    async fn remove(&mut self) {
        let reply = self
            .rpc(
                "engine/removePermissionRule",
                json!({"rule":"fixture_write"}),
            )
            .await;
        assert_eq!(reply["result"]["allow"], json!([]), "{reply}");
    }

    async fn run(&mut self, grant: Option<&str>) -> usize {
        let before = self.calls.load(Ordering::SeqCst);
        let mut events = vec![ev::message_start(None)];
        events.extend(ev::tool_use(
            "fixture-call",
            "fixture_write",
            &runtime_codec::decode_text::<RuntimeJson>(&RuntimeString::from("{}")).unwrap(),
        ));
        events.push(ev::stop(Some(ModelStopReason::ToolUse), None));
        self.scripts
            .lock()
            .unwrap()
            .extend([events, text_turn("done")]);
        self.id += 1;
        self.client.send(json!({"id":self.id,"method":"turn/start","params":{"sessionId":"live","input":"exercise fixture"}})).await;
        let (reply, seen) = self.client.response_for(self.id).await;
        assert!(reply.get("error").is_none(), "{reply}");
        let mut pending: VecDeque<_> = seen.into();
        let mut approvals = 0;
        loop {
            let event = match pending.pop_front() {
                Some(event) => event,
                None => self.client.next_message().await,
            };
            if event["method"] == "approval/request" {
                approvals += 1;
                let mut result = json!({"decision":"allow"});
                if let Some(destination) = grant {
                    result["updatedPermissions"] = json!([{"type":"addRules","behavior":"allow","destination":destination,"rules":[{"toolName":"fixture_write"}]}]);
                }
                self.client
                    .send(json!({"id":event["id"],"result":{"response":result}}))
                    .await;
            }
            assert_ne!(
                event["params"]["event"]["event"]["type"], "turn.failed",
                "{event}"
            );
            if event["method"] == "turn/completed" {
                break;
            }
        }
        assert!(self.scripts.lock().unwrap().is_empty());
        assert_eq!(
            self.calls.load(Ordering::SeqCst),
            before + 1,
            "{:?}",
            self.created
                .lock()
                .unwrap()
                .as_ref()
                .map(|session| session.history())
        );
        approvals
    }
}

#[tokio::test]
async fn live_permission_reload_removes_durable_allow_without_rebuilding_the_session() {
    let mut fixture = Fixture::new(false, true).await;
    assert_eq!(fixture.run(None).await, 0);
    let session = fixture.created.lock().unwrap().clone().unwrap();
    fixture.remove().await;
    assert_eq!(
        fixture.run(None).await,
        0,
        "disk mutation waits for explicit reload"
    );
    let history = session.history();
    let result = fixture.reload().await;
    assert_eq!(result["result"]["allow"], json!([]), "{result}");
    assert_eq!(session.history(), history);
    assert!(Arc::ptr_eq(
        &session,
        fixture.created.lock().unwrap().as_ref().unwrap()
    ));
    assert_eq!(fixture.run(None).await, 1);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 3);
    fixture.client.close().await;
}

#[tokio::test]
async fn live_permission_reload_keeps_identical_project_rules_and_session_only_grants() {
    let mut project = Fixture::new(true, true).await;
    assert_eq!(project.run(None).await, 0);
    project.remove().await;
    assert!(project.reload().await.get("error").is_none());
    assert_eq!(
        project.run(None).await,
        0,
        "project rule survives the durable deletion"
    );
    project.client.close().await;

    let mut session = Fixture::new(false, false).await;
    assert_eq!(session.run(Some("session")).await, 1);
    assert!(session.reload().await.get("error").is_none());
    assert_eq!(session.run(None).await, 0, "session grant remains separate");
    assert_eq!(
        session.rpc("engine/permissionRules", json!({})).await["result"]["allow"],
        json!([])
    );
    session.client.close().await;
}

#[tokio::test]
async fn live_permission_reload_does_not_keep_saved_approval_as_a_sticky_session_grant() {
    let mut fixture = Fixture::new(false, false).await;
    assert_eq!(fixture.run(Some("userSettings")).await, 1);
    let persisted = fixture.rpc("engine/permissionRules", json!({})).await;
    assert_eq!(
        persisted["result"]["allow"],
        json!(["fixture_write"]),
        "{persisted}"
    );
    assert_eq!(fixture.run(None).await, 0);
    fixture.remove().await;
    assert!(fixture.reload().await.get("error").is_none());
    assert_eq!(
        fixture.run(None).await,
        1,
        "the saved grant is removable from this same live session"
    );
    fixture.client.close().await;
}

#[tokio::test]
async fn live_permission_reload_read_failures_keep_the_previous_policy_and_bytes() {
    let mut fixture = Fixture::new(false, true).await;
    assert_eq!(fixture.run(None).await, 0);
    let path = fixture.root.path().join("rules.json");
    for corrupt in [
        "{corrupt",
        "{\"allow\":[false]}",
        "[]",
        "{\"deny\":[\"Bash(unclosed\"]}",
    ] {
        std::fs::write(&path, corrupt).unwrap();
        assert!(fixture.reload().await.get("error").is_some());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), corrupt);
        assert_eq!(fixture.run(None).await, 0);
    }
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    assert!(fixture.reload().await.get("error").is_some());
    assert_eq!(fixture.run(None).await, 0);
    std::fs::remove_dir(&path).unwrap();
    assert!(fixture.reload().await.get("error").is_none());
    assert_eq!(
        fixture.run(None).await,
        1,
        "only ENOENT replaces the store with empty rules"
    );
    fixture.client.close().await;
}

#[tokio::test]
async fn live_permission_reload_is_lazy_and_rejects_unknown_closed_or_host_supplied_rules() {
    let mut fixture = Fixture::new(false, true).await;
    assert!(fixture.reload().await.get("error").is_none());
    assert!(fixture.created.lock().unwrap().is_none());
    for params in [
        json!({"sessionId":"missing"}),
        json!({"sessionId":"live","allow":[]}),
    ] {
        assert!(fixture
            .rpc("session/reloadPermissionRules", params)
            .await
            .get("error")
            .is_some());
    }
    assert!(fixture.created.lock().unwrap().is_none());
    fixture
        .rpc("session/close", json!({"sessionId":"live"}))
        .await;
    assert!(fixture.reload().await.get("error").is_some());
    fixture.client.close().await;
}

#[tokio::test]
async fn live_permission_reload_keeps_ephemeral_side_task_prefix_binding_and_role() {
    let mut fixture = super::r20260908_live_side_task_fork::Fixture::new(false, None).await;
    let source = fixture.source.clone();
    let full = source.history();
    let visible = source.visible_history();
    let revision = source.model_revision();
    std::fs::write(
        fixture.root.path().join("rules.json"),
        json!({"deny":["Write"],"allow":[],"ask":[]}).to_string(),
    )
    .unwrap();
    let reply = request(
        &mut fixture.client,
        50,
        "session/reloadPermissionRules",
        json!({"sessionId":"side"}),
    )
    .await;
    assert_eq!(reply["result"]["deny"], json!(["Write"]), "{reply}");
    assert_eq!(source.id, "side");
    assert_eq!(
        source.meta.role,
        Some(klaus_agent_core::schemas::rollout::SessionRole::SideTask)
    );
    assert_eq!(source.history(), full);
    assert_eq!(source.visible_history(), visible);
    assert_eq!(source.model_revision(), revision);
    assert!(source.history().len() > source.visible_history().len());
    assert!(
        fixture.store.resume("side").await.is_err(),
        "reload must not fabricate a persisted side task"
    );
    fixture.client.close().await;
}
