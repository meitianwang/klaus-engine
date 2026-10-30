//! Parent restrictions received during real task preparation retain their authority.

use super::*;
use klaus_agent_core::engine::session::RuntimePolicyUpdate;
use klaus_agent_core::schemas::content::OutputBlock;
use klaus_agent_core::types::permission::{
    ApprovalPolicy, ApprovalsReviewer, PermissionBehavior, PermissionDestination, PermissionRule,
    PermissionUpdate, SandboxMode,
};
use tokio::sync::Semaphore;

const PREPARING_CHILD: &str = "PREPARING_SCOPED_PERMISSION_CHILD";

struct PermissionPreparationGate {
    entered: Arc<Semaphore>,
    release: Option<std::sync::mpsc::SyncSender<()>>,
}

impl PermissionPreparationGate {
    fn install(fixture: &Fixture) -> Self {
        let entered = Arc::new(Semaphore::new(0));
        let (release, receive) = std::sync::mpsc::sync_channel(1);
        let receive = Mutex::new(receive);
        let first = AtomicUsize::new(0);
        let phases = fixture.phases.clone();
        let origin = fixture.phase_origin;
        let signal = entered.clone();
        fixture
            .session
            .goal_runtime()
            .unwrap()
            .testing_set_phase_observer(Some(Arc::new(move |phase| {
                let entered_ms = origin.elapsed().as_millis();
                {
                    let mut trace = phases.lock().unwrap();
                    if trace.len() == 256 {
                        trace.remove(0);
                    }
                    trace.push((phase, entered_ms, origin.elapsed().as_millis()));
                }
                if phase == "subagent.identity"
                    && first
                        .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                {
                    signal.add_permits(1);
                    tokio::task::block_in_place(|| receive.lock().unwrap().recv().unwrap());
                }
            })));
        Self {
            entered,
            release: Some(release),
        }
    }

    fn release(&mut self) {
        if let Some(release) = self.release.take() {
            release.try_send(()).unwrap();
        }
    }
}

impl Drop for PermissionPreparationGate {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.try_send(());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_task_preparation_cannot_recapture_a_later_tighter_parent_as_its_origin() {
    let slot: Slot = Arc::new(Mutex::new(Weak::new()));
    let observations = Arc::new(Mutex::new(Vec::new()));
    let roots = Arc::new(AtomicUsize::new(0));
    let children = Arc::new(AtomicUsize::new(0));
    let fixture = Fixture::new(
        &slot,
        vec![Arc::new(ObserveTool {
            schema: json!({"type":"object","properties":{}}).as_object().unwrap().clone(),
            observations: observations.clone(),
        }), create_task_tool(TaskToolOptions::default())],
        |options| {
            options.config.as_mut().unwrap()["permissionMode"] = json!("default");
            options.capability_roots = vec![RuntimeCapabilityRoot::host("preparation-owner").with_browser_tool_mapping(BrowserHostToolMapping {
                tabs: "preparation-tabs-owner".to_string(), ..Default::default()
            })];
        },
        {
            let roots = roots.clone();
            let children = children.clone();
            move |request, _| {
                if directive(&request, PREPARING_CHILD) {
                    return match children.fetch_add(1, Ordering::SeqCst) {
                        0 => Box::pin(futures::stream::iter(observe_events("preparation-child-observation"))),
                        1 => Box::pin(futures::stream::iter(answer("Prepared child finished.", 1))),
                        call => panic!("unexpected prepared child request {call}"),
                    };
                }
                match roots.fetch_add(1, Ordering::SeqCst) {
                    0 => {
                        let mut events = vec![ModelStreamEvent::MessageStart { model: "goal-turn-override".into() }];
                        events.extend(call("preparing-child", "task", json!({"model": "low", "description":"Check preparation authority","prompt":PREPARING_CHILD,"fork_turns":"none","run_in_background":false})));
                        events.push(stop(ModelStopReason::ToolUse, 1));
                        Box::pin(futures::stream::iter(events))
                    }
                    1 => Box::pin(futures::stream::iter(answer("Parent finished.", 1))),
                    call => panic!("unexpected preparation parent request {call}"),
                }
            }
        },
    ).await;
    assert_eq!(
        fixture.session.runtime_policy_state().sandbox_mode,
        Some(klaus_agent_core::types::permission::SandboxMode::DangerFullAccess)
    );
    fixture
        .session
        .set_thread_goal(
            Some("Track the preparing task authority.".into()),
            None,
            None,
        )
        .await
        .unwrap();
    let mut gate = PermissionPreparationGate::install(&fixture);
    let turn = fixture.session.run_streamed(
        Input::Text("Run the task with a scoped grant.".into()),
        TurnOptions {
            selected_capability_roots: Some(vec![SelectedRuntimeCapabilityRoot::host(
                "preparation-owner",
            )]),
            ..Default::default()
        },
    );
    let events = {
        let root = collect(&fixture, turn);
        tokio::pin!(root);
        let entered = gate.entered.clone();
        tokio::select! {
            permit = tokio::time::timeout(WAIT, entered.acquire_owned()) => {
                permit.expect("actual child must enter identity preparation").unwrap().forget();
            }
            events = root.as_mut() => panic!("parent finished before the preparation gate: {events:?}"),
        }
        assert_eq!(roots.load(Ordering::SeqCst), 1);
        assert_eq!(
            children.load(Ordering::SeqCst),
            0,
            "the worker is held before provider entry"
        );
        assert!(observations.lock().unwrap().is_empty());
        fixture
            .session
            .set_runtime_policy(klaus_agent_core::engine::session::RuntimePolicyUpdate {
                approval_policy: Some(klaus_agent_core::types::permission::ApprovalPolicy::Never),
                sandbox_mode: Some(klaus_agent_core::types::permission::SandboxMode::ReadOnly),
                approvals_reviewer: None,
                sandbox_workspace_write: None,
                network_allowed: Some(false),
            })
            .unwrap();
        gate.release();
        root.await
    };
    assert_success(&events);
    assert_eq!(roots.load(Ordering::SeqCst), 2);
    assert_eq!(children.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.requests.lock().unwrap().len(), 4);
    {
        let observed = observations.lock().unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].depth, Some(1));
        assert_eq!(
            observed[0].sandbox,
            Some(klaus_agent_core::types::permission::SandboxMode::ReadOnly),
            "later tightening must be measured against the original Default baseline"
        );
        assert_eq!(observed[0].browser_tabs, "preparation-tabs-owner");
    }
    fixture.finish().await;
}

struct HeldTaskPreTool {
    entered: Arc<Semaphore>,
    receive: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<()>>,
    calls: AtomicUsize,
}

#[async_trait]
impl klaus_agent_core::hooks::types::HookFn for HeldTaskPreTool {
    async fn run(
        &self,
        input: klaus_agent_core::hooks::types::HookInput,
    ) -> klaus_agent_core::hooks::types::HookOutcome {
        assert_eq!(input.tool_name.as_ref(), Some(&RuntimeString::from("task")));
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(
            call < 2,
            "only the original Task and its retry may reach the hook"
        );
        if call == 0 {
            self.entered.add_permits(1);
            self.receive
                .lock()
                .await
                .recv()
                .await
                .expect("bounded hook release remains owned");
        }
        Default::default()
    }
}

struct HeldTaskPreToolRelease(Option<tokio::sync::mpsc::Sender<()>>);
impl HeldTaskPreToolRelease {
    fn release(&mut self) {
        if let Some(sender) = self.0.take() {
            sender.try_send(()).unwrap();
        }
    }
}
impl Drop for HeldTaskPreToolRelease {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.try_send(());
        }
    }
}

async fn actual_held_pretool_case(background: bool) {
    const PRETOOL_CHILD: &str = "HELD_PRETOOL_SCOPED_PERMISSION_CHILD";
    let slot: Slot = Arc::new(Mutex::new(Weak::new()));
    let observations = Arc::new(Mutex::new(Vec::new()));
    let roots = Arc::new(AtomicUsize::new(0));
    let children = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Semaphore::new(0));
    let (send, receive) = tokio::sync::mpsc::channel(1);
    let hook = Arc::new(HeldTaskPreTool {
        entered: entered.clone(),
        receive: tokio::sync::Mutex::new(receive),
        calls: AtomicUsize::new(0),
    });
    let mut release = HeldTaskPreToolRelease(Some(send));
    let fixture = Fixture::new(
        &slot,
        vec![Arc::new(ObserveTool {
            schema: json!({"type":"object","properties":{}}).as_object().unwrap().clone(),
            observations: observations.clone(),
        }), create_task_tool(TaskToolOptions::default())],
        {
            let hook = hook.clone();
            move |options| {
                options.hooks = vec![klaus_agent_core::hooks::types::Hook {
                    event: klaus_agent_core::hooks::types::HookEvent::PreToolUse,
                    matcher: Some("task".to_string()),
                    handler: klaus_agent_core::hooks::types::HookHandler::Fn(hook),
                    is_async: false,
                }];
                options.capability_roots = vec![RuntimeCapabilityRoot::host("pretool-owner").with_browser_tool_mapping(BrowserHostToolMapping {
                    tabs: "pretool-tabs-owner".to_string(), ..Default::default()
                })];
            }
        },
        {
            let roots = roots.clone();
            let children = children.clone();
            move |request, _| {
                if directive(&request, PRETOOL_CHILD) {
                    return match children.fetch_add(1, Ordering::SeqCst) {
                        0 => Box::pin(futures::stream::iter(observe_events("pretool-child-observation"))),
                        1 => Box::pin(futures::stream::iter(answer("Pretool child finished.", 1))),
                        call => panic!("unexpected pretool child request {call}"),
                    };
                }
                match roots.fetch_add(1, Ordering::SeqCst) {
                    0 => {
                        let mut events = vec![ModelStreamEvent::MessageStart { model: "goal-turn-override".into() }];
                        events.extend(call("held-pretool-child", "task", json!({"model": "low", "description":"Check pretool authority","prompt":PRETOOL_CHILD,"fork_turns":"none","run_in_background":background})));
                        events.push(stop(ModelStopReason::ToolUse, 1));
                        Box::pin(futures::stream::iter(events))
                    }
                    1 => {
                        let result = request.messages.iter().find_map(|message| match message {
                            Message::ToolResult(result) if result.tool_use_id == "held-pretool-child" => Some(result),
                            _ => None,
                        }).expect("the original Task must return a provider-visible retry error");
                        assert!(result.is_error);
                        assert!(result.content.iter().any(|block| matches!(
                            block, OutputBlock::Text(text)
                                if text.text == "Permissions changed before execution; retry this action under the current policy."
                        )));
                        let mut events = vec![ModelStreamEvent::MessageStart { model: "goal-turn-override".into() }];
                        events.extend(call("retried-pretool-child", "task", json!({"model": "low", "description":"Retry under current authority","prompt":PRETOOL_CHILD,"fork_turns":"none","run_in_background":background})));
                        events.push(stop(ModelStopReason::ToolUse, 1));
                        Box::pin(futures::stream::iter(events))
                    }
                    2 => {
                        let result = request.messages.iter().find_map(|message| match message {
                            Message::ToolResult(result) if result.tool_use_id == "retried-pretool-child" => Some(result),
                            _ => None,
                        }).expect("the retried Task must return its own provider-visible result");
                        assert!(!result.is_error, "the Task retry uses the tightened policy");
                        Box::pin(futures::stream::iter(answer("Pretool parent finished.", 1)))
                    }
                    call => panic!("unexpected pretool parent request {call}"),
                }
            }
        },
    ).await;
    fixture
        .session
        .apply_permission_update(PermissionUpdate::AddRules {
            rules: vec![PermissionRule {
                tool_name: "task".into(),
                argument_prefix: None,
                tool_name_prefix: None,
            }],
            behavior: PermissionBehavior::Allow,
            destination: PermissionDestination::Session,
        });
    fixture
        .session
        .set_thread_goal(
            Some("Track authority across a real tool hook.".into()),
            None,
            None,
        )
        .await
        .unwrap();
    let initial_policy = fixture.session.runtime_policy_state();
    assert_eq!(
        (
            initial_policy.approval_policy,
            initial_policy.sandbox_mode,
            initial_policy.approvals_reviewer
        ),
        (
            Some(ApprovalPolicy::Never),
            Some(SandboxMode::DangerFullAccess),
            Some(ApprovalsReviewer::User)
        )
    );
    let turn = fixture.session.run_streamed(
        Input::Text("Run a scoped task after the hook.".into()),
        TurnOptions {
            selected_capability_roots: Some(vec![SelectedRuntimeCapabilityRoot::host(
                "pretool-owner",
            )]),
            ..Default::default()
        },
    );
    let events = {
        let root = collect(&fixture, turn);
        tokio::pin!(root);
        tokio::select! {
            permit = tokio::time::timeout(WAIT, entered.acquire_owned()) => {
                permit.expect("the actual Task PreToolUse hook must enter").unwrap().forget();
            }
            events = root.as_mut() => panic!("root finished before the real pretool wait: {events:?}"),
        }
        assert_eq!(roots.load(Ordering::SeqCst), 1);
        assert_eq!(children.load(Ordering::SeqCst), 0);
        assert!(
            fixture
                .session
                .subagent_lifecycle_snapshot()
                .agents
                .is_empty(),
            "no worker may be admitted before the held Task hook returns"
        );
        assert!(observations.lock().unwrap().is_empty());
        fixture
            .session
            .set_runtime_policy(RuntimePolicyUpdate {
                approval_policy: Some(ApprovalPolicy::Never),
                sandbox_mode: Some(SandboxMode::ReadOnly),
                approvals_reviewer: None,
                sandbox_workspace_write: None,
                network_allowed: Some(false),
            })
            .unwrap();
        let tightened = fixture.session.runtime_policy_state();
        assert_eq!(
            (
                tightened.approval_policy,
                tightened.sandbox_mode,
                tightened.network_allowed
            ),
            (
                Some(ApprovalPolicy::Never),
                Some(SandboxMode::ReadOnly),
                false
            )
        );
        release.release();
        root.await
    };
    assert_success(&events);
    tokio::time::timeout(WAIT, async {
        while fixture
            .session
            .goal_runtime()
            .unwrap()
            .has_descendant_work()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the actual admitted child must settle");
    assert_eq!(hook.calls.load(Ordering::SeqCst), 2);
    assert_eq!(roots.load(Ordering::SeqCst), 3);
    assert_eq!(children.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.requests.lock().unwrap().len(), 5);
    {
        let observed = observations.lock().unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].depth, Some(1));
        assert_eq!(
            observed[0].sandbox,
            Some(SandboxMode::ReadOnly),
            "the retried child inherits the tightened filesystem policy"
        );
        assert_eq!(observed[0].browser_tabs, "pretool-tabs-owner");
    }
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn foreground_task_retries_under_tighter_policy_after_actual_pretool_wait() {
    actual_held_pretool_case(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn background_task_retries_under_tighter_policy_after_actual_pretool_wait() {
    actual_held_pretool_case(true).await;
}
