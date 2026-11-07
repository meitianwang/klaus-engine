//! Defect: task children could replace an admitted browser owner with the session default.
//! Root cause: child setup re-resolved default roots instead of retaining the admitted view.
//! Work item 20261017: selected capability ownership through nested tasks and recovery.

use super::*;
use klaus_agent_core::capabilities::RuntimeCapabilityRoot;
use klaus_agent_core::hooks::types::{
    Hook, HookEvent, HookFn, HookHandler, HookInput, HookOutcome,
};
use klaus_agent_core::planning::CollaborationMode;
use klaus_agent_core::store::session_store::AppendEventOptions;
use klaus_agent_core::tools::builtins::workflow;
use klaus_agent_core::types::tool::SpawnSubAgentOptions;
use klaus_agent_core::types::tool::SubagentMessageRequest;
use tokio::sync::{mpsc, Semaphore};

const CAP_CHILD: &str = "CAPTURED_CAPABILITY_CHILD";
const CAP_GRANDCHILD: &str = "CAPTURED_CAPABILITY_GRANDCHILD";

type CapabilityObservations = Arc<Mutex<Vec<(Option<u32>, Option<String>)>>>;

struct CapabilityProbe {
    schema: JsonSchema,
    observed: CapabilityObservations,
}
#[async_trait]
impl Tool for CapabilityProbe {
    fn static_name(&self) -> &str {
        "capability_probe"
    }
    fn static_description(&self) -> &str {
        "Record the actual admitted host capability owner."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn persistence_identity(&self) -> Option<RuntimeString> {
        Some("fixture:captured-capability-probe:1".into())
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_read_only(&self, _: &Json) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        false
    }
    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let mut observed = self.observed.lock().unwrap();
        assert!(observed.len() < 8);
        observed.push((
            ctx.depth,
            ctx.browser_tool_mapping
                .as_ref()
                .map(|mapping| mapping.tabs.clone()),
        ));
        Ok(text_result(
            input,
            "The admitted capability was inspected.",
            false,
        ))
    }
}
fn probe(observed: CapabilityObservations) -> Arc<dyn Tool> {
    Arc::new(CapabilityProbe {
        schema: json!({"type":"object","properties":{}})
            .as_object()
            .unwrap()
            .clone(),
        observed,
    })
}
fn roots(changed: bool) -> Vec<RuntimeCapabilityRoot> {
    vec![
        RuntimeCapabilityRoot::host("desired").with_browser_tool_mapping(BrowserHostToolMapping {
            tabs: if changed {
                "mcp__desired__changed_tabs"
            } else {
                "mcp__desired__tabs"
            }
            .to_string(),
            ..Default::default()
        }),
        RuntimeCapabilityRoot::host("fallback").with_browser_tool_mapping(BrowserHostToolMapping {
            tabs: "mcp__fallback__tabs".to_string(),
            ..Default::default()
        }),
    ]
}
fn probe_events(id: &str) -> Vec<ModelStreamEvent> {
    let mut events = vec![ModelStreamEvent::MessageStart {
        model: "goal-turn-override".into(),
    }];
    events.extend(call(id, "capability_probe", json!({})));
    events.push(stop(ModelStopReason::ToolUse, 1));
    events
}
fn task_events(id: &str, prompt: &str, background: bool) -> Vec<ModelStreamEvent> {
    let mut events = vec![ModelStreamEvent::MessageStart {
        model: "goal-turn-override".into(),
    }];
    events.extend(call(id, "task", json!({"model": "low", "description":"Inspect admitted browser scope","prompt":prompt,"fork_turns":"none","run_in_background":background})));
    events.push(stop(ModelStopReason::ToolUse, 1));
    events
}
struct BrowserHost {
    name: &'static str,
    server: &'static str,
    calls: Arc<AtomicUsize>,
}
#[async_trait]
impl Tool for BrowserHost {
    fn static_name(&self) -> &str {
        self.name
    }
    fn static_description(&self) -> &str {
        "Inspect the actual selected browser dispatch owner."
    }
    fn input_schema(&self) -> Json {
        runtime_json::parse(&RuntimeString::from("{\"type\":\"object\"}")).unwrap()
    }
    fn persistence_identity(&self) -> Option<RuntimeString> {
        Some(RuntimeString::from(self.name))
    }
    fn static_mcp_server(&self) -> Option<&str> {
        Some(self.server)
    }
    fn exposure(&self) -> klaus_agent_core::types::tool::ToolExposure {
        klaus_agent_core::types::tool::ToolExposure::Hidden
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        false
    }
    fn is_read_only(&self, _: &Json) -> bool {
        true
    }
    async fn call(
        &self,
        input: Json,
        _: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let request: klaus_agent_core::browser::BrowserRuntimeRequest =
            runtime_codec::decode_value(&input).unwrap();
        assert!(matches!(
            request.operation,
            klaus_agent_core::browser::BrowserOperation::Tabs { .. }
        ));
        assert_eq!(
            request.metadata.get(&RuntimeString::from("toolName")),
            Some(&Json::from("browser_tabs"))
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(text_result(
            runtime_codec::encode_value(&klaus_agent_core::browser::BrowserRuntimeResponse {
                message: Some("The selected browser host was invoked.".into()),
                ..Default::default()
            })
            .unwrap(),
            "Browser owner inspected.",
            false,
        ))
    }
}
fn browser_probe_events(id: &str) -> Vec<ModelStreamEvent> {
    let mut events = probe_events(id);
    events.pop();
    events.extend(call(
        &format!("{id}-browser"),
        "browser_tabs",
        json!({"action":"list"}),
    ));
    events.push(stop(ModelStopReason::ToolUse, 1));
    events
}
fn assert_browser_result(request: &ModelRequest, id: &str) {
    assert!(request.messages.iter().any(|message|matches!(message,Message::ToolResult(result) if result.tool_use_id==id&&!result.is_error)),"actual native browser dispatch must return a successful tool result");
    assert!(
        !request
            .tools
            .iter()
            .any(|tool| tool.name.starts_with("mcp__")),
        "implementation browser hosts remain hidden from the model tool surface"
    );
}

struct CapabilityHook {
    entered: Arc<Semaphore>,
    receive: tokio::sync::Mutex<mpsc::Receiver<()>>,
    first: AtomicUsize,
}
#[async_trait]
impl HookFn for CapabilityHook {
    async fn run(&self, _: HookInput) -> HookOutcome {
        if self.first.fetch_add(1, Ordering::SeqCst) == 0 {
            self.entered.add_permits(1);
            self.receive
                .lock()
                .await
                .recv()
                .await
                .expect("bounded task hook release remains owned");
        }
        HookOutcome::default()
    }
}
struct Release(Option<mpsc::Sender<()>>);
impl Release {
    fn release(&mut self) {
        if let Some(send) = self.0.take() {
            send.try_send(()).unwrap();
        }
    }
}
impl Drop for Release {
    fn drop(&mut self) {
        if let Some(send) = self.0.take() {
            let _ = send.try_send(());
        }
    }
}
struct ScopeWorkflowRunner;
#[async_trait]
impl workflow::WorkflowRunner for ScopeWorkflowRunner {
    async fn run<'a>(
        &self,
        input: workflow::WorkflowRunInput,
        deps: workflow::WorkflowRuntimeDeps<'a>,
    ) -> Result<workflow::WorkflowRunResult, RuntimeString> {
        assert_eq!(input.script, "inspect_original_scope");
        let mut events = (deps.spawn_workflow_agent)(SpawnSubAgentOptions {
            prompt: CAP_CHILD.into(),
            fork_context: Some(klaus_agent_core::types::tool::ForkContext::None),
            ..Default::default()
        });
        let mut count = 0;
        let mut completed = false;
        tokio::time::timeout(WAIT, async {
            while let Some(event) = events.next().await {
                count += 1;
                assert!(count <= 128, "bounded workflow event fixture");
                assert!(!matches!(event, EngineEvent::TurnFailed { .. }), "actual workflow child failed: {event:?}");
                if matches!(event, EngineEvent::ItemCompleted { item: klaus_agent_core::types::events::ThreadItem::AgentMessage { ref text, .. }, .. } if text == "Child inspected scope.") { completed = true; }
            }
        }).await.unwrap();
        assert!(
            completed,
            "the actual workflow child must finish its browser inspection and nested Task"
        );
        Ok(workflow::WorkflowRunResult {
            run_id: "admitted-capability-workflow".into(),
            result: runtime_json::parse(&"{\"scope\":\"inspected\"}".into()).unwrap(),
            meta: workflow::WorkflowMeta {
                name: "Inspect admitted scope".into(),
            },
            agents_spawned: 1,
            output_tokens_spent: 1,
        })
    }
}
fn scope_workflow_tool() -> Arc<dyn Tool> {
    workflow::create_workflow_tool(workflow::WorkflowToolOptions {
        workflows: None,
        allow_inline_script: Some(true),
        runner: Arc::new(ScopeWorkflowRunner),
    })
}
fn workflow_events() -> Vec<ModelStreamEvent> {
    let mut events = vec![ModelStreamEvent::MessageStart {
        model: "goal-turn-override".into(),
    }];
    events.extend(call(
        "scope-workflow",
        "workflow",
        json!({"script":"inspect_original_scope","args":{}}),
    ));
    events.push(stop(ModelStopReason::ToolUse, 1));
    events
}

async fn nested_scope(background: bool, native: bool, workflow: bool) {
    let slot: Slot = Arc::new(Mutex::new(Weak::new()));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let root_calls = Arc::new(AtomicUsize::new(0));
    let child_calls = Arc::new(AtomicUsize::new(0));
    let grand_calls = Arc::new(AtomicUsize::new(0));
    let desired_host_calls = Arc::new(AtomicUsize::new(0));
    let fallback_host_calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Semaphore::new(0));
    let (send, receive) = mpsc::channel(1);
    let hook = Arc::new(CapabilityHook {
        entered: entered.clone(),
        receive: tokio::sync::Mutex::new(receive),
        first: AtomicUsize::new(0),
    });
    let mut release = Release(Some(send));
    let selected_roots = if native {
        native_roots(desired_host_calls.clone(), fallback_host_calls.clone())
    } else {
        roots(false)
    };
    let fixture = Fixture::new(&slot, {let mut tools=vec![probe(observed.clone()),create_task_tool(TaskToolOptions::default())];if workflow {tools.push(scope_workflow_tool());}tools.extend(klaus_agent_core::tools::builtins::browser::create_browser_tools().into_iter().filter(|tool|tool.name()=="browser_tabs"));tools}, {
        let hook = hook.clone();
        let desired_host_calls=desired_host_calls.clone();
        let fallback_host_calls=fallback_host_calls.clone();
        move |options| {
            options.config.as_mut().unwrap()["permissionMode"] = json!("default");
            options.capability_roots = selected_roots;
            options.tools.as_mut().expect("explicit fixture tool set").extend([
                Arc::new(BrowserHost{name:"mcp__desired__tabs",server:"desired",calls:desired_host_calls}) as Arc<dyn Tool>,
                Arc::new(BrowserHost{name:"mcp__fallback__tabs",server:"fallback",calls:fallback_host_calls}) as Arc<dyn Tool>,
            ]);
            options.hooks = vec![Hook { event: HookEvent::PreToolUse, matcher: Some(if workflow {"workflow"} else {"task"}.to_string()), handler: HookHandler::Fn(hook), is_async: false }];
        }
    }, {
        let root_calls=root_calls.clone();let child_calls=child_calls.clone();let grand_calls=grand_calls.clone();
        move |request,_| {
            if directive(&request,CAP_GRANDCHILD) {
                return match grand_calls.fetch_add(1,Ordering::SeqCst) {
                    0=>Box::pin(futures::stream::iter(browser_probe_events("grandchild-scope"))),
                    1=>{assert_browser_result(&request,"grandchild-scope-browser");Box::pin(futures::stream::iter(answer("Grandchild inspected scope.",1)))},
                    n=>panic!("unexpected grandchild request {n}"),
                };
            }
            if directive(&request,CAP_CHILD) {
                return match child_calls.fetch_add(1,Ordering::SeqCst) {
                    0=>{let mut events=browser_probe_events("child-scope");events.pop();events.extend(call("nested-scope-task","task",json!({"model": "low", "description":"Inspect nested scope","prompt":CAP_GRANDCHILD,"fork_turns":"none","run_in_background":false})));events.push(stop(ModelStopReason::ToolUse,1));Box::pin(futures::stream::iter(events))},
                    1=>{assert_browser_result(&request,"child-scope-browser");Box::pin(futures::stream::iter(answer("Child inspected scope.",1)))},
                    n=>panic!("unexpected child request {n}"),
                };
            }
            match root_calls.fetch_add(1,Ordering::SeqCst) {
                0=>Box::pin(futures::stream::iter(if workflow { workflow_events() } else { task_events("captured-scope-task",CAP_CHILD,background) })),
                1=>Box::pin(futures::stream::iter(answer("Parent finished scope delegation.",1))),
                n=>panic!("unexpected root request {n}"),
            }
        }
    }).await;
    if workflow {
        fixture.session.apply_permission_update(
            klaus_agent_core::types::permission::PermissionUpdate::AddRules {
                rules: klaus_agent_core::permission::rules::parse_rule_strings(&["task".into()]),
                behavior: klaus_agent_core::types::permission::PermissionBehavior::Allow,
                destination: klaus_agent_core::types::permission::PermissionDestination::Session,
            },
        );
    }
    fixture
        .session
        .set_thread_goal(
            Some("Keep the admitted browser owner through delegation.".into()),
            None,
            None,
        )
        .await
        .unwrap();
    let turn = fixture.session.run_streamed(
        Input::Text("Inspect the selected owner.".into()),
        TurnOptions {
            selected_capability_roots: Some(vec![SelectedRuntimeCapabilityRoot::host("desired")]),
            ..Default::default()
        },
    );
    let events = {
        let root = collect(&fixture, turn);
        tokio::pin!(root);
        tokio::select! {
            permit=tokio::time::timeout(WAIT,entered.acquire_owned())=>{permit.unwrap().unwrap().forget();}
            events=root.as_mut()=>panic!("task ended before actual pretool barrier: {events:?}"),
        }
        assert_eq!(child_calls.load(Ordering::SeqCst), 0);
        assert_eq!(grand_calls.load(Ordering::SeqCst), 0);
        assert!(fixture
            .session
            .subagent_lifecycle_snapshot()
            .agents
            .is_empty());
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
    .unwrap();
    assert_eq!(root_calls.load(Ordering::SeqCst), 2);
    assert_eq!(child_calls.load(Ordering::SeqCst), 2);
    assert_eq!(grand_calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.requests.lock().unwrap().len(), 6);
    let mut actual = observed.lock().unwrap().clone();
    actual.sort_by_key(|entry| entry.0);
    let expected = if native {
        None
    } else {
        Some("mcp__desired__tabs".to_string())
    };
    assert_eq!(
        actual,
        vec![(Some(1), expected.clone()), (Some(2), expected)]
    );
    assert_eq!(desired_host_calls.load(Ordering::SeqCst), 2);
    assert_eq!(fallback_host_calls.load(Ordering::SeqCst), 0);
    fixture.finish().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn foreground_task_and_nested_task_retain_selected_owner_across_real_pretool_wait() {
    nested_scope(false, false, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn background_task_and_nested_task_retain_selected_owner_across_real_pretool_wait() {
    nested_scope(true, false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn foreground_task_and_nested_task_keep_exact_native_executor_across_pretool_wait() {
    nested_scope(false, true, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn background_task_and_nested_task_keep_exact_native_executor_across_pretool_wait() {
    nested_scope(true, true, false).await;
}

struct SelectedNativeBrowser(Arc<AtomicUsize>);
// This fixture executor implements its response in process and has no process effects.
#[async_trait]
unsafe impl klaus_agent_core::browser::BrowserExecutor for SelectedNativeBrowser {
    async fn execute(
        &self,
        request: klaus_agent_core::browser::BrowserRuntimeRequest,
        _: AbortSignal,
        _: klaus_agent_core::browser::BrowserEventSink,
    ) -> Result<
        klaus_agent_core::browser::BrowserRuntimeResponse,
        klaus_agent_core::browser::BrowserRuntimeError,
    > {
        assert!(matches!(
            request.operation,
            klaus_agent_core::browser::BrowserOperation::Tabs { .. }
        ));
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(klaus_agent_core::browser::BrowserRuntimeResponse {
            message: Some("The original selected native executor answered.".into()),
            ..Default::default()
        })
    }
}
fn native_roots(
    desired: Arc<AtomicUsize>,
    fallback: Arc<AtomicUsize>,
) -> Vec<RuntimeCapabilityRoot> {
    let register = |calls| {
        // This executor cannot create, control, or delegate a local process.
        unsafe {
            klaus_agent_core::browser::attest_spawn_free(Arc::new(SelectedNativeBrowser(calls)))
        }
    };
    vec![
        RuntimeCapabilityRoot::host("desired").with_browser_runtime(register(desired)),
        RuntimeCapabilityRoot::host("fallback").with_browser_runtime(register(fallback)),
    ]
}

struct OpaqueBrowserExecutor(Arc<AtomicUsize>);
// This fixture executor neither creates nor controls local processes.
#[async_trait]
unsafe impl klaus_agent_core::browser::BrowserExecutor for OpaqueBrowserExecutor {
    async fn execute(
        &self,
        _: klaus_agent_core::browser::BrowserRuntimeRequest,
        _: AbortSignal,
        _: klaus_agent_core::browser::BrowserEventSink,
    ) -> Result<
        klaus_agent_core::browser::BrowserRuntimeResponse,
        klaus_agent_core::browser::BrowserRuntimeError,
    > {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("unproved recovered browser authority must never execute")
    }
}
fn opaque_roots(calls: Arc<AtomicUsize>) -> Vec<RuntimeCapabilityRoot> {
    // The executor has no process effects, satisfying the registration contract.
    let runtime = unsafe {
        klaus_agent_core::browser::attest_spawn_free(Arc::new(OpaqueBrowserExecutor(calls)))
    };
    vec![RuntimeCapabilityRoot::host("desired").with_browser_runtime(runtime)]
}

struct ColdMutation(Arc<AtomicUsize>);
#[async_trait]
impl Tool for ColdMutation {
    fn static_name(&self) -> &str {
        "write"
    }
    fn static_description(&self) -> &str {
        "A mutation forbidden by the current planning query."
    }
    fn input_schema(&self) -> Json {
        runtime_json::parse(
            &"{\"type\":\"object\",\"properties\":{},\"additionalProperties\":false}".into(),
        )
        .unwrap()
    }
    fn persistence_identity(&self) -> Option<RuntimeString> {
        Some("fixture:cold-plan-mutation:1".into())
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_read_only(&self, _: &Json) -> bool {
        false
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        false
    }
    async fn call(
        &self,
        _: Json,
        _: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("a cold planning query must reject the mutation before execution")
    }
}
fn assert_collaboration_view(request: &ModelRequest, mode: CollaborationMode) {
    let actual = request
        .system
        .iter()
        .filter(|part| part.name == "collaboration-mode")
        .collect::<Vec<_>>();
    assert_eq!(
        actual.len(),
        1,
        "one complete current collaboration instruction view"
    );
    let expected = klaus_agent_core::planning::instructions::instruction_part(
        &klaus_agent_core::schemas::config::default_config(),
        mode,
    );
    assert_eq!(
        runtime_codec::encode_value(actual[0]).unwrap(),
        runtime_codec::encode_value(&expected).unwrap()
    );
    assert_eq!(
        request.tools.iter().any(|tool| tool.name == "write"),
        mode == CollaborationMode::Default
    );
}
fn reordered_members(value: &Json, depth: usize) -> Json {
    assert!(depth <= 16, "bounded known capability evidence shape");
    match value {
        Json::Object(fields) => {
            assert!(fields.len() <= 64);
            let mut result = klaus_agent_core::runtime_values::RuntimeObject::new();
            for (key, value) in fields.entries().collect::<Vec<_>>().into_iter().rev() {
                result.insert(key.clone(), reordered_members(value, depth + 1));
            }
            Json::Object(result)
        }
        Json::Array(values) => {
            assert!(values.len() <= 64);
            Json::Array(
                values
                    .iter()
                    .map(|value| reordered_members(value, depth + 1))
                    .collect(),
            )
        }
        _ => value.clone(),
    }
}

#[derive(Clone, Copy)]
enum Recovery {
    Selected,
    Empty,
    Changed,
    Missing,
    Opaque,
    Plan,
}
async fn recover_scope(case: Recovery) {
    let slot: Slot = Arc::new(Mutex::new(Weak::new()));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let child_calls = Arc::new(AtomicUsize::new(0));
    let root_calls = Arc::new(AtomicUsize::new(0));
    let native_calls = Arc::new(AtomicUsize::new(0));
    let mutations = Arc::new(AtomicUsize::new(0));
    let initial_roots = match case {
        Recovery::Empty => Vec::new(),
        Recovery::Opaque => opaque_roots(native_calls.clone()),
        _ => roots(false),
    };
    let mut tools = vec![
        probe(observed.clone()),
        create_task_tool(TaskToolOptions::default()),
    ];
    if matches!(case, Recovery::Plan) {
        tools.push(Arc::new(ColdMutation(mutations.clone())));
    }
    let fixture = Fixture::new(
        &slot,
        tools.clone(),
        move |options| {
            options.capability_roots = initial_roots;
        },
        {
            let child_calls = child_calls.clone();
            let root_calls = root_calls.clone();
            move |request, _| {
                if directive(&request, CAP_CHILD) {
                    return match child_calls.fetch_add(1, Ordering::SeqCst) {
                        0 => {
                            if matches!(case, Recovery::Plan) {
                                assert_collaboration_view(&request, CollaborationMode::Default);
                            }
                            Box::pin(futures::stream::iter(answer(
                                "Saved child capability scope.",
                                1,
                            )))
                        }
                        1 => {
                            let mut events = probe_events("recovered-scope");
                            if matches!(case, Recovery::Plan) {
                                assert_collaboration_view(&request, CollaborationMode::Plan);
                                events.pop();
                                events.extend(call("cold-forbidden-mutation", "write", json!({})));
                                events.push(stop(ModelStopReason::ToolUse, 1));
                            }
                            Box::pin(futures::stream::iter(events))
                        }
                        2 => Box::pin(futures::stream::iter(answer("Recovered child scope.", 1))),
                        n => panic!("unexpected recovery child request {n}"),
                    };
                }
                match root_calls.fetch_add(1, Ordering::SeqCst) {
                    0 => Box::pin(futures::stream::iter(task_events(
                        "saved-scope-task",
                        CAP_CHILD,
                        false,
                    ))),
                    1 => Box::pin(futures::stream::iter(answer(
                        "Parent saved delegated scope.",
                        1,
                    ))),
                    n => panic!("unexpected recovery root request {n}"),
                }
            }
        },
    )
    .await;
    let events = collect(
        &fixture,
        fixture.session.run_streamed(
            Input::Text("Save an admitted task.".into()),
            TurnOptions {
                selected_capability_roots: if matches!(case, Recovery::Empty) {
                    None
                } else {
                    Some(vec![SelectedRuntimeCapabilityRoot::host("desired")])
                },
                ..Default::default()
            },
        ),
    )
    .await;
    assert_success(&events);
    assert_eq!(fixture.requests.lock().unwrap().len(), 3);
    let lifecycle = fixture.session.subagent_lifecycle_snapshot();
    assert_eq!(lifecycle.agents.len(), 1);
    let mut id = lifecycle.agents[0].agent_id.clone();
    let thread = fixture.session.id.try_to_utf8().unwrap();
    fixture.session.shutdown_and_wait().await.unwrap();
    let saved = fixture
        .store
        .read_sub_agent(&thread, &id.try_to_utf8().unwrap())
        .await
        .unwrap();
    let original_creation = saved
        .raw_items
        .iter()
        .find_map(|item| match item {
            RolloutItem::Event(event) if event.event == "ordinary_subagent_creation" => {
                event.payload.clone()
            }
            _ => None,
        })
        .unwrap();
    if matches!(case, Recovery::Plan) {
        assert!(original_creation
            .get("baseInstructions")
            .and_then(Json::as_array)
            .is_some_and(|parts| !parts.is_empty()));
        assert!(
            original_creation.get("collaborationOrigin").is_some(),
            "new writer must retain the captured original collaboration origin"
        );
        assert!(original_creation.get("permissionOrigin").is_some());
    }
    let capture = saved
        .raw_items
        .iter()
        .find_map(|item| match item {
            RolloutItem::Event(event) if event.event == "ordinary_subagent_creation" => event
                .payload
                .as_ref()
                .and_then(|p| p.get("runtimeCapabilityCapture")),
            _ => None,
        })
        .expect("actual creation persists its admitted host scope");
    assert_eq!(
        capture
            .get("selectedRoots")
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        if matches!(case, Recovery::Empty) {
            0
        } else {
            1
        }
    );
    if matches!(case, Recovery::Selected) {
        let mut metadata = original_creation.clone();
        let original = metadata.get("runtimeCapabilityCapture").unwrap().clone();
        let reordered = reordered_members(&original, 0);
        assert_ne!(
            original, reordered,
            "the serialized evidence order must actually change"
        );
        assert!(
            original.semantic_eq_unordered(&reordered),
            "every key, value and array position remains exact"
        );
        metadata
            .as_object_mut()
            .unwrap()
            .insert("runtimeCapabilityCapture", reordered);
        let key = format!("{}-reordered", id.try_to_utf8().unwrap());
        let writer = fixture
            .store
            .open_sub_agent(&thread, &key, saved.meta.clone().unwrap())
            .await
            .unwrap();
        assert!(
            saved.raw_items.iter().any(|item| matches!(item,
                RolloutItem::Event(event)
                    if event.event == "ordinary_subagent_creation_committed"
                    && event.payload.as_ref().and_then(Json::as_bool) == Some(true)
            )),
            "the source fixture must contain a genuine accepted creation"
        );
        for item in &saved.raw_items {
            match item {
                RolloutItem::Event(event) => {
                    writer
                        .append_event(
                            event.event.clone(),
                            Some(AppendEventOptions {
                                turn_id: event.turn_id.clone(),
                                payload: if event.event == "ordinary_subagent_creation" {
                                    Some(metadata.clone())
                                } else {
                                    event.payload.clone()
                                },
                            }),
                        )
                        .await
                }
                RolloutItem::Message { message } => writer.append_message(message.clone()).await,
                RolloutItem::TurnContext { ctx } => writer.append_turn_context(ctx.clone()).await,
                RolloutItem::UsageLedger(usage) => writer.append_usage(usage.clone()).await,
                RolloutItem::SubagentLifecycle(item) => {
                    writer.append_subagent_lifecycle(item.record.clone()).await
                }
                RolloutItem::BackgroundProcessLifecycle(item) => {
                    writer
                        .append_background_process_lifecycle(item.record.clone())
                        .await
                }
                RolloutItem::SessionMeta { .. } => {}
                other => {
                    panic!("the bounded recovery fixture must not discard a record: {other:?}")
                }
            }
        }
        writer.flush().await.unwrap();
        writer.shutdown().await.unwrap();
        id = key.into();
    }
    let next_roots = match case {
        Recovery::Opaque => opaque_roots(native_calls.clone()),
        Recovery::Changed => roots(true),
        Recovery::Missing => vec![roots(false).remove(1)],
        Recovery::Selected | Recovery::Empty | Recovery::Plan => roots(false),
    };
    let mut cold_options =
        super::super::r7c876d49_spec_restores_latest_permission_policy::agent_options(
            fixture.process.clone(),
        );
    cold_options.provider = fixture.provider.clone();
    cold_options.tools = Some(tools);
    cold_options.compactor = Some(Arc::new(NoopCompactor));
    cold_options.cwd = Some(fixture._home.path.to_string_lossy().into_owned());
    cold_options.capability_roots = next_roots;
    let cold_agent = Agent::new(cold_options);
    if matches!(case, Recovery::Missing) {
        let capabilities = cold_agent.runtime_capability_descriptors();
        assert!(
            capabilities
                .iter()
                .all(|entry| entry.source.id != "desired"),
            "the restored agent must actually lack the original host"
        );
        assert!(
            capabilities
                .iter()
                .any(|entry| entry.source.id == "fallback"),
            "the alternative host remains available while original-owner restoration is refused"
        );
    }
    let cold = cold_agent
        .resume_session(
            fixture.store.clone(),
            &thread,
            StartSessionOptions {
                user_config_dir: Some(fixture._home.path.to_string_lossy().to_string().into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let ordinary_preset = cold.runtime_policy_state().sandbox_mode;
    if matches!(case, Recovery::Plan) {
        assert_eq!(
            ordinary_preset,
            Some(klaus_agent_core::types::permission::SandboxMode::DangerFullAccess)
        );
        assert_eq!(
            cold.runtime_policy_state().approval_policy,
            Some(klaus_agent_core::types::permission::ApprovalPolicy::Never)
        );
        cold.planning_runtime()
            .set_mode(CollaborationMode::Plan, None)
            .unwrap();
        assert_eq!(cold.collaboration_mode(), CollaborationMode::Plan);
        assert_eq!(cold.runtime_policy_state().sandbox_mode, ordinary_preset);
    }
    let reply = cold
        .send_subagent_message(SubagentMessageRequest {
            target: id.clone(),
            message: "Inspect the saved scope after restart.".into(),
            summary: "Inspect recovered scope".into(),
            sender_agent_id: None,
            trigger_turn: true,
            user_initiated: true,
        })
        .await
        .unwrap();
    if matches!(
        case,
        Recovery::Changed | Recovery::Missing | Recovery::Opaque
    ) {
        let error = reply.expect_err("unavailable or changed original owner cannot fall back");
        let expected_error = if matches!(case, Recovery::Opaque) {
            "The original opaque browser runtime cannot be proved after restart."
        } else {
            "The original host capability roots are unavailable or changed."
        };
        assert_eq!(error, expected_error);
        assert_eq!(native_calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.requests.lock().unwrap().len(), 3);
        assert_eq!(child_calls.load(Ordering::SeqCst), 1);
        assert!(observed.lock().unwrap().is_empty());
    } else {
        assert!(reply.unwrap().resumed);
        tokio::time::timeout(WAIT,async {loop {
            let agents=cold.subagent_lifecycle_snapshot().agents;
            if child_calls.load(Ordering::SeqCst) == 3 && agents.iter().any(|agent|agent.agent_id==id&&agent.status==klaus_agent_core::engine::subagent_lifecycle::SubagentStatus::Completed){break;}
            tokio::task::yield_now().await;
        }}).await.unwrap();
        assert_eq!(fixture.requests.lock().unwrap().len(), 5);
        let expected = if matches!(case, Recovery::Empty) {
            None
        } else {
            Some("mcp__desired__tabs".to_string())
        };
        assert_eq!(*observed.lock().unwrap(), vec![(Some(1), expected)]);
    }
    if matches!(case, Recovery::Plan) {
        assert_eq!(cold.runtime_policy_state().sandbox_mode, ordinary_preset);
        assert_eq!(mutations.load(Ordering::SeqCst), 0);
        {
            let requests = fixture.requests.lock().unwrap();
            let children = requests
                .iter()
                .filter(|request| directive(request, CAP_CHILD))
                .collect::<Vec<_>>();
            assert_eq!(children.len(), 3);
            assert_collaboration_view(children[0], CollaborationMode::Default);
            for request in &children[1..] {
                assert_collaboration_view(request, CollaborationMode::Plan);
            }
            assert!(children[2].messages.iter().any(|message| matches!(message, Message::ToolResult(result) if result.tool_use_id == "cold-forbidden-mutation" && result.is_error)), "actual next provider request must include the mutation refusal");
        }
        cold.shutdown_and_wait().await.unwrap();
        let final_saved = fixture
            .store
            .read_sub_agent(&thread, &id.try_to_utf8().unwrap())
            .await
            .unwrap();
        let creations = final_saved
            .raw_items
            .iter()
            .filter_map(|item| match item {
                RolloutItem::Event(event) if event.event == "ordinary_subagent_creation" => {
                    event.payload.as_ref()
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            creations.len(),
            1,
            "new query cannot rewrite original creation authority"
        );
        for key in [
            "baseInstructions",
            "collaborationOrigin",
            "permissionOrigin",
        ] {
            assert_eq!(
                creations[0].get(key),
                original_creation.get(key),
                "original authority must remain unchanged: {key}"
            );
        }
    } else {
        cold.dispose().await;
    }
    fixture.finish().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_task_restores_saved_selected_owner_instead_of_current_default() {
    recover_scope(Recovery::Selected).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_empty_admitted_scope_cannot_acquire_new_default_roots() {
    recover_scope(Recovery::Empty).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_task_rejects_changed_original_owner_before_provider_entry() {
    recover_scope(Recovery::Changed).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_task_rejects_missing_original_owner_without_fallback() {
    recover_scope(Recovery::Missing).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_task_rejects_unproved_native_browser_identity_before_provider_or_executor() {
    recover_scope(Recovery::Opaque).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_full_access_child_enters_current_plan_without_rewriting_creation_authority() {
    recover_scope(Recovery::Plan).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_workflow_and_nested_task_retain_selected_mapping_after_pretool_wait() {
    nested_scope(false, false, true).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_workflow_and_nested_task_retain_exact_native_executor_after_pretool_wait() {
    nested_scope(false, true, true).await;
}
