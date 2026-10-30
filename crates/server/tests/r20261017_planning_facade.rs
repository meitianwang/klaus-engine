//! Planning controls and receipts must survive named-facade retries and restart.
//! Root cause: client acknowledgments cannot serve as durable execution admission.
//! Work item: 20261017 structured planning and implementation receipt recovery.

use super::*;
use crate::app_server::wire;
use klaus_agent_core::planning::runtime::PlanningRuntime;
use klaus_agent_core::planning::CollaborationMode;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::content::OutputBlock;
use klaus_agent_core::schemas::message::Message;
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::types::errors::ToolError;
use klaus_agent_core::types::events::ToolDomainEvent;
use klaus_agent_core::types::json::{Json, JsonSchema};
use klaus_agent_core::types::provider::ModelRequest;
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tokio::sync::Notify;

#[derive(Clone)]
struct ExpectedImplementation {
    root: PathBuf,
    thread_id: String,
    implementation_id: String,
    input: String,
}

#[derive(Default)]
struct Probe {
    creates: AtomicUsize,
    calls: AtomicUsize,
    tool_runs: AtomicUsize,
    requests: Mutex<Vec<ModelRequest>>,
    expected_implementation: Mutex<Option<ExpectedImplementation>>,
    called: Notify,
}

struct RecordImplementationTool {
    probe: Arc<Probe>,
    schema: JsonSchema,
}

#[async_trait::async_trait]
impl Tool for RecordImplementationTool {
    fn static_name(&self) -> &str {
        "record_implementation"
    }

    fn static_description(&self) -> &str {
        "Record one implementation action for the facade regression."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }

    async fn call(
        &self,
        _input: Json,
        context: &ToolContext,
        on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.probe.tool_runs.fetch_add(1, Ordering::SeqCst);
        let event = ToolDomainEvent::Status {
            message: "Implementation action recorded".into(),
        };
        if let Some(on_progress) = on_progress {
            on_progress(event).await;
        } else {
            (context.emit)(event).await;
        }
        Ok(text_result(RuntimeJson::Null, "Recorded", false))
    }
}

fn runtime(root: &Path, thread: &str) -> PlanningRuntime {
    PlanningRuntime::new(thread.into(), Some(root.to_path_buf()), false, false)
}

fn seed_plan(root: &Path, thread: &str, content: &str) -> (String, u64) {
    let runtime = runtime(root, thread);
    runtime.set_mode(CollaborationMode::Plan, None).unwrap();
    let plan_id: RuntimeString = format!("{thread}-proposal").into();
    runtime
        .start_plan(plan_id.clone(), "proposal-source-turn".into())
        .unwrap();
    runtime.append_plan(&plan_id, content).unwrap();
    let plan = runtime.finish_plan(&plan_id, true).unwrap();
    (plan.id.try_to_utf8().unwrap(), plan.revision)
}

fn tool_turn(name: &str, input: Value) -> Vec<ModelStreamEvent> {
    let input = runtime_json::parse(&input.to_string().into()).unwrap();
    let mut events = vec![ev::message_start(None)];
    events.extend(ev::tool_use("planning-facade-tool-call", name, &input));
    events.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    events
}

fn factory(root: &Path, probe: Arc<Probe>, replies: Vec<Vec<ModelStreamEvent>>) -> CreateSession {
    let replies = Arc::new(Mutex::new(VecDeque::from(replies)));
    let provider_probe = probe.clone();
    let agent = make_agent_with_call_model_and_config(
        Box::new(move |request, _signal| {
            if let Some(expected) = provider_probe
                .expected_implementation
                .lock()
                .unwrap()
                .clone()
            {
                let saved = runtime(&expected.root, &expected.thread_id)
                    .snapshot()
                    .unwrap();
                let receipt = saved.implementation.as_ref().unwrap();
                assert_eq!(
                    receipt.implementation_id,
                    RuntimeString::from(expected.implementation_id.as_str())
                );
                assert_eq!(
                    receipt.admission, "started",
                    "receipt must be durable before provider execution"
                );
                assert_eq!(receipt.input, RuntimeString::from(expected.input.as_str()));
                assert_eq!(saved.plans.last().unwrap().status, "accepted");
                assert!(request.messages.iter().any(|message| matches!(message,
                    Message::User(user) if user.content.iter().any(|block| matches!(block,
                        klaus_agent_core::schemas::content::ContentBlock::Text(text) if text.text == expected.input.as_str()
                    ))
                )), "the provider must receive the exact reviewed implementation input");
            }
            provider_probe.requests.lock().unwrap().push(request);
            provider_probe.calls.fetch_add(1, Ordering::SeqCst);
            provider_probe.called.notify_one();
            let reply = replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected duplicate provider execution");
            Box::pin(futures::stream::iter(reply))
        }),
        Some(
            json!({"model":"test-model","autoMemory":false,"agentMemory":false,"autoDream":false,"loopDetection":false,"prewarm":false}),
        ),
    );
    let root = root.to_path_buf();
    Arc::new(move |context: SessionContext| {
        let agent = agent.clone();
        let root = root.clone();
        let probe = probe.clone();
        Box::pin(async move {
            probe.creates.fetch_add(1, Ordering::SeqCst);
            let tool = Arc::new(RecordImplementationTool {
                probe,
                schema: json!({"type":"object","properties":{},"additionalProperties":false})
                    .as_object()
                    .unwrap()
                    .clone(),
            });
            Ok::<Arc<Session>, BoxError>(agent.start_session(StartSessionOptions {
                id: context.init.thread_id,
                cwd: Some(root.to_string_lossy().into_owned().into()),
                user_config_dir: Some(root.to_string_lossy().into_owned().into()),
                durable_permissions: Some(context.durable_permissions),
                approval_handler: Some(context.approval_handler),
                elicit_handler: Some(context.elicit_handler),
                mcp_elicitation_presenter: Some(context.mcp_elicitation_presenter),
                permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                tools: Some(vec![tool]),
                ..Default::default()
            }))
        })
    })
}

async fn endpoint(root: &Path, create: CreateSession) -> TestClient {
    let mut client = TestClient::start_full(
        create,
        None,
        Some(root.join("permissions.json")),
        Some(root.join("config.json")),
    );
    client.initialize().await;
    client
}

async fn close_endpoint(mut client: TestClient) {
    tokio::io::AsyncWriteExt::shutdown(&mut client.writer)
        .await
        .expect("the client must close its request direction");
    drop(client.writer);
    tokio::time::timeout(REPLY_TIMEOUT, client.server)
        .await
        .expect("the endpoint must stop before another endpoint starts")
        .expect("the endpoint task must finish without a panic");
    drop(client.lines);
}

async fn request(client: &mut TestClient, id: u64, method: &str, params: Value) -> Value {
    client
        .send(json!({"id":id,"method":method,"params":params}))
        .await;
    client.response_for(id).await.0
}

fn result(response: &Value) -> &Value {
    assert!(response.get("error").is_none(), "{response}");
    let value = response.get("result").unwrap();
    assert!(value.get("error").is_none(), "{response}");
    if value.get("planning").is_some() {
        serde_json::from_value::<wire::PlanningResult>(value.clone())
            .expect("a real response obeys the published planning result type");
    }
    value
}

fn runtime_error(response: &Value) -> &str {
    assert!(response.get("error").is_none(), "{response}");
    response
        .pointer("/result/error")
        .and_then(Value::as_str)
        .expect("a rejected runtime operation returns its named error result")
}

async fn start_thread(client: &mut TestClient, thread: &str) -> String {
    let response = request(
        client,
        2,
        "session/start",
        json!({"init":{"threadId":thread}}),
    )
    .await;
    result(&response)["sessionId"].as_str().unwrap().to_owned()
}

async fn wait_turn(
    client: &mut TestClient,
    submission: &str,
    mut frames: Vec<Value>,
) -> Vec<Value> {
    while !frames
        .iter()
        .any(|frame| frame["method"] == "turn/completed" && frame["params"]["turnId"] == submission)
    {
        frames.push(client.next_message().await);
    }
    frames
}

async fn wait_calls(probe: &Probe, count: usize) {
    loop {
        let called = probe.called.notified();
        if probe.calls.load(Ordering::SeqCst) >= count {
            return;
        }
        tokio::time::timeout(REPLY_TIMEOUT, called)
            .await
            .expect("provider request within timeout");
    }
}

fn save_probe(label: &str, probe: &Probe) {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/planning-regression-snapshots");
    std::fs::create_dir_all(&directory).unwrap();
    let requests = probe.requests.lock().unwrap();
    let encoded = runtime_codec::encode_value(&*requests).unwrap();
    std::fs::write(
        directory.join(format!("{label}.json")),
        runtime_json::stringify_pretty(&encoded).to_node_utf8(),
    )
    .unwrap();
}

#[tokio::test]
async fn sleeping_thread_mode_and_document_requests_never_construct_a_session() {
    let root = tempfile::tempdir().unwrap();
    let thread = "sleeping-planning-thread";
    let body =
        "# Saved plan\n\nInspect the interface, implement the change, and verify its behavior.";
    let (plan_id, revision) = seed_plan(root.path(), thread, body);
    let probe = Arc::new(Probe::default());
    let mut client = endpoint(root.path(), factory(root.path(), probe.clone(), Vec::new())).await;
    let read = request(
        &mut client,
        3,
        "session/collaborationMode",
        json!({"threadId":thread}),
    )
    .await;
    assert_eq!(result(&read)["planning"]["mode"], "plan");
    let state_revision = result(&read)["planning"]["revision"].as_u64().unwrap();
    let changed = request(
        &mut client,
        4,
        "session/setCollaborationMode",
        json!({"threadId":thread,"mode":"default","expectedRevision":state_revision}),
    )
    .await;
    assert_eq!(result(&changed)["planning"]["mode"], "default");
    let stale = request(
        &mut client,
        5,
        "session/setCollaborationMode",
        json!({"threadId":thread,"mode":"plan","expectedRevision":state_revision}),
    )
    .await;
    assert!(stale.get("error").is_some(), "{stale}");
    let document = request(
        &mut client,
        6,
        "session/readPlanDocument",
        json!({"threadId":thread,"planId":plan_id,"revision":revision}),
    )
    .await;
    assert_eq!(result(&document)["document"]["content"], body);
    assert_eq!(result(&document)["document"]["revision"], revision);
    let path = result(&document)["document"]["path"].as_str().unwrap();
    assert_eq!(std::fs::read_to_string(path).unwrap(), body);
    let plans = request(&mut client, 7, "session/plans", json!({"threadId":thread})).await;
    assert_eq!(result(&plans)["planning"]["plans"][0]["content"], body);
    assert_eq!(result(&plans)["planning"]["mode"], "default");
    for (index, alias) in ["code", "pair_programming", "execute", "custom"]
        .iter()
        .enumerate()
    {
        let response = request(
            &mut client,
            10 + index as u64,
            "session/setCollaborationMode",
            json!({"threadId":thread,"mode":alias}),
        )
        .await;
        assert_eq!(result(&response)["planning"]["mode"], "default");
    }
    let invalid = request(
        &mut client,
        20,
        "session/setCollaborationMode",
        json!({"threadId":thread,"mode":"unsupported"}),
    )
    .await;
    assert!(invalid.get("error").is_some(), "{invalid}");
    assert_eq!(probe.creates.load(Ordering::SeqCst), 0);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    close_endpoint(client).await;
}

fn planning_question_turn() -> Vec<ModelStreamEvent> {
    tool_turn(
        "request_user_input",
        json!({"questions":[
            {"id":"primary_store","header":"Primary","question":"Where should data live?","options":[
                {"label":"Local (Recommended)","description":"Keep data on the device."},
                {"label":"Remote","description":"Keep data on a server."}
            ]},
            {"id":"backup_store","header":"Backup","question":"Where should data live?","options":[
                {"label":"Local (Recommended)","description":"Keep a local backup."},
                {"label":"Remote","description":"Keep an off-device backup."}
            ]}
        ]}),
    )
}

fn structured_request(frame: &Value) -> Option<&Value> {
    frame
        .pointer("/params/event/elicitation")
        .filter(|request| {
            request
                .pointer("/request/metadata/kind")
                .and_then(Value::as_str)
                == Some("request_user_input")
        })
}

#[tokio::test]
async fn named_answers_validate_ids_and_binding_before_resolving_the_pending_request() {
    let root = tempfile::tempdir().unwrap();
    let thread = "structured-planning-thread";
    runtime(root.path(), thread)
        .set_mode(CollaborationMode::Plan, None)
        .unwrap();
    let probe = Arc::new(Probe::default());
    let mut client = endpoint(
        root.path(),
        factory(
            root.path(),
            probe.clone(),
            vec![planning_question_turn(), text_turn("Answers received.")],
        ),
    )
    .await;
    let session = start_thread(&mut client, thread).await;
    client.send(json!({"id":3,"method":"turn/start","params":{"sessionId":session,"submissionId":"question-submission","input":"Refine the storage plan."}})).await;
    let (ack, mut frames) = client.response_for(3).await;
    result(&ack);
    let question = loop {
        if let Some(request) = frames.iter().find_map(structured_request) {
            break request.clone();
        }
        assert!(
            !frames
                .iter()
                .any(|frame| frame["method"] == "turn/completed"),
            "questions must wait for a valid response"
        );
        frames.push(client.next_message().await);
    };
    assert!(
        !frames
            .iter()
            .any(|frame| frame["method"] == "elicitation/request"),
        "planning questions use their named response path"
    );
    let metadata = &question["request"]["metadata"];
    assert_eq!(metadata["isBlocking"], true);
    assert_eq!(metadata["toolCallId"], "planning-facade-tool-call");
    assert!(metadata["turnId"]
        .as_str()
        .is_some_and(|turn| !turn.is_empty()));
    let base = json!({"sessionId":session,"requestId":question["requestId"],"turnId":metadata["turnId"],"toolCallId":metadata["toolCallId"],"status":"answered","answers":{}});
    let mut invalid = vec![(base.clone(), "An answered request must contain answers")];
    for (answers, error) in [
        (
            json!({"unknown_id":{"answers":["Local"]}}),
            "Answer refers to an unknown question identity",
        ),
        (
            json!({"primary_store":{"answers":[]}}),
            "Question answers must be nonempty",
        ),
        (
            json!({"primary_store":{"answers":["  "]}}),
            "Question answers must be nonempty",
        ),
    ] {
        let mut value = base.clone();
        value["answers"] = answers;
        invalid.push((value, error));
    }
    for (field, value) in [
        ("turnId", "another-turn"),
        ("toolCallId", "another-tool-call"),
    ] {
        let mut request = base.clone();
        request["answers"] = json!({"primary_store":{"answers":["Local"]}});
        request[field] = value.into();
        invalid.push((
            request,
            "Structured input request expired or belongs to another turn",
        ));
    }
    for (index, (params, error)) in invalid.into_iter().enumerate() {
        let id = 10 + index as u64 * 2;
        let rejected = request(&mut client, id, "session/answerUserInput", params).await;
        assert_eq!(runtime_error(&rejected), error, "{rejected}");
        let pending = request(
            &mut client,
            id + 1,
            "session/plans",
            json!({"sessionId":session}),
        )
        .await;
        let pending = &result(&pending)["planning"]["questions"][0];
        assert_eq!(pending["requestId"], question["requestId"]);
        assert_eq!(pending["status"], "pending");
        assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
    }
    let answers = json!({"primary_store":{"answers":["Local (Recommended)"]},"backup_store":{"answers":["Remote","Keep commas, and\nnewlines intact"]}});
    let mut correct = base.clone();
    correct["answers"] = answers.clone();
    client
        .send(json!({"id":30,"method":"session/answerUserInput","params":correct}))
        .await;
    let (accepted, observed) = client.response_for(30).await;
    assert_eq!(result(&accepted)["accepted"], true);
    drop(wait_turn(&mut client, "question-submission", observed).await);
    let resolved = request(
        &mut client,
        31,
        "session/plans",
        json!({"sessionId":session}),
    )
    .await;
    assert_eq!(
        result(&resolved)["planning"]["questions"][0]["status"],
        "answered"
    );
    assert_eq!(
        result(&resolved)["planning"]["questions"][0]["answers"],
        answers
    );
    let stale = request(&mut client, 32, "session/answerUserInput", correct).await;
    assert_eq!(
        runtime_error(&stale),
        "Structured input request expired or belongs to another turn"
    );
    assert_eq!(probe.calls.load(Ordering::SeqCst), 2);
    assert_eq!(probe.creates.load(Ordering::SeqCst), 1);
    {
        let requests = probe.requests.lock().unwrap();
        let response = requests[1]
            .messages
            .iter()
            .rev()
            .find_map(|message| match message {
                Message::ToolResult(result) => Some(result),
                _ => None,
            })
            .unwrap();
        assert!(!response.is_error);
        let OutputBlock::Text(text) = &response.content[0] else {
            panic!("answers must be visible in the provider tool result")
        };
        let output: Value = serde_json::from_slice(&text.text.to_node_utf8()).unwrap();
        assert_eq!(output, json!({"answers":answers,"status":"answered"}));
    }
    save_probe("named-question-facade", &probe);
    close_endpoint(client).await;
}

fn submission_error<'a>(frame: &'a Value, submission: &str) -> Option<&'a str> {
    let event = frame.pointer("/params/event")?;
    (event["submissionId"] == submission)
        .then(|| event["error"].as_str())
        .flatten()
}

#[tokio::test]
async fn lost_acknowledgment_and_endpoint_restart_cannot_execute_an_accepted_plan_twice() {
    let root = tempfile::tempdir().unwrap();
    let thread = "implementation-receipt-thread";
    let (plan_id, revision) = seed_plan(
        root.path(),
        thread,
        "# Initial plan\n\nInspect the current behavior.",
    );
    let probe = Arc::new(Probe::default());
    let mut client = endpoint(
        root.path(),
        factory(
            root.path(),
            probe.clone(),
            vec![
                tool_turn("record_implementation", json!({})),
                text_turn("Implementation recorded."),
            ],
        ),
    )
    .await;
    let document = request(
        &mut client,
        3,
        "session/readPlanDocument",
        json!({"threadId":thread,"planId":plan_id,"revision":revision}),
    )
    .await;
    let reviewed = "# Reviewed implementation\n\nPreserve this complete edited content, including the final verification step.";
    std::fs::write(
        result(&document)["document"]["path"].as_str().unwrap(),
        reviewed,
    )
    .unwrap();
    let edited = request(
        &mut client,
        4,
        "session/readPlanDocument",
        json!({"threadId":thread,"planId":plan_id,"revision":revision}),
    )
    .await;
    let revision = result(&edited)["document"]["revision"].as_u64().unwrap();
    assert_eq!(result(&edited)["document"]["content"], reviewed);
    let session = start_thread(&mut client, thread).await;
    let params = json!({"sessionId":session,"planId":plan_id,"revision":revision,"clientRequestId":"reviewed-implementation-action","content":reviewed});
    let mut outdated = params.clone();
    outdated["content"] = "stale document content".into();
    let rejected = request(&mut client, 5, "session/acceptPlan", outdated).await;
    assert_eq!(
        runtime_error(&rejected),
        "The plan document changed; reload it before acceptance"
    );
    let accepted = request(&mut client, 6, "session/acceptPlan", params.clone()).await;
    assert_eq!(result(&accepted)["accepted"], true);
    assert_eq!(result(&accepted)["planning"]["mode"], "default");
    assert_eq!(
        result(&accepted)["planning"]["implementation"]["admission"],
        "pending"
    );
    let input = result(&accepted)["implementationInput"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(input, format!("PLEASE IMPLEMENT THIS PLAN:\n{reviewed}"));
    let implementation = result(&accepted)["implementationId"]
        .as_str()
        .unwrap()
        .to_owned();
    let duplicate = request(&mut client, 7, "session/acceptPlan", params.clone()).await;
    assert_eq!(result(&duplicate)["accepted"], false);
    assert_eq!(result(&duplicate)["implementationId"], implementation);
    assert_eq!(result(&duplicate)["implementationInput"], input);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    *probe.expected_implementation.lock().unwrap() = Some(ExpectedImplementation {
        root: root.path().to_path_buf(),
        thread_id: thread.into(),
        implementation_id: implementation.clone(),
        input: input.clone(),
    });
    client.send(json!({"id":100,"method":"turn/start","params":{"sessionId":session,"submissionId":implementation,"input":input}})).await;
    // Deliberately discard the transport before reading the turn acknowledgment.
    // No host settlement is sent; the engine's durable admission is authoritative.
    wait_calls(&probe, 2).await;
    assert_eq!(probe.tool_runs.load(Ordering::SeqCst), 1);
    close_endpoint(client).await;
    let saved = runtime(root.path(), thread).snapshot().unwrap();
    assert_eq!(saved.implementation.as_ref().unwrap().admission, "started");
    assert_eq!(saved.plans.last().unwrap().status, "accepted");
    let mut restarted =
        endpoint(root.path(), factory(root.path(), probe.clone(), Vec::new())).await;
    let sleeping = request(
        &mut restarted,
        3,
        "session/plans",
        json!({"threadId":thread}),
    )
    .await;
    assert_eq!(
        result(&sleeping)["planning"]["implementation"]["implementationId"],
        implementation
    );
    assert_eq!(
        result(&sleeping)["planning"]["implementation"]["admission"],
        "started"
    );
    assert_eq!(probe.creates.load(Ordering::SeqCst), 1);
    let resumed_session = start_thread(&mut restarted, thread).await;
    let mut retry = params.clone();
    retry["sessionId"] = resumed_session.clone().into();
    let recovered = request(&mut restarted, 4, "session/acceptPlan", retry.clone()).await;
    assert_eq!(result(&recovered)["accepted"], false);
    assert_eq!(result(&recovered)["implementationId"], implementation);
    restarted.send(json!({"id":5,"method":"turn/start","params":{"sessionId":resumed_session,"submissionId":implementation,"input":input}})).await;
    let (ack, mut frames) = restarted.response_for(5).await;
    result(&ack);
    while !frames
        .iter()
        .any(|frame| submission_error(frame, &implementation).is_some())
    {
        frames.push(restarted.next_message().await);
    }
    assert!(
        frames
            .iter()
            .filter_map(|frame| submission_error(frame, &implementation))
            .any(|error| error.contains("already been admitted")),
        "{frames:?}"
    );
    assert!(!frames
        .iter()
        .any(|frame| frame["method"] == "turn/completed"
            && frame["params"]["turnId"] == implementation));
    let rollback = request(&mut restarted, 6, "session/settlePlanImplementation", json!({"sessionId":resumed_session,"planId":plan_id,"revision":revision,"clientRequestId":"reviewed-implementation-action","outcome":"failed"})).await;
    assert!(rollback.get("result").is_none(), "{rollback}");
    assert_eq!(
        rollback["error"],
        json!({"code":-32000,"message":"An admitted implementation cannot be rolled back"})
    );
    retry["clientRequestId"] = "another-implementation-action".into();
    let replacement = request(&mut restarted, 7, "session/acceptPlan", retry).await;
    assert_eq!(
        runtime_error(&replacement),
        "This plan is already implemented or implementation has started"
    );
    let final_state = request(
        &mut restarted,
        8,
        "session/plans",
        json!({"sessionId":resumed_session}),
    )
    .await;
    assert_eq!(
        result(&final_state)["planning"]["implementation"]["implementationId"],
        implementation
    );
    assert_eq!(
        result(&final_state)["planning"]["implementation"]["admission"],
        "started"
    );
    assert_eq!(
        result(&final_state)["planning"]["plans"][0]["status"],
        "accepted"
    );
    assert_eq!(probe.calls.load(Ordering::SeqCst), 2);
    assert_eq!(probe.tool_runs.load(Ordering::SeqCst), 1);
    assert_eq!(probe.creates.load(Ordering::SeqCst), 2);
    save_probe("implementation-receipt-facade", &probe);
    close_endpoint(restarted).await;
}

#[test]
fn planning_schema_and_answer_types_publish_stable_id_array_contracts() {
    let bundle = schema_bundle();
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/planning-regression-snapshots");
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
        directory.join("app-server-schema.json"),
        serde_json::to_vec_pretty(&bundle).unwrap(),
    )
    .unwrap();
    for method in [
        "session/collaborationMode",
        "session/setCollaborationMode",
        "session/plans",
        "session/readPlanDocument",
        "session/savePlanDocument",
        "session/acceptPlan",
        "session/planFeedback",
        "session/cancelPlanImplementation",
        "session/settlePlanImplementation",
        "session/answerUserInput",
    ] {
        assert!(bundle["requests"][method]["params"].is_object(), "{method}");
        assert!(bundle["requests"][method]["result"].is_object(), "{method}");
    }
    let answer_schema = &bundle["requests"]["session/answerUserInput"]["params"];
    let mut answer = &answer_schema["properties"]["answers"]["additionalProperties"];
    while let Some(reference) = answer.get("$ref").and_then(Value::as_str) {
        answer = answer_schema
            .pointer(reference.strip_prefix('#').unwrap())
            .unwrap();
    }
    assert_eq!(answer["properties"]["answers"]["type"], "array");
    assert_eq!(answer["properties"]["answers"]["items"]["type"], "string");
    let document = &bundle["requests"]["session/readPlanDocument"]["params"];
    assert!(document["properties"].get("threadId").is_some());
    assert!(!document["required"]
        .as_array()
        .unwrap()
        .iter()
        .any(|field| field == "sessionId"));
    let raw = json!({"sessionId":"s","requestId":"r","turnId":"t","toolCallId":"c","status":"answered","answers":{"stable_question":{"answers":["A, B","Second\nline"]}}});
    let decoded: wire::SessionAnswerUserInputParams = serde_json::from_value(raw.clone()).unwrap();
    assert_eq!(
        decoded.answers["stable_question"].answers,
        vec!["A, B", "Second\nline"]
    );
    assert_eq!(serde_json::to_value(decoded).unwrap(), raw);
    assert!(serde_json::from_value::<wire::SessionAnswerUserInputParams>(json!({"sessionId":"s","requestId":"r","turnId":"t","toolCallId":"c","status":"answered","answers":{"stable_question":"A, B"}})).is_err());
}
