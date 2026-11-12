//! Defect: replacing a side task's model required closing an ephemeral transcript.
//! Root cause: the live Session fixed its provider/deps and only changed model names.
//! Fixed for the 2026-09-08 Electron live inference reconfiguration work item.
//! These regressions drive the named facade and real loopback HTTP model transport.

use super::r20260908_live_side_task_fork::{
    request, run_turn, DiagnosticPeer, Fixture, FixtureDiagnostic,
};
use super::*;
use bytes::Bytes;
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full, StreamBody};
use hyper::{body::Frame, Request, Response};
use hyper_util::rt::TokioIo;
use klaus_agent_core::credential_store::{CredentialStore, CredentialStoreError};
use klaus_agent_core::engine::session::{Input, RuntimePolicyUpdate, Session, TurnOptions};
use klaus_agent_core::types::permission::{
    ApprovalPolicy, PermissionBehavior, PermissionDestination, PermissionRule, PermissionUpdate,
};
use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use tokio::sync::Notify;

struct LoopbackProvider {
    url: String,
    requests: Arc<Mutex<Vec<Value>>>,
    delegation: Arc<Mutex<Option<Value>>>,
    hold: Arc<AtomicBool>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    input_tokens: Arc<AtomicU64>,
    /// Announced once the fixture has written the first SSE frame of a paused
    /// response, so a test can publish a new model while a request is mid-flight.
    streamed: Arc<Notify>,
    /// Per-request scripted replies, consumed in order. A request with no queued
    /// script takes the provider's default branch below.
    scripts: Arc<Mutex<VecDeque<HttpScript>>>,
    task: tokio::task::JoinHandle<()>,
}

struct HttpScript {
    /// Inject a `read` tool call so the reply produces a tool-use step.
    tool_input: Option<Value>,
    /// Pause after the first delta instead of completing the response.
    pause_after_delta: bool,
    stop: &'static str,
}

fn full_body(bytes: Bytes) -> UnsyncBoxBody<Bytes, Infallible> {
    Full::new(bytes).boxed_unsync()
}

fn sse_frame(value: &Value) -> Result<Frame<Bytes>, Infallible> {
    Ok(Frame::data(Bytes::from(format!(
        "event: {}\ndata: {}\n\n",
        value["type"].as_str().unwrap(),
        value
    ))))
}

impl LoopbackProvider {
    async fn start() -> Self {
        Self::start_with_diagnostic(None).await
    }

    async fn start_traced(diagnostic: &Arc<FixtureDiagnostic>, peer: DiagnosticPeer) -> Self {
        Self::start_with_diagnostic(Some((diagnostic.clone(), peer))).await
    }

    async fn start_with_diagnostic(
        diagnostic: Option<(Arc<FixtureDiagnostic>, DiagnosticPeer)>,
    ) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let delegation = Arc::new(Mutex::new(None::<Value>));
        let hold = Arc::new(AtomicBool::new(false));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let input_tokens = Arc::new(AtomicU64::new(11));
        let streamed = Arc::new(Notify::new());
        let scripts = Arc::new(Mutex::new(VecDeque::<HttpScript>::new()));
        let task = tokio::spawn({
            let requests = requests.clone();
            let delegation = delegation.clone();
            let hold = hold.clone();
            let entered = entered.clone();
            let release = release.clone();
            let input_tokens = input_tokens.clone();
            let streamed = streamed.clone();
            let scripts = scripts.clone();
            let diagnostic = diagnostic.clone();
            async move {
                let mut connections = tokio::task::JoinSet::new();
                loop {
                    tokio::select! {
                        accepted = listener.accept() => {
                            let (socket, _) = accepted.unwrap();
                            let requests = requests.clone();
                            let delegation = delegation.clone();
                            let hold = hold.clone();
                            let entered = entered.clone();
                            let release = release.clone();
                            let input_tokens = input_tokens.clone();
                            let streamed = streamed.clone();
                            let scripts = scripts.clone();
                            let diagnostic = diagnostic.clone();
                            connections.spawn(async move {
                                let service = hyper::service::service_fn(move |request: Request<hyper::body::Incoming>| {
                                    let requests = requests.clone();
                                    let delegation = delegation.clone();
                                    let hold = hold.clone();
                                    let entered = entered.clone();
                                    let release = release.clone();
                                    let input_tokens = input_tokens.clone();
                                    let streamed = streamed.clone();
                                    let scripts = scripts.clone();
                                    let diagnostic = diagnostic.clone();
                                    async move {
                                        let mcp = request.uri().path() == "/mcp";
                                        if request.method() != http::Method::POST {
                                            if mcp { return Ok::<_, Infallible>(Response::builder().status(405).body(full_body(Bytes::new())).unwrap()); }
                                            return Ok::<_, Infallible>(Response::new(full_body(Bytes::new())));
                                        }
                                        let path = request.uri().path().to_string();
                                        if let (false, Some((diagnostic, peer))) = (mcp, &diagnostic) { diagnostic.http_enter(*peer, &path); }
                                        let auth = request.headers().get("x-api-key").or_else(|| request.headers().get("authorization")).and_then(|value| value.to_str().ok()).unwrap_or_default().to_string();
                                        let body = request.into_body().collect().await.unwrap().to_bytes();
                                        let mut value: Value = serde_json::from_slice(&body).unwrap();
                                        if mcp {
                                            let Some(id) = value.get("id") else { return Ok::<_, Infallible>(Response::builder().status(202).body(full_body(Bytes::new())).unwrap()); };
                                            let result = match value["method"].as_str().unwrap() {
                                                "initialize" => json!({"protocolVersion":"2025-03-26","capabilities":{"tools":{}},"serverInfo":{"name":"fixture","version":"1"}}),
                                                "tools/list" => json!({"tools":[]}),
                                                "resources/list" => json!({"resources":[]}),
                                                "resources/templates/list" => json!({"resourceTemplates":[]}),
                                                "prompts/list" => json!({"prompts":[]}),
                                                other => panic!("unexpected MCP fixture method {other}"),
                                            };
                                            return Ok::<_, Infallible>(Response::builder().header("content-type", "application/json").body(full_body(Bytes::from(json!({"jsonrpc":"2.0","id":id,"result":result}).to_string()))).unwrap());
                                        }
                                        value["_wireAuth"] = json!(auth);
                                        value["_wirePath"] = json!(path);
                                        requests.lock().unwrap().push(value.clone());
                                        if let Some((diagnostic, peer)) = &diagnostic { diagnostic.http_body(*peer, &value); }
                                        entered.notify_one();
                                        if hold.load(Ordering::SeqCst) { release.notified().await; }
                                        // A queued script overrides the default reply: it can inject a
                                        // tool call and pause mid-stream, which is what lets a test
                                        // publish a new model while THIS request is still in flight.
                                        if let Some(scripted) = scripts.lock().unwrap().pop_front() {
                                            let tool_input = scripted.tool_input.clone();
                                            let stop = scripted.stop;
                                            let mut events = vec![
                                                json!({"type":"message_start","message":{"id":"fixture-message","type":"message","role":"assistant","model":value["model"],"content":[],"usage":{"input_tokens":11,"output_tokens":0}}}),
                                                json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
                                                json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"The new provider completed this request."}}),
                                                json!({"type":"content_block_stop","index":0}),
                                                json!({"type":"message_delta","delta":{"stop_reason":stop,"stop_sequence":null},"usage":{"output_tokens":7}}),
                                                json!({"type":"message_stop"}),
                                            ];
                                            if let Some(input) = tool_input {
                                                events.splice(4..4, [
                                                    json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"snapshot-read","name":"read","input":{}}}),
                                                    json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":input.to_string()}}),
                                                    json!({"type":"content_block_stop","index":1}),
                                                ]);
                                            }
                                            if scripted.pause_after_delta {
                                                let body = StreamBody::new(futures::stream::unfold((events.into_iter(), 0), move |(mut events, index)| {
                                                    let streamed = streamed.clone();
                                                    let release = release.clone();
                                                    async move {
                                                        if index == 3 {
                                                            streamed.notify_one();
                                                            release.notified().await;
                                                        }
                                                        events.next().map(|event| (sse_frame(&event), (events, index + 1)))
                                                    }
                                                })).boxed_unsync();
                                                return Ok::<_, Infallible>(Response::builder().header("content-type", "text/event-stream").body(body).unwrap());
                                            }
                                            let body = events.iter().map(|event| format!("event: {}\ndata: {}\n\n", event["type"].as_str().unwrap(), event)).collect::<String>();
                                            return Ok::<_, Infallible>(Response::builder().header("content-type", "text/event-stream").body(full_body(Bytes::from(body))).unwrap());
                                        }
                                        let input_count = input_tokens.load(Ordering::SeqCst);
                                        if path.ends_with("/responses") {
                                            let events = [
                                                json!({"type":"response.output_text.delta","delta":"Fixture reply"}),
                                                json!({"type":"response.completed","response":{"status":"completed","output":[],"usage":{"input_tokens":input_count,"output_tokens":7,"total_tokens":input_count + 7}}}),
                                            ];
                                            let body = events.iter().map(|event| format!("event: {}\ndata: {}\n\n", event["type"].as_str().unwrap(), event)).collect::<String>();
                                            return Ok::<_, Infallible>(Response::builder().header("content-type","text/event-stream").body(full_body(Bytes::from(body))).unwrap());
                                        }
                                        if path.ends_with("/chat/completions") {
                                            let body = format!("data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
                                                json!({"id":"chat","model":value["model"],"choices":[{"index":0,"delta":{"role":"assistant","content":"{\"title\":\"Tier title\"}"},"finish_reason":null}]}),
                                                json!({"id":"chat","model":value["model"],"choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":11,"completion_tokens":7,"total_tokens":18}}));
                                            return Ok::<_, Infallible>(Response::builder().header("content-type","text/event-stream").body(full_body(Bytes::from(body))).unwrap());
                                        }
                                        let delegate = value.to_string().contains("delegate via task") && !value.to_string().contains("tool_result");
                                        let forced_output = value.pointer("/tool_choice/name").and_then(Value::as_str);
                                        let title = value["system"].to_string().contains("Generate a concise, sentence-case title");
                                        let events = if let Some(name) = forced_output { vec![
                                            json!({"type":"message_start","message":{"id":"fixture-title","type":"message","role":"assistant","model":value["model"],"content":[],"usage":{"input_tokens":11,"output_tokens":0}}}),
                                            json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"structured-1","name":name,"input":{}}}),
                                            json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"title\":\"Tier title\"}"}}),
                                            json!({"type":"content_block_stop","index":0}),
                                            json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":7}}),
                                            json!({"type":"message_stop"}),
                                        ] } else if delegate { vec![
                                            json!({"type":"message_start","message":{"id":"fixture-task","type":"message","role":"assistant","model":value["model"],"content":[],"usage":{"input_tokens":input_count,"output_tokens":0}}}),
                                            json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"task-1","name":"task","input":{}}}),
                                            json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":delegation.lock().unwrap().clone().unwrap_or_else(|| json!({"description":"Tier child","prompt":"Complete the child request","agentType":"Explore","model":"high","run_in_background":false})).to_string()}}),
                                            json!({"type":"content_block_stop","index":0}),
                                            json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":7}}),
                                            json!({"type":"message_stop"}),
                                        ] } else { vec![
                                            json!({"type":"message_start","message":{"id":"fixture-message","type":"message","role":"assistant","model":value["model"],"content":[],"usage":{"input_tokens":input_count,"output_tokens":0}}}),
                                            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
                                            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":if title {"{\"title\":\"Tier title\"}"} else {"The new provider completed this request."}}}),
                                            json!({"type":"content_block_stop","index":0}),
                                            json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":7}}),
                                            json!({"type":"message_stop"}),
                                        ] };
                                        let body = events.iter().map(|event| format!("event: {}\ndata: {}\n\n", event["type"].as_str().unwrap(), event)).collect::<String>();
                                        Ok::<_, Infallible>(Response::builder().header("content-type", "text/event-stream").body(full_body(Bytes::from(body))).unwrap())
                                    }
                                });
                                let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(socket), service).await;
                            });
                        }
                        _ = connections.join_next(), if !connections.is_empty() => {}
                    }
                }
            }
        });
        Self {
            url,
            requests,
            delegation,
            hold,
            entered,
            release,
            input_tokens,
            streamed,
            scripts,
            task,
        }
    }

    fn init(&self, model: &str) -> Value {
        json!({
            "provider":{"type":"anthropic","model":model,"baseUrl":self.url},
            "auth":{"type":"api_key","key":"fixture-only-key"},
            "config":{"thinking":true,"effort":"high","modelTiers":{"low":"fixture-low"},"contextWindow":31000,"maxOutputTokens":1024},
            "capabilities":{"contextWindow":29000,"maxOutputTokens":2048,"vision":true,"thinking":{"adaptive":true},"effort":{"levels":["low","high"],"default":"low"}},
            "capabilitiesByModel":[{"model":"fixture-low","capabilities":{"contextWindow":17000,"maxOutputTokens":1200,"vision":false}}],
            "pricing":[{"model":model,"inputPerMTok":1.0,"outputPerMTok":2.0,"cacheReadPerMTok":0.0,"cacheWritePerMTok":0.0}]
        })
    }
}

impl Drop for LoopbackProvider {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn tier_target(
    provider: &LoopbackProvider,
    source: &str,
    model_id: &str,
    key: &str,
    window: u64,
    output: u64,
    rate: f64,
) -> Value {
    json!({
        "type":"resolved","source":source,"modelId":model_id,
        "provider":{"type":"anthropic","model":"same-name","baseUrl":provider.url},
        "auth":{"type":"api_key","key":key},
        "capabilities":{"tools":true,"contextWindow":window,"maxOutputTokens":output},
        "pricing":[{"model":"same-name","inputPerMTok":rate,"outputPerMTok":rate,"cacheReadPerMTok":0.0,"cacheWritePerMTok":0.0}],
    })
}

#[path = "r20261007_advisor_target.rs"]
mod advisor_target;

#[path = "../../core/tests/regressions/r20261019_subagent_provider_binding.rs"]
mod subagent_provider_binding;

/// Compaction summary requests, identified by the owned instruction at its
/// system or final-user text boundary rather than by the receiving provider.
fn summary_requests(provider: &LoopbackProvider) -> Vec<Value> {
    let instruction = "Create a context checkpoint for the assistant that will continue this task.";
    let contains_instruction = |blocks: &Value| {
        blocks.as_array().is_some_and(|blocks| {
            blocks.iter().any(|block| {
                block["type"] == "text"
                    && block["text"]
                        .as_str()
                        .is_some_and(|text| text.starts_with(instruction))
            })
        })
    };
    provider
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|request| {
            contains_instruction(&request["system"])
                || request["messages"]
                    .as_array()
                    .and_then(|messages| messages.last())
                    .is_some_and(|message| {
                        message["role"] == "user" && contains_instruction(&message["content"])
                    })
        })
        .cloned()
        .collect()
}

// These fixtures require Task approval independently of collaboration mode.
// Each scripted call receives one reply without adding an automatic grant.
fn require_task_approval(session: &Session) {
    let policy = session
        .set_runtime_policy(RuntimePolicyUpdate {
            approval_policy: Some(ApprovalPolicy::OnRequest),
            sandbox_mode: None,
            approvals_reviewer: None,
            sandbox_workspace_write: None,
            network_allowed: None,
        })
        .unwrap();
    assert_eq!(
        policy.effective.approval_policy,
        Some(ApprovalPolicy::OnRequest)
    );
    assert_eq!(policy.effective.sandbox_mode, policy.previous.sandbox_mode);
    assert_eq!(
        policy.effective.sandbox_workspace_write,
        policy.previous.sandbox_workspace_write
    );
    session.apply_permission_update(PermissionUpdate::AddRules {
        rules: vec![PermissionRule {
            tool_name: "task".into(),
            argument_prefix: None,
            tool_name_prefix: None,
        }],
        behavior: PermissionBehavior::Ask,
        destination: PermissionDestination::Session,
    });
}

struct FixtureTaskApproval {
    turn_id: String,
    approved: bool,
}

impl FixtureTaskApproval {
    fn new(reply: &Value) -> Self {
        Self {
            turn_id: reply["result"]["turnId"].as_str().unwrap().to_string(),
            approved: false,
        }
    }

    async fn observe(&mut self, client: &mut TestClient, message: &Value) {
        assert!(
            message["params"]["event"].get("error").is_none(),
            "{message}"
        );
        assert_ne!(
            message["params"]["event"]["event"]["type"], "turn.failed",
            "{message}"
        );
        if message["method"] == "approval/request" {
            assert!(
                !self.approved,
                "the scripted Task must request approval once"
            );
            assert_eq!(message["params"]["sessionId"], "side", "{message}");
            assert_eq!(message["params"]["submissionId"], self.turn_id, "{message}");
            let request = &message["params"]["request"];
            assert_eq!(request["id"], "task-1", "{message}");
            assert_eq!(request["toolName"], "task", "{message}");
            assert_eq!(
                request["input"],
                json!({
                    "description":"Tier child", "prompt":"Complete the child request",
                    "agentType":"Explore", "model":"high", "run_in_background":false
                }),
                "{message}"
            );
            assert!(
                message["id"].is_number() || message["id"].is_string(),
                "{message}"
            );
            client
                .send(json!({
                    "id":message["id"], "result":{"response":{"decision":"allow"}}
                }))
                .await;
            self.approved = true;
        }
        if message["method"] == "turn/completed" && message["params"]["sessionId"] == "side" {
            assert!(message["params"].get("error").is_none(), "{message}");
        }
    }
}

async fn run_delegated_tier_turn(client: &mut TestClient) {
    client
        .send(json!({"id":21,"method":"turn/start","params":{
            "sessionId":"side","input":"delegate via task"
        }}))
        .await;
    let (reply, seen) = client.response_for(21).await;
    assert!(reply["result"]["turnId"].is_string(), "{reply}");
    let mut approval = FixtureTaskApproval::new(&reply);
    let mut completed = false;
    for event in &seen {
        approval.observe(client, event).await;
        completed |= event["method"] == "turn/completed" && event["params"]["sessionId"] == "side";
    }
    while !completed {
        let event = client.next_message().await;
        approval.observe(client, &event).await;
        completed = event["method"] == "turn/completed" && event["params"]["sessionId"] == "side";
    }
    assert!(
        approval.approved,
        "the delegated Task must receive its explicit approval"
    );
}

#[tokio::test]
async fn global_model_tiers_mix_real_http_title_agent_compaction_and_price_identities() {
    let diagnostic = FixtureDiagnostic::new("tiers_mixed");
    let diagnostic_guard = diagnostic.scope();
    let main = LoopbackProvider::start_traced(&diagnostic, DiagnosticPeer::Main).await;
    let high = LoopbackProvider::start_traced(&diagnostic, DiagnosticPeer::High).await;
    let medium = LoopbackProvider::start_traced(&diagnostic, DiagnosticPeer::Medium).await;
    let low = LoopbackProvider::start_traced(&diagnostic, DiagnosticPeer::Low).await;
    let mut fixture = Fixture::with_diagnostic(diagnostic.clone()).await;
    diagnostic.checkpoint("fixture_ready");
    let mut init = main.init("main-model");
    init["modelSource"] = json!({"source":"custom","modelId":"main-catalog-id"});
    init["modelTierTargets"] = json!({
        "high":tier_target(&high,"subscription","high-catalog-id","high-key",64000,2048,2.0),
        "medium":tier_target(&medium,"custom","medium-entry","medium-key",6000,700,3.0),
        "low":{
            "type":"resolved","source":"local","modelId":"local-file",
            "provider":{"type":"openai","api":"chat","model":"same-name","baseUrl":low.url},
            "auth":{"type":"api_key","key":"low-capability-token"},
            "capabilities":{"tools":false,"structuredOutput":true,"contextWindow":9000,"maxOutputTokens":300},
            "pricing":[{"model":"same-name","inputPerMTok":0.0,"outputPerMTok":0.0,"cacheReadPerMTok":0.0,"cacheWritePerMTok":0.0}]
        }
    });
    // Title generation rides its own feature slot rather than the low tier;
    // pin the same resolved provider so this leg keeps exercising real HTTP.
    init["featureTargets"] = json!({
        "title":{
            "type":"resolved","source":"local","modelId":"local-file",
            "provider":{"type":"openai","api":"chat","model":"same-name","baseUrl":low.url.clone()},
            "auth":{"type":"api_key","key":"low-capability-token"},
            "capabilities":{"tools":false,"structuredOutput":true,"contextWindow":9000,"maxOutputTokens":300},
            "pricing":[{"model":"same-name","inputPerMTok":0.0,"outputPerMTok":0.0,"cacheReadPerMTok":0.0,"cacheWritePerMTok":0.0}]
        }
    });
    let updated = reconfigure(&mut fixture, 20, init, Some(0)).await;
    assert_eq!(updated["result"]["revision"], 1, "{updated}");
    diagnostic.checkpoint("before_title");
    fixture
        .source
        .generate_title(None, None, false)
        .await
        .unwrap();
    diagnostic.checkpoint("after_title");
    assert_eq!(low.requests.lock().unwrap().len(), 1);
    let title = low.requests.lock().unwrap()[0].clone();
    assert_eq!(title["model"], "same-name");
    assert_eq!(title["_wireAuth"], "Bearer low-capability-token");
    assert!(title["_wirePath"]
        .as_str()
        .unwrap()
        .ends_with("/chat/completions"));
    require_task_approval(&fixture.source);
    let policy_before_planning = fixture.source.runtime_policy_state();
    diagnostic.checkpoint("before_plan_mode");
    fixture
        .source
        .set_permission_mode(klaus_agent_core::types::permission::ExternalPermissionMode::Plan)
        .unwrap();
    diagnostic.checkpoint("after_plan_mode");
    let policy_after_planning = fixture.source.runtime_policy_state();
    assert_eq!(
        (
            policy_after_planning.sandbox_mode,
            policy_after_planning.approval_policy,
            policy_after_planning.approvals_reviewer
        ),
        (
            policy_before_planning.sandbox_mode,
            policy_before_planning.approval_policy,
            policy_before_planning.approvals_reviewer
        ),
        "planning cannot change canonical permission authority",
    );
    assert_eq!(
        fixture.source.collaboration_mode(),
        klaus_agent_core::planning::CollaborationMode::Plan
    );

    run_delegated_tier_turn(&mut fixture.client).await;
    let child = high.requests.lock().unwrap().clone();
    assert!(
        !child.is_empty(),
        "tier-selected agent must reach the high provider"
    );
    assert!(child
        .iter()
        .all(|request| request["model"] == "same-name" && request["_wireAuth"] == "high-key"));
    assert!(main
        .requests
        .lock()
        .unwrap()
        .iter()
        .all(|request| request["model"] == "main-model"));
    let before = fixture.source.context_snapshot();
    run_turn(
        &mut fixture.client,
        23,
        "side",
        &format!(
            "{}{}",
            "carry context ".repeat(700),
            "跨来源上下文".repeat(500)
        ),
    )
    .await;
    diagnostic.checkpoint("before_compact");
    let _ = fixture
        .source
        .compact(Some("Summarize the current task".into()))
        .await
        .unwrap();
    assert_eq!(
        before.context_window, 29000,
        "main model retains its actual window"
    );
    assert_eq!(before.output_reserve_tokens, 1450);
    assert_eq!(before.compact_threshold_tokens, 26100);
    // Compaction is main-loop traffic: the summary rides the session's main
    // model even while a medium tier is configured for delegated work.
    let summaries = summary_requests(&main);
    assert!(!summaries.is_empty(), "compaction must summarize on main");
    assert!(
        summaries
            .iter()
            .all(|request| request["model"] == "main-model"),
        "the summary must not leave the main model"
    );
    assert!(
        medium.requests.lock().unwrap().is_empty(),
        "the medium tier must never receive compaction traffic"
    );
    let costs = fixture.source.cost_snapshot();
    assert!(costs
        .models
        .iter()
        .any(|model| model.totals.model == "subscription:high-catalog-id"
            && model.totals.cost_usd > 0.0));
    assert!(costs
        .models
        .iter()
        .any(|model| model.totals.model == "local:local-file" && model.totals.cost_usd == 0.0));
    let high_cost = costs
        .models
        .iter()
        .find(|model| model.totals.model == "subscription:high-catalog-id")
        .unwrap()
        .totals
        .cost_usd;
    assert!((high_cost - child.len() as f64 * 18.0 * 2.0 / 1_000_000.0).abs() < 1e-12);
    assert_eq!(runtime(&mut fixture, 22).await["model"], "main-model");
    diagnostic.checkpoint("before_close");
    fixture.close().await;
    drop(diagnostic_guard);
}

#[tokio::test]
async fn global_model_tiers_auto_compaction_uses_main_and_main_trigger_window() {
    let main = LoopbackProvider::start().await;
    let medium = LoopbackProvider::start().await;
    let mut fixture = Fixture::with_auto_compact_window(27000).await;
    let mut init = main.init("main-model");
    init["modelTierTargets"] =
        json!({"medium":tier_target(&medium,"custom","summary","summary-key",6000,600,1.0)});
    assert_eq!(
        reconfigure(&mut fixture, 20, init, Some(0)).await["result"]["revision"],
        1
    );
    let configured = fixture.source.context_snapshot();
    assert_eq!(configured.context_window, 29000);
    assert_eq!(configured.output_reserve_tokens, 1450);
    assert_eq!(configured.compact_threshold_tokens, 26100);
    main.input_tokens.store(1000, Ordering::SeqCst);
    run_turn(&mut fixture.client, 21, "side", "Keep this context").await;
    assert!(
        medium.requests.lock().unwrap().is_empty(),
        "medium's small window must not trigger main compaction"
    );
    let low_pressure = fixture.source.context_snapshot();
    assert!(
        low_pressure.total_tokens < low_pressure.compact_threshold_tokens,
        "{low_pressure:?}"
    );
    assert!(
        summary_requests(&main).is_empty(),
        "main usage below its own threshold must not summarize"
    );
    main.input_tokens.store(26_500, Ordering::SeqCst);
    run_turn(&mut fixture.client, 22, "side", "Fill the main window").await;
    let pressure = fixture.source.context_snapshot();
    assert!(
        pressure.total_tokens >= pressure.compact_threshold_tokens,
        "{pressure:?}"
    );
    assert!(
        pressure.total_tokens < pressure.context_window - pressure.output_reserve_tokens,
        "{pressure:?}"
    );
    run_turn(
        &mut fixture.client,
        23,
        "side",
        "Continue after the main threshold",
    )
    .await;
    let summaries = summary_requests(&main);
    assert!(
        !summaries.is_empty(),
        "main pressure must summarize on the main model"
    );
    assert!(
        summaries
            .iter()
            .all(|request| request["model"] == "main-model"),
        "the summary must not leave the main model"
    );
    assert!(
        medium.requests.lock().unwrap().is_empty(),
        "the medium tier must never receive compaction traffic"
    );
    let after = fixture.source.context_snapshot();
    assert_eq!(after.context_window, 29000);
    assert_eq!(after.output_reserve_tokens, 1450);
    assert_eq!(after.compact_threshold_tokens, 26100);
    fixture.close().await;
}

#[tokio::test]
async fn global_model_tiers_hot_update_keeps_running_agent_on_old_provider() {
    let diagnostic = FixtureDiagnostic::new("tiers_hot_update");
    let diagnostic_guard = diagnostic.scope();
    let main = LoopbackProvider::start_traced(&diagnostic, DiagnosticPeer::Main).await;
    // The child inherits this response's usage through the full-history task seed.
    // It exceeds the old target's 9,000-token threshold while staying below main's.
    main.input_tokens.store(9_500, Ordering::SeqCst);
    let old = LoopbackProvider::start_traced(&diagnostic, DiagnosticPeer::High).await;
    // Reply to the summary and the resumed child request without interpreting
    // retained historical delegation text as a fresh tool request.
    old.scripts.lock().unwrap().extend([
        HttpScript {
            tool_input: None,
            pause_after_delta: false,
            stop: "end_turn",
        },
        HttpScript {
            tool_input: None,
            pause_after_delta: false,
            stop: "end_turn",
        },
    ]);
    let next = LoopbackProvider::start_traced(&diagnostic, DiagnosticPeer::Next).await;
    let mut fixture = Fixture::with_diagnostic(diagnostic.clone()).await;
    diagnostic.checkpoint("fixture_ready");
    let mut init = main.init("main-model");
    init["modelTierTargets"] =
        json!({"high":tier_target(&old,"custom","old-agent","old-key",10000,700,1.0)});
    assert_eq!(
        reconfigure(&mut fixture, 20, init, Some(0)).await["result"]["revision"],
        1
    );
    require_task_approval(&fixture.source);
    let policy_before_planning = fixture.source.runtime_policy_state();
    diagnostic.checkpoint("before_plan_mode");
    fixture
        .source
        .set_permission_mode(klaus_agent_core::types::permission::ExternalPermissionMode::Plan)
        .unwrap();
    diagnostic.checkpoint("after_plan_mode");
    let policy_after_planning = fixture.source.runtime_policy_state();
    assert_eq!(
        (
            policy_after_planning.sandbox_mode,
            policy_after_planning.approval_policy,
            policy_after_planning.approvals_reviewer
        ),
        (
            policy_before_planning.sandbox_mode,
            policy_before_planning.approval_policy,
            policy_before_planning.approvals_reviewer
        ),
        "planning cannot change canonical permission authority",
    );
    assert_eq!(
        fixture.source.collaboration_mode(),
        klaus_agent_core::planning::CollaborationMode::Plan
    );

    old.hold.store(true, Ordering::SeqCst);
    fixture.client.send(json!({"id":21,"method":"turn/start","params":{"sessionId":"side","input":"delegate via task","options":{"model":"main-model"}}})).await;
    let (started, seen) = fixture.client.response_for(21).await;
    assert!(started["error"].is_null(), "{started}");
    let mut approval = FixtureTaskApproval::new(&started);
    for event in &seen {
        approval.observe(&mut fixture.client, event).await;
    }
    diagnostic.checkpoint("before_old_http_wait");
    let entered = tokio::time::timeout(REPLY_TIMEOUT, async {
        let entered = old.entered.notified();
        tokio::pin!(entered);
        loop {
            tokio::select! {
                _ = &mut entered => break,
                event = fixture.client.next_message() => {
                    approval.observe(&mut fixture.client, &event).await;
                    assert_ne!(event["method"], "turn/completed", "old request is held: {event}");
                }
            }
        }
    })
    .await;
    if entered.is_err() {
        diagnostic.dump("old_http_timeout");
    }
    entered.unwrap();
    assert!(
        approval.approved,
        "the delegated Task must receive its explicit approval"
    );
    assert_eq!(
        summary_requests(&old).len(),
        1,
        "the held request must summarize the child's inherited token pressure"
    );
    diagnostic.checkpoint("after_old_http_wait");
    let update = reconfigure(&mut fixture,22,json!({"modelTierTargets":{"high":tier_target(&next,"custom","next-agent","next-key",11000,600,2.0)}}),Some(1)).await;
    assert_eq!(update["result"]["revision"], 2, "{update}");
    assert!(next.requests.lock().unwrap().is_empty());
    old.hold.store(false, Ordering::SeqCst);
    old.release.notify_waiters();
    loop {
        let message = fixture.client.next_message().await;
        approval.observe(&mut fixture.client, &message).await;
        if message["method"] == "turn/completed" && message["params"]["sessionId"] == "side" {
            break;
        }
    }
    let child_requests = old.requests.lock().unwrap().clone();
    assert_eq!(child_requests.len(), 2);
    assert!(child_requests
        .iter()
        .all(|request| request["model"] == "same-name" && request["_wireAuth"] == "old-key"));
    let child_summaries = summary_requests(&old);
    assert_eq!(child_summaries.len(), 1);
    assert_eq!(child_summaries[0], child_requests[0]);
    assert!(next.requests.lock().unwrap().is_empty());
    let parent_requests = main.requests.lock().unwrap().clone();
    assert_eq!(parent_requests.len(), 2);
    assert!(parent_requests.iter().all(|request| {
        request["model"] == "main-model" && request["_wireAuth"] == "fixture-only-key"
    }));
    let child_result = parent_requests[1]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|message| message["content"].as_array().into_iter().flatten())
        .find(|block| block["type"] == "tool_result" && block["tool_use_id"] == "task-1")
        .expect("the parent must receive the delegated task result");
    assert!(
        !child_result["is_error"].as_bool().unwrap_or(false),
        "{child_result}"
    );
    assert!(
        child_result["content"]
            .to_string()
            .contains("The new provider completed this request."),
        "{child_result}"
    );
    diagnostic.checkpoint("before_close");
    fixture.close().await;
    drop(diagnostic_guard);
}

#[tokio::test]
async fn global_model_tiers_unavailable_tiers_degrade_to_main_and_null_follows_main() {
    let main = LoopbackProvider::start().await;
    let mut fixture = Fixture::new(false, None).await;
    let mut init = main.init("low");
    init["modelTierTargets"] = json!({
        "low":{"type":"unavailable","source":"custom","modelId":"deleted","code":"MODEL_REMOVED","message":"The selected model was deleted."},
        "medium":{"type":"unavailable","source":"local","modelId":"deleted-file","code":"MODEL_REMOVED","message":"The selected model was deleted."}
    });
    assert_eq!(
        reconfigure(&mut fixture, 20, init, Some(0)).await["result"]["revision"],
        1
    );
    // A configured tier whose model is currently unavailable degrades to the
    // session's main model instead of failing the auxiliary path.
    fixture
        .source
        .generate_title(None, None, false)
        .await
        .unwrap();
    assert_eq!(main.requests.lock().unwrap().len(), 1);
    assert_eq!(main.requests.lock().unwrap()[0]["model"], "low");
    run_turn(&mut fixture.client, 21, "side", "Main stays available").await;
    let count = main.requests.lock().unwrap().len();
    let compacted = fixture.source.compact(None).await.unwrap();
    assert!(
        !compacted.diagnostics.iter().any(|diagnostic| diagnostic
            .message
            .as_ref()
            .is_some_and(|message| message.try_to_utf8().unwrap().contains("MODEL_REMOVED"))),
        "an unavailable medium tier must not poison compaction"
    );
    let update = reconfigure(
        &mut fixture,
        22,
        json!({"modelTierTargets":{"low":null}}),
        Some(1),
    )
    .await;
    assert_eq!(update["result"]["revision"], 2, "{update}");
    fixture
        .source
        .generate_title(None, None, false)
        .await
        .unwrap();
    assert!(main.requests.lock().unwrap().len() > count);
    assert_eq!(
        main.requests.lock().unwrap().last().unwrap()["model"],
        "low"
    );
    fixture.close().await;
}

#[tokio::test]
async fn feature_targets_reject_unknown_slots_and_null_slots_follow_main() {
    let main = LoopbackProvider::start().await;
    let mut fixture = Fixture::new(false, None).await;
    let mut init = main.init("main-model");
    init["featureTargets"] = json!({"unknown":null});
    let failed = reconfigure(&mut fixture, 20, init, Some(0)).await;
    assert!(
        failed["error"]["message"]
            .as_str()
            .unwrap()
            .contains("featureTargets"),
        "{failed}"
    );
    // Null slots stay unset: the feature follows the session's main model.
    let mut init = main.init("main-model");
    init["featureTargets"] = json!({"title":null,"prediction":null,"memory":null});
    assert_eq!(
        reconfigure(&mut fixture, 21, init, Some(0)).await["result"]["revision"],
        1
    );
    fixture
        .source
        .generate_title(None, None, false)
        .await
        .unwrap();
    assert_eq!(main.requests.lock().unwrap().len(), 1);
    assert_eq!(main.requests.lock().unwrap()[0]["model"], "main-model");
    let runtime = runtime(&mut fixture, 22).await;
    assert!(runtime["featureTargets"]["title"].is_null(), "{runtime}");
    assert!(
        runtime["featureTargets"]["prediction"].is_null(),
        "{runtime}"
    );
    assert!(runtime["featureTargets"]["memory"].is_null(), "{runtime}");
    fixture.close().await;
}

#[tokio::test]
async fn global_model_tiers_selection_resolve_is_readonly_and_does_not_return_auth() {
    let provider = LoopbackProvider::start().await;
    let mut fixture = Fixture::new(false, None).await;
    fixture.client.send(json!({"id":20,"method":"model/selection/resolve","params":{
        "source":"local","selection":"main-file",
        "catalog":[{"value":"local:main-file"}],
        "modelTierTargets":{
            "high":tier_target(&provider,"custom","other-entry","secret-never-returned",8000,300,1.0),
            "low":{"type":"source","source":"custom","modelId":"deleted-entry"}
        }
    }})).await;
    let (reply, _) = fixture.client.response_for(20).await;
    assert_eq!(reply["result"]["selection"], "local:main-file", "{reply}");
    assert_eq!(reply["result"]["modelTierTargets"]["high"]["type"], "ready");
    assert_eq!(
        reply["result"]["modelTierTargets"]["low"]["type"],
        "unavailable"
    );
    assert!(reply["result"]["modelTierTargets"]["medium"].is_null());
    assert!(!reply.to_string().contains("secret-never-returned"));
    assert!(!reply.to_string().contains(&provider.url));
    assert!(provider.requests.lock().unwrap().is_empty());
    assert_eq!(runtime(&mut fixture, 21).await["revision"], 0);
    fixture.close().await;
}

#[test]
fn global_model_tiers_oauth_ids_share_login_not_model_and_leave_legacy_main_unchanged() {
    let auth = json!({"type":"oauth_token","accessToken":"initial-access","refreshToken":"login-generation-one"});
    let mut first = json!({"type":"resolved","source":"subscription","modelId":"a","auth":auth});
    let mut second = json!({"type":"resolved","source":"subscription","modelId":"b","auth":auth});
    super::super::service::qualify_tier_oauth(&mut first);
    super::super::service::qualify_tier_oauth(&mut second);
    assert_eq!(
        first["auth"]["credentialId"],
        second["auth"]["credentialId"]
    );
    assert!(first["auth"]["credentialId"]
        .as_str()
        .unwrap()
        .starts_with("subscription-"));
    assert!(!first["auth"]["credentialId"]
        .as_str()
        .unwrap()
        .contains("login-generation"));
    let mut another_login = json!({"type":"resolved","source":"subscription","modelId":"a","auth":{"type":"oauth_token","accessToken":"new-access","refreshToken":"login-generation-two"}});
    super::super::service::qualify_tier_oauth(&mut another_login);
    assert_ne!(
        first["auth"]["credentialId"],
        another_login["auth"]["credentialId"]
    );
    let mut legacy_main = json!({"provider":{"type":"anthropic","model":"main"},"auth":auth});
    let unchanged = legacy_main.clone();
    super::super::service::qualify_tier_oauth(&mut legacy_main);
    assert_eq!(legacy_main, unchanged);
}

#[tokio::test]
async fn global_model_tiers_hot_update_keeps_inflight_low_request_on_old_provider() {
    let main = LoopbackProvider::start().await;
    let old = LoopbackProvider::start().await;
    let next = LoopbackProvider::start().await;
    let mut fixture = Fixture::new(false, None).await;
    let mut init = main.init("main-model");
    init["modelTierTargets"] =
        json!({"low":tier_target(&old,"custom","old","old-key",12000,500,1.0)});
    assert_eq!(
        reconfigure(&mut fixture, 20, init, Some(0)).await["result"]["revision"],
        1
    );
    old.hold.store(true, Ordering::SeqCst);
    let session = fixture.source.clone();
    let running = tokio::spawn(async move { session.generate_title(None, None, false).await });
    tokio::time::timeout(REPLY_TIMEOUT, old.entered.notified())
        .await
        .unwrap();
    let changed = reconfigure(&mut fixture,21,json!({"modelTierTargets":{"low":tier_target(&next,"custom","next","next-key",16000,400,2.0)}}),Some(1)).await;
    assert_eq!(changed["result"]["revision"], 2, "{changed}");
    assert!(next.requests.lock().unwrap().is_empty());
    old.hold.store(false, Ordering::SeqCst);
    old.release.notify_waiters();
    tokio::time::timeout(REPLY_TIMEOUT, running)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    fixture
        .source
        .generate_title(None, None, false)
        .await
        .unwrap();
    assert_eq!(old.requests.lock().unwrap().len(), 1);
    assert_eq!(next.requests.lock().unwrap().len(), 1);
    assert_eq!(next.requests.lock().unwrap()[0]["_wireAuth"], "next-key");
    assert_eq!(runtime(&mut fixture, 22).await["model"], "main-model");
    fixture.close().await;
}

async fn reconfigure(fixture: &mut Fixture, id: u64, init: Value, revision: Option<u64>) -> Value {
    let mut params = json!({"sessionId":"side","init":init});
    if let Some(revision) = revision {
        params["expectedRevision"] = json!(revision);
    }
    request(&mut fixture.client, id, "session/reconfigureModel", params).await
}

async fn runtime(fixture: &mut Fixture, id: u64) -> Value {
    let reply = request(
        &mut fixture.client,
        id,
        "session/modelRuntime",
        json!({"sessionId":"side"}),
    )
    .await;
    assert!(reply.get("error").is_none(), "{reply}");
    reply["result"].clone()
}

#[tokio::test]
async fn runtime_settings_named_reconfigure_publishes_controls_and_resets_model_defaults() {
    use klaus_agent_core::schemas::config::WebSearchAccess;
    use klaus_agent_core::types::provider::{OutputVerbosity, ReasoningSummary};
    let provider = LoopbackProvider::start().await;
    let mut fixture = Fixture::new(false, None).await;
    let identity = fixture.source.meta.clone();
    for (index, (verbosity, summary, access, expected_verbosity)) in [
        (json!("high"), "detailed", "indexed", "high"),
        (Value::Null, "none", "cached", "medium"),
        (json!("low"), "concise", "live", "low"),
        (Value::Null, "auto", "disabled", "medium"),
    ]
    .into_iter()
    .enumerate()
    {
        let selection = json!({
            "provider":{"type":"openai","api":"responses","model":"settings-fixture","baseUrl":provider.url},
            "auth":{"type":"api_key","key":"fixture-only-key"},
            "config":{"modelVerbosity":verbosity,"reasoningSummary":summary,"webSearch":{"access":access}},
            "capabilities":{"contextWindow":31000,"maxOutputTokens":1024,"outputVerbosity":"medium","effort":{"levels":["high"]}}
        });
        let response = reconfigure(
            &mut fixture,
            20 + index as u64 * 2,
            selection,
            Some(index as u64),
        )
        .await;
        assert_eq!(
            response["result"]["revision"],
            json!(index + 1),
            "{response}"
        );
        run_turn(
            &mut fixture.client,
            21 + index as u64 * 2,
            "side",
            "Describe the selected settings.",
        )
        .await;
        let body = provider.requests.lock().unwrap().last().cloned().unwrap();
        assert_eq!(body["text"]["verbosity"], expected_verbosity, "{body}");
        if summary == "none" {
            assert!(body["reasoning"].get("summary").is_none(), "{body}");
        } else {
            assert_eq!(body["reasoning"]["summary"], summary, "{body}");
        }
        let settings = fixture.source.runtime_inference_settings();
        assert_eq!(
            settings.model_verbosity,
            match verbosity.as_str() {
                Some("high") => Some(OutputVerbosity::High),
                Some("low") => Some(OutputVerbosity::Low),
                _ => None,
            }
        );
        assert_eq!(
            settings.reasoning_summary,
            match summary {
                "detailed" => ReasoningSummary::Detailed,
                "none" => ReasoningSummary::None,
                "concise" => ReasoningSummary::Concise,
                _ => ReasoningSummary::Auto,
            }
        );
        assert_eq!(
            settings.web_search_access,
            match access {
                "indexed" => WebSearchAccess::Indexed,
                "cached" => WebSearchAccess::Cached,
                "live" => WebSearchAccess::Live,
                _ => WebSearchAccess::Disabled,
            }
        );
        assert_eq!(fixture.source.meta, identity);
    }
    assert_eq!(provider.requests.lock().unwrap().len(), 4);
    fixture.close().await;
}

#[tokio::test]
async fn codex_fast_mode_publication_sets_priority_and_clears_it_for_standard_and_other_sources() {
    let provider = LoopbackProvider::start().await;
    let mut fixture = Fixture::new(false, None).await;
    let mut id = 20;
    for (index, (source, enabled, priority)) in [
        ("codex", false, false),
        ("codex", true, true),
        ("codex", false, false),
        ("custom", true, false),
    ]
    .into_iter()
    .enumerate()
    {
        let init = json!({
            "provider":{"type":"openai","api":"responses","model":"fast-fixture","baseUrl":format!("{}/chatgpt.com/backend-api/codex", provider.url)},
            "auth":{"type":"api_key","key":"fixture-only-key"},
            "modelSource":{"source":source,"modelId":"fast-fixture"},
            "config":{"fastMode":enabled,"thinking":true,"effort":"high"},
            "capabilities":{"contextWindow":31000,"maxOutputTokens":1024,"effort":{"levels":["high"],"default":"high"}}
        });
        let reply = reconfigure(&mut fixture, id, init, Some(index as u64)).await;
        assert!(reply.get("error").is_none(), "{reply}");
        id += 1;
        let current = runtime(&mut fixture, id).await;
        assert_eq!(current["fastMode"], enabled, "published flag");
        assert_eq!(current["effort"], "high", "reasoning remains independent");
        id += 1;
        run_turn(
            &mut fixture.client,
            id,
            "side",
            "Reply to this Fast mode fixture",
        )
        .await;
        id += 1;
        let body = provider
            .requests
            .lock()
            .unwrap()
            .last()
            .cloned()
            .expect("a real HTTP request was captured");
        assert_eq!(
            body["service_tier"],
            if priority {
                json!("priority")
            } else {
                Value::Null
            },
            "{body}"
        );
        assert_eq!(body["reasoning"]["effort"], "high", "{body}");
    }
    fixture.close().await;
}

#[tokio::test]
async fn live_model_reconfigure_keeps_ephemeral_state_and_routes_turns_and_compaction_to_the_new_provider(
) {
    let provider = LoopbackProvider::start().await;
    let mut fixture = Fixture::new(false, None).await;
    let source = fixture.source.clone();
    let history = source.history();
    let visible = source.visible_history();
    let meta = source.meta.clone();
    let turns = source.turn_index_snapshot();
    let files = source.file_changes_snapshot();
    let queue = source.queue_snapshot();
    let initial = runtime(&mut fixture, 20).await;
    let result = reconfigure(
        &mut fixture,
        21,
        provider.init("fixture-next"),
        Some(initial["revision"].as_u64().unwrap()),
    )
    .await;
    assert_eq!(result["result"]["revision"], 1, "{result}");
    let current = runtime(&mut fixture, 22).await;
    assert_eq!(current["model"], "fixture-next");
    assert_eq!(current["contextWindow"], 31000);
    assert_eq!(current["modelTiers"]["low"], "fixture-low");
    assert_eq!(current["effort"], "high");
    assert_eq!(current["thinking"], true);
    assert_eq!(current["capabilities"]["vision"], true);
    assert!(!current.to_string().contains("fixture-only-key"));
    assert_eq!(source.history(), history);
    assert_eq!(source.visible_history(), visible);
    assert_eq!(source.meta, meta);
    assert_eq!(source.turn_index_snapshot(), turns);
    assert_eq!(source.file_changes_snapshot(), files);
    assert_eq!(source.queue_snapshot(), queue);
    run_turn(
        &mut fixture.client,
        23,
        "side",
        "Continue without losing the parent context",
    )
    .await;
    let requests = provider.requests.lock().unwrap().clone();
    assert!(!requests.is_empty());
    assert_eq!(requests[0]["model"], "fixture-next");
    assert_eq!(requests[0]["max_tokens"], 1024);
    assert_eq!(
        requests[0]["thinking"]["type"], "adaptive",
        "{}",
        requests[0]
    );
    assert_eq!(
        requests[0]["output_config"]["effort"], "high",
        "{}",
        requests[0]
    );
    assert!(requests[0].to_string().contains("hidden parent prompt"));
    assert!(!source
        .visible_history()
        .iter()
        .any(|message| format!("{message:?}").contains("hidden parent prompt")));
    let before_compact = provider.requests.lock().unwrap().len();
    let _ = source
        .compact(Some("Summarize the current conversation".into()))
        .await
        .unwrap();
    assert!(
        provider.requests.lock().unwrap().len() > before_compact,
        "compactor must use the new provider"
    );
    let compact_requests = provider.requests.lock().unwrap()[before_compact..].to_vec();
    assert!(
        compact_requests
            .iter()
            .all(|request| request["model"] == "fixture-next"),
        "{compact_requests:?}"
    );
    let history_before_title = source.history();
    source.generate_title(None, None, false).await.unwrap();
    let title = provider.requests.lock().unwrap().last().unwrap().clone();
    assert_eq!(title["model"], "fixture-next");
    assert_eq!(title["max_tokens"], 2048);
    assert_eq!(
        title["thinking"]["type"], "adaptive",
        "the selected model requires adaptive thinking"
    );
    assert_eq!(
        title["output_config"]["effort"], "low",
        "title generation uses the selected model's lowest declared effort"
    );
    assert_eq!(source.history(), history_before_title);
    fixture.close().await;
}

#[tokio::test]
async fn live_model_reconfigure_publishes_during_active_work_but_keeps_cas_and_invalid_config_failures(
) {
    let provider = LoopbackProvider::start().await;
    let mut fixture = Fixture::new(false, None).await;
    let held = fixture.source.run_streamed(
        Input::Text("An admitted unpolled turn".into()),
        TurnOptions::default(),
    );
    assert_eq!(runtime(&mut fixture, 20).await["revision"], 0);
    let published = reconfigure(&mut fixture, 21, provider.init("fixture-unpolled"), Some(0)).await;
    assert_eq!(published["result"]["revision"], 1, "{published}");
    assert_eq!(runtime(&mut fixture, 22).await["model"], "fixture-unpolled");
    drop(held);
    assert_eq!(
        reconfigure(&mut fixture, 23, provider.init("fixture-active"), Some(1)).await["result"]
            ["revision"],
        2
    );
    provider.hold.store(true, Ordering::SeqCst);
    fixture.client.send(json!({"id":24,"method":"session/sideQuestion","params":{"sessionId":"side","question":"A separate readonly question","submissionId":"held-side-question"}})).await;
    let ack = fixture.client.response_for(24).await.0;
    assert!(ack.get("error").is_none(), "{ack}");
    tokio::time::timeout(REPLY_TIMEOUT, provider.entered.notified())
        .await
        .unwrap();
    let captured = provider.requests.lock().unwrap().clone();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0]["model"], "fixture-active");
    let published = reconfigure(
        &mut fixture,
        25,
        provider.init("fixture-published"),
        Some(2),
    )
    .await;
    assert_eq!(published["result"]["revision"], 3, "{published}");
    assert_eq!(
        runtime(&mut fixture, 26).await["model"],
        "fixture-published"
    );
    assert_eq!(
        *provider.requests.lock().unwrap(),
        captured,
        "the in-flight side question is still A"
    );
    provider.hold.store(false, Ordering::SeqCst);
    provider.release.notify_waiters();
    loop {
        let event = fixture.client.next_message().await;
        if event["params"]["event"]["submissionId"] == "held-side-question" {
            assert!(
                event["params"]["event"]["sideQuestion"]["response"]
                    .as_str()
                    .is_some_and(|text| text.contains("new provider")),
                "{event}"
            );
            break;
        }
    }
    let failed = reconfigure(&mut fixture, 27, provider.init("fixture-stale"), Some(0)).await;
    assert!(failed["error"]["message"]
        .as_str()
        .unwrap()
        .contains("revision conflict"));
    let old = runtime(&mut fixture, 28).await;
    for (index, init) in [
        json!({"provider":false,"modelSelection":"custom:missing"}),
        json!({"provider":{"type":"anthropic"},"modelSelection":false}),
        json!({"provider":{"type":"invalid"}}),
        json!({"modelSelection":"custom:missing"}),
        json!({"provider":{"type":"anthropic"},"auth":{"type":"api_key_stored","credentialId":"missing"}}),
        json!({"provider":{"type":"anthropic"},"config":{"thinking":"invalid"}}),
        json!({"provider":{"type":"anthropic"},"config":{"sandbox":"process"}}),
        json!({"provider":{"type":"anthropic"},"cwd":"/tmp"}),
    ].into_iter().enumerate() {
        assert!(reconfigure(&mut fixture, 30 + index as u64, init, None).await.get("error").is_some());
    }
    assert_eq!(runtime(&mut fixture, 40).await, old);
    run_turn(
        &mut fixture.client,
        41,
        "side",
        "The existing provider still works after all failed changes",
    )
    .await;
    assert_eq!(
        provider.requests.lock().unwrap().last().unwrap()["model"],
        "fixture-published"
    );
    fixture.close().await;
}

#[tokio::test]
async fn live_model_reconfigure_preserves_a_healthy_connected_mcp_server() {
    let provider = LoopbackProvider::start().await;
    let mut fixture = Fixture::new(false, None).await;
    let reply = request(&mut fixture.client, 20, "session/reloadMcpServers", json!({"sessionId":"side","servers":[{"transport":"http","name":"connected-fixture","url":format!("{}/mcp", provider.url)}],"submissionId":"fixture-reload"})).await;
    assert!(reply.get("error").is_none(), "{reply}");
    let before = request(
        &mut fixture.client,
        21,
        "session/mcpRuntime",
        json!({"sessionId":"side"}),
    )
    .await;
    assert_eq!(
        before["result"]["mcpRuntime"]["runtime"]["servers"][0]["state"], "ready",
        "{before}"
    );
    let switched = reconfigure(&mut fixture, 22, provider.init("fixture-connected"), None).await;
    assert_eq!(switched["result"]["revision"], 1, "{switched}");
    let after = request(
        &mut fixture.client,
        23,
        "session/mcpRuntime",
        json!({"sessionId":"side"}),
    )
    .await;
    assert_eq!(
        after["result"]["mcpRuntime"],
        before["result"]["mcpRuntime"]
    );
    run_turn(
        &mut fixture.client,
        24,
        "side",
        "Continue with the existing MCP connection",
    )
    .await;
    fixture.close().await;
}

#[derive(Default)]
struct PausedCredentialStore {
    entered: Notify,
    release: Notify,
    finished: std::sync::atomic::AtomicUsize,
}

struct CredentialRead<'a>(&'a PausedCredentialStore);
impl Drop for CredentialRead<'_> {
    fn drop(&mut self) {
        self.0.finished.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl CredentialStore for PausedCredentialStore {
    async fn get(&self, _: &str) -> Result<Option<String>, CredentialStoreError> {
        let _read = CredentialRead(self);
        self.entered.notify_one();
        self.release.notified().await;
        Ok(Some("fixture-only-key".to_string()))
    }
    async fn set(&self, _: &str, _: &str) -> Result<(), CredentialStoreError> {
        unreachable!()
    }
    async fn delete(&self, _: &str) -> Result<(), CredentialStoreError> {
        unreachable!()
    }
}

#[tokio::test]
async fn live_model_reconfigure_preparation_keeps_the_old_session_available_and_cas_rejects_stale_completion(
) {
    let provider = LoopbackProvider::start().await;
    let credentials = Arc::new(PausedCredentialStore::default());
    let mut fixture = Fixture::with_credentials(false, None, Some(credentials.clone())).await;
    let mut slow = provider.init("fixture-slow");
    slow["auth"] = json!({"type":"api_key_stored","credentialId":"slow"});
    fixture.client.send(json!({"id":20,"method":"session/reconfigureModel","params":{"sessionId":"side","init":slow}})).await;
    tokio::time::timeout(REPLY_TIMEOUT, credentials.entered.notified())
        .await
        .unwrap();
    run_turn(
        &mut fixture.client,
        21,
        "side",
        "The old provider remains available while credentials load",
    )
    .await;
    let history = fixture.source.history();
    let newer = reconfigure(&mut fixture, 22, provider.init("fixture-winner"), Some(0)).await;
    assert_eq!(newer["result"]["revision"], 1, "{newer}");
    credentials.release.notify_waiters();
    let failed = fixture.client.response_for(20).await.0;
    assert!(
        failed["error"]["message"]
            .as_str()
            .unwrap()
            .contains("revision conflict"),
        "{failed}"
    );
    assert_eq!(runtime(&mut fixture, 23).await["model"], "fixture-winner");
    assert_eq!(fixture.source.history(), history);
    run_turn(
        &mut fixture.client,
        24,
        "side",
        "Only the committed provider receives this request",
    )
    .await;
    assert!(provider
        .requests
        .lock()
        .unwrap()
        .iter()
        .all(|request| request["model"] == "fixture-winner"));
    fixture.close().await;
}

#[tokio::test]
async fn live_model_reconfigure_closing_cancels_preparation_without_a_late_provider_commit() {
    let provider = LoopbackProvider::start().await;
    let credentials = Arc::new(PausedCredentialStore::default());
    let mut fixture = Fixture::with_credentials(false, None, Some(credentials.clone())).await;
    let before = fixture.source.history();
    let mut slow = provider.init("fixture-never-committed");
    slow["auth"] = json!({"type":"api_key_stored","credentialId":"slow"});
    fixture.client.send(json!({"id":20,"method":"session/reconfigureModel","params":{"sessionId":"side","init":slow}})).await;
    tokio::time::timeout(REPLY_TIMEOUT, credentials.entered.notified())
        .await
        .unwrap();
    fixture
        .client
        .send(json!({"id":21,"method":"session/close","params":{"sessionId":"side"}}))
        .await;
    let (mut cancelled, mut closed) = (false, false);
    while !cancelled || !closed {
        let message = fixture.client.next_message().await;
        if message["id"] == 20 {
            assert!(
                message["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("closing"),
                "{message}"
            );
            cancelled = true;
        }
        if message["id"] == 21 {
            assert!(message.get("error").is_none(), "{message}");
            closed = true;
        }
    }
    assert_eq!(
        credentials.finished.load(Ordering::SeqCst),
        1,
        "closing drops the pending credential read"
    );
    assert_eq!(fixture.source.history(), before);
    assert!(fixture.source.is_disposed());
    assert!(provider.requests.lock().unwrap().is_empty());
    fixture.close().await;
}

#[tokio::test]
async fn live_model_reconfigure_resolves_stored_byo_and_clears_previous_tiers_context_and_thinking()
{
    let provider = LoopbackProvider::start().await;
    let credentials =
        Arc::new(klaus_agent_core::mcp::credential_store::InMemoryCredentialStore::new());
    credentials
        .set(
            &klaus_agent_core::credential_store::model_credential_key("replacement"),
            "fixture-only-key",
        )
        .await
        .unwrap();
    let mut fixture = Fixture::with_credentials(false, None, Some(credentials)).await;
    assert_eq!(
        reconfigure(&mut fixture, 20, provider.init("fixture-original"), None).await["result"]
            ["revision"],
        1
    );
    run_turn(
        &mut fixture.client,
        21,
        "side",
        "Record some usage before replacement",
    )
    .await;
    let history = fixture.source.history();
    let costs = fixture.source.cost.lock().unwrap().snapshot();
    std::fs::write(
        fixture.root.path().join("config.toml"),
        format!(
            r#"
model = "opus[1m]"
[modelProviders.replacement]
provider = "anthropic"
model = "fixture-byo"
baseUrl = "{}"
maxContextTokens = 24680
[modelProviders.replacement.capabilities]
vision = false
contextWindow = 24000
maxOutputTokens = 1337
"#,
            provider.url
        ),
    )
    .unwrap();
    let reply = tokio::time::timeout(REPLY_TIMEOUT, async {
        loop {
            let reply = reconfigure(
                &mut fixture,
                22,
                json!({"modelSelection":"custom:replacement","config":{"thinking":false}}),
                Some(1),
            )
            .await;
            if reply["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("inference is running"))
            {
                tokio::task::yield_now().await;
                continue;
            }
            break reply;
        }
    })
    .await
    .unwrap();
    assert_eq!(reply["result"]["revision"], 2, "{reply}");
    let current = runtime(&mut fixture, 23).await;
    assert_eq!(current["model"], "fixture-byo");
    assert_eq!(current["contextWindow"], 24680);
    assert_eq!(current["maxOutputTokens"], 1337);
    assert_eq!(current["modelTiers"], json!({}));
    assert_eq!(current["effort"], Value::Null);
    assert_eq!(current["thinking"], false);
    assert_eq!(current["capabilities"]["vision"], false);
    assert_eq!(fixture.source.history(), history);
    assert_eq!(fixture.source.cost.lock().unwrap().snapshot(), costs);
    run_turn(
        &mut fixture.client,
        24,
        "side",
        "Use the stored-key provider",
    )
    .await;
    assert_eq!(
        provider.requests.lock().unwrap().last().unwrap()["model"],
        "fixture-byo"
    );
    assert!(provider
        .requests
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .get("thinking")
        .is_none());
    let explicit = reconfigure(
        &mut fixture,
        25,
        json!({"modelSelection":"custom:replacement","config":{"model":"fixture-explicit"}}),
        Some(2),
    )
    .await;
    assert_eq!(explicit["result"]["revision"], 3, "{explicit}");
    assert_eq!(runtime(&mut fixture, 26).await["model"], "fixture-explicit");
    run_turn(
        &mut fixture.client,
        27,
        "side",
        "Use an explicit model override",
    )
    .await;
    assert_eq!(
        provider.requests.lock().unwrap().last().unwrap()["model"],
        "fixture-explicit"
    );
    fixture.close().await;
}

fn snapshot_init(provider: &LoopbackProvider, model: &str, source: &str, b: bool) -> Value {
    let mut init = provider.init(model);
    init["modelSource"] = json!({
        "source":source, "modelId":model, "selection":format!("{source}:{model}")
    });
    init["config"] = json!({
        "thinking":true, "effort":if b { "high" } else { "low" },
        "contextWindow":if b { 61000 } else { 31000 },
        "maxOutputTokens":if b { 1536 } else { 1024 },
        "maxOutputRetryTokens":if b { 3072 } else { 2048 }
    });
    init["capabilities"]["vision"] = json!(b);
    init["capabilities"]["maxOutputTokens"] = json!(if b { 4096 } else { 2048 });
    init["pricing"][0]["inputPerMTok"] = json!(if b { 10.0 } else { 1.0 });
    init["pricing"][0]["outputPerMTok"] = json!(if b { 20.0 } else { 2.0 });
    init
}

async fn named_with_events(
    client: &mut TestClient,
    id: u64,
    method: &str,
    params: Value,
    events: &mut Vec<Value>,
) -> Value {
    client
        .send(json!({"id":id,"method":method,"params":params}))
        .await;
    let (reply, seen) = client.response_for(id).await;
    events.extend(seen);
    reply
}

async fn begin_named(
    client: &mut TestClient,
    id: u64,
    session: &str,
    change: &str,
    revision: u64,
) -> Value {
    request(
        client,
        id,
        "session/beginModelUpdate",
        json!({
            "sessionId":session, "changeId":change, "expectedRevision":revision
        }),
    )
    .await
}

async fn publish_named(
    client: &mut TestClient,
    id: u64,
    session: &str,
    change: &str,
    revision: u64,
    init: Value,
) -> Value {
    request(
        client,
        id,
        "session/reconfigureModel",
        json!({
            "sessionId":session, "changeId":change, "expectedRevision":revision, "init":init
        }),
    )
    .await
}

fn inner_event(message: &Value) -> &Value {
    &message["params"]["event"]["event"]
}

fn facade_identity(event: &Value, revision: u64, model: &str, source: &str, change: &str) {
    assert_eq!(event["inference"]["revision"], revision, "{event}");
    assert_eq!(event["inference"]["changeId"], change, "{event}");
    assert_eq!(event["inference"]["model"], model, "{event}");
    assert_eq!(event["inference"]["source"], source, "{event}");
    assert_eq!(
        event["inference"]["selection"],
        format!("{source}:{model}"),
        "{event}"
    );
    assert!(!event.to_string().contains("fixture-only-key"));
    assert!(!event.to_string().contains("http://127.0.0.1"));
}

#[tokio::test]
async fn snapshot_named_facade_streams_a_then_requests_b_in_the_same_main_turn_with_request_attribution(
) {
    let a = LoopbackProvider::start().await;
    let b = LoopbackProvider::start().await;
    let mut fixture = Fixture::new(false, None).await;
    let main = "snapshot-main";
    let mut events = Vec::new();
    let mut init_a = snapshot_init(&a, "fixture-a", "custom", false);
    // Keep prompt and tool catalog pressure below the compaction trigger so
    // the paused script belongs to the main streaming request.
    init_a["config"]["contextWindow"] = json!(131000);
    let mut start_init = init_a.clone();
    start_init["threadId"] = json!(main);
    start_init["cwd"] = json!(fixture.root.path());
    let start = request(
        &mut fixture.client,
        50,
        "session/start",
        json!({"init":start_init}),
    )
    .await;
    assert_eq!(start["result"]["sessionId"], main, "{start}");
    // Initialize a live session through the facade without starting a model request.
    let capabilities = request(
        &mut fixture.client,
        500,
        "session/runtimeCapabilities",
        json!({"sessionId":main}),
    )
    .await;
    assert!(capabilities.get("error").is_none(), "{capabilities}");
    let begun = begin_named(&mut fixture.client, 51, main, "main-a", 0).await;
    assert_eq!(begun["result"]["revision"], 0, "{begun}");
    assert_eq!(
        publish_named(&mut fixture.client, 52, main, "main-a", 0, init_a).await["result"]
            ["revision"],
        1
    );
    let path = fixture.root.path().join("snapshot-tool.txt");
    std::fs::write(&path, "The same main turn must continue after this tool.").unwrap();
    a.scripts.lock().unwrap().push_back(HttpScript {
        tool_input: Some(json!({"path":path})),
        pause_after_delta: true,
        stop: "tool_use",
    });
    b.scripts.lock().unwrap().extend([
        HttpScript {
            tool_input: None,
            pause_after_delta: false,
            stop: "max_tokens",
        },
        HttpScript {
            tool_input: None,
            pause_after_delta: false,
            stop: "end_turn",
        },
    ]);
    let admitted = named_with_events(
        &mut fixture.client,
        53,
        "turn/start",
        json!({
            "sessionId":main, "input":"Read snapshot-tool.txt and finish this same turn.",
            "submissionId":"one-snapshot-turn"
        }),
        &mut events,
    )
    .await;
    assert_eq!(
        admitted["result"]["turnId"], "one-snapshot-turn",
        "{admitted}"
    );
    tokio::time::timeout(REPLY_TIMEOUT, a.streamed.notified())
        .await
        .unwrap();
    // Read the actual streamed delta, not merely the server's accepted request.
    while !events
        .iter()
        .any(|message| inner_event(message)["type"] == "text.delta")
    {
        let event = fixture.client.next_message().await;
        assert_ne!(
            inner_event(&event)["type"],
            "compaction.progress",
            "the streaming fixture must leave room for its prompt and tool catalog: {event}"
        );
        events.push(event);
    }
    assert_eq!(a.requests.lock().unwrap().len(), 1);
    assert!(b.requests.lock().unwrap().is_empty());
    assert!(!events
        .iter()
        .any(|message| message["method"] == "turn/completed"));
    let begin = named_with_events(
        &mut fixture.client,
        54,
        "session/beginModelUpdate",
        json!({
            "sessionId":main, "changeId":"main-b", "expectedRevision":1
        }),
        &mut events,
    )
    .await;
    assert_eq!(begin["result"]["revision"], 1, "{begin}");
    let mut init_b = snapshot_init(&b, "fixture-b", "local", true);
    init_b["config"]["contextWindow"] = json!(161000);
    let published = named_with_events(
        &mut fixture.client,
        55,
        "session/reconfigureModel",
        json!({
            "sessionId":main, "changeId":"main-b", "expectedRevision":1, "init":init_b.clone()
        }),
        &mut events,
    )
    .await;
    assert_eq!(published["result"]["revision"], 2, "{published}");
    assert_eq!(published["result"]["changeId"], "main-b");
    let model_runtime = named_with_events(
        &mut fixture.client,
        56,
        "session/modelRuntime",
        json!({"sessionId":main}),
        &mut events,
    )
    .await;
    assert_eq!(
        model_runtime["result"]["changeId"], "main-b",
        "{model_runtime}"
    );
    assert_eq!(model_runtime["result"]["pendingChangeId"], Value::Null);
    assert_eq!(model_runtime["result"]["model"], "fixture-b");
    assert_eq!(model_runtime["result"]["contextWindow"], 161000);
    assert_eq!(model_runtime["result"]["maxOutputRetryTokens"], 3072);
    assert_eq!(model_runtime["result"]["capabilities"]["vision"], true);
    assert!(
        b.requests.lock().unwrap().is_empty(),
        "publishing must not create a request or steal A's stream"
    );
    a.release.notify_one();
    while !events.iter().any(|message| {
        message["method"] == "turn/completed" && message["params"]["turnId"] == "one-snapshot-turn"
    }) {
        let event = fixture.client.next_message().await;
        assert_ne!(inner_event(&event)["type"], "turn.failed", "{event}");
        assert!(event["params"]["event"].get("error").is_none(), "{event}");
        events.push(event);
    }
    let a_requests = a.requests.lock().unwrap().clone();
    let b_requests = b.requests.lock().unwrap().clone();
    assert_eq!(a_requests.len(), 1);
    assert_eq!(
        b_requests.len(),
        2,
        "B's truncated response and continuation belong to the original turn"
    );
    assert_eq!(a_requests[0]["model"], "fixture-a");
    assert_eq!(a_requests[0]["max_tokens"], 1024);
    assert_eq!(a_requests[0]["output_config"]["effort"], "low");
    assert_eq!(b_requests[0]["model"], "fixture-b");
    assert_eq!(b_requests[0]["max_tokens"], 1536);
    assert_eq!(b_requests[1]["max_tokens"], 3072);
    assert_eq!(b_requests[0]["output_config"]["effort"], "high");
    assert!(a_requests[0]["system"].to_string().contains("fixture-a"));
    assert!(b_requests[0]["system"].to_string().contains("fixture-b"));
    assert!(!b_requests[0]["system"].to_string().contains("fixture-a"));
    assert!(b_requests[0]["messages"]
        .to_string()
        .contains("tool_result"));
    let request_events = events
        .iter()
        .map(inner_event)
        .filter(|event| event["type"] == "stream.request_start")
        .collect::<Vec<_>>();
    assert_eq!(request_events.len(), 3);
    facade_identity(request_events[0], 1, "fixture-a", "custom", "main-a");
    for event in &request_events[1..] {
        facade_identity(event, 2, "fixture-b", "local", "main-b");
    }
    for event in events.iter().map(inner_event).filter(|event| {
        matches!(
            event["type"].as_str(),
            Some("text.delta" | "item.completed" | "provider.decision")
        )
    }) {
        match event["inference"]["revision"].as_u64() {
            Some(1) => facade_identity(event, 1, "fixture-a", "custom", "main-a"),
            Some(2) => facade_identity(event, 2, "fixture-b", "local", "main-b"),
            other => panic!("request-level attribution missing or unexpected: {other:?} {event}"),
        }
    }
    assert_eq!(
        events
            .iter()
            .filter(|message| message["method"] == "turn/completed"
                && message["params"]["turnId"] == "one-snapshot-turn")
            .count(),
        1
    );
    let replay = publish_named(&mut fixture.client, 57, main, "main-b", 1, init_b.clone()).await;
    assert_eq!(
        replay["result"]["revision"], 2,
        "lost-ACK replay is idempotent: {replay}"
    );
    let mut different = init_b;
    different["config"]["effort"] = json!("low");
    let refused = publish_named(&mut fixture.client, 58, main, "main-b", 1, different).await;
    assert!(
        refused["error"]["message"]
            .as_str()
            .unwrap()
            .contains("different configuration"),
        "{refused}"
    );
    let cost = request(
        &mut fixture.client,
        59,
        "session/cost",
        json!({"sessionId":main}),
    )
    .await;
    let actual = cost["result"]["cost"]["totalCostUsd"]
        .as_f64()
        .expect("public cost snapshot");
    assert!((actual - 0.000525).abs() < 1e-12, "{cost}");
    let side = runtime(&mut fixture, 60).await;
    assert_eq!(
        side["revision"], 0,
        "selection scope must not leak to the sibling session"
    );
    fixture.close().await;
}

#[tokio::test]
async fn snapshot_named_host_failure_fails_only_its_fence_and_never_sends_cached_a() {
    let provider = LoopbackProvider::start().await;
    let mut fixture = Fixture::new(false, None).await;
    begin_named(&mut fixture.client, 100, "side", "initial", 0).await;
    let published = publish_named(
        &mut fixture.client,
        101,
        "side",
        "initial",
        0,
        snapshot_init(&provider, "fixture-a", "custom", false),
    )
    .await;
    assert_eq!(published["result"]["revision"], 1);
    begin_named(&mut fixture.client, 102, "side", "older", 1).await;
    begin_named(&mut fixture.client, 103, "side", "latest", 1).await;
    let stale = request(
        &mut fixture.client,
        104,
        "session/failModelUpdate",
        json!({"sessionId":"side","changeId":"older","error":"stale preparation failed"}),
    )
    .await;
    assert_eq!(stale["result"], json!({"revision":1,"changeId":"older"}));
    let pending = runtime(&mut fixture, 105).await;
    assert_eq!(pending["pendingChangeId"], "latest");
    assert!(pending.get("modelUpdateError").is_none());
    let failed = request(
        &mut fixture.client,
        106,
        "session/failModelUpdate",
        json!({"sessionId":"side","changeId":"latest","error":"host target unavailable"}),
    )
    .await;
    assert_eq!(failed["result"], json!({"revision":1,"changeId":"latest"}));
    let runtime = runtime(&mut fixture, 107).await;
    assert_eq!(runtime["changeId"], "initial");
    assert_eq!(runtime["pendingChangeId"], "latest");
    assert_eq!(runtime["modelUpdateError"], "host target unavailable");
    let mut turn = fixture
        .source
        .run_streamed(
            Input::Text("Do not send cached A".into()),
            TurnOptions::default(),
        )
        .events;
    let mut failed_turn = false;
    while let Some(event) = futures::StreamExt::next(&mut turn).await {
        failed_turn |= matches!(
            event,
            klaus_agent_core::types::events::EngineEvent::TurnFailed { .. }
        );
    }
    assert!(failed_turn);
    assert!(provider.requests.lock().unwrap().is_empty());
    fixture.close().await;
}

#[tokio::test]
async fn snapshot_named_begin_failure_blocks_new_work_instead_of_using_cached_a() {
    let provider = LoopbackProvider::start().await;
    let mut fixture = Fixture::new(false, None).await;
    assert_eq!(
        begin_named(&mut fixture.client, 20, "side", "a", 0).await["result"]["revision"],
        0
    );
    assert_eq!(
        publish_named(
            &mut fixture.client,
            21,
            "side",
            "a",
            0,
            snapshot_init(&provider, "fixture-a", "custom", false)
        )
        .await["result"]["revision"],
        1
    );
    let unbegun = publish_named(
        &mut fixture.client,
        22,
        "side",
        "unbegun",
        1,
        snapshot_init(&provider, "fixture-b", "local", true),
    )
    .await;
    assert!(
        unbegun["error"]["message"]
            .as_str()
            .unwrap()
            .contains("has not begun"),
        "{unbegun}"
    );
    assert_eq!(
        begin_named(&mut fixture.client, 23, "side", "invalid-b", 1).await["result"]["revision"],
        1
    );
    let failed = publish_named(
        &mut fixture.client,
        24,
        "side",
        "invalid-b",
        1,
        json!({"provider":{"type":"invalid"}}),
    )
    .await;
    assert!(failed.get("error").is_some(), "{failed}");
    let current = runtime(&mut fixture, 25).await;
    assert_eq!(current["revision"], 1);
    assert_eq!(current["changeId"], "a");
    assert_eq!(current["pendingChangeId"], "invalid-b");
    assert!(
        current["modelUpdateError"]
            .as_str()
            .is_some_and(|error| !error.is_empty()),
        "{current}"
    );
    let mut turn = fixture
        .source
        .run_streamed(
            Input::Text("This must not use cached A after a failed explicit begin".into()),
            TurnOptions::default(),
        )
        .events;
    let mut failed_turn = false;
    while let Some(event) = futures::StreamExt::next(&mut turn).await {
        if matches!(
            event,
            klaus_agent_core::types::events::EngineEvent::TurnFailed { .. }
        ) {
            failed_turn = true;
        }
    }
    assert!(
        failed_turn,
        "the failure must not become a successful empty turn"
    );
    assert!(provider.requests.lock().unwrap().is_empty());
    assert_eq!(
        begin_named(&mut fixture.client, 26, "side", "recover-b", 1).await["result"]["revision"],
        1
    );
    assert_eq!(
        publish_named(
            &mut fixture.client,
            27,
            "side",
            "recover-b",
            1,
            snapshot_init(&provider, "fixture-b", "local", true)
        )
        .await["result"]["revision"],
        2
    );
    run_turn(
        &mut fixture.client,
        28,
        "side",
        "Only the recovered target may receive this input",
    )
    .await;
    assert_eq!(provider.requests.lock().unwrap().len(), 1);
    assert_eq!(provider.requests.lock().unwrap()[0]["model"], "fixture-b");
    fixture.close().await;
}

#[tokio::test]
async fn snapshot_latest_named_selector_supersedes_slow_preparation_without_clearing_its_fence() {
    let provider = LoopbackProvider::start().await;
    let credentials = Arc::new(PausedCredentialStore::default());
    let mut fixture = Fixture::with_credentials(false, None, Some(credentials.clone())).await;
    assert_eq!(
        begin_named(&mut fixture.client, 20, "side", "slow", 0).await["result"]["revision"],
        0
    );
    let mut slow = snapshot_init(&provider, "fixture-slow", "custom", false);
    slow["auth"] = json!({"type":"api_key_stored","credentialId":"slow"});
    fixture
        .client
        .send(
            json!({"id":21,"method":"session/reconfigureModel","params":{
                "sessionId":"side","init":slow,"changeId":"slow","expectedRevision":0
            }}),
        )
        .await;
    tokio::time::timeout(REPLY_TIMEOUT, credentials.entered.notified())
        .await
        .unwrap();
    assert_eq!(
        begin_named(&mut fixture.client, 22, "side", "winner", 0).await["result"]["revision"],
        0
    );
    let wrong_cas = publish_named(
        &mut fixture.client,
        23,
        "side",
        "winner",
        99,
        snapshot_init(&provider, "fixture-winner", "local", true),
    )
    .await;
    assert!(
        wrong_cas["error"]["message"]
            .as_str()
            .unwrap()
            .contains("revision conflict"),
        "{wrong_cas}"
    );
    let failed = runtime(&mut fixture, 24).await;
    assert_eq!(failed["pendingChangeId"], "winner");
    assert!(failed["modelUpdateError"]
        .as_str()
        .unwrap()
        .contains("revision conflict"));
    assert_eq!(
        begin_named(&mut fixture.client, 25, "side", "winner-retry", 0).await["result"]["revision"],
        0
    );
    credentials.release.notify_one();
    let stale = fixture.client.response_for(21).await.0;
    assert!(
        stale["error"]["message"]
            .as_str()
            .unwrap()
            .contains("superseded"),
        "{stale}"
    );
    let pending = runtime(&mut fixture, 26).await;
    assert_eq!(pending["pendingChangeId"], "winner-retry");
    assert_eq!(
        pending["modelUpdateError"],
        Value::Null,
        "stale failure cannot poison the newest selector"
    );
    assert!(provider.requests.lock().unwrap().is_empty());
    assert_eq!(
        publish_named(
            &mut fixture.client,
            27,
            "side",
            "winner-retry",
            0,
            snapshot_init(&provider, "fixture-winner", "local", true)
        )
        .await["result"]["revision"],
        1
    );
    run_turn(
        &mut fixture.client,
        28,
        "side",
        "The latest selector owns the only new request",
    )
    .await;
    assert_eq!(provider.requests.lock().unwrap().len(), 1);
    assert_eq!(
        provider.requests.lock().unwrap()[0]["model"],
        "fixture-winner"
    );
    fixture.close().await;
}

#[tokio::test]
async fn snapshot_named_preparation_has_a_75_second_deadline_and_never_uses_old_provider_after_timeout(
) {
    let provider = LoopbackProvider::start().await;
    let credentials = Arc::new(PausedCredentialStore::default());
    let mut fixture = Fixture::with_credentials(false, None, Some(credentials.clone())).await;
    assert_eq!(
        begin_named(&mut fixture.client, 20, "side", "deadline", 0).await["result"]["revision"],
        0
    );
    let mut slow = snapshot_init(&provider, "fixture-timeout", "custom", false);
    slow["auth"] = json!({"type":"api_key_stored","credentialId":"slow"});
    fixture
        .client
        .send(
            json!({"id":21,"method":"session/reconfigureModel","params":{
                "sessionId":"side","init":slow,"changeId":"deadline","expectedRevision":0
            }}),
        )
        .await;
    tokio::time::timeout(REPLY_TIMEOUT, credentials.entered.notified())
        .await
        .unwrap();
    // Pause only after real HTTP/endpoint setup, so fixture I/O is not racing
    // Tokio's automatic clock advance. The workspace core test target supplies
    // Tokio's test-util feature; run this together with the core test targets.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(74)).await;
    assert_eq!(credentials.finished.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.source.model_revision(), 0);
    assert_eq!(
        fixture.source.model_update_status(),
        (None, Some("deadline".to_string()), None)
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    let timeout = fixture.client.response_for(21).await.0;
    assert!(
        timeout["error"]["message"]
            .as_str()
            .unwrap()
            .contains("timed out"),
        "{timeout}"
    );
    assert_eq!(
        credentials.finished.load(Ordering::SeqCst),
        1,
        "the abandoned credential read must be dropped"
    );
    let current = runtime(&mut fixture, 22).await;
    assert_eq!(current["revision"], 0);
    assert_eq!(current["pendingChangeId"], "deadline");
    assert!(
        current["modelUpdateError"]
            .as_str()
            .unwrap()
            .contains("timed out"),
        "{current}"
    );
    let mut turn = fixture
        .source
        .run_streamed(
            Input::Text("Never silently send the old provider after preparation timeout".into()),
            TurnOptions::default(),
        )
        .events;
    let mut failed = false;
    while let Some(event) = futures::StreamExt::next(&mut turn).await {
        failed |= matches!(
            event,
            klaus_agent_core::types::events::EngineEvent::TurnFailed { .. }
        );
    }
    assert!(failed);
    assert!(provider.requests.lock().unwrap().is_empty());
    tokio::time::resume();
    fixture.close().await;
}

#[tokio::test]
async fn snapshot_closing_an_explicit_named_preparation_cancels_it_without_a_commit_or_request() {
    let provider = LoopbackProvider::start().await;
    let credentials = Arc::new(PausedCredentialStore::default());
    let mut fixture = Fixture::with_credentials(false, None, Some(credentials.clone())).await;
    assert_eq!(
        begin_named(&mut fixture.client, 20, "side", "closing", 0).await["result"]["revision"],
        0
    );
    let before = fixture.source.history();
    let mut slow = snapshot_init(&provider, "fixture-never-used", "custom", false);
    slow["auth"] = json!({"type":"api_key_stored","credentialId":"slow"});
    fixture
        .client
        .send(
            json!({"id":21,"method":"session/reconfigureModel","params":{
                "sessionId":"side","init":slow,"changeId":"closing","expectedRevision":0
            }}),
        )
        .await;
    tokio::time::timeout(REPLY_TIMEOUT, credentials.entered.notified())
        .await
        .unwrap();
    fixture
        .client
        .send(json!({"id":22,"method":"session/close","params":{"sessionId":"side"}}))
        .await;
    let (mut closed, mut cancelled) = (false, false);
    while !closed || !cancelled {
        let message = fixture.client.next_message().await;
        if message["id"] == 21 {
            assert!(
                message["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("closing"),
                "{message}"
            );
            cancelled = true;
        }
        if message["id"] == 22 {
            assert!(message.get("error").is_none(), "{message}");
            closed = true;
        }
    }
    assert_eq!(credentials.finished.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.source.model_revision(), 0);
    assert_eq!(fixture.source.history(), before);
    assert!(fixture.source.is_disposed());
    assert!(provider.requests.lock().unwrap().is_empty());
    fixture.close().await;
}
