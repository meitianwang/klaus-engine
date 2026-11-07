//! Public facade coverage for source-local model transport and tool replay.
use super::*;
use klaus_agent_core::runtime_values::RuntimePath;
use std::sync::Mutex;
use tokio::{io::AsyncReadExt, net::TcpListener};

struct Fixture {
    url: String,
    requests: Arc<Mutex<Vec<(String, Value)>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn fixture() -> Fixture {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let (path, body) = loop {
                let mut chunk = [0; 8192];
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
                assert!(bytes.len() < 8 * 1024 * 1024);
                let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") else {
                    continue;
                };
                let headers = String::from_utf8_lossy(&bytes[..end]);
                let length = headers
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .map(|(_, v)| v.trim().parse::<usize>().unwrap())
                    .unwrap_or(0);
                if bytes.len() < end + 4 + length {
                    continue;
                }
                let path = headers
                    .lines()
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .nth(1)
                    .unwrap()
                    .to_owned();
                let body = if length == 0 {
                    Value::Null
                } else {
                    serde_json::from_slice(&bytes[end + 4..end + 4 + length]).unwrap()
                };
                break (path, body);
            };
            let (kind, response) = if path.starts_with("/List-Available-Models?") {
                ("application/json",json!({"defaultModel":{"modelId":"auto"},"models":[{"modelId":"auto","modelName":"Auto"},{"modelId":"shared-model","modelName":"Fixture model","supportedInputTypes":["TEXT","IMAGE"],"tokenLimits":{"maxInputTokens":200000,"maxOutputTokens":64000},"additionalModelRequestFieldsSchema":{"properties":{"reasoning":{"properties":{"effort":{"enum":["none","high","max"],"default":"high"}}}}}}]}).to_string())
            } else if path == "/mcp" {
                assert_eq!(body["method"], "tools/call");
                assert_eq!(body["params"]["name"], "web_search");
                ("application/json",json!({"jsonrpc":"2.0","id":body["id"],"error":null,"result":{"content":[{"type":"text","text":json!({"results":[{"title":"Fixture result","url":"https://example.com/fixture","snippet":"Fixture search result"}]}).to_string()}],"isError":false}}).to_string())
            } else {
                assert!(
                    matches!(path.as_str(), "/v1/responses" | "/direct/responses"),
                    "unexpected route {path}"
                );
                let unsupported = [
                    "max_output_tokens",
                    "tool_choice",
                    "parallel_tool_calls",
                    "instructions",
                ]
                .into_iter()
                .find(|key| body.get(*key).is_some())
                .or_else(|| {
                    body["tools"].as_array().and_then(|tools| {
                        tools
                            .iter()
                            .any(|tool| {
                                tool.get("strict").is_some() || tool.get("defer_loading").is_some()
                            })
                            .then_some("tools[0].strict")
                    })
                });
                if let Some(parameter) = unsupported.filter(|_| path == "/v1/responses") {
                    let rejected = json!({"error":{"message":"This request contains parameters or input that are not supported.","type":"invalid_request","param":parameter,"code":"unsupported_parameter"}}).to_string();
                    socket.write_all(format!("HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{rejected}",rejected.len()).as_bytes()).await.unwrap();
                    socket.shutdown().await.unwrap();
                    continue;
                }
                captured.lock().unwrap().push((path.clone(), body.clone()));
                let tool_result = body["input"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|item| item["type"] == "function_call_output");
                let frames = if let Some(result) = tool_result {
                    assert!(result["output"]
                        .as_str()
                        .unwrap()
                        .contains("fixture-tool-value"));
                    vec![
                        json!({"type":"response.output_text.delta","item_id":"message-1","delta":"FACADE_TOOL_OK"}),
                        json!({"type":"response.completed","response":{"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"FACADE_TOOL_OK"}]}],"usage":{"kiro:credits":{"value":0.25}}}}),
                    ]
                } else {
                    let native=json!({"reasoningText":{"text":"bounded thoughts","signature":"native-signature"}}).to_string();
                    vec![
                        json!({"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","id":"rs-1","summary":[]}}),
                        json!({"type":if path=="/v1/responses"{"response.reasoning_text.delta"}else{"response.reasoning_summary_text.delta"},"item_id":"rs-1","delta":"bounded thoughts"}),
                        json!({"type":"response.output_item.done","output_index":0,"item":{"type":"reasoning","id":"rs-1","summary":[],"encrypted_content":native}}),
                        json!({"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","id":"fc-1","call_id":"call-1","name":"fixture_marker","arguments":""}}),
                        json!({"type":"response.function_call_arguments.delta","item_id":"fc-1","delta":"{\"value\":\"marker\"}"}),
                        json!({"type":"response.function_call_arguments.done","item_id":"fc-1","arguments":"{\"value\":\"marker\"}"}),
                        json!({"type":"response.output_item.done","output_index":1,"item":{"type":"function_call","id":"fc-1","call_id":"call-1","name":"fixture_marker","arguments":"{\"value\":\"marker\"}"}}),
                        json!({"type":"response.completed","response":{"status":"completed","output":[{"type":"function_call","id":"fc-1","call_id":"call-1","name":"fixture_marker","arguments":"{\"value\":\"marker\"}"}]}}),
                    ]
                };
                (
                    "text/event-stream",
                    frames
                        .into_iter()
                        .map(|v| format!("data: {v}\n\n"))
                        .collect::<String>()
                        + "data: [DONE]\n\n",
                )
            };
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",response.len()).as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
        }
    });
    Fixture {
        url,
        requests,
        task,
    }
}
struct Environment(Vec<(&'static str, Option<std::ffi::OsString>)>);
impl Environment {
    fn new(url: &str) -> Self {
        let mut vars = Vec::new();
        for (key, value) in [
            ("KLAUS_TEST", "1"),
            ("KLAUS_KIRO_MANAGEMENT_URL", url),
            ("KLAUS_KIRO_RUNTIME_URL", url),
        ] {
            vars.push((key, std::env::var_os(key)));
            std::env::set_var(key, value);
        }
        Self(vars)
    }
}
impl Drop for Environment {
    fn drop(&mut self) {
        for (key, value) in self.0.drain(..) {
            if let Some(value) = value {
                std::env::set_var(key, value);
            } else {
                std::env::remove_var(key);
            }
        }
    }
}

#[tokio::test]
async fn kiro_public_facade_replays_tools_and_signatures_without_changing_another_source() {
    let root = tempfile::tempdir().unwrap();
    let gateway = fixture().await;
    let _env = Environment::new(&gateway.url);
    let config = root.path().join("config.toml");
    std::fs::write(&config, "autoMemory = false\ncheckpointing = false\n").unwrap();
    let agent = make_agent_with_provider_and_config(Arc::new(fake_provider()), None, None);
    let create = crate::lifecycle::engine_sessions_in_test_root(
        agent,
        None,
        RuntimePath::from(root.path().to_string_lossy().into_owned()),
    )
    .create_session;
    let mut client = TestClient::start_full(
        create,
        None,
        Some(root.path().join("rules.json")),
        Some(config),
    );
    client.send(json!({"id":1,"method":"initialize","params":{"clientInfo":{"name":"source-fixture"},"capabilities":{"productEvents":true}}})).await;
    assert!(client.response_for(1).await.0.get("error").is_none());
    client.send(json!({"method":"initialized"})).await;
    client.send(json!({"id":1,"method":"kiro/adoptCredential","params":{"credential":{"accessToken":"fixture-token","authMethod":"social","provider":"Google","profileArn":"arn:aws:codewhisperer:us-east-1:123:profile/fixture"}}})).await;
    assert_eq!(client.response_for(1).await.0["result"]["loggedIn"], true);
    client
        .send(json!({"id":9,"method":"kiro/models","params":{}}))
        .await;
    let catalog = client.response_for(9).await.0;
    assert_eq!(catalog["result"]["models"].as_array().unwrap().len(), 1);
    assert_eq!(catalog["result"]["models"][0]["key"], "shared-model");
    assert!(catalog["result"]["models"]
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["key"] != "auto"));
    client.send(json!({"id":7,"method":"model/selection/resolve","params":{"source":"kiro","selection":"auto"}})).await;
    assert_eq!(
        client.response_for(7).await.0["result"]["selection"],
        "kiro:shared-model"
    );
    client.send(json!({"id":8,"method":"model/selection/resolve","params":{"source":"kiro","selection":"kiro:auto"}})).await;
    assert!(client.response_for(8).await.0["error"]["message"]
        .as_str()
        .unwrap()
        .contains("kiro_model_unavailable"));
    client.send(json!({"id":6,"method":"kiro/search","params":{"query":"fixture query","operationId":"fixture-search"}})).await;
    let search = client.response_for(6).await.0;
    assert_eq!(
        search["result"]["results"][0]["url"],
        "https://example.com/fixture"
    );
    for source in ["kiro", "ordinary"] {
        let mut init = json!({"cwd":root.path(),"permissionMode":"bypassPermissions","baseInstructions":"Use fixture_marker and report its output.","config":{"prewarm":false,"autoMemory":false,"checkpointing":false,"shellEnvCapture":false,"includeGitContext":false,"maxRetries":0,"maxOutputTokens":256,"maxTurns":4,"unboundedNetworkRetries":false},"dynamicTools":[{"type":"function","name":"fixture_marker","description":"Return a marker","inputSchema":{"type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false},"deferLoading":false,"readOnly":true}]});
        if source == "kiro" {
            init["modelSelection"] = json!("kiro:shared-model::effort=max");
        } else {
            init["provider"] = json!({"type":"openai","api":"responses","model":"shared-model","baseUrl":format!("{}/direct",gateway.url)});
            init["auth"] = json!({"type":"static_bearer","token":"ordinary-token"});
            init["capabilities"] = json!({"tools":true,"vision":true,"contextWindow":200000,"maxOutputTokens":64000,"supportedReasoningEfforts":["high"],"defaultReasoningEffort":"high"});
        }
        let before = gateway.requests.lock().unwrap().len();
        client
            .send(json!({"id":2,"method":"session/start","params":{"init":init}}))
            .await;
        let started = client.response_for(2).await.0;
        assert!(started.get("error").is_none(), "{started}");
        let session = started["result"]["sessionId"].as_str().unwrap().to_owned();
        client.send(json!({"id":3,"method":"turn/start","params":{"sessionId":session,"input":"Call the marker tool."}})).await;
        let (started, early) = client.response_for(3).await;
        assert!(started.get("error").is_none(), "{started}");
        let mut pending: std::collections::VecDeque<_> = early.into();
        let mut tool_called = false;
        let mut text = String::new();
        for index in 0..1024 {
            assert!(index < 1023, "bounded facade events");
            let event = match pending.pop_front() {
                Some(v) => v,
                None => client.next_message().await,
            };
            assert_ne!(event["method"], "turn/failed", "{source}: {event}");
            if let Some(call) = event["params"]["event"].get("dynamicToolCall") {
                if tool_called {
                    assert_eq!(call["callId"], "call-1");
                    continue;
                }
                assert_eq!(call["tool"], "fixture_marker");
                tool_called = true;
                client.send(json!({"id":4,"method":"session/resolveDynamicToolCall","params":{"sessionId":session,"callId":call["callId"],"response":{"success":true,"contentItems":[{"type":"inputText","text":"fixture-tool-value"}]}}})).await;
            }
            if event["method"] == "item/agentMessage/delta" {
                text.push_str(event["params"]["delta"].as_str().unwrap());
            }
            if event["method"] == "turn/completed" {
                break;
            }
        }
        assert!(tool_called);
        assert_eq!(text, "FACADE_TOOL_OK");
        {
            let requests = gateway.requests.lock().unwrap();
            let calls = &requests[before..];
            assert_eq!(calls.len(), 2);
            for (path, body) in calls {
                assert_eq!(body["model"], "shared-model");
                assert!(body["tools"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|tool| tool["name"] == "fixture_marker"));
                assert_eq!(
                    path,
                    if source == "kiro" {
                        "/v1/responses"
                    } else {
                        "/direct/responses"
                    }
                );
            }
            let replay = &calls[1].1;
            let items = replay["input"].as_array().unwrap();
            let reasoning = items
                .iter()
                .find(|item| item["type"] == "reasoning")
                .unwrap();
            let opaque: Value =
                serde_json::from_str(reasoning["encrypted_content"].as_str().unwrap()).unwrap();
            assert_eq!(opaque["reasoningText"]["signature"], "native-signature");
            if source == "kiro" {
                assert!(replay.get("max_output_tokens").is_none());
                assert_eq!(replay["origin"], "AI_EDITOR");
                assert_eq!(replay["reasoning"]["effort"], "max");
                assert!(replay.get("include").is_none());
            } else {
                assert_eq!(replay["max_output_tokens"], 256);
                assert!(replay.get("origin").is_none());
                assert!(replay.get("profileArn").is_none());
                assert!(replay["include"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|v| v == "reasoning.encrypted_content"));
            }
        }
        client
            .send(json!({"id":5,"method":"session/close","params":{"sessionId":session}}))
            .await;
        assert!(client.response_for(5).await.0.get("error").is_none());
    }
    client.close().await;
}
