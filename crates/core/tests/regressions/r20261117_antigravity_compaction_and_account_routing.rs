//! Defect: Antigravity requests stalled for minutes when multiple chats or
//! side requests ran concurrently, let million-token catalog windows exceed
//! the intended source-specific compaction budget, and funneled
//! every conversation onto the head account without rotating on HTTP 503
//! prefill-queue overloads.
//! Root cause: `cloud.rs` gated each account behind a two-stream client
//! semaphore, `catalog.rs` projected million-token `contextWindow` values
//! without a `256_000` `autoCompactTokenLimit` ceiling, and pooled requests
//! always picked `accounts[0]` while excluding `antigravity_service_unavailable`
//! from pre-response account rotation.
//! Work item: 20261117 Antigravity concurrency, 256k compaction cap, and sticky
//! multi-account failover.

use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

fn app_server_binary() -> PathBuf {
    let path = std::env::var_os("KLAUS_TEST_APP_SERVER_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let target = std::env::var_os("CARGO_TARGET_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"));
            target.join("debug").join(if cfg!(windows) {
                "klaus-agent-stdio.exe"
            } else {
                "klaus-agent-stdio"
            })
        });
    assert!(
        path.is_file(),
        "set KLAUS_TEST_APP_SERVER_BIN to the built klaus-agent-stdio binary; {} is missing",
        path.display()
    );
    path
}

#[derive(Clone)]
enum MockAction {
    HoldUntilBarrier { text: &'static str },
    Http503Overloaded,
    Reply(&'static str),
}

struct MockUpstream {
    endpoint: String,
    stop: Arc<AtomicBool>,
    plans: Arc<Mutex<HashMap<String, VecDeque<MockAction>>>>,
    seen: Arc<Mutex<Vec<(String, String)>>>,
    active_streams: Arc<AtomicUsize>,
    peak_streams: Arc<AtomicUsize>,
    barrier: Arc<(Mutex<bool>, Condvar)>,
    entered_notify: Arc<(Mutex<usize>, Condvar)>,
    accept_thread: Option<JoinHandle<()>>,
}

impl MockUpstream {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let plans: Arc<Mutex<HashMap<String, VecDeque<MockAction>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let seen: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
        let active_streams = Arc::new(AtomicUsize::new(0));
        let peak_streams = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new((Mutex::new(false), Condvar::new()));
        let entered_notify = Arc::new((Mutex::new(0usize), Condvar::new()));

        let thread_stop = stop.clone();
        let thread_plans = plans.clone();
        let thread_seen = seen.clone();
        let thread_active = active_streams.clone();
        let thread_peak = peak_streams.clone();
        let thread_barrier = barrier.clone();
        let thread_entered = entered_notify.clone();

        let accept_thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::SeqCst) {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                if thread_stop.load(Ordering::SeqCst) {
                    break;
                }
                let plans = thread_plans.clone();
                let seen = thread_seen.clone();
                let active = thread_active.clone();
                let peak = thread_peak.clone();
                let barrier = thread_barrier.clone();
                let entered = thread_entered.clone();
                thread::spawn(move || {
                    let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
                    let mut raw = Vec::new();
                    let mut buf = [0u8; 4096];
                    let (header_end, content_len) = loop {
                        let Ok(n) = stream.read(&mut buf) else {
                            return;
                        };
                        if n == 0 {
                            return;
                        }
                        raw.extend_from_slice(&buf[..n]);
                        if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                            let hdr = String::from_utf8_lossy(&raw[..pos]);
                            let len = hdr
                                .lines()
                                .find_map(|line| {
                                    line.to_ascii_lowercase()
                                        .strip_prefix("content-length:")
                                        .and_then(|v| v.trim().parse::<usize>().ok())
                                })
                                .unwrap_or(0);
                            break (pos + 4, len);
                        }
                    };
                    while raw.len() < header_end + content_len {
                        let Ok(n) = stream.read(&mut buf) else {
                            break;
                        };
                        if n == 0 {
                            break;
                        }
                        raw.extend_from_slice(&buf[..n]);
                    }
                    let hdr = String::from_utf8_lossy(&raw[..header_end]);
                    let path = hdr
                        .lines()
                        .next()
                        .and_then(|l| l.split_whitespace().nth(1))
                        .unwrap_or("")
                        .to_string();
                    let bearer = hdr
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("authorization: bearer ")
                                .map(|tail| line[line.len() - tail.len()..].trim().to_string())
                        })
                        .unwrap_or_default();

                    if path.contains("fetchAvailableModels") {
                        let catalog = json!({
                            "defaultAgentModelId": "gemini-4-carbon-high",
                            "models": {
                                "gemini-4-carbon-high": {
                                    "displayName": "Gemini 4 Carbon (High)",
                                    "recommended": true,
                                    "maxTokens": 1_048_576,
                                    "maxOutputTokens": 262_144,
                                    "supportsThinking": true,
                                    "thinkingLevel": 3
                                }
                            }
                        })
                        .to_string();
                        let reply = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{catalog}",
                            catalog.len()
                        );
                        let _ = stream.write_all(reply.as_bytes());
                        return;
                    }
                    if !path.contains("streamGenerateContent") {
                        let body = "{}";
                        let reply = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = stream.write_all(reply.as_bytes());
                        return;
                    }

                    let body_json: Value = serde_json::from_slice(
                        &raw[header_end..raw.len().min(header_end + content_len)],
                    )
                    .unwrap_or(Value::Null);
                    let project = body_json["project"].as_str().unwrap_or("").to_string();
                    seen.lock().unwrap().push((bearer.clone(), project));

                    let action = plans
                        .lock()
                        .unwrap()
                        .get_mut(&bearer)
                        .and_then(VecDeque::pop_front)
                        .unwrap_or(MockAction::Reply("ok"));

                    match action {
                        MockAction::HoldUntilBarrier { text } => {
                            let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                            peak.fetch_max(now, Ordering::SeqCst);
                            {
                                let (lock, cvar) = &*entered;
                                let mut count = lock.lock().unwrap();
                                *count += 1;
                                cvar.notify_all();
                            }
                            {
                                let (lock, cvar) = &*barrier;
                                let mut open = lock.lock().unwrap();
                                while !*open {
                                    let (next, timeout) =
                                        cvar.wait_timeout(open, Duration::from_secs(10)).unwrap();
                                    open = next;
                                    if timeout.timed_out() {
                                        break;
                                    }
                                }
                            }
                            active.fetch_sub(1, Ordering::SeqCst);
                            write_sse_ok(&mut stream, text);
                        }
                        MockAction::Http503Overloaded => {
                            let err = json!({
                                "error": {
                                    "code": 503,
                                    "status": "UNAVAILABLE",
                                    "message": "overloaded prefill queue",
                                    "details": [{"reason": "PREFILL_QUEUE_OVERLOADED"}]
                                }
                            })
                            .to_string();
                            let reply = format!(
                                "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{err}",
                                err.len()
                            );
                            let _ = stream.write_all(reply.as_bytes());
                        }
                        MockAction::Reply(text) => {
                            write_sse_ok(&mut stream, text);
                        }
                    }
                });
            }
        });

        Self {
            endpoint,
            stop,
            plans,
            seen,
            active_streams,
            peak_streams,
            barrier,
            entered_notify,
            accept_thread: Some(accept_thread),
        }
    }

    fn wait_for_entered(&self, target: usize, timeout: Duration) -> usize {
        let deadline = Instant::now() + timeout;
        let (lock, cvar) = &*self.entered_notify;
        let mut count = lock.lock().unwrap();
        while *count < target {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let (next, _) = cvar.wait_timeout(count, remaining).unwrap();
            count = next;
        }
        *count
    }

    fn open_barrier(&self) {
        let (lock, cvar) = &*self.barrier;
        *lock.lock().unwrap() = true;
        cvar.notify_all();
    }
}

fn write_sse_ok(stream: &mut std::net::TcpStream, text: &str) {
    let sse = format!(
        "data: {{\"response\":{{\"candidates\":[{{\"content\":{{\"role\":\"model\",\"parts\":[{{\"text\":\"{text}\"}}]}},\"finishReason\":\"STOP\"}}],\"usageMetadata\":{{\"promptTokenCount\":120,\"candidatesTokenCount\":4,\"totalTokenCount\":124}}}}}}\n\n"
    );
    let reply = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{sse}",
        sse.len()
    );
    let _ = stream.write_all(reply.as_bytes());
}

impl Drop for MockUpstream {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.open_barrier();
        if let Some(addr) = self.endpoint.strip_prefix("http://") {
            let _ = std::net::TcpStream::connect(addr);
        }
        if let Some(handle) = self.accept_thread.take() {
            let _ = handle.join();
        }
    }
}

struct AppServerFacade {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Receiver<String>,
    pending: VecDeque<Value>,
    next_id: u64,
}

impl AppServerFacade {
    fn start(binary: &Path, root: &Path, endpoint: &str) -> Self {
        let mut child = Command::new(binary)
            .arg("--app-server")
            .env("KLAUS_CONFIG_DIR", root)
            .env("KLAUS_SESSION_DIR", root.join("sessions"))
            .env("KLAUS_PLUGIN_HOME", root.join("plugins"))
            .env("KLAUS_DISABLE_INSTALLED_PLUGINS", "1")
            .env("KLAUS_ANTIGRAVITY_API_URL", endpoint)
            .env("KLAUS_ANTIGRAVITY_HUB_IDENTITY", "9.9.9:123456")
            .env("KLAUS_ANTIGRAVITY_ROTATION_COOLDOWN_MS", "0")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn klaus-agent-stdio app-server");
        let stdin = child.stdin.take().expect("app-server stdin");
        let stdout = child.stdout.take().expect("app-server stdout");
        let (sender, lines) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if !line.trim().is_empty() && sender.send(line).is_err() {
                    break;
                }
            }
        });
        let mut facade = Self {
            child,
            stdin: Some(stdin),
            lines,
            pending: VecDeque::new(),
            next_id: 1,
        };
        let init = facade.call(
            "initialize",
            json!({
                "clientInfo": {"name": "antigravity-routing-regression", "version": "1"},
                "capabilities": {"productEvents": true}
            }),
        );
        assert_eq!(init["result"]["userAgent"], "klaus-app-server");
        facade.send(json!({"method": "initialized"}));
        facade
    }

    fn send(&mut self, value: Value) {
        let stdin = self.stdin.as_mut().expect("open app-server stdin");
        writeln!(stdin, "{value}").expect("write JSON-RPC line");
        stdin.flush().expect("flush JSON-RPC line");
    }

    fn call(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        self.send(json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }));
        self.next_until(Duration::from_secs(20), |msg| {
            msg.get("id").and_then(Value::as_u64) == Some(id) && msg.get("method").is_none()
        })
    }

    fn next_until(&mut self, timeout: Duration, mut pred: impl FnMut(&Value) -> bool) -> Value {
        if let Some(index) = self.pending.iter().position(&mut pred) {
            return self.pending.remove(index).unwrap();
        }
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "timed out waiting for facade message");
            let line = self
                .lines
                .recv_timeout(remaining)
                .expect("receive facade message before timeout");
            let message: Value = serde_json::from_str(&line).expect("valid facade JSON");
            if pred(&message) {
                return message;
            }
            self.pending.push_back(message);
        }
    }
}

impl Drop for AppServerFacade {
    fn drop(&mut self) {
        drop(self.stdin.take());
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn contains_side_question_reply(value: &Value, submission_id: &str) -> Option<String> {
    match value {
        Value::Object(map) => {
            for key in ["sideQuestion", "side_question"] {
                if let Some(sq) = map.get(key).and_then(Value::as_object) {
                    let sub_ok = map
                        .get("submissionId")
                        .or_else(|| map.get("submission_id"))
                        .and_then(Value::as_str)
                        == Some(submission_id);
                    if sub_ok {
                        if let Some(resp) = sq.get("response").and_then(Value::as_str) {
                            return Some(resp.to_string());
                        }
                    }
                }
            }
            for child in map.values() {
                if let Some(found) = contains_side_question_reply(child, submission_id) {
                    return Some(found);
                }
            }
            None
        }
        Value::Array(items) => items
            .iter()
            .find_map(|item| contains_side_question_reply(item, submission_id)),
        _ => None,
    }
}

#[test]
fn antigravity_sessions_cap_compaction_at_256k_run_four_streams_concurrently_and_shard_sticky_accounts(
) {
    let binary = app_server_binary();
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(root.path().join("cwd")).unwrap();

    let upstream = MockUpstream::start();

    std::fs::write(
        root.path().join("antigravity-auth.json"),
        json!({
            "accounts": [
                {
                    "id": "acct-id-1",
                    "generation": "gen-1",
                    "credential": {
                        "accessToken": "tok-1",
                        "refreshToken": null,
                        "expiresAt": null,
                        "account": "one@example.invalid",
                        "authMethod": "consumer",
                        "project": "proj-1",
                        "clientId": null,
                        "clientSecret": null
                    },
                    "needsLogin": false
                },
                {
                    "id": "acct-id-2",
                    "generation": "gen-2",
                    "credential": {
                        "accessToken": "tok-2",
                        "refreshToken": null,
                        "expiresAt": null,
                        "account": "two@example.invalid",
                        "authMethod": "consumer",
                        "project": "proj-2",
                        "clientId": null,
                        "clientSecret": null
                    },
                    "needsLogin": false
                }
            ]
        })
        .to_string(),
    )
    .unwrap();

    // Seed the fresh catalog snapshot for gen-1 so `modelSelection` resolves immediately.
    std::fs::write(
        root.path().join("antigravity-models.json"),
        json!({
            "generation": "gen-1",
            "fetchedAt": chrono::Utc::now().timestamp_millis(),
            "clientIdentity": format!(
                "antigravity/hub/9.9.9 (aidev_client; os_type={}; arch={}; cl=123456)",
                if cfg!(target_os = "macos") { "darwin" } else { std::env::consts::OS },
                match std::env::consts::ARCH {
                    "aarch64" => "arm64",
                    "x86_64" => "amd64",
                    other => other,
                }
            ),
            "payload": {
                "defaultAgentModelId": "gemini-4-carbon-high",
                "models": {
                    "gemini-4-carbon-high": {
                        "displayName": "Gemini 4 Carbon (High)",
                        "recommended": true,
                        "maxTokens": 1_048_576,
                        "maxOutputTokens": 262_144,
                        "supportsThinking": true,
                        "thinkingLevel": 3
                    }
                }
            }
        })
        .to_string(),
    )
    .unwrap();

    let mut facade = AppServerFacade::start(&binary, root.path(), &upstream.endpoint);

    // 1. Catalog capability projection caps 1M context window auto-compaction at 256,000 tokens.
    let models_reply = facade.call("antigravity/models", json!({"warm": false}));
    let carbon_row = models_reply["result"]["models"]
        .as_array()
        .expect("models array")
        .iter()
        .find(|row| row["key"] == "gemini-4-carbon-high")
        .expect("gemini-4-carbon-high row");
    assert_eq!(carbon_row["contextWindow"], 1_048_576);
    assert_eq!(
        carbon_row["capabilities"]["autoCompactTokenLimit"], 256_000,
        "1M Antigravity chat models must project a 256k autoCompactTokenLimit"
    );

    let start_session = |facade: &mut AppServerFacade| -> String {
        let started = facade.call(
            "session/start",
            json!({
                "init": {
                    "modelSelection": "antigravity:gemini-4-carbon-high",
                    "cwd": root.path().join("cwd"),
                    "baseInstructions": "Reply concisely."
                }
            }),
        );
        let sid = started["result"]["sessionId"]
            .as_str()
            .expect("sessionId")
            .to_string();
        let _ = facade.call("session/prepare", json!({"sessionId": sid}));
        let snapshot = facade.call("session/contextSnapshot", json!({"sessionId": sid}));
        assert_eq!(
            snapshot["result"]["context"]["contextWindow"], 1_048_576,
            "{snapshot}"
        );
        assert_eq!(
            snapshot["result"]["context"]["compactThresholdTokens"], 256_000,
            "{snapshot}"
        );
        assert_eq!(
            snapshot["result"]["context"]["autoCompactTokenLimit"], 256_000,
            "snapshot must expose active model-declared working ceiling for ring display: {snapshot}"
        );
        sid
    };

    let sid_1 = start_session(&mut facade);
    let sid_2 = start_session(&mut facade);

    // 2. Four concurrent requests on the same session/account all enter upstream in parallel
    //    without blocking behind a 2-stream client semaphore.
    upstream.plans.lock().unwrap().insert(
        "tok-1".into(),
        VecDeque::from([
            MockAction::HoldUntilBarrier { text: "s1" },
            MockAction::HoldUntilBarrier { text: "s2" },
            MockAction::HoldUntilBarrier { text: "s3" },
            MockAction::HoldUntilBarrier { text: "s4" },
        ]),
    );
    for idx in 1..=4 {
        let sub = format!("conc-{idx}");
        let res = facade.call(
            "session/sideQuestion",
            json!({
                "sessionId": sid_1,
                "question": "ping",
                "submissionId": sub,
            }),
        );
        assert!(res.get("error").is_none(), "sideQuestion rejected: {res}");
    }
    let entered = upstream.wait_for_entered(4, Duration::from_secs(8));
    assert_eq!(
        entered, 4,
        "all 4 concurrent streams must reach upstream without a 2-stream local lock"
    );
    assert_eq!(upstream.active_streams.load(Ordering::SeqCst), 4);
    assert_eq!(upstream.peak_streams.load(Ordering::SeqCst), 4);
    upstream.open_barrier();
    let mut pending_subs: std::collections::BTreeSet<String> =
        (1..=4).map(|idx| format!("conc-{idx}")).collect();
    while !pending_subs.is_empty() {
        let _ = facade.next_until(Duration::from_secs(10), |msg| {
            let matched = pending_subs
                .iter()
                .find(|sub| contains_side_question_reply(msg, sub).is_some())
                .cloned();
            if let Some(done) = matched {
                pending_subs.remove(&done);
                true
            } else {
                false
            }
        });
    }

    // 3. By default (`accountSharding == false`), Session 2 binds to the head account (`tok-1`)
    //    without automatic sharding; once `antigravity/setAccountSharding` enables sharding, a new
    //    Session 3 shards onto standby `tok-2` while existing sessions remain sticky.
    let initial_status = facade.call("antigravity/status", json!({}));
    assert_eq!(initial_status["result"]["accountSharding"], false);

    let ask_side = |facade: &mut AppServerFacade, sid: &str, sub: &str| -> String {
        let _ = facade.call(
            "session/sideQuestion",
            json!({
                "sessionId": sid,
                "question": "hello",
                "submissionId": sub,
            }),
        );
        let mut answer = String::new();
        let _ = facade.next_until(Duration::from_secs(10), |msg| {
            if let Some(reply) = contains_side_question_reply(msg, sub) {
                answer = reply;
                true
            } else {
                false
            }
        });
        answer
    };

    upstream.seen.lock().unwrap().clear();
    upstream.plans.lock().unwrap().insert(
        "tok-1".into(),
        VecDeque::from([MockAction::Reply("head-b1")]),
    );
    assert_eq!(ask_side(&mut facade, &sid_2, "sticky-only-b1"), "head-b1");
    assert_eq!(
        upstream.seen.lock().unwrap().clone(),
        vec![("tok-1".into(), "proj-1".into())],
        "with accountSharding off by default, second conversation must stick to the head account"
    );

    let sharding_reply = facade.call("antigravity/setAccountSharding", json!({"enabled": true}));
    assert_eq!(sharding_reply["result"]["ok"], true);
    assert_eq!(sharding_reply["result"]["accountSharding"], true);
    let sid_3 = start_session(&mut facade);

    upstream.seen.lock().unwrap().clear();
    {
        let mut plans = upstream.plans.lock().unwrap();
        plans.insert(
            "tok-1".into(),
            VecDeque::from([MockAction::Reply("from-1")]),
        );
        plans.insert(
            "tok-2".into(),
            VecDeque::from([MockAction::Reply("from-3a"), MockAction::Reply("from-3b")]),
        );
    }

    assert_eq!(ask_side(&mut facade, &sid_3, "shard-c1"), "from-3a");
    assert_eq!(ask_side(&mut facade, &sid_1, "sticky-a2"), "from-1");
    assert_eq!(ask_side(&mut facade, &sid_3, "sticky-c2"), "from-3b");
    assert_eq!(
        upstream.seen.lock().unwrap().clone(),
        vec![
            ("tok-2".into(), "proj-2".into()),
            ("tok-1".into(), "proj-1".into()),
            ("tok-2".into(), "proj-2".into()),
        ],
        "with accountSharding enabled, new conversation must shard onto standby account and remain sticky"
    );

    // 4. Pre-response HTTP 503 overload on `tok-1` fails Session 1 over to `tok-2` immediately.
    upstream.seen.lock().unwrap().clear();
    {
        let mut plans = upstream.plans.lock().unwrap();
        plans.insert(
            "tok-1".into(),
            VecDeque::from([MockAction::Http503Overloaded]),
        );
        plans.insert(
            "tok-2".into(),
            VecDeque::from([MockAction::Reply("failover-to-2")]),
        );
    }
    assert_eq!(
        ask_side(&mut facade, &sid_1, "overload-a3"),
        "failover-to-2"
    );
    assert_eq!(
        upstream.seen.lock().unwrap().clone(),
        vec![
            ("tok-1".into(), "proj-1".into()),
            ("tok-2".into(), "proj-2".into()),
        ]
    );
}
