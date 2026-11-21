//! Defect: generic upstream HTTP 429 `RESOURCE_EXHAUSTED` responses with the
//! default gateway text `"Resource has been exhausted (e.g. check quota)."`
//! were misclassified as terminal `antigravity_quota_exhausted` failures even
//! when the user's model allowance pool was full, aborting the turn immediately
//! without rate-limit backoff retries.
//! Root cause: `cloud.rs::service_error` matched any error message containing
//! both `"quota"` and `"exhaust"` as explicit account quota exhaustion, hitting
//! the parenthetical `"(e.g. check quota)"` hint on generic upstream throttling.
//! Work item: 20261120 Antigravity generic 429 rate-limit vs hard quota classification.

use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
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
    Http429Json { message: &'static str },
    Reply(&'static str),
}

struct MockUpstream {
    endpoint: String,
    stop: Arc<AtomicBool>,
    plans: Arc<Mutex<HashMap<String, VecDeque<MockAction>>>>,
    seen: Arc<Mutex<Vec<(String, String)>>>,
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

        let thread_stop = stop.clone();
        let thread_plans = plans.clone();
        let thread_seen = seen.clone();

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
                            "defaultAgentModelId": "claude-opus-5-5-high",
                            "models": {
                                "claude-opus-5-5-high": {
                                    "displayName": "Claude Opus 5.5 (High)",
                                    "recommended": true,
                                    "maxTokens": 1_000_000,
                                    "maxOutputTokens": 128_000,
                                    "supportsThinking": true,
                                    "supportsAdaptiveThinking": true,
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

                    let body_raw = String::from_utf8_lossy(
                        &raw[header_end..raw.len().min(header_end + content_len)],
                    )
                    .into_owned();
                    let body_json: Value = serde_json::from_str(&body_raw).unwrap_or(Value::Null);
                    let project = body_json["project"].as_str().unwrap_or("").to_string();
                    if !body_raw.contains("probe-429-case") {
                        write_sse_ok(&mut stream, "aux");
                        return;
                    }
                    seen.lock().unwrap().push((bearer.clone(), project));

                    let action = plans
                        .lock()
                        .unwrap()
                        .get_mut(&bearer)
                        .and_then(VecDeque::pop_front)
                        .unwrap_or(MockAction::Reply("ok"));

                    match action {
                        MockAction::Http429Json { message } => {
                            let err = json!({
                                "error": {
                                    "code": 429,
                                    "status": "RESOURCE_EXHAUSTED",
                                    "message": message
                                }
                            })
                            .to_string();
                            let reply = format!(
                                "HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{err}",
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
            accept_thread: Some(accept_thread),
        }
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
                "clientInfo": {"name": "antigravity-429-regression", "version": "1"},
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
            let line = self.lines.recv_timeout(remaining).unwrap_or_else(|err| {
                panic!(
                    "receive facade message before timeout ({err}); pending={:?}",
                    self.pending
                )
            });
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

#[test]
fn generic_429_resource_exhausted_retries_as_rate_limit_while_explicit_quota_fails_fast() {
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
                "defaultAgentModelId": "claude-opus-5-5-high",
                "models": {
                    "claude-opus-5-5-high": {
                        "displayName": "Claude Opus 5.5 (High)",
                        "recommended": true,
                        "maxTokens": 1_000_000,
                        "maxOutputTokens": 128_000,
                        "supportsThinking": true,
                        "supportsAdaptiveThinking": true,
                        "thinkingLevel": 3
                    }
                }
            }
        })
        .to_string(),
    )
    .unwrap();

    let mut facade = AppServerFacade::start(&binary, root.path(), &upstream.endpoint);

    let started = facade.call(
        "session/start",
        json!({
            "init": {
                "modelSelection": "antigravity:claude-opus-5-5-high",
                "cwd": root.path().join("cwd"),
                "baseInstructions": "Reply concisely."
            }
        }),
    );
    let sid = started["result"]["sessionId"]
        .as_str()
        .unwrap_or_else(|| panic!("session/start failed: {started}"))
        .to_string();
    let _ = facade.call("session/prepare", json!({"sessionId": sid}));
    let _ = facade.call("session/subscribe", json!({"sessionId": sid}));

    // 1. Both stored accounts encounter the generic gateway 429 text
    //    `"Resource has been exhausted (e.g. check quota)."` on the first attempt;
    //    because this is transient capacity/rate throttling rather than exhausted
    //    account allowance, establishment backs off and succeeds on the retry.
    upstream.seen.lock().unwrap().clear();
    {
        let mut plans = upstream.plans.lock().unwrap();
        plans.insert(
            "tok-1".into(),
            VecDeque::from([
                MockAction::Http429Json {
                    message: "Resource has been exhausted (e.g. check quota).",
                },
                MockAction::Reply("recovered-from-generic-429"),
            ]),
        );
        plans.insert(
            "tok-2".into(),
            VecDeque::from([
                MockAction::Http429Json {
                    message: "Resource has been exhausted (e.g. check quota).",
                },
                MockAction::Reply("recovered-from-generic-429"),
            ]),
        );
    }

    let submit_ok = facade.call(
        "turn/start",
        json!({
            "sessionId": sid,
            "submissionId": "turn-generic-429",
            "input": "probe-429-case generic"
        }),
    );
    assert!(submit_ok.get("error").is_none(), "{submit_ok}");
    let terminal_ok = facade.next_until(Duration::from_secs(15), |msg| {
        matches!(
            msg.get("method").and_then(Value::as_str),
            Some("turn/completed" | "turn/failed")
        ) && msg["params"]
            .get("submissionId")
            .or_else(|| msg["params"].get("turnId"))
            .and_then(Value::as_str)
            == Some("turn-generic-429")
    });
    assert_eq!(
        terminal_ok["method"], "turn/completed",
        "generic 429 RESOURCE_EXHAUSTED must back off and recover instead of failing as quota exhaustion: {terminal_ok}"
    );
    assert_eq!(
        terminal_ok["params"]["result"]["finalText"],
        "recovered-from-generic-429"
    );
    assert!(
        upstream.seen.lock().unwrap().len() >= 3,
        "must retry after both accounts hit generic 429: {:?}",
        upstream.seen.lock().unwrap()
    );

    // 2. Explicit personal allowance exhaustion (`Individual quota reached...`)
    //    across all stored accounts must still fail fast as `antigravity_quota_exhausted`
    //    without waiting through same-account rate-limit backoff retries.
    upstream.seen.lock().unwrap().clear();
    {
        let mut plans = upstream.plans.lock().unwrap();
        plans.insert(
            "tok-1".into(),
            VecDeque::from([
                MockAction::Http429Json {
                    message: "Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 49m7s.",
                },
                MockAction::Reply("unexpected-same-account-retry"),
            ]),
        );
        plans.insert(
            "tok-2".into(),
            VecDeque::from([
                MockAction::Http429Json {
                    message: "Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 49m7s.",
                },
                MockAction::Reply("unexpected-same-account-retry"),
            ]),
        );
    }

    let submit_quota = facade.call(
        "turn/start",
        json!({
            "sessionId": sid,
            "submissionId": "turn-hard-quota",
            "input": "probe-429-case quota"
        }),
    );
    assert!(submit_quota.get("error").is_none(), "{submit_quota}");
    let terminal_quota = facade.next_until(Duration::from_secs(15), |msg| {
        matches!(
            msg.get("method").and_then(Value::as_str),
            Some("turn/completed" | "turn/failed")
        ) && msg["params"]
            .get("submissionId")
            .or_else(|| msg["params"].get("turnId"))
            .and_then(Value::as_str)
            == Some("turn-hard-quota")
    });
    assert_eq!(
        terminal_quota["method"], "turn/failed",
        "explicit individual quota exhaustion must fail fast: {terminal_quota}"
    );
    let err_obj = &terminal_quota["params"]["detail"]["error"];
    assert_eq!(
        err_obj["providerError"]["code"], "antigravity_quota_exhausted",
        "expected antigravity_quota_exhausted code, got: {terminal_quota}"
    );
    assert_eq!(err_obj["recoveryReason"], "not-retryable");
    assert_eq!(
        upstream.seen.lock().unwrap().len(),
        2,
        "explicit quota exhaustion must try each stored account once without same-account backoff retries"
    );
}
