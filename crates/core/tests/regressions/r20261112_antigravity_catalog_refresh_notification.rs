//! Defect: newly offered Antigravity models stayed missing after sign-in until a manual reload.
//! Root cause: background catalog refresh updated the snapshot without notifying connected clients.
//! Work item: 20261112 Antigravity catalog refresh notification.

use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::Arc;
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

struct CatalogServer {
    endpoint: String,
    stop: Arc<AtomicBool>,
    task: Option<JoinHandle<()>>,
}

impl CatalogServer {
    fn start(payload: Value) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let running = Arc::clone(&stop);
        let body = payload.to_string();
        let task = thread::spawn(move || {
            while !running.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut socket, _)) => {
                        let _ = socket.set_read_timeout(Some(Duration::from_millis(500)));
                        let mut buffer = [0u8; 4096];
                        let mut received = Vec::new();
                        while let Ok(count) = socket.read(&mut buffer) {
                            if count == 0 {
                                break;
                            }
                            received.extend_from_slice(&buffer[..count]);
                            if received.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                                break;
                            }
                        }
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        );
                        let _ = socket.write_all(response.as_bytes());
                        let _ = socket.flush();
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("catalog server accept failed: {error}"),
                }
            }
        });
        Self {
            endpoint,
            stop,
            task: Some(task),
        }
    }
}

impl Drop for CatalogServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(task) = self.task.take() {
            task.join().expect("catalog server thread");
        }
    }
}

struct Facade {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Receiver<String>,
}

impl Facade {
    fn start(binary: &Path, root: &Path, endpoint: &str) -> Self {
        let mut child = Command::new(binary)
            .arg("--app-server")
            .env("KLAUS_CONFIG_DIR", root)
            .env("KLAUS_SESSION_DIR", root.join("sessions"))
            .env("KLAUS_PLUGIN_HOME", root.join("plugins"))
            .env("KLAUS_ANTIGRAVITY_API_URL", endpoint)
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
        Self {
            child,
            stdin: Some(stdin),
            lines,
        }
    }

    fn send(&mut self, value: Value) {
        let stdin = self.stdin.as_mut().expect("open app-server stdin");
        writeln!(stdin, "{value}").expect("write JSON-RPC line");
        stdin.flush().expect("flush JSON-RPC line");
    }

    fn next_until(&mut self, timeout: Duration, mut pred: impl FnMut(&Value) -> bool) -> Value {
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
        }
    }

    fn initialize(&mut self) {
        self.send(json!({
            "id": 1,
            "method": "initialize",
            "params": {"clientInfo": {"name": "catalog-refresh-regression"}}
        }));
        let response = self.next_until(Duration::from_secs(20), |msg| {
            msg.get("id").and_then(Value::as_u64) == Some(1) && msg.get("method").is_none()
        });
        assert_eq!(response["result"]["userAgent"], "klaus-app-server");
        self.send(json!({"method": "initialized"}));
    }
}

impl Drop for Facade {
    fn drop(&mut self) {
        drop(self.stdin.take());
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn cold_baseline_read_emits_catalog_changed_once_live_models_land() {
    let binary = app_server_binary();
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("antigravity-auth.json"),
        json!({
            "generation": "fixture-generation",
            "credential": {
                "accessToken": "fixture-token",
                "refreshToken": null,
                "expiresAt": null,
                "account": null,
                "authMethod": "consumer",
                "project": "fixture-project",
                "clientId": null,
                "clientSecret": null
            },
            "needsLogin": false
        })
        .to_string(),
    )
    .unwrap();
    let server = CatalogServer::start(json!({
        "defaultAgentModelId": "gemini-4-carbon-medium",
        "agentModelSorts": [{
            "displayName": "Recommended",
            "groups": [{"modelIds": ["gemini-4-carbon-medium"]}]
        }],
        "models": {
            "gemini-4-carbon-medium": {
                "displayName": "Gemini 4 Carbon (Medium)",
                "recommended": true,
                "supportsThinking": true,
                "thinkingLevel": 2,
                "modelProvider": "MODEL_PROVIDER_GOOGLE"
            }
        }
    }));
    let mut facade = Facade::start(&binary, root.path(), &server.endpoint);
    facade.initialize();

    facade.send(json!({
        "id": 2,
        "method": "antigravity/models",
        "params": {"read": "warm"}
    }));
    let mut notified_during_read = false;
    let first = facade.next_until(Duration::from_secs(5), |msg| {
        if msg["method"] == "model/catalogChanged" {
            assert_eq!(msg["params"], json!({"source": "antigravity"}));
            notified_during_read = true;
        }
        msg.get("id").and_then(Value::as_u64) == Some(2) && msg.get("method").is_none()
    });
    assert_eq!(first["result"]["catalogBaseline"], true);
    assert!(!first["result"]["models"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["id"] == "gemini-4-carbon-medium"));

    if !notified_during_read {
        let notification = facade.next_until(Duration::from_secs(5), |msg| {
            msg["method"] == "model/catalogChanged"
        });
        assert_eq!(notification["params"], json!({"source": "antigravity"}));
    }

    facade.send(json!({
        "id": 3,
        "method": "antigravity/models",
        "params": {"read": "warm"}
    }));
    let updated = facade.next_until(Duration::from_secs(5), |msg| {
        msg.get("id").and_then(Value::as_u64) == Some(3) && msg.get("method").is_none()
    });
    assert_eq!(updated["result"]["catalogBaseline"], false);
    assert!(updated["result"]["models"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["id"] == "gemini-4-carbon-medium" && row["inRecommendedSort"] == true));
}
