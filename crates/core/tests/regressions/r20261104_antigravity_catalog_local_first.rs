//! Defect: switching to Antigravity blocked on a live catalog fetch.
//! Root cause: an empty local cache awaited the refresh lock and network.
//! Work item: 20261104 local-first Antigravity catalog reads.
//!
//! The regression drives the public stdio app-server facade. The engine binary
//! is named explicitly with `KLAUS_TEST_APP_SERVER_BIN`; its `--version` output
//! is verified before the facade is used.

use serde_json::{json, Value};
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
    let output = Command::new(&path)
        .arg("--version")
        .output()
        .expect("run klaus-agent-stdio --version");
    assert!(
        output.status.success(),
        "binary identity command failed: {:?}",
        output
    );
    let identity = String::from_utf8_lossy(&output.stdout);
    assert!(
        identity.starts_with("klaus-agent-stdio — engine build "),
        "unexpected engine identity: {identity:?}"
    );
    path
}

struct HoldServer {
    endpoint: String,
    requests: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    task: Option<JoinHandle<()>>,
}

impl HoldServer {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let stop = Arc::new(AtomicBool::new(false));
        let running = Arc::clone(&stop);
        let task = thread::spawn(move || {
            let mut held = Vec::new();
            while !running.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut socket, _)) => {
                        let _ = socket.set_read_timeout(Some(Duration::from_millis(200)));
                        let mut buffer = [0u8; 4096];
                        if let Ok(count) = socket.read(&mut buffer) {
                            let text = String::from_utf8_lossy(&buffer[..count]);
                            if let Some(path) = text
                                .lines()
                                .next()
                                .and_then(|line| line.split_whitespace().nth(1))
                            {
                                captured.lock().unwrap().push(path.to_string());
                            }
                        }
                        held.push(socket);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("catalog fixture accept failed: {error}"),
                }
            }
        });
        Self {
            endpoint,
            requests,
            stop,
            task: Some(task),
        }
    }

    fn request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

impl Drop for HoldServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(task) = self.task.take() {
            task.join().expect("catalog fixture stops");
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
            .env("KLAUS_PLUGIN_HOME", root.join("plugins"))
            .env("KLAUS_SESSION_DIR", root.join("sessions"))
            .env("KLAUS_ANTIGRAVITY_API_URL", endpoint)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn the public app-server facade");
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().expect("app-server stdout");
        let (sender, lines) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                match line {
                    Ok(line) => {
                        if sender.send(line).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            child,
            stdin,
            lines,
        }
    }

    fn send(&mut self, message: Value) {
        let stdin = self.stdin.as_mut().expect("app-server stdin");
        serde_json::to_writer(&mut *stdin, &message).unwrap();
        stdin.write_all(b"\n").unwrap();
        stdin.flush().unwrap();
    }

    fn response(&self, id: u64, timeout: Duration) -> Value {
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "no response for request {id}");
            let line = self
                .lines
                .recv_timeout(remaining)
                .unwrap_or_else(|_| panic!("no response for request {id}"));
            let message: Value = serde_json::from_str(&line).unwrap();
            if message.get("id").and_then(Value::as_u64) == Some(id)
                && message.get("method").is_none()
            {
                return message;
            }
        }
    }

    fn initialize(&mut self) {
        self.send(json!({
            "id": 1,
            "method": "initialize",
            "params": {"clientInfo": {"name": "catalog-regression"}}
        }));
        let response = self.response(1, Duration::from_secs(20));
        assert_eq!(
            response["result"]["userAgent"], "klaus-app-server",
            "unexpected facade identity: {response}"
        );
        self.send(json!({"method": "initialized"}));
    }

    fn models(&mut self, id: u64) -> (Value, Duration) {
        let started = Instant::now();
        self.send(json!({
            "id": id,
            "method": "antigravity/models",
            "params": {"read": "warm"}
        }));
        let response = self.response(id, Duration::from_secs(5));
        (response, started.elapsed())
    }
}

impl Drop for Facade {
    fn drop(&mut self) {
        drop(self.stdin.take());
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn write_credential(root: &Path) {
    std::fs::write(
        root.join("antigravity-auth.json"),
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
}

#[test]
fn local_first_catalog_facade_does_not_wait_for_an_offline_refresh() {
    let binary = app_server_binary();
    let root = tempfile::tempdir().unwrap();
    write_credential(root.path());
    let server = HoldServer::start();
    let mut facade = Facade::start(&binary, root.path(), &server.endpoint);
    facade.initialize();
    let (response, elapsed) = facade.models(2);
    assert!(
        elapsed < Duration::from_secs(4),
        "catalog read waited on the offline refresh: {elapsed:?}"
    );
    let rows = response["result"]["models"]
        .as_array()
        .unwrap_or_else(|| panic!("catalog response: {response}"));
    assert_eq!(
        response["result"]["catalogBaseline"], true,
        "the cold answer must identify itself as the bundled baseline"
    );
    let baseline = rows
        .iter()
        .find(|row| row["id"] == "gemini-4-argon-medium")
        .unwrap_or_else(|| panic!("bundled catalog missing default: {rows:?}"));
    assert_eq!(baseline["available"], false);
    assert_eq!(baseline["entitlementUnknown"], true);
    assert!(rows
        .iter()
        .any(|row| row["id"] == "gemini-3.8-flash-medium"));

    let deadline = Instant::now() + Duration::from_secs(3);
    while server.request_count() == 0 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        server.request_count(),
        1,
        "the first catalog read starts one background refresh"
    );
    for id in 3..=5 {
        let (response, elapsed) = facade.models(id);
        assert!(
            elapsed < Duration::from_secs(4),
            "request {id} waited on the in-flight refresh: {elapsed:?}"
        );
        assert!(response["result"]["models"].is_array());
    }
    thread::sleep(Duration::from_millis(300));
    assert_eq!(
        server.request_count(),
        1,
        "concurrent readers share one background refresh"
    );
}
