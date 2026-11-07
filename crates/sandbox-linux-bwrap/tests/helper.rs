#![cfg(target_os = "linux")]

use std::fs::{self, File};
use std::io::Read;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "klaus-native-helper-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn error_text(&self) -> String {
        let mut text = String::new();
        File::open(self.0.join("stderr"))
            .unwrap()
            .take(8192)
            .read_to_string(&mut text)
            .unwrap();
        text
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn published_helper_installs_no_new_privileges_before_the_command() {
    let fixture = Fixture::new();
    let status = Command::new(env!("CARGO_BIN_EXE_klaus-sandbox-linux"))
        .args([
            "--klaus-linux-sandbox",
            "--network-allowed",
            "--",
            "/bin/sh",
            "-c",
            "grep -q '^NoNewPrivs:[[:space:]]*1$' /proc/self/status",
        ])
        .stderr(Stdio::from(File::create(fixture.0.join("stderr")).unwrap()))
        .status()
        .unwrap();
    assert!(
        status.success(),
        "native syscall helper failed: {}",
        fixture.error_text()
    );
}

#[test]
fn published_helper_confines_workspace_or_reports_namespace_setup_failure() {
    let fixture = Fixture::new();
    let workspace = fixture.0.join("workspace");
    let outside = fixture.0.join("outside");
    fs::create_dir(&workspace).unwrap();
    fs::create_dir(&outside).unwrap();
    let helper = env!("CARGO_BIN_EXE_klaus-sandbox-linux");
    let status = Command::new(helper)
        .args(["--klaus-bubblewrap", "--", "--die-with-parent", "--new-session",
            "--unshare-user", "--unshare-pid", "--unshare-ipc", "--unshare-net",
            "--ro-bind", "/", "/", "--bind"])
        .arg(&workspace).arg(&workspace)
        .args(["--proc", "/proc", "--", helper, "--klaus-linux-sandbox", "--network-restricted",
            "--", "/bin/sh", "-c", "printf allowed > \"$1/allowed\" || exit 10; if printf denied > \"$2/denied\" 2>/dev/null; then exit 11; fi",
            "fixture"])
        .arg(&workspace).arg(&outside)
        .stderr(Stdio::from(File::create(fixture.0.join("stderr")).unwrap()))
        .status().unwrap();
    assert!(
        !outside.join("denied").exists(),
        "a setup failure must never fall back to an unrestricted command"
    );
    if status.success() {
        assert_eq!(fs::read(workspace.join("allowed")).unwrap(), b"allowed");
    } else {
        let error = fixture.error_text();
        assert!(
            !workspace.join("allowed").exists(),
            "the command ran and then failed: {error}"
        );
        assert!(
            error.contains("namespace")
                || error.contains("unshare")
                || error.contains("Operation not permitted"),
            "unexpected helper failure: {error}"
        );
        eprintln!("The namespace setup was unavailable and failed closed: {error}");
    }
}
