//! Public helper execution fixtures. These require a real Linux kernel; other
//! targets do not claim that namespace or syscall execution was exercised.
#![cfg(target_os = "linux")]

use std::ffi::OsString;
use std::io::Read;
use std::path::PathBuf;
use std::process::{Command, ExitStatus, Stdio};

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        let id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("klaus-legacy-exec-{}-{id}", std::process::id()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn run(args: Vec<OsString>, probe_address: Option<String>) -> (ExitStatus, String) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_klaus-sandbox-linux"));
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    if let Some(address) = probe_address {
        command.env("KLAUS_LEGACY_TEST_ADDRESS", address);
    }
    let mut child = command.spawn().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let mut tail = Vec::new();
    let mut chunk = [0u8; 2048];
    loop {
        let read = stderr.read(&mut chunk).unwrap();
        if read == 0 {
            break;
        }
        let discard = (tail.len() + read).saturating_sub(8192);
        tail.drain(..discard);
        tail.extend_from_slice(&chunk[..read]);
    }
    (
        child.wait().unwrap(),
        String::from_utf8_lossy(&tail).into_owned(),
    )
}

#[test]
fn restricted_filesystem_legacy_requests_fail_before_the_workload() {
    let scratch = Scratch::new();
    let output = scratch.0.join("must-not-exist");
    let (status, stderr) = run(
        vec![
            "--klaus-linux-sandbox".into(),
            "--network-restricted".into(),
            "--use-legacy-landlock".into(),
            "--".into(),
            "/bin/sh".into(),
            "-c".into(),
            "printf bypass > \"$1\"".into(),
            "sh".into(),
            output.clone().into_os_string(),
        ],
        None,
    );
    assert!(!status.success());
    assert!(
        stderr.contains(
            "filesystem-restricted execution requires bubblewrap to isolate app-server sockets"
        ),
        "{stderr}"
    );
    assert!(!output.exists());
}

#[test]
fn full_filesystem_legacy_requests_execute_without_namespace_setup() {
    let scratch = Scratch::new();
    let output = scratch.0.join("created");
    let (status, stderr) = run(
        vec![
            "--klaus-linux-sandbox".into(),
            "--network-allowed".into(),
            "--use-legacy-landlock".into(),
            "--full-filesystem-write".into(),
            "--".into(),
            "/bin/sh".into(),
            "-c".into(),
            "printf native > \"$1\"".into(),
            "sh".into(),
            output.clone().into_os_string(),
        ],
        None,
    );
    assert!(status.success(), "{stderr}");
    assert_eq!(std::fs::read(output).unwrap(), b"native");
}

#[test]
fn full_filesystem_legacy_requests_apply_real_network_seccomp() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    // A parent-side connection establishes that the listener itself is usable.
    std::net::TcpStream::connect(address).unwrap();
    let (status, stderr) = run(
        vec![
            "--klaus-linux-sandbox".into(),
            "--network-restricted".into(),
            "--use-legacy-landlock".into(),
            "--full-filesystem-write".into(),
            "--".into(),
            std::env::current_exe().unwrap().into_os_string(),
            "--ignored".into(),
            "--exact".into(),
            "kernel_network_probe".into(),
        ],
        Some(address.to_string()),
    );
    assert!(status.success(), "{stderr}");
}

#[test]
#[ignore = "invoked inside the published helper by the network execution fixture"]
fn kernel_network_probe() {
    let address = std::env::var("KLAUS_LEGACY_TEST_ADDRESS")
        .unwrap()
        .parse()
        .unwrap();
    let error = std::net::TcpStream::connect_timeout(&address, std::time::Duration::from_secs(2))
        .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::EPERM));
}
