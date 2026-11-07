//! Defect: queue snapshots failed with disk I/O errors after endpoint startup.
//! Root cause: closing an unmanaged database descriptor released SQLite's process locks.
//! Fixed by: 01b91dae4f0e4b591cb17863ad8fc24a1c3a1077.

use super::{make_create_session, TestClient};
use serde_json::{json, Value};

async fn request(client: &mut TestClient, id: u64, method: &str, params: Value) -> Value {
    client
        .send(json!({"id": id, "method": method, "params": params}))
        .await;
    let (reply, _) = client.response_for(id).await;
    assert!(reply.get("error").is_none(), "{reply}");
    reply["result"].clone()
}

fn endpoint(root: &std::path::Path) -> TestClient {
    TestClient::start_full(
        make_create_session(&[]),
        None,
        Some(root.join("rules.json")),
        Some(root.join("config.json")),
    )
}

#[cfg(unix)]
#[tokio::test]
async fn opening_another_endpoint_preserves_sqlite_process_locks_and_queue_rows() {
    let root = tempfile::tempdir().unwrap();
    let mut first = endpoint(root.path());
    first.initialize().await;
    request(
        &mut first,
        901,
        "thread/queue/add",
        json!({
            "threadId": "lock-owner", "clientUserMessageId": "first",
            "input": [{"type": "text", "text": "retained input"}]
        }),
    )
    .await;
    use std::os::unix::fs::PermissionsExt;
    let permissions = std::fs::metadata(root.path().join("thread-queue.sqlite3"))
        .unwrap()
        .permissions();
    assert_eq!(permissions.mode() & 0o777, 0o600);

    let mut second = endpoint(root.path());
    second.initialize().await;
    let snapshot = request(
        &mut second,
        902,
        "thread/queue/list",
        json!({
            "threadId": "lock-owner"
        }),
    )
    .await;
    assert_eq!(snapshot["data"].as_array().unwrap().len(), 1);

    let module = module_path!().split_once("::").unwrap().1;
    let mut probe = tokio::process::Command::new(std::env::current_exe().unwrap());
    probe
        .args([
            "--ignored",
            "--exact",
            &format!("{module}::exclusive_lock_probe"),
            "--nocapture",
        ])
        .env(
            "KLAUS_QUEUE_LOCK_PROBE",
            root.path().join("thread-queue.sqlite3"),
        )
        .kill_on_drop(true);
    let output = tokio::time::timeout(std::time::Duration::from_secs(10), probe.output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "lock probe failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("QUEUE_LOCK_PROBE_CONFIRMED"),
        "the independent lock probe must execute"
    );

    request(
        &mut second,
        903,
        "thread/queue/add",
        json!({
            "threadId": "lock-owner", "clientUserMessageId": "second", "input": []
        }),
    )
    .await;
    let snapshot = request(
        &mut first,
        904,
        "thread/queue/list",
        json!({
            "threadId": "lock-owner"
        }),
    )
    .await;
    assert_eq!(snapshot["data"].as_array().unwrap().len(), 2);
    assert_eq!(snapshot["data"][0]["input"][0]["text"], "retained input");
    first.close().await;
    second.close().await;
}

#[cfg(unix)]
#[test]
#[ignore = "independent process fixture for database lock ownership"]
fn exclusive_lock_probe() {
    use std::os::fd::AsRawFd;
    let path = std::env::var_os("KLAUS_QUEUE_LOCK_PROBE").unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    // Probe from another process: POSIX locks are shared by every connection
    // in the owner process, so another thread cannot detect their loss.
    let mut lock: libc::flock = unsafe { std::mem::zeroed() };
    lock.l_type = libc::F_WRLCK as _;
    lock.l_whence = libc::SEEK_SET as _;
    let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &lock) };
    assert_eq!(
        result, -1,
        "an active SQLite endpoint must exclude a foreign whole-file writer"
    );
    let error = std::io::Error::last_os_error();
    assert!(
        matches!(error.raw_os_error(), Some(libc::EACCES | libc::EAGAIN)),
        "{error}"
    );
    println!("QUEUE_LOCK_PROBE_CONFIRMED");
}
