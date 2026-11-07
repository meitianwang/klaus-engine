//! Defect: idle task subscriptions recreated native watchers and rewrote lock artifacts.
//! Root cause: the fallback always reopened observation and fully reconciled unchanged lists.
//! Work item 20261021: idle observation must retain registration without disk mutation.

#![cfg(unix)]

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::fs::paths::tasks_dir;
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var, RuntimeProcessContext,
};
use std::path::PathBuf;
use std::time::Duration;

#[tokio::test]
async fn idle_task_subscription_does_not_modify_its_directory_on_fallback() {
    let home = std::env::temp_dir().join(format!(
        "klaus-task-idle-observation-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&home).unwrap();
    let previous = std::env::var_os("KLAUS_CONFIG_DIR");
    set_process_environment_var("KLAUS_CONFIG_DIR", &home);
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let agent = Agent::new(
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone()),
    );
    let session = agent.start_session(StartSessionOptions {
        cwd: Some(home.to_string_lossy().to_string().into()),
        ..Default::default()
    });
    let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
    let unsubscribe = session.subscribe_task_list_changes(std::sync::Arc::new(move |change| {
        let _ = send.send(change);
    }));
    tokio::time::timeout(Duration::from_secs(5), receive.recv())
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let dir = PathBuf::from(tasks_dir(
        &String::from_utf8(session.task_list_id().to_node_utf8()).unwrap(),
    ));
    let before = std::fs::metadata(&dir).unwrap().modified().unwrap();
    tokio::time::sleep(Duration::from_secs(11)).await;
    let after = std::fs::metadata(&dir).unwrap().modified().unwrap();
    unsubscribe();
    session.dispose().await;
    process.shutdown().await.unwrap();
    match previous {
        Some(value) => set_process_environment_var("KLAUS_CONFIG_DIR", value),
        None => remove_process_environment_var("KLAUS_CONFIG_DIR"),
    }
    let _ = std::fs::remove_dir_all(home);
    assert_eq!(
        after, before,
        "unchanged observation must not acquire a write lock or rewrite task state"
    );
}
