//! Defect: background output reads returned immediately and interrupted long work.
//! Root cause: observations had no bounded wait or per-process collection owner.
//! Work item: 20261028 resumable command observation lifecycle.

use std::sync::Arc;
use std::time::{Duration, Instant};

use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{RuntimePath, RuntimeString};
use klaus_agent_core::tools::background_shells::{
    BackgroundShellRegistry, BackgroundShellStatus, StartShellOptions,
};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::sandbox::{
    ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
};

use super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor;

struct ProcessSandbox;
impl SandboxBackend for ProcessSandbox {
    fn kind(&self) -> SandboxKind {
        SandboxKind::Process
    }
    fn is_available(&self) -> bool {
        true
    }
    fn transform(&self, request: &ExecRequest, _: &SandboxPolicy) -> SpawnSpec {
        SpawnSpec {
            file: request.argv[0].clone(),
            args: request.argv[1..].to_vec(),
            cwd: request.cwd.clone(),
            env: request.env.clone().unwrap_or_default(),
        }
    }
}

async fn start(
    command: &str,
) -> (
    Arc<BackgroundShellRegistry>,
    RuntimeString,
    tempfile::TempDir,
) {
    let directory = tempfile::tempdir().unwrap();
    let registry = Arc::new(BackgroundShellRegistry::new());
    let id = registry
        .start(
            RuntimeProcessContext::initialize(process_supervisor()).unwrap(),
            StartShellOptions {
                command: command.into(),
                cwd: RuntimePath::from(directory.path().to_string_lossy().as_ref()),
                sandbox: Arc::new(ProcessSandbox),
                env: None,
                max_output_bytes: 1024 * 1024,
                writable_roots: None,
                unconfined_writes: true,
                network_allowed: None,
                managed_network: None,
                on_complete: None,
            },
        )
        .await
        .unwrap();
    (registry, id, directory)
}

#[test]
fn observation_window_bounds_and_configuration_are_independent_of_execution_timeout() {
    let registry = BackgroundShellRegistry::new();
    assert_eq!(registry.poll_wait_ms(None), 5_000);
    assert_eq!(registry.poll_wait_ms(Some(0)), 5_000);
    assert_eq!(registry.poll_wait_ms(Some(300_001)), 300_000);
    registry.set_max_poll_wait_ms(500);
    assert_eq!(registry.poll_wait_ms(Some(60_000)), 5_000);
    registry.set_max_poll_wait_ms(600_000);
    assert_eq!(registry.poll_wait_ms(Some(600_000)), 600_000);
}

#[tokio::test]
async fn observation_collects_early_and_late_output_through_its_deadline() {
    let (registry, id, directory) = start("printf early; sleep 0.15; printf late; sleep 30").await;
    let started = Instant::now();
    let output = registry
        .read_new_wait(&id, 400, &AbortSignal::new(), None)
        .await;
    let elapsed = started.elapsed();
    registry
        .close_and_wait(tokio::time::Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    drop(directory);
    let output = output.unwrap();
    assert!(
        elapsed >= Duration::from_millis(370),
        "early output must not finish the observation: {elapsed:?}"
    );
    assert!(output.output.contains("early") && output.output.contains("late"));
    assert_eq!(output.status, BackgroundShellStatus::Running);
}

#[tokio::test]
async fn completed_output_is_returned_once_while_host_history_remains_available() {
    let (registry, id, directory) = start("printf final-output; exit 7").await;
    let started = Instant::now();
    let output = registry
        .read_new_wait(&id, 5_000, &AbortSignal::new(), None)
        .await
        .unwrap();
    let again = registry
        .read_new_wait(&id, 5_000, &AbortSignal::new(), None)
        .await
        .unwrap();
    let history = registry.read_output(&id, None, None);
    registry
        .close_and_wait(tokio::time::Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    drop(directory);
    assert!(started.elapsed() < Duration::from_secs(4));
    assert_eq!(output.status, BackgroundShellStatus::Exited);
    assert_eq!(output.exit_code, Some(7));
    assert!(output.output.contains("final-output"));
    assert!(!again.found);
    assert!(history.found && history.output.contains("final-output"));
}

#[tokio::test]
async fn cancelled_observation_preserves_process_and_output_for_the_next_observer() {
    let (registry, id, directory) = start("printf retained-output; sleep 30").await;
    let signal = AbortSignal::new();
    let cancel = signal.clone();
    let cancellation = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        cancel.cancel();
    });
    let result = registry.read_new_wait(&id, 5_000, &signal, None).await;
    cancellation.await.unwrap();
    let live = registry
        .list()
        .into_iter()
        .find(|process| process.id == id)
        .unwrap();
    let next = registry
        .read_new_wait(&id, 50, &AbortSignal::new(), None)
        .await;
    registry
        .close_and_wait(tokio::time::Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    drop(directory);
    assert!(result.is_err());
    assert_eq!(live.status, BackgroundShellStatus::Running);
    assert!(next.unwrap().output.contains("retained-output"));
}

#[tokio::test]
async fn concurrent_observations_serialize_and_start_their_windows_after_admission() {
    let (registry, id, directory) = start("sleep 30").await;
    let first_registry = registry.clone();
    let first_id = id.clone();
    let first = tokio::spawn(async move {
        first_registry
            .read_new_wait(&first_id, 250, &AbortSignal::new(), None)
            .await
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    let started = Instant::now();
    let second = registry
        .read_new_wait(&id, 250, &AbortSignal::new(), None)
        .await;
    let elapsed = started.elapsed();
    let first = first.await.unwrap();
    registry
        .close_and_wait(tokio::time::Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    drop(directory);
    assert!(first.is_ok() && second.is_ok());
    assert!(
        elapsed >= Duration::from_millis(420),
        "queued observation must receive its own full window: {elapsed:?}"
    );
}

#[tokio::test]
async fn user_interaction_pause_does_not_spend_the_observation_window() {
    let (registry, id, directory) = start("sleep 30").await;
    let (pause, receiver) = tokio::sync::watch::channel(false);
    let changes = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        pause.send_replace(true);
        tokio::time::sleep(Duration::from_millis(250)).await;
        pause.send_replace(false);
        tokio::time::sleep(Duration::from_secs(1)).await;
    });
    let started = Instant::now();
    let output = registry
        .read_new_wait(&id, 200, &AbortSignal::new(), Some(receiver))
        .await;
    let elapsed = started.elapsed();
    changes.abort();
    let _ = changes.await;
    registry
        .close_and_wait(tokio::time::Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    drop(directory);
    assert!(output.is_ok());
    assert!(
        elapsed >= Duration::from_millis(420),
        "paused time must extend the deadline: {elapsed:?}"
    );
}

#[tokio::test]
async fn process_exit_does_not_wait_forever_for_inherited_output_pipes() {
    let (registry, id, directory) = start("(sleep 2) & printf parent-finished; exit 0").await;
    let started = Instant::now();
    let output = registry
        .read_new_wait(&id, 5_000, &AbortSignal::new(), None)
        .await;
    let elapsed = started.elapsed();
    registry
        .close_and_wait(tokio::time::Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    drop(directory);
    let output = output.unwrap();
    assert!(
        elapsed < Duration::from_secs(1),
        "exit should bound trailing pipe collection: {elapsed:?}"
    );
    assert!(output.output.contains("parent-finished"));
    assert_eq!(output.exit_code, Some(0));
}
