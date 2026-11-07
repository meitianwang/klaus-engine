//! Regression coverage for background lifecycle ownership under writer pressure.
//! Stops must signal before capacity arrives, while retained monitors own their records.

use super::tests::start_options;
use super::*;
use crate::command_runtime::process::{LocalExecProcessRegistry, LocalProcessStartParams};
use crate::runtime_process::test_runtime_process_context;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
struct PausedPersistence {
    capacity: Arc<Semaphore>,
    records: Arc<Mutex<Vec<BackgroundProcessRecord>>>,
    attempts: Arc<AtomicUsize>,
}

impl PausedPersistence {
    fn new() -> Self {
        Self {
            capacity: Arc::new(Semaphore::new(1)),
            records: Arc::new(Mutex::new(Vec::new())),
            attempts: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn sink(&self) -> BackgroundProcessPersistenceSink {
        Arc::new(self.clone())
    }

    async fn pause(&self) -> OwnedSemaphorePermit {
        self.capacity.clone().acquire_owned().await.unwrap()
    }

    fn registry(
        &self,
        events: Arc<Mutex<Vec<BackgroundProcessEvent>>>,
    ) -> BackgroundCommandRegistry {
        BackgroundCommandRegistry::with_options(
            None,
            Some(self.sink()),
            Some(Arc::new(move |event| events.lock().unwrap().push(event))),
            None,
            Vec::new(),
        )
    }

    fn records(&self) -> Vec<BackgroundProcessRecord> {
        self.records.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl BackgroundProcessPersistence for PausedPersistence {
    async fn prepare(&self) -> Result<PreparedBackgroundProcessAppend<'_>, RuntimeString> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        let permit = self
            .capacity
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| RuntimeString::from(error.to_string()))?;
        Ok(PreparedBackgroundProcessAppend::new(move |record| {
            self.records.lock().unwrap().push(record);
            drop(permit);
        }))
    }
}

async fn until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("observable process condition");
}

async fn close(registry: &BackgroundCommandRegistry) {
    registry
        .close_and_wait(tokio::time::Instant::now() + Duration::from_secs(10))
        .await
        .unwrap();
}

fn shell_path(path: &std::path::Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"))
}

fn assert_no_running_after_stop(records: &[BackgroundProcessRecord]) {
    let stopped = records
        .iter()
        .position(|record| record.status == BackgroundProcessStatus::Stopped)
        .expect("persisted stop");
    assert!(records[stopped..]
        .iter()
        .all(|record| record.status == BackgroundProcessStatus::Stopped));
}

#[tokio::test]
async fn exit_grace_preserves_bytes_already_read_while_the_writer_is_paused() {
    let persistence = PausedPersistence::new();
    let registry = persistence.registry(Arc::new(Mutex::new(Vec::new())));
    let temp = tempfile::tempdir().unwrap();
    let release = temp.path().join("release-output");
    let command = format!(
        "while [ ! -f {} ]; do sleep 0.01; done; printf retained-after-exit",
        shell_path(&release)
    );
    let id = registry
        .start(
            test_runtime_process_context(),
            start_options(&command, None),
        )
        .await
        .unwrap();
    until(|| {
        registry
            .list()
            .iter()
            .any(|entry| entry.id == id && entry.status == BackgroundCommandStatus::Running)
    })
    .await;
    let paused = persistence.pause().await;
    let attempts = persistence.attempts.load(Ordering::SeqCst);
    std::fs::write(&release, b"release").unwrap();
    until(|| persistence.attempts.load(Ordering::SeqCst) > attempts).await;
    tokio::time::sleep(Duration::from_millis(180)).await;
    drop(paused);
    until(|| {
        registry
            .list()
            .iter()
            .any(|entry| entry.id == id && entry.status == BackgroundCommandStatus::Exited)
    })
    .await;
    let output = registry.read_new(&id);
    close(&registry).await;
    assert!(output.output.contains("retained-after-exit"));
}

#[tokio::test]
async fn start_waits_for_capacity_before_publication_and_spawn() {
    let persistence = PausedPersistence::new();
    let paused = persistence.pause().await;
    let registry = persistence.registry(Arc::new(Mutex::new(Vec::new())));
    let temp = tempfile::tempdir().unwrap();
    let marker = temp.path().join("launched");
    let command = format!("printf launched > {}; sleep 30", shell_path(&marker));
    let mut start = Box::pin(registry.start(
        test_runtime_process_context(),
        start_options(&command, None),
    ));
    assert!(tokio::time::timeout(Duration::from_millis(30), &mut start)
        .await
        .is_err());
    assert!(registry.snapshot().processes.is_empty());
    assert!(!marker.exists());
    assert!(persistence.records().is_empty());
    drop(start);
    drop(paused);
    tokio::task::yield_now().await;
    assert!(registry.snapshot().processes.is_empty());
    assert!(!marker.exists());
    close(&registry).await;
}

#[tokio::test]
async fn stop_before_monitor_spawn_never_launches_the_command() {
    let persistence = PausedPersistence::new();
    let events = Arc::new(Mutex::new(Vec::new()));
    let registry = persistence.registry(events.clone());
    let temp = tempfile::tempdir().unwrap();
    let marker = temp.path().join("unwanted-child");
    let command = format!("printf launched > {}", shell_path(&marker));
    let id = registry
        .start(
            test_runtime_process_context(),
            start_options(&command, None),
        )
        .await
        .unwrap();
    // On the current-thread runtime the newly retained monitor has not run yet.
    let paused = persistence.pause().await;
    assert!(registry.stop_with_signal(&id, Signal::Kill).accepted);
    assert!(!registry.stop_with_signal(&id, Signal::Kill).accepted);
    assert!(!marker.exists());
    drop(paused);
    close(&registry).await;
    assert!(!marker.exists());
    assert_no_running_after_stop(&persistence.records());
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.subtype == BackgroundProcessEventSubtype::TaskNotification)
            .count(),
        1
    );
}

#[tokio::test]
async fn stop_signals_a_real_process_while_persistence_and_output_wait() {
    let persistence = PausedPersistence::new();
    let events = Arc::new(Mutex::new(Vec::new()));
    let registry = persistence.registry(events.clone());
    let temp = tempfile::tempdir().unwrap();
    let produce = temp.path().join("produce");
    let signalled = temp.path().join("signalled");
    let command = format!(
        "trap \"printf yes > {}; printf late-output; exit 0\" TERM; \
         printf ready; until [ -e {} ]; do sleep 0.02; done; \
         printf queued-output; while :; do sleep 0.02; done",
        shell_path(&signalled),
        shell_path(&produce),
    );
    let id = registry
        .start(
            test_runtime_process_context(),
            start_options(&command, None),
        )
        .await
        .unwrap();
    until(|| {
        registry
            .read_output(&id, None, None)
            .output
            .contains("ready")
    })
    .await;
    let paused = persistence.pause().await;
    let attempts = persistence.attempts.load(Ordering::SeqCst);
    std::fs::write(&produce, b"go").unwrap();
    until(|| persistence.attempts.load(Ordering::SeqCst) > attempts).await;
    let committed = persistence.records().len();
    let mut stop = Box::pin(registry.stop_with_signal_and_wait(&id, Signal::Term));
    assert!(tokio::time::timeout(Duration::from_millis(30), &mut stop)
        .await
        .is_err());
    until(|| signalled.exists()).await;
    assert_eq!(persistence.records().len(), committed);
    assert_eq!(
        registry.snapshot().processes[0].status,
        BackgroundProcessStatus::Stopped
    );
    assert!(!registry.stop_with_signal(&id, Signal::Term).accepted);
    // Cancelling the public caller must not cancel its accepted stop record.
    drop(stop);
    drop(paused);
    until(|| {
        registry
            .read_output(&id, None, None)
            .output
            .contains("late-output")
    })
    .await;
    close(&registry).await;
    let output = registry.read_output(&id, None, None);
    assert!(output.output.contains("queued-output"));
    assert!(output.output.contains("late-output"));
    assert_eq!(output.status, Some(BackgroundProcessStatus::Stopped));
    assert_no_running_after_stop(&persistence.records());
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.subtype == BackgroundProcessEventSubtype::TaskNotification)
            .count(),
        1
    );
}

#[tokio::test]
async fn remove_and_drop_retain_the_pending_stop_and_process_owner() {
    let persistence = PausedPersistence::new();
    let registry = persistence.registry(Arc::new(Mutex::new(Vec::new())));
    let (completed, completion) = oneshot::channel();
    let id = registry
        .start(
            test_runtime_process_context(),
            start_options(
                "printf ready; sleep 30",
                Some(Box::new(move |result| {
                    let _ = completed.send(result);
                })),
            ),
        )
        .await
        .unwrap();
    until(|| {
        registry
            .read_output(&id, None, None)
            .output
            .contains("ready")
    })
    .await;
    let paused = persistence.pause().await;
    registry.remove(&id);
    assert!(!registry.has(&id));
    // Drop must stop the removed process through its retained monitor.
    drop(registry);
    assert!(!persistence
        .records()
        .iter()
        .any(|record| record.status == BackgroundProcessStatus::Stopped));
    drop(paused);
    let complete = tokio::time::timeout(Duration::from_secs(10), completion)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(complete.status, BackgroundCommandStatus::Killed);
    assert_no_running_after_stop(&persistence.records());
}

#[tokio::test]
async fn cancelled_close_retains_join_handles_and_rejects_later_starts() {
    let persistence = PausedPersistence::new();
    let registry = persistence.registry(Arc::new(Mutex::new(Vec::new())));
    let id = registry
        .start(
            test_runtime_process_context(),
            start_options("printf ready; sleep 30", None),
        )
        .await
        .unwrap();
    until(|| {
        registry
            .read_output(&id, None, None)
            .output
            .contains("ready")
    })
    .await;
    let paused = persistence.pause().await;
    let mut closing =
        Box::pin(registry.close_and_wait(tokio::time::Instant::now() + Duration::from_secs(10)));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut closing)
            .await
            .is_err()
    );
    drop(closing);
    assert!(registry
        .start(
            test_runtime_process_context(),
            start_options("printf unwanted", None),
        )
        .await
        .is_err());
    drop(paused);
    close(&registry).await;
    assert!(registry
        .lifecycle
        .lock()
        .unwrap()
        .tasks
        .iter()
        .all(|monitor| monitor.task.try_lock().unwrap().is_none()));
    assert_no_running_after_stop(&persistence.records());
}

#[tokio::test]
async fn flush_waits_for_a_cancelled_stop_callers_record() {
    let persistence = PausedPersistence::new();
    let registry = persistence.registry(Arc::new(Mutex::new(Vec::new())));
    let id = registry
        .start(
            test_runtime_process_context(),
            start_options("printf ready; sleep 30", None),
        )
        .await
        .unwrap();
    until(|| {
        registry
            .read_output(&id, None, None)
            .output
            .contains("ready")
    })
    .await;
    let paused = persistence.pause().await;
    assert!(registry.kill(&id));
    let mut flushing = Box::pin(registry.flush());
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut flushing)
            .await
            .is_err()
    );
    drop(paused);
    flushing.await.unwrap();
    assert_no_running_after_stop(&persistence.records());
    close(&registry).await;
}

async fn interactive_process(
    owner: &Arc<LocalExecProcessRegistry>,
    command: &str,
) -> RuntimeString {
    let options = start_options(command, None);
    owner
        .start(
            test_runtime_process_context(),
            LocalProcessStartParams {
                argv: vec!["/bin/sh".into(), "-c".into(), command.into()],
                cwd: options.cwd,
                sandbox: options.sandbox,
                max_output_bytes: Some(1024),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .process_id
}

fn adoption(
    owner: &Arc<LocalExecProcessRegistry>,
    id: &RuntimeString,
) -> AdoptBackgroundCommandOptions {
    let stop_owner = owner.clone();
    AdoptBackgroundCommandOptions {
        id: id.clone(),
        command: "interactive command".into(),
        cwd: super::tests::cwd(),
        max_output_bytes: 1024,
        events: owner.subscribe_events(id).unwrap(),
        stop: Arc::new(move |id, signal| stop_owner.kill_with_signal(id, signal)),
        on_complete: None,
    }
}

#[tokio::test]
async fn adoption_waits_for_capacity_and_rechecks_closed_admission() {
    let persistence = PausedPersistence::new();
    let registry = persistence.registry(Arc::new(Mutex::new(Vec::new())));
    let owner = Arc::new(LocalExecProcessRegistry::new());
    let id = interactive_process(&owner, "printf ready; sleep 30").await;
    let paused = persistence.pause().await;
    let mut adopting = Box::pin(registry.adopt(adoption(&owner, &id)));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut adopting)
            .await
            .is_err()
    );
    assert!(registry.snapshot().processes.is_empty());
    registry.close_admission_and_kill_all();
    drop(paused);
    assert!(adopting.await.is_err());
    assert!(registry.snapshot().processes.is_empty());
    assert!(persistence.records().is_empty());
    owner.dispose_all();
    close(&registry).await;
}

#[tokio::test]
async fn adopted_process_stop_and_late_output_remain_owned_after_remove() {
    let persistence = PausedPersistence::new();
    let events = Arc::new(Mutex::new(Vec::new()));
    let registry = persistence.registry(events.clone());
    let owner = Arc::new(LocalExecProcessRegistry::new());
    let temp = tempfile::tempdir().unwrap();
    let signalled = temp.path().join("adopted-signalled");
    let command = format!(
        "trap \"printf yes > {}; printf adopted-late; exit 0\" TERM; \
         printf adopted-ready; while :; do sleep 0.02; done",
        shell_path(&signalled),
    );
    let id = interactive_process(&owner, &command).await;
    registry.adopt(adoption(&owner, &id)).await.unwrap();
    until(|| {
        registry
            .read_output(&id, None, None)
            .output
            .contains("adopted-ready")
    })
    .await;
    let paused = persistence.pause().await;
    assert!(registry.stop_with_signal(&id, Signal::Term).accepted);
    until(|| signalled.exists()).await;
    until(|| owner.info(&id).is_none_or(|process| process.exited)).await;
    registry.remove(&id);
    assert!(!registry.has(&id));
    drop(paused);
    close(&registry).await;
    assert!(registry
        .lifecycle
        .lock()
        .unwrap()
        .tasks
        .iter()
        .all(|monitor| monitor.shared.upgrade().is_none()));
    let records = persistence.records();
    assert_no_running_after_stop(&records);
    let last = records.last().unwrap();
    assert_eq!(last.status, BackgroundProcessStatus::Stopped);
    assert_eq!(last.exit_code, Some(0));
    let output = events
        .lock()
        .unwrap()
        .iter()
        .filter_map(|event| event.output.as_ref())
        .fold(RuntimeString::default(), |output, chunk| {
            output.concat(chunk)
        });
    assert!(output.contains("adopted-ready"));
    assert!(output.contains("adopted-late"));
    assert_eq!(last.output_next_offset, output.to_node_utf8().len() as u64);
    owner.dispose_all();
}

#[test]
fn recovery_transfers_owned_records_without_a_runtime_or_writer_call() {
    let persistence = PausedPersistence::new();
    let record = BackgroundProcessRecord {
        shell_id: "recovered".into(),
        command: "sleep 30".into(),
        cwd: "/tmp".into(),
        status: BackgroundProcessStatus::Running,
        started_at_ms: 1,
        updated_at_ms: 1,
        completed_at_ms: None,
        exit_code: None,
        termination_signal: None,
        failure: None,
        stop_requested: false,
        output_start_offset: 0,
        output_next_offset: 0,
        dropped_bytes: 0,
        output_available: false,
    };
    let registry = BackgroundCommandRegistry::with_options(
        Some(Arc::new(|| 2)),
        Some(persistence.sink()),
        None,
        None,
        vec![record],
    );
    assert_eq!(persistence.attempts.load(Ordering::SeqCst), 0);
    let initial = registry.take_initial_records();
    assert_eq!(initial.len(), 1);
    assert_eq!(initial[0].status, BackgroundProcessStatus::Stopped);
    assert!(registry.take_initial_records().is_empty());
    assert_eq!(persistence.attempts.load(Ordering::SeqCst), 0);
}
