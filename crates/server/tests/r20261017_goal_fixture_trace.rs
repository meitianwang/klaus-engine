//! Bounded diagnostics survive a test process timeout or fixture-directory cleanup.

use std::io::Write;
use std::sync::{Arc, Mutex};

pub(super) struct GoalFixtureTrace {
    case: String,
    path: std::path::PathBuf,
    started: std::time::Instant,
    file: Mutex<(std::fs::File, usize)>,
}
impl GoalFixtureTrace {
    pub(super) fn new(case: &str) -> Arc<Self> {
        assert!(!case.is_empty() && case.len() <= 96);
        assert!(case
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'));
        let directory = std::env::temp_dir().join("klaus-goal-facade-traces");
        std::fs::create_dir_all(&directory).expect("create fixture trace directory");
        let path = directory.join(format!("{case}-{}.log", std::process::id()));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .expect("open fixture trace");
        let trace = Arc::new(Self {
            case: case.to_owned(),
            path,
            started: std::time::Instant::now(),
            file: Mutex::new((file, 0)),
        });
        eprintln!(
            "[goal-fixture-trace] case={case} path={}",
            trace.path.display()
        );
        trace.note("created");
        trace
    }
    pub(super) fn note(&self, stage: &'static str) {
        assert!(
            stage.len() <= 160,
            "fixture trace stages must remain bounded"
        );
        let mut state = self.file.lock().expect("fixture trace mutex");
        assert!(state.1 < 96, "fixture trace stage count exceeded its bound");
        state.1 += 1;
        let elapsed = self.started.elapsed().as_millis();
        writeln!(&mut state.0, "elapsed_ms={elapsed} stage={stage}").expect("write fixture trace");
        state.0.flush().expect("flush fixture trace");
        eprintln!(
            "[goal-fixture-trace] case={} elapsed_ms={elapsed} stage={stage}",
            self.case
        );
    }
}
