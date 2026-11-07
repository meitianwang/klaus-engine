//! Bounded transition diagnostics for explicitly instrumented test stages.
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const MAX_RECORDS: usize = 256;
const MAX_BYTES: usize = 64 * 1024;
const MAX_LINE_BYTES: usize = 2048;
static NEXT_SPAN_ID: AtomicU64 = AtomicU64::new(1);
static STDERR_FAILURES: AtomicU64 = AtomicU64::new(0);

fn receipt(arguments: std::fmt::Arguments<'_>) {
    let mut line = arguments.to_string();
    if line.len() >= MAX_LINE_BYTES {
        let mut end = MAX_LINE_BYTES - 1;
        while !line.is_char_boundary(end) {
            end -= 1;
        }
        line.truncate(end);
    }
    line.push('\n');
    if std::io::stderr().lock().write_all(line.as_bytes()).is_err() {
        STDERR_FAILURES.fetch_add(1, Ordering::Relaxed);
    }
}

struct Sink {
    file: Option<File>,
    origin: Instant,
    records: usize,
    bytes: usize,
    capped: bool,
}
fn sink() -> &'static Mutex<Sink> {
    static SINK: OnceLock<Mutex<Sink>> = OnceLock::new();
    SINK.get_or_init(|| {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "klaus-phase-probe-{}-{nonce}.log",
            std::process::id()
        ));
        let opened = OpenOptions::new().create_new(true).write(true).open(&path);
        receipt(format_args!(
            "[phase-probe] pid={} path={} opened={}",
            std::process::id(),
            path.display(),
            opened.is_ok()
        ));
        if let Err(error) = &opened {
            receipt(format_args!("[phase-probe] missing=log-open error={error}"));
        }
        Mutex::new(Sink {
            file: opened.ok(),
            origin: Instant::now(),
            records: 0,
            bytes: 0,
            capped: false,
        })
    })
}
fn clipped(value: &str, characters: usize) -> String {
    value
        .chars()
        .take(characters)
        .map(|ch| if ch == '\n' || ch == '\r' { ' ' } else { ch })
        .collect()
}
pub fn mark(phase: &str, detail: &str) {
    if std::env::var_os("KLAUS_PHASE_PROBE").as_deref() != Some(std::ffi::OsStr::new("1")) {
        return;
    }
    let called = Instant::now();
    let mut sink = sink().lock().unwrap_or_else(|error| error.into_inner());
    let lock_us = called.elapsed().as_micros();
    if sink.capped {
        return;
    }
    let phase = clipped(phase, 80);
    let mut line = format!(
        "pid={} thread={:?} seq={} elapsed_us={} stderr_failures={} phase={} {}\n",
        std::process::id(),
        std::thread::current().id(),
        sink.records,
        sink.origin.elapsed().as_micros(),
        STDERR_FAILURES.load(Ordering::Relaxed),
        phase,
        clipped(detail, 700)
    );
    if line.len() > MAX_LINE_BYTES {
        let mut end = MAX_LINE_BYTES - 1;
        while !line.is_char_boundary(end) {
            end -= 1;
        }
        line.truncate(end);
        line.push('\n');
    }
    if sink.records == MAX_RECORDS || sink.bytes + line.len() > MAX_BYTES {
        sink.capped = true;
        receipt(format_args!(
            "[phase-probe] pid={} missing=capacity records={} bytes={}",
            std::process::id(),
            sink.records,
            sink.bytes
        ));
        return;
    }
    sink.records += 1;
    sink.bytes += line.len();
    let write_started = Instant::now();
    let result = match &mut sink.file {
        Some(file) => file.write_all(line.as_bytes()),
        None => Err(std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "diagnostic log unavailable",
        )),
    };
    let write_us = write_started.elapsed().as_micros();
    receipt(format_args!("[phase-probe] pid={} seq={} phase={} stored={} marker_lock_us={} marker_write_us={} marker_before_receipt_us={}",
        std::process::id(), sink.records - 1, phase, result.is_ok(), lock_us, write_us, called.elapsed().as_micros()));
    if let Err(error) = result {
        receipt(format_args!("[phase-probe] missing=write error={error}"));
    }
}
pub struct Span {
    span_id: u64,
    phase: String,
    detail: String,
    started: Instant,
    finished: bool,
}
impl Span {
    pub fn start(phase: &str, detail: &str) -> Self {
        let span_id = NEXT_SPAN_ID.fetch_add(1, Ordering::Relaxed);
        let phase = clipped(phase, 80);
        let detail = clipped(detail, 700);
        mark(&phase, &format!("edge=begin span_id={span_id} {detail}"));
        Self {
            span_id,
            phase,
            detail,
            started: Instant::now(),
            finished: false,
        }
    }
    pub fn finish(mut self, outcome: &str) {
        let awaited_us = self.started.elapsed().as_micros();
        self.finished = true;
        mark(
            &self.phase,
            &format!(
                "edge=end span_id={} outcome={} awaited_us={awaited_us} {}",
                self.span_id,
                clipped(outcome, 80),
                self.detail
            ),
        );
    }
}
impl Drop for Span {
    fn drop(&mut self) {
        if !self.finished {
            let awaited_us = self.started.elapsed().as_micros();
            mark(
                &self.phase,
                &format!(
                    "edge=end span_id={} outcome={} awaited_us={awaited_us} {}",
                    self.span_id,
                    if std::thread::panicking() {
                        "unwinding"
                    } else {
                        "scope-dropped"
                    },
                    self.detail
                ),
            );
        }
    }
}
