//! Defect regression: `klaus-agent-stdio` hung on arguments it did not know.
//!
//! What broke: `klaus-agent-stdio --help` printed nothing and never exited. One
//! such probe stayed alive for three hours holding the engine binary mapped;
//! when the staged copy of that binary was then overwritten in place, macOS
//! invalidated its per-inode code-signature cache and SIGKILLed every later
//! exec of the engine, leaving the desktop client with no engine at all.
//!
//! Root cause: the binary only recognized `generate-json-schema`,
//! `--app-server` and the Windows sandbox modes; every other argument was
//! IGNORED and fell through to the default newline-delimited Op/Event endpoint,
//! which reads stdin until EOF — so an unknown flag became a stuck orphan.
//!
//! Fixed by: the argv classifier in `crates/server/src/bin/klaus-agent-stdio.rs`
//! (`--help` / `--version` answer and exit, unknown arguments exit 2). This test
//! drives the real process, which is the surface that exposed the defect; it
//! fails on the pre-fix binary by timing out.
//!
//! Convention note: it lives beside the crate that owns the binary because
//! `CARGO_BIN_EXE_*` is only available to that crate's own test targets.

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const BINARY: &str = env!("CARGO_BIN_EXE_klaus-agent-stdio");
/// Generous next to a synchronous print-and-exit, tight next to "never exits".
const EXIT_TIMEOUT: Duration = Duration::from_secs(20);

struct Outcome {
    code: Option<i32>,
    stdout: String,
    stderr: String,
    waited: Duration,
}

/// Run the binary with stdin left OPEN and empty (a pipe nobody writes to).
///
/// This is the shape that used to hang: with stdin at EOF the endpoint loop
/// would end on its own and the defect would not reproduce, so closing stdin
/// here would make the test pass against the pre-fix binary.
fn run(args: &[&str]) -> Outcome {
    let mut child = Command::new(BINARY)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn klaus-agent-stdio");
    let stdin = child.stdin.take().expect("piped stdin");
    let started = Instant::now();
    let status = loop {
        match child.try_wait().expect("try_wait") {
            Some(status) => break Some(status),
            None if started.elapsed() >= EXIT_TIMEOUT => break None,
            None => std::thread::sleep(Duration::from_millis(25)),
        }
    };
    let waited = started.elapsed();
    let status = match status {
        Some(status) => status,
        None => {
            let _ = child.kill();
            let _ = child.wait();
            panic!("klaus-agent-stdio {args:?} did not exit within {EXIT_TIMEOUT:?}");
        }
    };
    // Held until here so the child cannot see EOF on stdin while we wait.
    drop(stdin);
    let mut stdout = String::new();
    let mut stderr = String::new();
    child
        .stdout
        .take()
        .expect("piped stdout")
        .read_to_string(&mut stdout)
        .expect("read stdout");
    child
        .stderr
        .take()
        .expect("piped stderr")
        .read_to_string(&mut stderr)
        .expect("read stderr");
    Outcome {
        code: status.code(),
        stdout,
        stderr,
        waited,
    }
}

#[test]
fn help_answers_and_exits_instead_of_waiting_on_stdin() {
    for flag in ["--help", "-h"] {
        let outcome = run(&[flag]);
        assert_eq!(outcome.code, Some(0), "{flag}: stderr={}", outcome.stderr);
        assert!(
            outcome.stdout.contains("USAGE:") && outcome.stdout.contains("--app-server"),
            "{flag} printed no usage: {:?}",
            outcome.stdout
        );
        assert!(
            outcome.waited < EXIT_TIMEOUT,
            "{flag} took {:?}",
            outcome.waited
        );
    }
}

#[test]
fn version_answers_and_exits() {
    for flag in ["--version", "-V"] {
        let outcome = run(&[flag]);
        assert_eq!(outcome.code, Some(0), "{flag}: stderr={}", outcome.stderr);
        assert!(
            outcome.stdout.starts_with("klaus-agent-stdio "),
            "{flag} printed {:?}",
            outcome.stdout
        );
    }
}

#[test]
fn an_unknown_argument_fails_fast_rather_than_starting_an_endpoint() {
    // A typo of the one flag hosts actually pass. Before the fix this started
    // the Op/Event endpoint and waited on stdin forever.
    let outcome = run(&["--app-servr"]);
    assert_eq!(
        outcome.code,
        Some(2),
        "an unknown argument exits 2: stdout={:?} stderr={:?}",
        outcome.stdout,
        outcome.stderr
    );
    assert!(
        outcome
            .stderr
            .contains("unrecognized argument `--app-servr`"),
        "the refusal names the argument: {:?}",
        outcome.stderr
    );
    assert!(
        outcome.stderr.contains("USAGE:"),
        "the refusal shows the usage: {:?}",
        outcome.stderr
    );
}
