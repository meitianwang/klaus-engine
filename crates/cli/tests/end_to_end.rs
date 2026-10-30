//! The compiled binary, run the way a caller runs it.
//!
//! These spawn `klaus` rather than calling into the library, because what they
//! check only exists at the process boundary: the exit status, and whether
//! `--json` stdout is machine-readable. Neither is observable from inside — the
//! printer writes to the real stdout and the exit code is produced by `main` —
//! so a library-level test would be checking something else and calling it the
//! same name.
//!
//! Every run is given its own `HOME` and session directory and is started with
//! `--no-discovery --no-plugins`, so nothing on the developer's machine can
//! decide whether these pass. None of them cause a model call: the engine is
//! doing real work here, but not paid work.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use klaus_agent_cli::turn::concluded;
use klaus_agent_core::types::errors::TerminalReason;

/// `klaus` invoked against a private, empty environment.
fn bare(home: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_klaus"));
    command
        // The engine reads config out of the home directory, so pointing it at an
        // empty one is what makes these runs answer for themselves.
        .env("HOME", home)
        .arg("--no-discovery")
        // Closed rather than inherited: a command that unexpectedly reads stdin
        // would otherwise block the whole test run instead of failing.
        .stdin(Stdio::null());
    command
}

/// `klaus` as its own engine: the flags below only mean anything to a process
/// that is assembling one, and the CLI rejects them alongside `--remote`.
fn klaus(home: &Path) -> Command {
    let mut command = bare(home);
    command
        .arg("--no-plugins")
        .arg("--session-dir")
        .arg(home.join("sessions"));
    command
}

fn run(command: &mut Command) -> Output {
    command
        .output()
        .expect("the klaus binary should be runnable")
}

fn code(output: &Output) -> i32 {
    output.status.code().expect("klaus should not be signalled")
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("stdout should be utf-8")
}

/// A fresh directory under the system temporary directory.
///
/// Written by hand rather than with a temp-file crate: this is the only thing
/// these tests need from one, and the crate ships no dev-dependencies today.
fn home() -> PathBuf {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let path = std::env::temp_dir().join(format!(
        "klaus-cli-e2e-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&path).expect("a temporary directory should be creatable");
    path
}

/// Being called wrongly is a distinct exit code from running and failing.
///
/// A script has to be able to tell "you passed me something I do not
/// understand" from "I did what you asked and it did not work", because the
/// first is never worth retrying and the second sometimes is. Both the argument
/// parser's own rejections and the ones the commands raise themselves have to
/// land on the same code, or the distinction only half exists.
#[test]
fn calling_it_wrongly_exits_two() {
    let home = home();
    for argv in [
        // Rejected by clap: an unknown flag, and a value outside a flag's set.
        vec!["--definitely-not-a-flag"],
        vec!["--permission-mode", "nonsense", "capabilities"],
        // Rejected by the command itself: rewinding zero turns is refused rather
        // than sent, so this proves the app's own usage errors share the code.
        // Spelled with the flag on purpose — `rewind 0` is exit 2 as well, but
        // only because clap rejects the stray positional, which would leave the
        // app's own refusal untested while looking like it was covered.
        vec!["rewind", "--turns", "0"],
    ] {
        let output = run(klaus(&home).args(&argv));
        assert_eq!(
            code(&output),
            2,
            "`klaus {}` should be a usage error, got {:?}\n{}",
            argv.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

/// `--no-plugins` refuses the `plugin` commands by naming itself, rather than
/// letting the engine refuse them by symptom.
///
/// The flag tells the endpoint not to read the installed-plugin ledger, and an
/// endpoint that never read the ledger declines to administer it. So every
/// `plugin` subcommand would otherwise come back "installed plugin
/// administration is unavailable" — true, but naming neither the flag nor the
/// fix, and reading like an engine that lacks the feature rather than a client
/// that switched it off one word earlier on the same command line.
///
/// Both halves are pinned, because either alone would be misleading: the refusal
/// has to name the flag, AND the identical command has to succeed once the flag
/// is dropped. Without the second assertion this test would still pass if
/// `plugin` were broken outright.
#[test]
fn no_plugins_refuses_plugin_administration_by_name() {
    let home = home();
    let refused = run(klaus(&home).args(["plugin", "list"]));
    let said = String::from_utf8_lossy(&refused.stderr).to_string();
    assert_eq!(
        code(&refused),
        2,
        "`--no-plugins plugin list` should be a usage error\n{said}"
    );
    assert!(
        said.contains("--no-plugins"),
        "the refusal has to name the flag that caused it\n{said}"
    );

    // The same command against the same home, with the flag dropped. The plugin
    // home is named explicitly so an inherited `KLAUS_PLUGIN_HOME` cannot decide
    // the outcome.
    let allowed = run(bare(&home)
        .arg("--session-dir")
        .arg(home.join("sessions"))
        .arg("--plugin-home")
        .arg(home.join("plugins"))
        .args(["plugin", "list"]));
    assert_eq!(
        code(&allowed),
        0,
        "`plugin list` should work once --no-plugins is dropped\n{}",
        String::from_utf8_lossy(&allowed.stderr)
    );
}

/// `--json` stdout is machine-readable, one whole value per line.
///
/// This is the contract the flag exists for. It is checked on a command that
/// reads engine state rather than a canned one, so the assertion covers the
/// path an actual consumer uses: every line parses, every line is an object,
/// and every line carries the submission id that correlates it.
#[test]
fn json_stdout_is_one_parseable_value_per_line() {
    let home = home();
    let output = run(klaus(&home).args(["--json", "capabilities"]));
    assert_eq!(code(&output), 0, "capabilities should succeed");

    let body = stdout(&output);
    let mut lines = 0;
    for line in body.lines().filter(|line| !line.trim().is_empty()) {
        let value: serde_json::Value = serde_json::from_str(line).unwrap_or_else(|error| {
            panic!("--json wrote a line that is not JSON: {error}\n{line}")
        });
        assert!(
            value.get("submissionId").is_some(),
            "a --json line has no submissionId to correlate it:\n{line}"
        );
        lines += 1;
    }
    assert!(lines > 0, "--json capabilities printed nothing");
}

/// A queued message outlives the process that queued it.
///
/// Worth pinning because it is the non-obvious half of `queue add`. The queue
/// looks like process state — it is an in-memory structure on a live session —
/// but the engine journals every mutation into the thread's rollout and rebuilds
/// it on resume. That is what makes `queue add` useful from a script at all, and
/// nothing in the CLI would notice if the engine stopped doing it.
#[test]
fn a_queued_message_outlives_the_process_that_queued_it() {
    let home = home();
    let queued = run(klaus(&home).args([
        "--json",
        "queue",
        "add",
        "--priority",
        "later",
        "survive me",
    ]));
    assert_eq!(code(&queued), 0, "queue add should succeed");

    let snapshot: serde_json::Value = serde_json::from_str(
        stdout(&queued)
            .lines()
            .next()
            .expect("queue add printed nothing"),
    )
    .expect("queue add should print JSON");
    let message = &snapshot["queueSnapshot"]["items"][0]["message"];
    assert_eq!(message["value"], "survive me");
    assert_eq!(
        message["priority"], "later",
        "--priority should reach the engine"
    );

    // The thread the implicit session created. Resuming by it is how a later
    // invocation reaches the same queue.
    let thread = message["sessionId"]
        .as_str()
        .expect("a queued message should name its session")
        .to_string();
    let identity = message["uuid"]
        .as_str()
        .expect("a queued message should have an id")
        .to_string();

    // A second process, so the only thing that could carry the message across is
    // the engine's own persistence.
    let listed = run(klaus(&home).args(["--resume", &thread, "--json", "queue", "list"]));
    assert_eq!(code(&listed), 0, "queue list should succeed after resume");
    let body = stdout(&listed);
    assert!(
        body.contains(&identity),
        "the queued message did not survive the process that queued it\n{body}"
    );
    assert!(
        body.contains("survive me"),
        "the message text was lost\n{body}"
    );
}

/// A `/model` naming a source the endpoint cannot honour does NOT end the REPL.
///
/// Changing which SOURCE answers replaces the session, because the endpoint
/// resolves a selection once, while starting one. That makes the failure mode the
/// thing worth pinning: the replacement is started before the old session is
/// released, so a selection naming nothing has to leave the loop with a working
/// session and its history — the same promise the REPL makes about a refused turn.
/// Getting the order wrong loses the user's conversation to a typo.
///
/// No model call: the selection is refused while the session is being started, so
/// nothing is ever sent to a provider.
#[test]
fn a_refused_model_selection_leaves_the_repl_running() {
    let home = home();
    let mut command = klaus(&home);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("klaus should start a REPL");
    child
        .stdin
        .as_mut()
        .expect("stdin should be piped")
        .write_all(b"/model custom:names-nothing\n/exit\n")
        .expect("the REPL should accept input");
    let output = child
        .wait_with_output()
        .expect("the REPL should exit on /exit");

    let body = format!(
        "{}{}",
        stdout(&output),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        code(&output),
        0,
        "a refused selection must not fail the session\n{body}"
    );
    assert!(
        body.contains("was not applied"),
        "the refusal has to be reported, not swallowed\n{body}"
    );
    assert!(
        body.contains("names-nothing"),
        "the refusal has to name what was asked for\n{body}"
    );
}

/// Named goal controls persist state without resolving or invoking a provider.
/// Saved reads remain available when automatic pursuit is disabled.

#[test]
fn goal_controls_and_disabled_saved_reads_remain_provider_free() {
    let home = home();
    let set = run(klaus(&home).args([
        "--json",
        "goal",
        "set",
        "ship the cli",
        "--status",
        "paused",
    ]));
    assert_eq!(
        code(&set),
        0,
        "goal set should not need a flag to be typed first\n{}",
        String::from_utf8_lossy(&set.stderr)
    );
    let reply: serde_json::Value = serde_json::from_str(
        stdout(&set)
            .lines()
            .next()
            .expect("goal set printed nothing"),
    )
    .expect("goal set should print JSON");
    let goal = &reply["threadGoal"];
    assert_eq!(goal["objective"], "ship the cli");
    assert_eq!(goal["status"], "paused");
    let thread = goal["threadId"]
        .as_str()
        .expect("a goal should name its thread")
        .to_string();

    // The plain response describes the persisted state and admission outcome.
    let fresh = home.join("second-home");
    std::fs::create_dir_all(&fresh).expect("a second home should be creatable");
    let told = run(klaus(&fresh).args(["goal", "set", "ship the cli", "--status", "paused"]));
    let advice = stdout(&told);
    assert_eq!(code(&told), 0, "goal set should succeed\n{advice}");
    assert!(
        advice.contains("[paused]") && advice.contains("goal work: deferred"),
        "a paused create must describe durable state without scheduling inference\n{advice}"
    );

    // A separate process observes the same durable goal without model access.
    let got = run(klaus(&home).args(["--resume", &thread, "goal", "get"]));
    let seen = stdout(&got);
    assert_eq!(
        code(&got),
        0,
        "goal get should succeed after resume\n{seen}"
    );
    assert!(
        seen.contains("ship the cli"),
        "goal get did not read back the goal that is on disk\n{seen}"
    );

    // Disabling pursuit does not hide an already-persisted goal.
    let blind = run(klaus(&home).args([
        "--config-json",
        r#"{"goal":false}"#,
        "--resume",
        &thread,
        "goal",
        "get",
    ]));
    assert_eq!(code(&blind), 0, "goal get should still succeed");
    assert!(
        stdout(&blind).contains("ship the cli") && stdout(&blind).contains("[paused]"),
        "saved state must remain observable with pursuit disabled\n{}",
        stdout(&blind)
    );

    let cleared = run(klaus(&home).args(["--resume", &thread, "goal", "clear"]));
    assert_eq!(
        code(&cleared),
        0,
        "goal clear should not need a flag either\n{}",
        String::from_utf8_lossy(&cleared.stderr)
    );
    assert!(
        stdout(&cleared).contains("goal cleared"),
        "clear should report what it did\n{}",
        stdout(&cleared)
    );
    let after = run(klaus(&home).args(["--resume", &thread, "goal", "get"]));
    assert!(
        stdout(&after).contains("(no goal)"),
        "the goal survived being cleared\n{}",
        stdout(&after)
    );
}

/// A `klaus serve` running in the background, killed when the test lets go of it.
///
/// The kill is in `Drop` rather than at the end of the test body so a failed
/// assertion does not leave an engine holding a port for the rest of the run.
struct Served {
    child: Child,
    /// The `ws://…` the server printed, which is the only way to learn the port
    /// when `--port 0` was asked for.
    url: String,
    /// Where the server's stderr went. Kept so a failed assertion can quote what
    /// the server thought was happening, which is otherwise invisible: it is a
    /// different process and it never writes to the test's own streams.
    log: PathBuf,
}

impl Served {
    fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }
}

impl Drop for Served {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Start `klaus serve` on an ephemeral port and wait until it is listening.
///
/// The readiness signal is the banner on stderr, which is also where the URL
/// comes from: an ephemeral port is only knowable from the server, and polling a
/// guessed one would either race or need a fixed port that a parallel test could
/// already hold.
///
/// stderr goes to a FILE and not a pipe, which matters more than it looks: a pipe
/// would have to be read for as long as the server runs, because `std` restores
/// the default SIGPIPE disposition in spawned children — so the moment this
/// function returned and dropped the read end, the server's next line to stderr
/// would kill it. That failure surfaces nowhere near its cause, as a connection
/// refused by a port that was serving happily a moment earlier.
fn serve(home: &Path, token: &str) -> Served {
    let log = home.join("serve.log");
    let sink = std::fs::File::create(&log).expect("a log file should be creatable");
    let child = klaus(home)
        .args(["serve", "--port", "0", "--token", token])
        .stdout(Stdio::null())
        .stderr(Stdio::from(sink))
        .spawn()
        .expect("klaus serve should start");
    // Owned by the guard before anything that can fail, so a panic while waiting
    // for the banner still reaps the child instead of orphaning an engine.
    let mut served = Served {
        child,
        url: String::new(),
        log,
    };

    for _ in 0..SERVE_READY_ATTEMPTS {
        let text = served.log();
        if let Some((_, rest)) = text.split_once("serving ") {
            served.url = rest
                .split_whitespace()
                .next()
                .expect("the banner should name a url")
                .to_string();
            return served;
        }
        // Checked after reading, so a server that announced and exited in the
        // same interval is still reported as having announced.
        if let Some(status) = served.child.try_wait().expect("the child is waitable") {
            panic!("klaus serve exited ({status}) before it was listening:\n{text}");
        }
        std::thread::sleep(SERVE_READY_INTERVAL);
    }
    panic!("klaus serve never announced a url:\n{}", served.log());
}

/// Long enough to cover a cold engine start on a machine already running the
/// rest of this suite in parallel, and short enough to fail rather than hang.
const SERVE_READY_ATTEMPTS: usize = 1_500;
const SERVE_READY_INTERVAL: Duration = Duration::from_millis(20);

/// `--remote` is the same client, not a lesser one.
///
/// Everything above the transport in this CLI is written against one
/// `ProtocolClient`, so the commands cannot themselves tell which one they got —
/// which means a gap in the served transport shows up as a command that works in
/// process and fails over a socket, with nothing in the command to blame. This
/// drives the persisted-thread lifecycle over the socket, because that is the
/// part that was actually missing: the WebSocket transport only ever received a
/// connect-time resume/fork intent, so `--resume`, `session list` and
/// `session title` all answered "requires a session store" against a server that
/// plainly had one.
///
/// A model is never called: seeding uses `queue add`, which persists a thread.
#[test]
fn remote_reaches_the_same_thread_lifecycle_as_in_process() {
    let home = home();
    let queued = run(klaus(&home).args(["--json", "queue", "add", "survive me"]));
    assert_eq!(code(&queued), 0, "queue add should succeed");
    let snapshot: serde_json::Value =
        serde_json::from_str(stdout(&queued).lines().next().expect("no output"))
            .expect("queue add should print JSON");
    let thread = snapshot["queueSnapshot"]["items"][0]["message"]["sessionId"]
        .as_str()
        .expect("a queued message should name its session")
        .to_string();

    let server = serve(&home, "e2e-token");
    // A client of that server, and nothing more: the flags that configure a local
    // engine are refused with `--remote`, which is itself the CLI insisting that a
    // remote run cannot quietly half-configure one.
    let remote = |args: &[&str]| {
        let mut command = bare(&home);
        command
            .args(["--remote", &server.url, "--token", "e2e-token"])
            .args(args);
        run(&mut command)
    };

    let listed = remote(&["session", "list"]);
    assert_eq!(
        code(&listed),
        0,
        "session list over --remote failed\n{}\nserver said:\n{}",
        String::from_utf8_lossy(&listed.stderr),
        server.log()
    );
    assert!(
        stdout(&listed).contains(&thread),
        "session list over --remote did not see the seeded thread\n{}",
        stdout(&listed)
    );

    // `--resume` is the headline global flag, and reading the queue back through
    // it proves the socket's session really is bound to that thread rather than a
    // fresh one that merely accepted the id.
    let resumed = remote(&["--resume", &thread, "queue", "list"]);
    assert_eq!(
        code(&resumed),
        0,
        "--resume over --remote failed\n{}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    assert!(
        stdout(&resumed).contains("survive me"),
        "--resume over --remote bound a thread without the seeded queue\n{}",
        stdout(&resumed)
    );

    // A store write, to show the direction is not read-only.
    let titled = remote(&["session", "title", &thread, "named over a socket"]);
    assert_eq!(
        code(&titled),
        0,
        "session title over --remote failed\n{}\nserver said:\n{}",
        String::from_utf8_lossy(&titled.stderr),
        server.log()
    );
    let relisted = remote(&["session", "list"]);
    assert!(
        stdout(&relisted).contains("named over a socket"),
        "the title written over --remote did not reach the store\n{}",
        stdout(&relisted)
    );
}

/// The server's token is enforced, and a client without it is told so.
///
/// `serve` is the one thing this CLI does that opens a port, so the failure has
/// to be the connection being refused rather than a session that half exists.
#[test]
fn a_served_engine_refuses_a_client_with_the_wrong_token() {
    let home = home();
    let server = serve(&home, "the-right-token");

    for token in ["the-wrong-token", ""] {
        let mut command = bare(&home);
        command.args(["--remote", &server.url]);
        if !token.is_empty() {
            command.args(["--token", token]);
        }
        let output = run(command.args(["capabilities"]));
        assert_ne!(
            code(&output),
            0,
            "a client with token {token:?} was allowed in"
        );
    }

    // The right token still works, so the check above is not passing because the
    // server is simply unreachable.
    let mut command = bare(&home);
    let allowed = run(command
        .args(["--remote", &server.url, "--token", "the-right-token"])
        .args(["capabilities"]));
    assert_eq!(
        code(&allowed),
        0,
        "the right token should be let in\n{}\nserver said:\n{}",
        String::from_utf8_lossy(&allowed.stderr),
        server.log()
    );
}

/// A turn that did not complete never reports success.
///
/// The mapping is asserted here rather than by spawning a failing turn on
/// purpose: making the engine fail requires either no credentials or no network,
/// and which of the two a machine has decides WHICH reason comes back. Pinning
/// the mapping covers every reason at once and needs neither.
///
/// The match has no wildcard, so a seventh reason added to the engine breaks this
/// test at compile time and has to be classified deliberately as either "the
/// turn did what was asked" or "it did not".
#[test]
fn a_turn_that_did_not_complete_never_reports_success() {
    for reason in [
        TerminalReason::Completed,
        TerminalReason::Aborted,
        TerminalReason::PromptTooLong,
        TerminalReason::AuthFailed,
        TerminalReason::TurnLimitExhausted,
        TerminalReason::SpendLimitExhausted,
        TerminalReason::Error,
    ] {
        let succeeded = match reason {
            TerminalReason::Completed => true,
            TerminalReason::Aborted
            | TerminalReason::PromptTooLong
            | TerminalReason::AuthFailed
            | TerminalReason::TurnLimitExhausted
            | TerminalReason::SpendLimitExhausted
            | TerminalReason::Error => false,
        };
        assert_eq!(
            concluded(reason).is_ok(),
            succeeded,
            "{reason:?} is classified wrongly"
        );
    }
}

/// The exit code for a failed turn is the general failure, not the usage one.
///
/// A refused turn is the engine answering, so retrying it with different
/// arguments is not the fix — and 2 would tell a caller exactly that. This pins
/// the two apart, since both are just integers at the boundary.
#[test]
fn a_failed_turn_is_a_failure_not_a_usage_error() {
    let error =
        concluded(TerminalReason::AuthFailed).expect_err("a failed turn should be an error");
    assert_eq!(error.exit_code(), 1);
    assert!(
        error.to_string().contains("auth failed"),
        "the reason should survive into the message, got {error}"
    );
}
