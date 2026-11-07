//! `klaus` — the engine's terminal client.
//!
//! This file is only the process boundary: build a runtime, initialize the
//! engine's process supervision, parse argv, dispatch, tear down in order, and
//! turn the outcome into an exit code. Everything the CLI actually does lives in
//! the library, which is what lets the integration tests exercise the whole
//! surface without spawning a binary.
//!
//! The ORDER of teardown at the bottom is the part that matters. The runtime
//! supervisor owns every child process the engine spawned — sandboxed commands,
//! MCP servers, language servers — and the process context owns the runtime
//! handles the sessions were built on. Releasing either while the other is still
//! stopping strands children, so the three shutdowns run in sequence and their
//! failures are reported only after the command's own result, which is the one
//! the user asked for.

use std::process::ExitCode;

use clap::Parser;

use klaus_agent_cli::cli::Cli;
use klaus_agent_cli::commands;
use klaus_agent_cli::error::{CliError, EXIT_FAILURE};
use klaus_agent_core::runtime_process::{RuntimeProcessContext, RuntimeProcessSupervisor};

/// The engine's tool stack recurses deeply enough (nested sub-agents, each with
/// its own tool loop) that the platform default worker stack is not enough. Same
/// value the stdio endpoint uses, for the same reason: this process runs the
/// engine, not just a client of it.
const WORKER_STACK_SIZE_BYTES: usize = 8 * 1024 * 1024;

fn main() -> ExitCode {
    klaus_agent_core::sandbox::linux::dispatch();
    #[cfg(windows)]
    klaus_agent_server::windows_process_mode::dispatch();

    // argv is parsed BEFORE the runtime is built. A bad invocation must not be
    // able to start an engine, and clap's own errors (`--help`, `--version`, a
    // typo) must reach the terminal without waiting on anything.
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            // clap decides the stream and the code: help and version are a
            // successful request for output, a parse failure is exit 2.
            let _ = error.print();
            return ExitCode::from(u8::try_from(error.exit_code()).unwrap_or(2));
        }
    };

    // The descriptor budget for the engine this process embeds, claimed before
    // the runtime exists (see `klaus_agent_server::fd_limit`). Silent, unlike the
    // hosted engine binaries: this one owns a terminal a user is reading.
    klaus_agent_server::raise_open_file_limit();

    let runtime = match build_runtime() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("klaus: failed to initialize the async runtime: {error}");
            return ExitCode::from(EXIT_FAILURE as u8);
        }
    };

    let code = runtime.block_on(run(cli));
    // Dropped explicitly and last: a runtime dropped while a spawned reader task
    // is still parked would block the drop, and doing it here rather than at the
    // end of `run` keeps that out of the async frame.
    drop(runtime);
    code
}

fn build_runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(WORKER_STACK_SIZE_BYTES)
        .build()
}

async fn run(cli: Cli) -> ExitCode {
    let supervisor = match RuntimeProcessSupervisor::initialize_current() {
        Ok(supervisor) => supervisor,
        Err(error) => {
            eprintln!("klaus: {}", CliError::engine(error.to_string()));
            return ExitCode::from(EXIT_FAILURE as u8);
        }
    };
    let process_context = match RuntimeProcessContext::initialize(supervisor.clone()) {
        Ok(context) => context,
        Err(error) => {
            eprintln!("klaus: {}", CliError::engine(error.to_string()));
            let _ = supervisor.shutdown().await;
            return ExitCode::from(EXIT_FAILURE as u8);
        }
    };

    let outcome = commands::dispatch(cli, process_context.clone()).await;

    let context_shutdown = process_context.shutdown().await;
    drop(process_context);
    let supervisor_shutdown = supervisor.shutdown().await;

    // The command's failure is the answer; a teardown failure is reported next to
    // it rather than instead of it, because a caller debugging "why did my turn
    // fail" is not helped by being told the supervisor also had trouble.
    let mut code = match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("klaus: {error}");
            ExitCode::from(u8::try_from(error.exit_code()).unwrap_or(1))
        }
    };
    for failure in [context_shutdown.err(), supervisor_shutdown.err()]
        .into_iter()
        .flatten()
    {
        eprintln!("klaus: engine teardown failed: {failure}");
        code = ExitCode::from(EXIT_FAILURE as u8);
    }
    code
}
