//! The code-mode host: serves one engine's cells over stdio.
//!
//! The engine that owns the sessions runs elsewhere; this process only executes
//! cells and asks that engine — over the connection — whenever a cell needs a
//! nested tool call or wants to surface status text. Diagnostics go to stderr
//! because stdout carries the framed protocol.

use klaus_agent_core::engine::code_mode::host_service;

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    match host_service::run_stdio().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("code-mode host failed: {error:?}");
            std::process::ExitCode::FAILURE
        }
    }
}
