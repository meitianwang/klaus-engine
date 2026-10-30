use std::path::PathBuf;

use klaus_agent_core::engine::rewind::BoxError;
#[cfg(windows)]
use klaus_agent_core::network_proxy::{NetworkProxyConfig, WindowsNetworkBroker};
use klaus_agent_core::runtime_process::{RuntimeProcessContext, RuntimeProcessSupervisor};
use klaus_agent_server::{run_app_server, schema_bundle, Endpoint, EndpointOptions};
#[cfg(windows)]
use serde::Deserialize;
#[cfg(windows)]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(windows)]
use windows_sys::Win32::Foundation::CloseHandle;
#[cfg(windows)]
use windows_sys::Win32::System::Threading::{OpenProcess, WaitForSingleObject};

const STDIO_WORKER_STACK_SIZE_BYTES: usize = 8 * 1024 * 1024;
#[cfg(windows)]
const WINDOWS_BROKER_PROTOCOL_VERSION: u32 = 1;
#[cfg(windows)]
const WINDOWS_BROKER_MAX_FRAME_BYTES: usize = 1024 * 1024;
#[cfg(windows)]
const PROCESS_SYNCHRONIZE: u32 = 0x0010_0000;

fn main() {
    klaus_agent_core::sandbox::linux::dispatch();
    #[cfg(windows)]
    klaus_agent_server::windows_process_mode::dispatch();

    // argv is CLASSIFIED before any runtime is built, and an argument this
    // binary does not know is REFUSED instead of ignored.
    match classify_invocation(std::env::args().skip(1)) {
        StdioInvocation::Help => {
            print!("{}", help_text());
            return;
        }
        StdioInvocation::Version => {
            println!("{}", build_identity());
            return;
        }
        StdioInvocation::Unknown(argument) => {
            eprintln!("klaus-agent-stdio: unrecognized argument `{argument}`");
            eprint!("{}", help_text());
            std::process::exit(2);
        }
        StdioInvocation::Run => {}
    }

    // `generate-json-schema [--out DIR]` runs synchronously and exits: it only
    // serializes the app-server API contract, no engine runtime is needed.
    if std::env::args().nth(1).as_deref() == Some("generate-json-schema") {
        std::process::exit(match run_generate_json_schema() {
            Ok(path) => {
                eprintln!("klaus-agent-stdio: wrote {}", path.display());
                0
            }
            Err(error) => {
                eprintln!("klaus-agent-stdio: generate-json-schema failed: {error}");
                1
            }
        });
    }

    // The descriptor budget, claimed before the runtime exists and before any
    // session can spend one. A GUI-launched host hands this process launchd's
    // soft limit of 256, which one ordinary desktop session exhausts; the number
    // is reported because an EMFILE cascade is otherwise invisible from outside
    // (see `klaus_agent_server::fd_limit`).
    eprintln!(
        "klaus-agent-stdio: open-file limit {}",
        klaus_agent_server::raise_open_file_limit()
    );

    let runtime = match build_stdio_runtime() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("klaus-agent-stdio: failed to initialize async runtime: {error}");
            std::process::exit(1);
        }
    };

    #[cfg(windows)]
    let result = if std::env::args().any(|arg| arg == "--windows-network-broker") {
        runtime.block_on(run_windows_network_broker())
    } else {
        runtime.block_on(run())
    };
    #[cfg(not(windows))]
    let result = runtime.block_on(run());

    if let Err(error) = result {
        eprintln!("klaus-agent-stdio: startup failed: {error}");
        std::process::exit(1);
    }
}

fn build_stdio_runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(STDIO_WORKER_STACK_SIZE_BYTES)
        .build()
}

/// What this invocation is asking for. Anything outside the known argv surface
/// is `Unknown`, which FAILS the process instead of starting a protocol loop.
///
/// Why it matters: an unrecognized argument used to fall through to the default
/// newline-delimited Op/Event endpoint, which reads stdin until EOF. So
/// `klaus-agent-stdio --help` printed nothing, exited never, and sat there
/// holding the engine binary mapped — measured once at three hours of uptime for
/// a one-off probe. On macOS that is not merely untidy: while a process maps an
/// executable, rewriting that file in place invalidates the kernel's per-inode
/// code-signature cache, after which every exec of it is SIGKILLed. A typo in a
/// flag must not be able to start that chain.
#[derive(Debug, PartialEq, Eq)]
enum StdioInvocation {
    Help,
    Version,
    Run,
    Unknown(String),
}

fn classify_invocation<I: IntoIterator<Item = String>>(args: I) -> StdioInvocation {
    let args: Vec<String> = args.into_iter().collect();
    let first = args.first().map(String::as_str);
    // Subcommand modes own the rest of their argv (`generate-json-schema` parses
    // its own `--out DIR`; the Windows sandbox modes are spawned by the engine
    // itself with positional arguments).
    if first == Some("generate-json-schema") {
        return StdioInvocation::Run;
    }
    #[cfg(windows)]
    {
        // The sandbox wrapper is spawned by the engine itself
        // with positional arguments, so they are not subject to the flag parsing
        // below.
        if first == Some(klaus_sandbox_win::WRAPPER_ARG) {
            return StdioInvocation::Run;
        }
    }
    for argument in &args {
        match argument.as_str() {
            "-h" | "--help" => return StdioInvocation::Help,
            "-V" | "--version" => return StdioInvocation::Version,
            "--app-server" => {}
            #[cfg(windows)]
            "--windows-network-broker" => {}
            #[cfg(windows)]
            other if other.starts_with("--parent-pid=") => {}
            _ => return StdioInvocation::Unknown(argument.clone()),
        }
    }
    StdioInvocation::Run
}

/// What `--version` answers, and the first line of `--help`.
///
/// The engine is not versioned, so there is no number to print here: it is not
/// published, not installed on its own, and the crate version is a fixed 0.0.0
/// that no release moves (see the workspace manifest). Printing that would be a
/// number a caller could mistake for a release identity. What actually identifies
/// a built engine is the commit it came from, which packaging records next to the
/// binary as `engineCommit`, so this points there instead of inventing a version.
/// The two products that DO have versions answer for themselves: `klaus
/// --version` for the CLI, and the desktop app's own about panel.
fn build_identity() -> String {
    format!(
        "klaus-agent-stdio — engine build {} (the manifest.json packaged beside \
         this binary records the same commit as `engineCommit`)",
        env!("KLAUS_ENGINE_BUILD_COMMIT")
    )
}

fn help_text() -> String {
    format!(
        "{identity}

USAGE:
    klaus-agent-stdio                       newline-delimited Op/Event protocol
                                            over stdin/stdout
    klaus-agent-stdio --app-server          JSON-RPC app-server facade over
                                            stdin/stdout
    klaus-agent-stdio generate-json-schema [--out DIR]
                                            write the app-server API schema
                                            bundle and exit
    klaus-agent-stdio -h, --help            show this help and exit
    klaus-agent-stdio -V, --version         show the build identity and exit

Both protocol modes read stdin until EOF and are meant to be SPAWNED BY A HOST,
not run interactively. Any other argument is rejected with exit code 2 rather
than silently starting a protocol loop that would wait on stdin forever.

Configuration is taken from the environment (KLAUS_PROVIDER, KLAUS_MODEL,
KLAUS_AGENT_CONFIG_JSON, KLAUS_SESSION_DIR, KLAUS_PLUGIN_HOME, ...).
",
        identity = build_identity()
    )
}

/// Write the app-server API schema bundle to `--out DIR` (default: cwd).
fn run_generate_json_schema() -> Result<PathBuf, BoxError> {
    let args: Vec<String> = std::env::args().collect();
    let out_dir = args
        .iter()
        .position(|arg| arg == "--out")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    std::fs::create_dir_all(&out_dir)?;
    let path = out_dir.join("klaus-app-server.schema.json");
    let bundle = serde_json::to_vec_pretty(&schema_bundle())?;
    std::fs::write(&path, bundle)?;
    Ok(path)
}

async fn run() -> Result<(), BoxError> {
    let supervisor = RuntimeProcessSupervisor::initialize_current()?;
    let process_context = RuntimeProcessContext::initialize(supervisor.clone())?;

    let endpoint_result = run_endpoint(process_context.clone()).await;
    let context_shutdown_result = process_context.shutdown().await;
    drop(process_context);
    let supervisor_shutdown_result = supervisor.shutdown().await;

    endpoint_result?;
    context_shutdown_result?;
    supervisor_shutdown_result?;
    Ok(())
}

#[cfg(windows)]
#[derive(Debug, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum WindowsBrokerRequest {
    RegisterRoute {
        request_id: String,
        session_id: String,
        config: NetworkProxyConfig,
        policy_digest: String,
    },
    UnregisterRoute {
        request_id: String,
        session_id: String,
        route_sid: String,
        route_lease_id: String,
        broker_generation: String,
    },
    RouteMatches {
        request_id: String,
        session_id: String,
        route_sid: String,
        policy_digest: String,
    },
    Health {
        request_id: String,
    },
    Shutdown {
        request_id: String,
    },
}

#[cfg(windows)]
async fn run_windows_network_broker() -> Result<(), BoxError> {
    // No preferred ports. The sandbox does not provision host state, so there is no
    // marker file recording which ports a previous run used; the broker's own
    // fallback picks a fixed pair and then an ephemeral one, which is what actually
    // decided the ports even when a marker existed.
    let broker = WindowsNetworkBroker::start_with_preferred_ports(&[]).await?;
    let mut input = tokio::io::stdin();
    let mut output = tokio::io::stdout();
    let parent_pid = windows_broker_parent_pid()?;
    let mut parent_exit = Box::pin(wait_for_parent_exit(parent_pid));
    write_broker_message(
        &mut output,
        &serde_json::json!({
            "type": "ready",
            "protocolVersion": WINDOWS_BROKER_PROTOCOL_VERSION,
            "generation": broker.generation(),
            "httpAddr": broker.http_addr().to_string(),
            "socksAddr": broker.socks_addr().to_string(),
        }),
    )
    .await?;

    loop {
        let encoded = tokio::select! {
            frame = read_broker_message(&mut input) => frame?,
            _ = &mut parent_exit => None,
        };
        let Some(encoded) = encoded else {
            break;
        };
        let request = match serde_json::from_slice::<WindowsBrokerRequest>(&encoded) {
            Ok(request) => request,
            Err(error) => {
                write_broker_message(
                    &mut output,
                    &serde_json::json!({
                        "type": "error",
                        "requestId": null,
                        "code": "invalid_request",
                        "message": error.to_string(),
                    }),
                )
                .await?;
                continue;
            }
        };
        match request {
            WindowsBrokerRequest::RegisterRoute {
                request_id,
                session_id,
                config,
                policy_digest,
            } => match broker
                .register_route(session_id, config, policy_digest)
                .await
            {
                Ok(route) => {
                    write_broker_message(
                        &mut output,
                        &serde_json::json!({
                            "type": "route_registered",
                            "requestId": request_id,
                            "sessionId": route.session_id,
                            "routeSid": route.route_sid,
                            "routeLeaseId": route.route_lease_id,
                            "httpAddr": route.http_addr.to_string(),
                            "socksAddr": route.socks_addr.map(|addr| addr.to_string()),
                            "brokerGeneration": route.broker_generation,
                            "policyDigest": route.policy_digest,
                        }),
                    )
                    .await?;
                }
                Err(error) => {
                    write_broker_error(
                        &mut output,
                        &request_id,
                        "route_registration_failed",
                        &error,
                    )
                    .await?;
                }
            },
            WindowsBrokerRequest::UnregisterRoute {
                request_id,
                session_id,
                route_sid,
                route_lease_id,
                broker_generation,
            } => {
                let removed = match broker
                    .unregister_route(&session_id, &route_sid, &route_lease_id, &broker_generation)
                    .await
                {
                    Ok(removed) => removed,
                    Err(error) => {
                        write_broker_error(
                            &mut output,
                            &request_id,
                            "route_unregistration_failed",
                            &error,
                        )
                        .await?;
                        continue;
                    }
                };
                write_broker_message(
                    &mut output,
                    &serde_json::json!({
                        "type": "route_unregistered",
                        "requestId": request_id,
                        "sessionId": session_id,
                        "removed": removed,
                    }),
                )
                .await?;
            }
            WindowsBrokerRequest::RouteMatches {
                request_id,
                session_id,
                route_sid,
                policy_digest,
            } => {
                write_broker_message(
                    &mut output,
                    &serde_json::json!({
                        "type": "route_match",
                        "requestId": request_id,
                        "matches": broker.route_matches(
                            &session_id,
                            &route_sid,
                            &policy_digest,
                        ),
                    }),
                )
                .await?;
            }
            WindowsBrokerRequest::Health { request_id } => {
                let health = broker.health();
                write_broker_message(
                    &mut output,
                    &serde_json::json!({
                        "type": "health",
                        "requestId": request_id,
                        "generation": health.generation,
                        "httpAddr": health.http_addr.to_string(),
                        "socksAddr": health.socks_addr.to_string(),
                        "routeCount": health.route_count,
                        "httpListenerRunning": health.http_listener_running,
                        "socksListenerRunning": health.socks_listener_running,
                    }),
                )
                .await?;
            }
            WindowsBrokerRequest::Shutdown { request_id } => {
                write_broker_message(
                    &mut output,
                    &serde_json::json!({
                        "type": "shutdown",
                        "requestId": request_id,
                    }),
                )
                .await?;
                break;
            }
        }
    }
    broker.shutdown().await;
    Ok(())
}

#[cfg(windows)]
async fn write_broker_error(
    output: &mut tokio::io::Stdout,
    request_id: &str,
    code: &str,
    error: &dyn std::fmt::Display,
) -> Result<(), BoxError> {
    write_broker_message(
        output,
        &serde_json::json!({
            "type": "error",
            "requestId": request_id,
            "code": code,
            "message": error.to_string(),
        }),
    )
    .await
}

#[cfg(windows)]
async fn write_broker_message(
    output: &mut tokio::io::Stdout,
    message: &serde_json::Value,
) -> Result<(), BoxError> {
    let encoded = serde_json::to_vec(message)?;
    if encoded.is_empty() || encoded.len() > WINDOWS_BROKER_MAX_FRAME_BYTES {
        return Err(format!(
            "Windows broker response frame length {} is invalid",
            encoded.len()
        )
        .into());
    }
    output
        .write_all(&(encoded.len() as u32).to_le_bytes())
        .await?;
    output.write_all(&encoded).await?;
    output.flush().await?;
    Ok(())
}

#[cfg(windows)]
async fn read_broker_message(input: &mut tokio::io::Stdin) -> Result<Option<Vec<u8>>, BoxError> {
    let mut length = [0u8; 4];
    match input.read_exact(&mut length).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let length = u32::from_le_bytes(length) as usize;
    if length == 0 || length > WINDOWS_BROKER_MAX_FRAME_BYTES {
        return Err(format!("Windows broker request frame length {length} is invalid").into());
    }
    let mut encoded = vec![0u8; length];
    input.read_exact(&mut encoded).await?;
    Ok(Some(encoded))
}

#[cfg(windows)]
fn windows_broker_parent_pid() -> Result<Option<u32>, BoxError> {
    let value = std::env::args().find_map(|arg| {
        arg.strip_prefix("--parent-pid=")
            .map(std::string::ToString::to_string)
    });
    value
        .map(|value| {
            value
                .parse::<u32>()
                .map(Some)
                .map_err(|error| format!("invalid Windows broker parent PID: {error}").into())
        })
        .unwrap_or(Ok(None))
}

#[cfg(windows)]
async fn wait_for_parent_exit(parent_pid: Option<u32>) {
    let Some(parent_pid) = parent_pid else {
        std::future::pending::<()>().await;
        return;
    };
    let _ = tokio::task::spawn_blocking(move || unsafe {
        let process = OpenProcess(PROCESS_SYNCHRONIZE, 0, parent_pid);
        if process.is_null() {
            return;
        }
        WaitForSingleObject(process, u32::MAX);
        CloseHandle(process);
    })
    .await;
}

async fn run_endpoint(process_context: RuntimeProcessContext) -> Result<(), BoxError> {
    let endpoint = Endpoint::build(EndpointOptions::new(process_context)).await?;

    // `--app-server` swaps the endpoint loop for the JSON-RPC facade. The
    // default newline-delimited Op/Event path below stays untouched.
    if std::env::args().any(|arg| arg == "--app-server") {
        eprintln!("klaus-agent-stdio: app-server ready");
        run_app_server(
            tokio::io::stdin(),
            tokio::io::stdout(),
            endpoint.app_server_options(),
        )
        .await;
        return Ok(());
    }

    eprintln!("klaus-agent-stdio: ready");
    let server =
        endpoint.protocol_server(Box::new(tokio::io::stdin()), Box::new(tokio::io::stdout()));
    server.clone().start().await;
    // Do not release the process context while Session-owned MCP/LSP/rollout
    // resources or a serialized protocol op are still shutting down.
    server.shutdown_and_wait().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classify(args: &[&str]) -> StdioInvocation {
        classify_invocation(args.iter().map(|arg| (*arg).to_string()))
    }

    #[test]
    fn builds_runtime_with_the_product_worker_stack_contract() {
        assert_eq!(STDIO_WORKER_STACK_SIZE_BYTES, 8 * 1024 * 1024);
        let runtime = build_stdio_runtime().expect("stdio runtime");
        runtime.block_on(async { tokio::task::yield_now().await });
    }

    #[test]
    fn the_argv_surface_is_classified_and_unknown_arguments_are_refused() {
        assert_eq!(classify(&[]), StdioInvocation::Run);
        assert_eq!(classify(&["--app-server"]), StdioInvocation::Run);
        assert_eq!(
            classify(&["generate-json-schema", "--out", "/tmp/x"]),
            StdioInvocation::Run,
            "a subcommand owns the rest of its argv"
        );
        assert_eq!(classify(&["--help"]), StdioInvocation::Help);
        assert_eq!(classify(&["-h"]), StdioInvocation::Help);
        assert_eq!(classify(&["--version"]), StdioInvocation::Version);
        assert_eq!(classify(&["-V"]), StdioInvocation::Version);
        // Help wins over a protocol mode: asking for help never starts a loop.
        assert_eq!(classify(&["--app-server", "--help"]), StdioInvocation::Help);
        // The defect: an unknown argument must NOT fall through to the endpoint
        // loop, where it would wait on stdin forever.
        assert_eq!(
            classify(&["--app-servr"]),
            StdioInvocation::Unknown("--app-servr".to_string()),
            "a typo is refused, not ignored"
        );
        assert_eq!(
            classify(&["--app-server", "--nope"]),
            StdioInvocation::Unknown("--nope".to_string()),
            "an unknown argument next to a known one is still refused"
        );
    }

    #[test]
    fn the_help_text_names_every_supported_mode() {
        let help = help_text();
        // The engine has no version to print, so the help opens with the build
        // identity instead and must not grow a version number back.
        assert!(help.starts_with(&build_identity()), "{help}");
        assert!(
            !help.contains(env!("CARGO_PKG_VERSION")),
            "no version leaks in: {help}"
        );
        for expected in [
            "--app-server",
            "generate-json-schema",
            "--help",
            "--version",
            "read stdin until EOF",
        ] {
            assert!(
                help.contains(expected),
                "help is missing {expected}: {help}"
            );
        }
    }
}
