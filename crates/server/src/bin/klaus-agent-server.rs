//! klaus-agent-server — serve the Op/Event protocol over a loopback
//! WebSocket for embedding hosts.
//!
//! One connection = one session; hosts open a fresh connection per session
//! activation (optionally with `?resume=<threadId>` / `?fork=<threadId>`)
//! and pass all session-specific configuration on InitSession: provider
//! override (base URL, api key, custom headers), dynamic tools, agents,
//! and skills.
//!
//! Differences from the stock stdio endpoint:
//! - WebSocket transport instead of stdio (stdio binds one session per
//!   process; embedding hosts need cheap session switching).
//! - No OS keychain: auth is per-session and ephemeral, so the credential
//!   store is in-memory.
//! - No installed-plugin loading: host capabilities arrive on InitSession.
//! - The default provider is Anthropic with NO api key; every session is
//!   expected to carry a provider override.
//!
//! Environment:
//! - `KLAUS_SERVER_PORT`  bind port (default: ephemeral)
//! - `KLAUS_SERVER_TOKEN` bearer token required from clients (optional on loopback)
//! - `KLAUS_SESSION_DIR`  JSONL session store directory
//! - `KLAUS_MODEL`        default model name
//!
//! Startup handshake: prints `KLAUS_AGENT_SERVER_READY <ws-url>` on stdout
//! once listening. Shuts down when stdin reaches EOF (parent process exited)
//! or on SIGTERM.

use std::sync::Arc;

use tokio::io::AsyncReadExt;

use klaus_agent_core::browser::BrowserRuntime;
use klaus_agent_core::capabilities::RuntimeCapabilityRoot;
use klaus_agent_core::engine::agent::{Agent, AgentOptions};
use klaus_agent_core::engine::deps::EngineDepsOverrides;
use klaus_agent_core::engine::rewind::BoxError;
use klaus_agent_core::mcp::credential_store::{CredentialStore, InMemoryCredentialStore};
use klaus_agent_core::provider::anthropic::{
    AnthropicProvider, AnthropicProviderOptions, BedrockConfig, FoundryRouting, VertexConfig,
};
use klaus_agent_core::runtime_process::{RuntimeProcessContext, RuntimeProcessSupervisor};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::types::provider::ModelProvider;
use klaus_agent_server::{
    engine_sessions, protocol_lifecycle, serve, EngineSessions, ServeOptions,
};

const WORKER_STACK_SIZE_BYTES: usize = 8 * 1024 * 1024;

fn main() {
    klaus_agent_core::sandbox::linux::dispatch();
    #[cfg(windows)]
    klaus_agent_server::windows_process_mode::dispatch();

    // The descriptor budget, claimed before the runtime exists (see
    // `klaus_agent_server::fd_limit`): an embedding host that launches this from
    // a GUI process passes down a soft limit one session can exhaust.
    eprintln!(
        "klaus-agent-server: open-file limit {}",
        klaus_agent_server::raise_open_file_limit()
    );

    let runtime = match build_runtime() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("klaus-agent-server: failed to initialize async runtime: {error}");
            std::process::exit(1);
        }
    };

    if let Err(error) = runtime.block_on(run()) {
        eprintln!("klaus-agent-server: startup failed: {error}");
        std::process::exit(1);
    }
}

fn build_runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(WORKER_STACK_SIZE_BYTES)
        .build()
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

async fn run_endpoint(process_context: RuntimeProcessContext) -> Result<(), BoxError> {
    let credential_store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
    let bedrock = BedrockConfig::from_env();
    let vertex = if bedrock.is_none() {
        VertexConfig::from_env()
    } else {
        None
    };
    let foundry = if bedrock.is_none() && vertex.is_none() {
        FoundryRouting::from_env()
    } else {
        None
    };
    let provider: Arc<dyn ModelProvider> =
        Arc::new(AnthropicProvider::new(AnthropicProviderOptions {
            default_model: env_nonempty("KLAUS_MODEL"),
            bedrock,
            vertex,
            base_url: foundry.as_ref().map(|f| f.base_url.clone()),
            api_key: foundry.and_then(|f| f.api_key),
            ..Default::default()
        }));

    let agent = Arc::new(Agent::new(AgentOptions {
        process_context,
        provider,
        config: None,
        tools: None,
        web_fetch_page: None,
        browser_runtime: None::<Arc<dyn BrowserRuntime>>,
        capability_roots: Vec::<RuntimeCapabilityRoot>::new(),
        cwd: std::env::current_dir()
            .ok()
            .map(|path| path.to_string_lossy().into_owned()),
        deps: EngineDepsOverrides::default(),
        compactor: None,
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        credential_store: Some(credential_store.clone()),
        hooks: Vec::new(),
        observer: None,
        logger: None,
        pricing: None,
        agents: None,
        include_default_agents: None,
        skills: None,
        output_styles: None,
        workflows: None,
        mcp_catalog: None,
        plugin_catalog: None,
        lsp_servers: None,
        agent_memory: None,
        mcp_clients: None,
        mcp_server_configs: Vec::new(),
        mcp_tools: None,
        refresh_tools: None,
        system_prompt_provider: None,
        plugins: Vec::new(),
        plugin_load_report: None,
    }));

    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: env_nonempty("KLAUS_SESSION_DIR"),
        archive_dir: None,
    }));
    let EngineSessions {
        create_session,
        list_threads,
    } = engine_sessions(agent.clone(), Some(store.clone()));
    // Wired so a served client can enumerate and resume the threads in the store
    // this process was pointed at, instead of being told there is no store.
    let lifecycle = protocol_lifecycle(agent, Some(store));

    let port = env_nonempty("KLAUS_SERVER_PORT")
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(0);
    let server = serve(ServeOptions {
        windows_sandbox_context: None,
        create_session,
        credential_store: Some(credential_store),
        list_threads,
        lifecycle: Some(lifecycle),
        // This binary has no plugin ledger of its own.
        plugin_admin: None,
        host: None, // loopback-only
        port: Some(port),
        token: env_nonempty("KLAUS_SERVER_TOKEN"),
        allowed_origins: None,
        silent: true,
    })
    .await?;

    // Host handshake: announce the listening address on stdout
    println!("KLAUS_AGENT_SERVER_READY {}", server.url);
    use std::io::Write as _;
    std::io::stdout().flush().ok();
    eprintln!("klaus-agent-server: listening on {}", server.url);

    // Graceful shutdown on stdin EOF (parent exited) or SIGTERM
    wait_for_shutdown_signal().await;
    eprintln!("klaus-agent-server: shutting down");
    server.close().await;
    Ok(())
}

async fn wait_for_shutdown_signal() {
    let stdin_eof = async {
        let mut stdin = tokio::io::stdin();
        let mut buf = [0u8; 1024];
        loop {
            match stdin.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(_) => continue,
            }
        }
    };
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler must install");
        tokio::select! {
            _ = stdin_eof => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        stdin_eof.await;
    }
}

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}
