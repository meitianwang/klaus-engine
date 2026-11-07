//! The endpoint assembly every engine binary shares.
//!
//! An "endpoint" is one engine instance plus the endpoint-scoped state around
//! it: the credential store, the rollout store, the installed-plugin ledger, and
//! the session factory/lifecycle the protocol drives. The stdio endpoint and the
//! CLI's in-process transport are the SAME endpoint reached over different
//! transports, so this module owns the assembly and both callers ask it for the
//! pieces instead of each keeping its own copy that then drifts.
//!
//! Configuration comes from the environment on purpose: an endpoint is spawned
//! by a host (or by the CLI's own process) that has no argv channel for provider
//! selection, so `KLAUS_PROVIDER` / `KLAUS_MODEL` / `KLAUS_AGENT_CONFIG_JSON` /
//! `KLAUS_SESSION_DIR` / `KLAUS_PLUGIN_HOME` are the contract.

use std::path::PathBuf;
use std::sync::Arc;

use klaus_agent_core::browser::BrowserRuntime;
use klaus_agent_core::capabilities::RuntimeCapabilityRoot;
use klaus_agent_core::engine::agent::{Agent, AgentOptions};
use klaus_agent_core::engine::deps::EngineDepsOverrides;
use klaus_agent_core::engine::rewind::BoxError;
use klaus_agent_core::extensions::plugins::installed::default_plugin_home;
use klaus_agent_core::mcp::credential_store::{CredentialStore, FileCredentialStore};
use klaus_agent_core::provider::anthropic::{
    AnthropicProvider, AnthropicProviderOptions, BedrockConfig, FoundryRouting, VertexConfig,
};
use klaus_agent_core::provider::gemini::{GeminiProvider, GeminiProviderOptions};
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::tools::builtins::plugin_management::{
    PluginManageMutation, PluginManageOutcome,
};
use klaus_agent_core::types::provider::ModelProvider;
use klaus_agent_protocol::{
    CreateSession, ProtocolLifecycle, ProtocolPluginAdmin, ProtocolServer, ProtocolServerOptions,
};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::app_server::AppServerOptions;
use crate::lifecycle::{engine_sessions, protocol_create_session, protocol_lifecycle};
use crate::plugin_admin::{load_endpoint_installed_plugins, protocol_plugin_admin};
use crate::EngineSessions;

/// What an endpoint may override; every `None` reads the documented environment
/// default so a plain `EndpointOptions::new(ctx)` is the stock endpoint.
pub struct EndpointOptions {
    pub process_context: RuntimeProcessContext,
    /// Default: [`FileCredentialStore`] (the endpoint-owned encrypted file).
    pub credential_store: Option<Arc<dyn CredentialStore>>,
    /// Default: [`provider_from_env`].
    pub provider: Option<Arc<dyn ModelProvider>>,
    /// Default: the process working directory.
    pub cwd: Option<String>,
    /// Default: `KLAUS_SESSION_DIR` (and the store's own fallback below it).
    pub session_dir: Option<String>,
    /// Default: `KLAUS_PLUGIN_HOME`, else [`default_plugin_home`].
    pub plugin_home: Option<PathBuf>,
    /// Default: on, unless `KLAUS_DISABLE_INSTALLED_PLUGINS` is truthy.
    pub load_installed_plugins: Option<bool>,
}

impl EndpointOptions {
    pub fn new(process_context: RuntimeProcessContext) -> Self {
        Self {
            process_context,
            credential_store: None,
            provider: None,
            cwd: None,
            session_dir: None,
            plugin_home: None,
            load_installed_plugins: None,
        }
    }
}

/// One assembled engine endpoint. Cheap to hand around: everything inside is
/// already shared, so the protocol server, the app-server facade and the
/// lifecycle handlers can all be built from the same instance.
pub struct Endpoint {
    pub agent: Arc<Agent>,
    pub credential_store: Arc<dyn CredentialStore>,
    pub store: Arc<dyn SessionStore>,
    /// Absent when installed-plugin loading is off: an endpoint that never read
    /// the ledger must not offer to mutate it.
    pub plugin_admin: Option<Arc<dyn ProtocolPluginAdmin>>,
    pub plugin_home: PathBuf,
    windows_sandbox_context: crate::app_server::windows_sandbox::Context,
    rollout_maintenance: Arc<klaus_agent_protocol::server::RolloutMaintenance>,
}

impl Endpoint {
    /// Build the endpoint: resolve provider/config/plugins, then construct the
    /// [`Agent`] and the rollout store around them.
    pub async fn build(opts: EndpointOptions) -> Result<Self, BoxError> {
        let credential_store = opts
            .credential_store
            .unwrap_or_else(|| Arc::new(FileCredentialStore::new()) as Arc<dyn CredentialStore>);
        let provider = opts.provider.unwrap_or_else(provider_from_env);
        let config = config_from_env();
        let plugin_home = opts.plugin_home.unwrap_or_else(|| {
            env_nonempty("KLAUS_PLUGIN_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(default_plugin_home)
        });
        let load_installed_plugins = opts
            .load_installed_plugins
            .unwrap_or_else(|| !env_truthy("KLAUS_DISABLE_INSTALLED_PLUGINS"));
        let plugin_load_report = if load_installed_plugins {
            Some(load_endpoint_installed_plugins(&plugin_home).await?)
        } else {
            None
        };
        let cwd = opts.cwd.or_else(|| {
            std::env::current_dir()
                .ok()
                .map(|path| path.to_string_lossy().into_owned())
        });
        let windows_sandbox_context = crate::app_server::windows_sandbox::Context::new(
            cwd.as_ref().map(PathBuf::from),
            config.clone(),
        );
        let agent = Arc::new(Agent::new(AgentOptions {
            process_context: opts.process_context,
            provider,
            config,
            tools: None,
            web_fetch_page: None,
            browser_runtime: None::<Arc<dyn BrowserRuntime>>,
            capability_roots: Vec::<RuntimeCapabilityRoot>::new(),
            cwd,
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
            plugin_load_report,
        }));
        let plugin_admin = load_installed_plugins
            .then(|| protocol_plugin_admin(agent.clone(), plugin_home.clone()));
        if let Some(admin) = &plugin_admin {
            let weak = Arc::downgrade(admin);
            agent
                .set_plugin_management_handler(Arc::new(move |mutation| {
                    let weak = weak.clone();
                    Box::pin(async move {
                        let admin = weak
                            .upgrade()
                            .ok_or_else(|| "plugin endpoint is closed".to_string())?;
                        let result = match mutation {
                            PluginManageMutation::SetEnabled { name, enabled } => {
                                admin.set_installed_plugin_enabled(&name, enabled).await
                            }
                            PluginManageMutation::Uninstall { name, remove_files } => {
                                admin.uninstall_plugin(&name, remove_files).await
                            }
                        }
                        .map_err(|error| error.to_string())?;
                        Ok(result.map(|mutation| PluginManageOutcome {
                            record: mutation.plugin,
                            live: mutation.published_to_loaded_sessions,
                        }))
                    })
                }))
                .map_err(std::io::Error::other)?;
        }
        let store: Arc<dyn SessionStore> =
            Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
                dir: opts
                    .session_dir
                    .or_else(|| env_nonempty("KLAUS_SESSION_DIR")),
                archive_dir: None,
            }));
        let rollout_maintenance = crate::lifecycle::rollout_maintenance(store.clone());
        Ok(Self {
            agent,
            credential_store,
            store,
            rollout_maintenance,
            plugin_admin,
            plugin_home,
            windows_sandbox_context,
        })
    }

    /// The protocol's session factory for this endpoint.
    pub fn create_session(&self) -> CreateSession {
        protocol_create_session(self.agent.clone(), Some(self.store.clone()))
    }

    /// The persisted-thread lifecycle ops (list/resume/fork/archive/...).
    pub fn lifecycle(&self) -> ProtocolLifecycle {
        let mut lifecycle = protocol_lifecycle(self.agent.clone(), Some(self.store.clone()));
        lifecycle.rollout_maintenance = Some(self.rollout_maintenance.clone());
        lifecycle
    }

    /// The connect-time session wiring the WebSocket/HTTP transports need.
    pub fn engine_sessions(&self) -> EngineSessions {
        engine_sessions(self.agent.clone(), Some(self.store.clone()))
    }

    /// Captured startup context shared by every facade transport.
    pub fn windows_sandbox_context(&self) -> crate::app_server::windows_sandbox::Context {
        self.windows_sandbox_context.clone()
    }

    /// The facade's assembly for this endpoint.
    ///
    /// This is the engine's ONE outward surface, so every way of reaching it —
    /// `klaus app-server` on stdio, a served connection asking for
    /// `?surface=appServer`, the CLI's own in-process client — builds its
    /// options here. Assembling them at each call site is how a surface ends up
    /// quietly offering a lesser engine than its siblings: a missing
    /// `lifecycle` turns `session list` into "requires a session store" against
    /// an endpoint that plainly has one, and nothing fails loudly enough to say
    /// so.
    pub fn app_server_options(&self) -> AppServerOptions {
        AppServerOptions {
            windows_sandbox_context: Some(self.windows_sandbox_context.clone()),
            // The connect-time factory, not [`Self::create_session`]: the facade
            // reaches the engine through the same per-session wiring the served
            // transports do, so `session/start`'s resume/fork intent is honoured
            // by the same code path a `?resume=` socket goes through.
            create_session: self.engine_sessions().create_session,
            credential_store: Some(self.credential_store.clone()),
            lifecycle: Some(self.lifecycle()),
            plugin_admin: self.plugin_admin.clone(),
            // The config home's defaults: a process holding an `Endpoint` IS
            // the endpoint, so the permission rules and user config it serves
            // are the user's own.
            permission_rules_path: None,
            config_path: None,
            startup_init_ack_budget: None,
        }
    }

    /// A [`ProtocolServer`] speaking newline-delimited Op/Event over `input` /
    /// `output`. Stdio passes the process streams; the CLI passes one half of an
    /// in-memory duplex.
    pub fn protocol_server(
        &self,
        input: Box<dyn AsyncRead + Unpin + Send>,
        output: Box<dyn AsyncWrite + Unpin + Send>,
    ) -> Arc<ProtocolServer> {
        Arc::new(ProtocolServer::new(ProtocolServerOptions {
            create_session: self.create_session(),
            lifecycle: Some(self.lifecycle()),
            plugin_admin: self.plugin_admin.clone(),
            credential_store: Some(self.credential_store.clone()),
            input,
            output,
        }))
    }
}

/// The provider named by `KLAUS_PROVIDER` (default `anthropic`), configured from
/// the environment.
pub fn provider_from_env() -> Arc<dyn ModelProvider> {
    provider_named(&env_nonempty("KLAUS_PROVIDER").unwrap_or_else(|| "anthropic".to_string()))
}

/// The named provider, configured from the environment.
///
/// Split from [`provider_from_env`] so a caller that learns the backend some
/// other way — a CLI flag, a host's own configuration — selects it through the
/// same code path instead of reimplementing the routing below. Anthropic
/// additionally routes through Bedrock, Vertex or Foundry when exactly one of
/// those is configured, in that precedence. An unknown name falls back to
/// Anthropic rather than failing, matching what the environment path has always
/// done; callers that want a name validated should do it where the name is typed.
pub fn provider_named(name: &str) -> Arc<dyn ModelProvider> {
    match name.to_ascii_lowercase().as_str() {
        "openai" => Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
            api_key: env_nonempty("OPENAI_API_KEY"),
            base_url: env_nonempty("OPENAI_BASE_URL"),
            default_model: env_nonempty("KLAUS_MODEL"),
            api: Some(openai_api_from_env()),
            ..Default::default()
        })),
        "gemini" => Arc::new(GeminiProvider::new(GeminiProviderOptions {
            api_key: env_nonempty("GEMINI_API_KEY"),
            base_url: env_nonempty("GEMINI_BASE_URL"),
            default_model: env_nonempty("KLAUS_MODEL"),
            ..Default::default()
        })),
        _ => {
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
            Arc::new(AnthropicProvider::new(AnthropicProviderOptions {
                api_key: foundry
                    .as_ref()
                    .and_then(|f| f.api_key.clone())
                    .or_else(|| env_nonempty("ANTHROPIC_API_KEY")),
                default_model: env_nonempty("KLAUS_MODEL"),
                bedrock,
                vertex,
                base_url: foundry.map(|f| f.base_url),
                ..Default::default()
            }))
        }
    }
}

fn openai_api_from_env() -> OpenAiApi {
    match env_nonempty("KLAUS_OPENAI_API")
        .unwrap_or_else(|| "responses".to_string())
        .to_ascii_lowercase()
        .as_str()
    {
        "chat" => OpenAiApi::Chat,
        _ => OpenAiApi::Responses,
    }
}

/// The engine config object from `KLAUS_AGENT_CONFIG_JSON`, with `KLAUS_MODEL`
/// filled in when the object does not already name a model. A malformed or
/// non-object value is reported and ignored rather than failing startup: the
/// endpoint is still usable without it.
pub fn config_from_env() -> Option<serde_json::Value> {
    let mut config = env_nonempty("KLAUS_AGENT_CONFIG_JSON")
        .and_then(
            |raw| match serde_json::from_str::<serde_json::Value>(&raw) {
                Ok(value @ serde_json::Value::Object(_)) => Some(value),
                Ok(_) => {
                    eprintln!(
                        "klaus engine: ignored KLAUS_AGENT_CONFIG_JSON because it is not an object"
                    );
                    None
                }
                Err(err) => {
                    eprintln!("klaus engine: ignored invalid KLAUS_AGENT_CONFIG_JSON: {err}");
                    None
                }
            },
        )
        .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
    if let Some(model) = env_nonempty("KLAUS_MODEL") {
        if let serde_json::Value::Object(map) = &mut config {
            map.entry("model".to_string())
                .or_insert_with(|| serde_json::Value::String(model));
        }
    }
    match config {
        serde_json::Value::Object(map) if map.is_empty() => None,
        value => Some(value),
    }
}

/// A trimmed environment variable, or `None` when unset or blank. Blank counts
/// as unset because a shell that exports an empty value means "not configured",
/// not "configured to the empty string".
pub fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Whether an environment flag is set to one of the accepted true spellings.
pub fn env_truthy(key: &str) -> bool {
    matches!(
        env_nonempty(key)
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}
