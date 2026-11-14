//! Session lifecycle: the context a lifecycle-capable server passes to
//! [`CreateSession`] per connection, and [`engine_sessions`] which wires an
//! [`Agent`] (+ optional store) into the `{ create_session, list_threads }` a
//! lifecycle server needs — the network equivalent of the SDK's engine.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use klaus_agent_core::capabilities::{RuntimeCapabilityRoot, SelectedRuntimeCapabilityRoot};
use klaus_agent_core::credential_store::CredentialStore;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::agent_definitions::AgentDefinition;
use klaus_agent_core::engine::session::{LiveForkSnapshot, LiveSideTaskSnapshot, Session};
use klaus_agent_core::extensions::output_styles::OutputStyle;
use klaus_agent_core::extensions::skills::loader::Skill;
use klaus_agent_core::hooks::types::Hook;
use klaus_agent_core::mcp::client::McpAuthorizeHandler;
use klaus_agent_core::mcp::elicitation::McpElicitationPresenter;
use klaus_agent_core::mcp::types::McpServerConfig;
use klaus_agent_core::network_proxy::TrustedWindowsSandboxContext;
use klaus_agent_core::observability::cost_tracker::PricingTable;
use klaus_agent_core::runtime_process::RuntimeEnv;
use klaus_agent_core::runtime_values::{
    runtime_codec, try_to_scalar_json, RuntimeJson, RuntimePath, RuntimeString,
};
use klaus_agent_core::store::session_store::{ListOptions, SessionStore, ThreadPage};
use klaus_agent_core::types::permission::ApprovalHandler;
use klaus_agent_core::types::provider::ModelProvider;
use klaus_agent_core::types::tool::ElicitHandler;
use klaus_agent_core::types::tool::Tool;
use klaus_agent_protocol::{
    ArchiveSessionResult, CreateSession as ProtocolCreateSession,
    CreateSessionArgs as ProtocolSessionHandlers,
    CreateSessionFuture as ProtocolCreateSessionFuture, DeleteSessionResult, ProtocolLifecycle,
    ProtocolSessionInit, ProtocolTrustedWindowsSandboxContext, RenameThreadResult,
    UpdateThreadHostMetadataResult, UpdateTitleResult,
};

/// Error surfaced by a session factory / thread lister.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Engine-owned context for an atomic ephemeral side-task start.
#[derive(Clone)]
pub struct SideTaskStart {
    pub parent_thread_id: String,
    pub live_snapshot: Option<Arc<LiveSideTaskSnapshot>>,
}

/// Context a lifecycle-capable server passes to [`CreateSession`] per
/// connection. Beyond the approval/elicit handlers (forward both to the
/// session), it carries the connect-time intent parsed from the URL:
///   - `resume` — a persisted thread id to RESUME (continue an existing conversation).
///   - `fork`   — a persisted thread id to FORK (a NEW session seeded from that thread's history).
///
/// A plain fresh session results when neither is set.
pub struct SessionContext {
    pub durable_permissions: Arc<klaus_agent_core::permission::durable::DurablePermissionLayer>,
    pub approval_handler: Arc<dyn ApprovalHandler>,
    pub elicit_handler: Arc<dyn ElicitHandler>,
    pub mcp_elicitation_presenter: Arc<dyn McpElicitationPresenter>,
    pub mcp_authorize_handler: Arc<dyn McpAuthorizeHandler>,
    pub credential_store: Option<Arc<dyn CredentialStore>>,
    pub init: ProtocolSessionInit,
    pub provider: Option<Arc<dyn ModelProvider>>,
    pub mcp_server_configs: Vec<McpServerConfig>,
    pub capability_roots: Vec<RuntimeCapabilityRoot>,
    pub selected_capability_roots: Option<Vec<SelectedRuntimeCapabilityRoot>>,
    pub dynamic_tools: Vec<Arc<dyn Tool>>,
    pub agents: Vec<AgentDefinition>,
    pub skills: Vec<Skill>,
    pub output_styles: Vec<OutputStyle>,
    pub hooks: Vec<Hook>,
    pub pricing: Option<PricingTable>,
    pub resume: Option<String>,
    pub fork: Option<String>,
    /// Engine-generated snapshot for the named live-fork facade. Never decoded
    /// from host-supplied history and never sent through an external transport.
    pub fork_live: Option<Arc<LiveForkSnapshot>>,
    /// The named side-task facade captures live context before child startup;
    /// a cold parent is loaded from its persisted record by the factory.
    pub side_task: Option<SideTaskStart>,
}

/// Mints a [`Session`] per connection. May be async (e.g. resume reads a store)
/// and may fail (surfaced to the client as a correlated `error` event).
pub type CreateSession = Arc<
    dyn Fn(SessionContext) -> Pin<Box<dyn Future<Output = Result<Arc<Session>, BoxError>> + Send>>
        + Send
        + Sync,
>;

/// Enumerate persisted threads for `GET /threads`.
pub type ListThreads = Arc<
    dyn Fn(ListOptions) -> Pin<Box<dyn Future<Output = Result<ThreadPage, BoxError>> + Send>>
        + Send
        + Sync,
>;

/// Connect-time `?resume=<id>` / `?fork=<id>` intent (independent of each other).
#[derive(Debug, Clone, Default)]
pub struct Lifecycle {
    pub resume: Option<String>,
    pub fork: Option<String>,
}

/// Read connect-time `?resume=<id>` / `?fork=<id>` from a request target.
pub fn parse_lifecycle(url: Option<&str>) -> Lifecycle {
    let qs = match url.and_then(|u| u.split_once('?')) {
        Some((_, q)) => q,
        None => return Lifecycle::default(),
    };
    let get = |key: &str| -> Option<String> {
        url::form_urlencoded::parse(qs.as_bytes())
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.into_owned())
            .filter(|s| !s.is_empty())
    };
    Lifecycle {
        resume: get("resume"),
        fork: get("fork"),
    }
}

/// The `{ create_session, list_threads }` a lifecycle server needs.
pub struct EngineSessions {
    pub create_session: CreateSession,
    pub list_threads: Option<ListThreads>,
}

/// Wire an [`Agent`] (+ optional store) into the factory a lifecycle server
/// needs. Fresh sessions come from `start_session`/`open_session`;
/// `?resume=<id>` continues a persisted thread via `resume_session`;
/// `?fork=<id>` seeds a NEW persisted session from that thread's history;
/// `list_threads` from `store.list`. resume/fork/list all require a `store`.
pub fn engine_sessions(agent: Arc<Agent>, store: Option<Arc<dyn SessionStore>>) -> EngineSessions {
    engine_sessions_with_config_root(agent, store, None)
}

#[cfg(test)]
pub(crate) fn engine_sessions_in_test_root(
    agent: Arc<Agent>,
    store: Option<Arc<dyn SessionStore>>,
    user_config_dir: RuntimePath,
) -> EngineSessions {
    engine_sessions_with_config_root(agent, store, Some(user_config_dir))
}

fn engine_sessions_with_config_root(
    agent: Arc<Agent>,
    store: Option<Arc<dyn SessionStore>>,
    user_config_dir: Option<RuntimePath>,
) -> EngineSessions {
    let create_session: CreateSession = {
        let agent = agent.clone();
        let store = store.clone();
        Arc::new(move |ctx: SessionContext| {
            let agent = agent.clone();
            let store = store.clone();
            let user_config_dir = user_config_dir.clone();
            Box::pin(async move {
                let SessionContext {
                    durable_permissions,
                    approval_handler,
                    elicit_handler,
                    mcp_elicitation_presenter,
                    mcp_authorize_handler,
                    credential_store,
                    init,
                    provider,
                    mcp_server_configs,
                    capability_roots,
                    selected_capability_roots,
                    dynamic_tools,
                    agents,
                    skills,
                    output_styles,
                    hooks,
                    pricing,
                    resume,
                    fork,
                    fork_live,
                    side_task,
                } = ctx;

                let session_options = || -> Result<StartSessionOptions, BoxError> {
                    let lsp_servers = init.lsp_servers.clone();
                    let skills = skills.clone();
                    let output_styles = output_styles.clone();
                    Ok(StartSessionOptions {
                        user_config_dir: user_config_dir.clone(),
                        durable_permissions: Some(durable_permissions.clone()),
                        // A host-supplied thread id becomes the persistent thread
                        // identity; absent, the engine mints one.
                        id: optional_runtime_string(init.thread_id.as_ref()),
                        approval_handler: Some(approval_handler.clone()),
                        elicit_handler: Some(elicit_handler.clone()),
                        mcp_elicitation_presenter: Some(mcp_elicitation_presenter.clone()),
                        mcp_authorize_handler: Some(mcp_authorize_handler.clone()),
                        mcp_oauth_redirect_uri: optional_scalar_string(
                            init.mcp_oauth_redirect_uri.as_ref(),
                            "MCP OAuth redirect URI",
                        )?,
                        credential_store: credential_store.clone(),
                        cwd: optional_runtime_path(init.cwd.as_ref()),
                        config: optional_runtime_json(init.config.as_ref(), "session config")?,
                        trusted_windows_sandbox_context: trusted_windows_sandbox_context(
                            init.trusted_windows_sandbox_context.as_ref(),
                        )?,
                        network_allowed: init.network_allowed,
                        permission_mode: init.permission_mode,
                        pre_plan_permission_mode: init.pre_plan_permission_mode,
                        provider: provider.clone(),
                        mcp_server_configs: Some(mcp_server_configs.clone()),
                        capability_roots: Some(capability_roots.clone()),
                        selected_capability_roots: selected_capability_roots.clone(),
                        base_instructions: optional_runtime_string(init.base_instructions.as_ref()),
                        developer_instructions: optional_runtime_string(
                            init.developer_instructions.as_ref(),
                        ),
                        agent_memory_snapshot_policy: init.agent_memory_snapshot_policy.clone(),
                        mcp_tools: (!dynamic_tools.is_empty()).then_some(dynamic_tools.clone()),
                        lsp_servers: (!lsp_servers.is_empty())
                            .then_some(Arc::new(move || lsp_servers.clone())),
                        agents: (!agents.is_empty()).then_some(agents.clone()),
                        agent_definition_name: optional_runtime_string(
                            init.agent_definition_name.as_ref(),
                        ),
                        skills: (!skills.is_empty()).then_some(Arc::new(move || skills.clone())),
                        output_styles: (!output_styles.is_empty())
                            .then_some(Arc::new(move || output_styles.clone())),
                        selected_output_style: optional_runtime_string(
                            init.selected_output_style.as_ref(),
                        ),
                        hooks: (!hooks.is_empty()).then_some(hooks.clone()),
                        pricing: pricing.clone(),
                        shell_env: optional_runtime_env(init.shell_env.as_ref()),
                        web_search_factory: subscription_web_search_factory(),
                        web_fetch_factory: Some(
                            crate::app_server::native_web_fetch::source_page_fetcher_factory(),
                        ),
                        ..Default::default()
                    })
                };

                if let Some(side) = side_task {
                    let sopts = session_options()?;
                    return match side.live_snapshot {
                        Some(snapshot) => {
                            agent
                                .create_side_task_from_live(&snapshot, sopts, store.clone())
                                .await
                        }
                        None => {
                            let store =
                                store.ok_or_else(|| boxed("side task requires a session store"))?;
                            agent
                                .create_side_task(store, &side.parent_thread_id, sopts)
                                .await
                        }
                    }
                    .map_err(|error| Box::new(error) as BoxError);
                }

                if let Some(snapshot) = fork_live {
                    let store = store.ok_or_else(|| boxed("live fork requires a session store"))?;
                    return agent
                        .fork_live_session(store, &snapshot, session_options()?)
                        .await
                        .map_err(|error| Box::new(error) as BoxError);
                }

                if let Some(resume_id) = resume {
                    let store = store.ok_or_else(|| {
                        boxed("resume requires a store — pass one to engineSessions(agent, store)")
                    })?;
                    let sopts = session_options()?;
                    let s = agent
                        .resume_session(store, &resume_id, sopts)
                        .await
                        .map_err(|e| Box::new(e) as BoxError)?;
                    return Ok(s);
                }

                if let Some(fork_id) = fork {
                    let store = store.ok_or_else(|| {
                        boxed("fork requires a store — pass one to engineSessions(agent, store)")
                    })?;
                    let sopts = session_options()?;
                    let s = agent
                        .fork_session(store, &fork_id, sopts)
                        .await
                        .map_err(|e| Box::new(e) as BoxError)?;
                    return Ok(s);
                }

                let sopts = session_options()?;
                match store {
                    Some(store) => agent
                        .open_session(store, sopts)
                        .await
                        .map_err(|e| Box::new(e) as BoxError),
                    None => agent
                        .try_start_session_async(sopts)
                        .await
                        .map_err(|e| Box::new(e) as BoxError),
                }
            }) as Pin<Box<dyn Future<Output = Result<Arc<Session>, BoxError>> + Send>>
        })
    };

    let list_threads: Option<ListThreads> = store.map(|store| -> ListThreads {
        Arc::new(move |opts: ListOptions| {
            let store = store.clone();
            Box::pin(async move { store.list(opts).await.map_err(|e| Box::new(e) as BoxError) })
                as Pin<Box<dyn Future<Output = Result<ThreadPage, BoxError>> + Send>>
        })
    });

    EngineSessions {
        create_session,
        list_threads,
    }
}

/// The `create_session` factory for the newline-delimited Op/Event protocol:
/// mint the session from what the host declared on `init_session`, persisting it
/// when a store is present.
///
/// This is the [`engine_sessions`] wiring expressed against the protocol's own
/// `CreateSessionArgs` instead of a connect-time [`SessionContext`], so every
/// endpoint that speaks the protocol — stdio, the CLI's in-process transport —
/// mints sessions the same way rather than each keeping its own copy.
pub fn protocol_create_session(
    agent: Arc<Agent>,
    store: Option<Arc<dyn SessionStore>>,
) -> ProtocolCreateSession {
    Arc::new(move |handlers: ProtocolSessionHandlers| {
        let agent = agent.clone();
        let store = store.clone();
        Box::pin(async move {
            // A host-supplied thread id becomes the persistent thread identity;
            // absent, the engine mints one.
            let thread_id = handlers.init.thread_id.clone();
            let mut opts = session_options_from_handlers(handlers)?;
            opts.id = thread_id;
            match store {
                Some(store) => agent
                    .open_session(store, opts)
                    .await
                    .map_err(|e| Box::new(e) as BoxError),
                None => agent
                    .try_start_session_async(opts)
                    .await
                    .map_err(|e| Box::new(e) as BoxError),
            }
        }) as ProtocolCreateSessionFuture
    })
}

/// Adapt the store's maintenance operations without opening a model session.
pub fn rollout_maintenance(
    store: Arc<dyn SessionStore>,
) -> Arc<klaus_agent_protocol::server::RolloutMaintenance> {
    let compression_store = store.clone();
    Arc::new(klaus_agent_protocol::server::RolloutMaintenance {
        compress: Some(Arc::new(move |cancelled| {
            let store = compression_store.clone();
            Box::pin(async move {
                let report = store
                    .compress_cold_rollouts_with_cancellation(cancelled)
                    .await?;
                Ok::<_, BoxError>(
                    klaus_agent_core::runtime_values::runtime_codec::encode_value(&report)?,
                )
            })
        })),
        migrate: Some(Arc::new(move |request, cancelled| {
            let store = store.clone();
            Box::pin(async move {
                let report = store.migrate_rollouts(request, cancelled).await?;
                Ok::<_, BoxError>(
                    klaus_agent_core::runtime_values::runtime_codec::encode_value(&report)?,
                )
            })
        })),
        ..Default::default()
    })
}

/// Persisted-thread lifecycle handlers shared by the facade and endpoint transports.
pub fn protocol_lifecycle(
    agent: Arc<Agent>,
    store: Option<Arc<dyn SessionStore>>,
) -> ProtocolLifecycle {
    protocol_lifecycle_with_config_root(agent, store, None)
}

#[cfg(test)]
pub(crate) fn protocol_lifecycle_in_test_root(
    agent: Arc<Agent>,
    store: Option<Arc<dyn SessionStore>>,
    user_config_dir: RuntimePath,
) -> ProtocolLifecycle {
    protocol_lifecycle_with_config_root(agent, store, Some(user_config_dir))
}

fn protocol_lifecycle_with_config_root(
    agent: Arc<Agent>,
    store: Option<Arc<dyn SessionStore>>,
    user_config_dir: Option<RuntimePath>,
) -> ProtocolLifecycle {
    let skill_agent = agent.clone();
    let set_skills: klaus_agent_protocol::server::SetSkills =
        Arc::new(move |session, skills| skill_agent.replace_session_skills(&session, skills));
    let Some(store) = store else {
        return ProtocolLifecycle {
            set_skills: Some(set_skills),
            ..Default::default()
        };
    };
    let rollout_maintenance = rollout_maintenance(store.clone());
    let classifier_agent = agent.clone();
    let classifier_config_root = user_config_dir.clone();
    let classify_goal_pursuit: klaus_agent_protocol::server::ClassifyGoalPursuit =
        Arc::new(move |init| {
            let options = StartSessionOptions {
                user_config_dir: classifier_config_root.clone(),
                id: init.thread_id,
                cwd: optional_runtime_path(init.cwd.as_ref()),
                config: optional_runtime_json(init.config.as_ref(), "session config")
                    .map_err(|error| error.to_string())?,
                network_allowed: init.network_allowed,
                permission_mode: init.permission_mode,
                pre_plan_permission_mode: init.pre_plan_permission_mode,
                agents: (!init.agents.is_empty()).then_some(init.agents),
                agent_definition_name: init.agent_definition_name,
                ..Default::default()
            };
            classifier_agent
                .goal_pursuit_eligible_options(&options)
                .map_err(|error| error.to_string())
        });
    let goal_store = store.clone();
    let read_store = store.clone();
    let page_store = read_store.clone();
    let summary_store = store.clone();
    let profile_store = store.clone();
    let read_subagent_store = store.clone();
    let import_store = store.clone();
    let list_store = store.clone();
    let resume_agent = agent.clone();
    let resume_store = store.clone();
    let fork_agent = agent.clone();
    let fork_store = store.clone();
    let side_agent = agent.clone();
    let side_store = store.clone();
    let resume_config_root = user_config_dir.clone();
    let fork_config_root = user_config_dir.clone();
    let side_config_root = user_config_dir;
    let archive_store = store.clone();
    let unarchive_store = store.clone();
    let delete_store = store.clone();
    let title_store = store.clone();
    let rename_store = store.clone();
    let host_metadata_store = store.clone();
    let transcript_store = store;

    ProtocolLifecycle {
        rollout_maintenance: Some(rollout_maintenance),
        set_skills: Some(set_skills),
        classify_goal_pursuit: Some(classify_goal_pursuit),
        saved_goal_request: Some(Arc::new(move |thread_id, request| {
            let store = goal_store.clone();
            Box::pin(async move {
                klaus_agent_core::goal::service::saved_goal_request(
                    store,
                    thread_id,
                    request,
                    Arc::new(|| uuid::Uuid::new_v4().to_string()),
                    Arc::new(|| {
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis()
                            .min(i64::MAX as u128) as i64
                    }),
                )
                .await
                .map_err(|error| Box::new(error) as BoxError)
            })
        })),
        read_profile_activity: Some(Arc::new(move || {
            let store = profile_store.clone();
            Box::pin(async move {
                store
                    .profile_activity(chrono::Utc::now().timestamp_millis())
                    .await
                    .map_err(|error| Box::new(error) as BoxError)
            })
        })),
        read_session_summary: Some(Arc::new(move |thread_id: String| {
            let store = summary_store.clone();
            Box::pin(async move {
                store
                    .read_session_summary(&thread_id)
                    .await
                    .map_err(|error| Box::new(error) as BoxError)
            })
        })),
        import_external_session: Some(Arc::new(move |import| {
            let store = import_store.clone();
            Box::pin(async move {
                store
                    .import_external_session(import)
                    .await
                    .map_err(|e| Box::new(e) as BoxError)
            })
        })),
        read_thread_file_changes: Some(Arc::new({
            let store = read_store.clone();
            move |thread_id| {
                let store = store.clone();
                Box::pin(async move {
                    store
                        .saved_file_changes(&thread_id)
                        .await
                        .map_err(|error| Box::new(error) as BoxError)
                })
            }
        })),
        read_thread: Some(Arc::new(move |thread_id: String| {
            let store = read_store.clone();
            Box::pin(async move {
                store
                    .read_thread(&thread_id)
                    .await
                    .map_err(|e| Box::new(e) as BoxError)
            })
        })),
        read_thread_page: Some(Arc::new(
            move |thread_id: String, request: klaus_agent_protocol::server::ThreadPageRequest| {
                let store = page_store.clone();
                Box::pin(async move {
                    let snapshot = match request.turn_id {
                        Some(turn_id) if request.before_item_id.is_some() => {
                            store
                                .paged_turn_slice_before(
                                    &thread_id,
                                    &turn_id,
                                    request
                                        .before_item_id
                                        .as_deref()
                                        .expect("guarded item anchor"),
                                    request.limit,
                                )
                                .await
                        }
                        Some(turn_id) => {
                            store
                                .paged_turn_slice(
                                    &thread_id,
                                    &turn_id,
                                    request.cursor.as_deref(),
                                    request.limit,
                                    request.sort_direction.as_deref(),
                                )
                                .await
                        }
                        None => {
                            store
                                .paged_thread_history(
                                    &thread_id,
                                    request.cursor.as_deref(),
                                    request.limit,
                                    request.items_view.as_deref(),
                                    request.sort_direction.as_deref(),
                                )
                                .await
                        }
                    };
                    snapshot.map_err(|e| Box::new(e) as BoxError)
                })
            },
        )),
        read_subagent_history: Some(Arc::new(move |thread_id: String, agent_id: String| {
            let store = read_subagent_store.clone();
            Box::pin(async move {
                store
                    .read_sub_agent(&thread_id, &agent_id)
                    .await
                    .map_err(|error| Box::new(error) as BoxError)
            })
        })),
        list_sessions: Some(Arc::new(move |opts: ListOptions| {
            let store = list_store.clone();
            Box::pin(async move { store.list(opts).await.map_err(|e| Box::new(e) as BoxError) })
        })),
        resume_session: Some(Arc::new(
            move |handlers: ProtocolSessionHandlers,
                  thread_id: String,
                  _new_thread_id: Option<String>,
                  _fork_cut: Option<klaus_agent_core::engine::agent::ForkCut>| {
                let agent = resume_agent.clone();
                let store = resume_store.clone();
                let user_config_dir = resume_config_root.clone();
                Box::pin(async move {
                    let mut sopts = session_options_from_handlers(handlers)?;
                    sopts.user_config_dir = user_config_dir;
                    agent
                        .resume_session(store, &thread_id, sopts)
                        .await
                        .map_err(|e| Box::new(e) as BoxError)
                }) as ProtocolCreateSessionFuture
            },
        )),
        fork_session: Some(Arc::new(
            move |handlers: ProtocolSessionHandlers,
                  thread_id: String,
                  new_thread_id: Option<String>,
                  fork_cut: Option<klaus_agent_core::engine::agent::ForkCut>| {
                let agent = fork_agent.clone();
                let store = fork_store.clone();
                let user_config_dir = fork_config_root.clone();
                Box::pin(async move {
                    let mut sopts = session_options_from_handlers(handlers)?;
                    sopts.user_config_dir = user_config_dir;
                    // The host may pin the forked thread's identity so its own
                    // session key stays the single id end to end.
                    sopts.id = new_thread_id.map(Into::into);
                    agent
                        .fork_session_with_cut(store, &thread_id, fork_cut, sopts)
                        .await
                        .map_err(|e| Box::new(e) as BoxError)
                }) as ProtocolCreateSessionFuture
            },
        )),
        create_side_task: Some(Arc::new(
            move |handlers: ProtocolSessionHandlers,
                  thread_id: String,
                  new_thread_id: Option<String>,
                  _fork_cut: Option<klaus_agent_core::engine::agent::ForkCut>| {
                let agent = side_agent.clone();
                let store = side_store.clone();
                let user_config_dir = side_config_root.clone();
                Box::pin(async move {
                    let mut sopts = session_options_from_handlers(handlers)?;
                    sopts.user_config_dir = user_config_dir;
                    // Same single-id contract as fork: the side-task thread
                    // adopts the host-supplied id when one is given.
                    sopts.id = new_thread_id.map(Into::into);
                    agent
                        .create_side_task(store, &thread_id, sopts)
                        .await
                        .map_err(|error| Box::new(error) as BoxError)
                }) as ProtocolCreateSessionFuture
            },
        )),
        archive_session: Some(Arc::new(move |thread_id: String| {
            let store = archive_store.clone();
            Box::pin(async move {
                store
                    .archive(&thread_id)
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?;
                Ok(ArchiveSessionResult {
                    thread_id: thread_id.into(),
                    archived: true,
                })
            })
        })),
        unarchive_session: Some(Arc::new(move |thread_id: String| {
            let store = unarchive_store.clone();
            Box::pin(async move { store.unarchive(&thread_id).await })
        })),
        delete_session: Some(Arc::new(move |thread_id: String| {
            let store = delete_store.clone();
            Box::pin(async move {
                store
                    .delete(&thread_id)
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?;
                Ok(DeleteSessionResult {
                    thread_id: thread_id.into(),
                    deleted: true,
                })
            })
        })),
        update_title: Some(Arc::new(move |thread_id: String, title: String| {
            let store = title_store.clone();
            Box::pin(async move {
                store
                    .update_title(&thread_id, &title)
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?;
                Ok(UpdateTitleResult {
                    thread_id: thread_id.into(),
                    title: title.into(),
                })
            })
        })),
        rename_thread: Some(Arc::new(move |thread_id: String, new_thread_id: String| {
            let store = rename_store.clone();
            Box::pin(async move {
                store
                    .rename(&thread_id, &new_thread_id)
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?;
                Ok(RenameThreadResult {
                    thread_id: new_thread_id.into(),
                    renamed: true,
                })
            })
        })),
        update_thread_host_metadata: Some(Arc::new(move |thread_id: String, host_metadata| {
            let store = host_metadata_store.clone();
            Box::pin(async move {
                store
                    .update_host_metadata(&thread_id, host_metadata)
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?;
                Ok(UpdateThreadHostMetadataResult {
                    thread_id: thread_id.into(),
                    updated: true,
                })
            })
        })),
        load_subagent_transcript: Some(Arc::new(move |thread_id: String, agent_id: String| {
            let store = transcript_store.clone();
            Box::pin(async move {
                store
                    .load_sub_agent(&thread_id, &agent_id)
                    .await
                    .map_err(|e| Box::new(e) as BoxError)
            })
        })),
    }
}

fn session_options_from_handlers(
    handlers: ProtocolSessionHandlers,
) -> Result<StartSessionOptions, BoxError> {
    let lsp_servers = handlers.init.lsp_servers.clone();
    let skills = handlers.skills.clone();
    let output_styles = handlers.output_styles.clone();
    Ok(StartSessionOptions {
        durable_permissions: Some(handlers.durable_permissions),
        approval_handler: Some(handlers.approval_handler),
        elicit_handler: Some(handlers.elicit_handler),
        mcp_elicitation_presenter: Some(handlers.mcp_elicitation_presenter),
        mcp_authorize_handler: Some(handlers.mcp_authorize_handler),
        mcp_oauth_redirect_uri: optional_scalar_string(
            handlers.init.mcp_oauth_redirect_uri.as_ref(),
            "MCP OAuth redirect URI",
        )?,
        cwd: optional_runtime_path(handlers.init.cwd.as_ref()),
        config: optional_runtime_json(handlers.init.config.as_ref(), "session config")?,
        trusted_windows_sandbox_context: trusted_windows_sandbox_context(
            handlers.init.trusted_windows_sandbox_context.as_ref(),
        )?,
        network_allowed: handlers.init.network_allowed,
        permission_mode: handlers.init.permission_mode,
        provider: handlers.provider,
        mcp_server_configs: Some(handlers.mcp_server_configs),
        capability_roots: Some(handlers.capability_roots),
        selected_capability_roots: handlers.selected_capability_roots,
        base_instructions: optional_runtime_string(handlers.init.base_instructions.as_ref()),
        developer_instructions: optional_runtime_string(
            handlers.init.developer_instructions.as_ref(),
        ),
        agent_memory_snapshot_policy: handlers.init.agent_memory_snapshot_policy,
        mcp_tools: (!handlers.dynamic_tools.is_empty()).then_some(handlers.dynamic_tools),
        lsp_servers: (!lsp_servers.is_empty()).then_some(Arc::new(move || lsp_servers.clone())),
        agents: (!handlers.agents.is_empty()).then_some(handlers.agents),
        agent_definition_name: optional_runtime_string(
            handlers.init.agent_definition_name.as_ref(),
        ),
        skills: (!skills.is_empty()).then_some(Arc::new(move || skills.clone())),
        output_styles: (!output_styles.is_empty())
            .then_some(Arc::new(move || output_styles.clone())),
        selected_output_style: optional_runtime_string(
            handlers.init.selected_output_style.as_ref(),
        ),
        hooks: (!handlers.hooks.is_empty()).then_some(handlers.hooks),
        pricing: handlers.pricing,
        shell_env: optional_runtime_env(handlers.init.shell_env.as_ref()),
        web_search_factory: subscription_web_search_factory(),
        web_fetch_factory: Some(crate::app_server::native_web_fetch::source_page_fetcher_factory()),
        ..Default::default()
    })
}

/// The config-driven search backend picker: a session whose resolved config
/// names a model subscription (`codex`, `qoder`) searches through
/// that subscription's own service. Absent credentials fail the search loudly
/// instead of silently falling back to the browser backend.
fn subscription_web_search_factory(
) -> Option<klaus_agent_core::tools::builtins::web_search::WebSearchBackendFactory> {
    Some(crate::app_server::native_web_search::source_backend_factory())
}

fn boxed(msg: &str) -> BoxError {
    msg.to_string().into()
}

fn optional_scalar_string(
    value: Option<&RuntimeString>,
    name: &str,
) -> Result<Option<String>, BoxError> {
    value
        .map(|value| {
            value
                .try_to_utf8()
                .map_err(|error| format!("invalid {name}: {error}").into())
        })
        .transpose()
}

fn optional_runtime_string(value: Option<&RuntimeString>) -> Option<RuntimeString> {
    value.cloned()
}

fn optional_runtime_path(value: Option<&RuntimeString>) -> Option<RuntimePath> {
    value.cloned().map(RuntimePath::from_runtime_string)
}

fn optional_runtime_env(value: Option<&RuntimeEnv>) -> Option<RuntimeEnv> {
    value.cloned()
}

fn trusted_windows_sandbox_context(
    value: Option<&ProtocolTrustedWindowsSandboxContext>,
) -> Result<Option<TrustedWindowsSandboxContext>, BoxError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let parse_addr = |name: &str, value: &RuntimeString| -> Result<SocketAddr, BoxError> {
        let value = value
            .try_to_utf8()
            .map_err(|error| format!("invalid trusted Windows {name}: {error}"))?;
        value
            .parse::<SocketAddr>()
            .map_err(|error| format!("invalid trusted Windows {name}: {error}").into())
    };
    Ok(Some(TrustedWindowsSandboxContext {
        route_sid: value.route_sid.clone(),
        http_addr: parse_addr("HTTP broker address", &value.http_addr)?,
        socks_addr: value
            .socks_addr
            .as_ref()
            .map(|addr| parse_addr("SOCKS broker address", addr))
            .transpose()?,
        broker_generation: value.broker_generation.clone(),
        policy_digest: value.policy_digest.clone(),
    }))
}

fn optional_runtime_json(
    value: Option<&RuntimeJson>,
    name: &str,
) -> Result<Option<serde_json::Value>, BoxError> {
    value
        .map(|value| {
            let scalar = try_to_scalar_json(value)
                .map_err(|error| -> BoxError { format!("invalid {name}: {error}").into() })?;
            let bytes = runtime_codec::encode_bytes(&scalar).map_err(|error| -> BoxError {
                format!("failed to encode {name}: {error}").into()
            })?;
            serde_json::from_slice(&bytes).map_err(|error| -> BoxError {
                format!("failed to project {name}: {error}").into()
            })
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_resume_and_fork() {
        let l = parse_lifecycle(Some("/sessions/x/events?resume=abc&fork=def"));
        assert_eq!(l.resume.as_deref(), Some("abc"));
        assert_eq!(l.fork.as_deref(), Some("def"));

        let l2 = parse_lifecycle(Some("/x"));
        assert!(l2.resume.is_none() && l2.fork.is_none());

        // An empty value is treated as absent.
        let l3 = parse_lifecycle(Some("/x?resume="));
        assert!(l3.resume.is_none());

        let l4 = parse_lifecycle(None);
        assert!(l4.resume.is_none() && l4.fork.is_none());
    }
}
