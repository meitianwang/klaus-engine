//! Defect: an existing session retained the skill catalog captured at startup.
//! Root cause: the host skill snapshot had no publication path into loaded sessions.
//! Work item: 2026-10-15 live skill catalog refresh.

use std::sync::{Arc, Mutex};

use futures::StreamExt;

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, Session, TurnOptions};
use klaus_agent_core::extensions::skills::loader::Skill;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::message::Usage;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

struct RecordingProvider {
    info: ProviderInfo,
    requests: Mutex<Vec<ModelRequest>>,
}

impl ModelProvider for RecordingProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 8_192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }
    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        self.requests.lock().unwrap().push(request);
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::MessageStart {
                model: "live-skills-test".into(),
            },
            ModelStreamEvent::TextDelta { text: "ok".into() },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 10,
                    output_tokens: 2,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: Some(12),
                },
            },
        ]))
    }
}

fn skill(body: &str) -> Skill {
    Skill {
        name: "draft".into(),
        description: "Prepare a draft".into(),
        body: body.into(),
        path: "/skills/draft/SKILL.md".into(),
        ..Default::default()
    }
}

async fn run(session: &Arc<Session>, input: &str) {
    let result = session
        .run(Input::Text(input.into()), TurnOptions::default())
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
}

#[tokio::test]
async fn loaded_session_adds_edits_and_removes_host_skills_without_restart() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let provider = Arc::new(RecordingProvider {
        info: ProviderInfo {
            name: "live-skills-test".into(),
            base_url: None,
            env_key: None,
            default_model: "live-skills-test".into(),
        },
        requests: Mutex::new(Vec::new()),
    });
    let mut options = agent_options(process);
    options.provider = provider.clone();
    options.tools = None;
    options.include_default_agents = Some(false);
    options.config.as_mut().unwrap()["model"] = json!("live-skills-test");
    options.config.as_mut().unwrap()["agentMemory"] = json!(false);
    options.config.as_mut().unwrap()["autoMemory"] = json!(false);
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions::default());
    let id = session.id.clone();
    run(&session, "before discovery").await;
    assert!(session.visible_skill_names().is_empty());

    agent
        .replace_session_skills(&session, vec![skill("FIRST_SKILL_BODY")])
        .unwrap();
    assert_eq!(
        session.visible_skill_names(),
        vec![RuntimeString::from("draft")]
    );
    assert_eq!(session.extension_runtime_snapshot().skills.len(), 1);
    run(&session, "/draft").await;
    let first = format!("{:?}", provider.requests.lock().unwrap().last().unwrap());
    assert!(first.contains("FIRST_SKILL_BODY"), "{first}");

    agent
        .replace_session_skills(&session, vec![skill("UPDATED_SKILL_BODY")])
        .unwrap();
    run(&session, "/draft").await;
    let edited = format!("{:?}", provider.requests.lock().unwrap().last().unwrap());
    assert!(edited.contains("UPDATED_SKILL_BODY"), "{edited}");
    assert!(
        edited.contains("replaces all previous skill listings"),
        "{edited}"
    );

    agent.replace_session_skills(&session, Vec::new()).unwrap();
    assert!(session.visible_skill_names().is_empty());
    assert!(session.extension_runtime_snapshot().skills.is_empty());
    run(&session, "after removal").await;
    let removed = format!("{:?}", provider.requests.lock().unwrap().last().unwrap());
    assert!(
        removed.contains("No skills are currently available"),
        "{removed}"
    );
    assert_eq!(session.id, id);
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn admitted_turn_retains_its_skill_generation_until_the_next_turn() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut options = agent_options(process);
    options.include_default_agents = Some(false);
    let agent = Agent::new(options);
    let initial = vec![skill("OLD_BODY")];
    let session = agent.start_session(StartSessionOptions {
        skills: Some(Arc::new(move || initial.clone())),
        ..Default::default()
    });
    let admitted = session.run_streamed(Input::Text("held turn".into()), TurnOptions::default());
    agent.replace_session_skills(&session, Vec::new()).unwrap();
    assert_eq!(
        session.visible_skill_names(),
        vec![RuntimeString::from("draft")]
    );
    assert_eq!(session.extension_runtime_snapshot().skills.len(), 1);
    drop(admitted);
    let _ = session
        .run(Input::Text("next turn".into()), TurnOptions::default())
        .await;
    assert!(session.visible_skill_names().is_empty());
    assert!(session.extension_runtime_snapshot().skills.is_empty());
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn replacement_preserves_endpoint_plugin_and_builtin_contributions() {
    use klaus_agent_core::extensions::plugins::loader::LoadedPlugin;
    use klaus_agent_core::extensions::runtime_snapshot::ExtensionSourceKind;
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut options = agent_options(process);
    let endpoint = Skill {
        name: "endpoint".into(),
        ..skill("ENDPOINT_BODY")
    };
    options.skills = Some(Arc::new(move || vec![endpoint.clone()]));
    options.plugins.push(LoadedPlugin {
        name: "skill-package".to_string(),
        version: Some("1.0.0".to_string()),
        display_name: None,
        description: None,
        plugin_namespace: "skill-package".to_string(),
        keywords: Vec::new(),
        dependencies: Vec::new(),
        user_config: Default::default(),
        settings: Default::default(),
        dir: "/plugins/skill-package".to_string(),
        skills: vec![Skill {
            name: "package-skill".into(),
            ..skill("PLUGIN_BODY")
        }],
        commands: Vec::new(),
        agents: Vec::new(),
        hooks: Vec::new(),
        output_styles: Vec::new(),
        mcp_servers: Vec::new(),
        lsp_servers: Default::default(),
        runtime_capabilities: Vec::new(),
        interface_capabilities: Vec::new(),
        app_connectors: Vec::new(),
    });
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions::default());
    let baseline = session.visible_skill_names();
    assert!(baseline.iter().any(|name| name == "package-skill"));
    assert!(baseline.iter().any(|name| name == "endpoint"));
    assert!(session
        .extension_runtime_snapshot()
        .skills
        .iter()
        .any(|row| row.source.kind == ExtensionSourceKind::Builtin));
    agent
        .replace_session_skills(&session, vec![skill("HOST_BODY")])
        .unwrap();
    for name in &baseline {
        assert!(session.visible_skill_names().contains(name));
    }
    assert!(session
        .visible_skill_names()
        .iter()
        .any(|name| name == "draft"));
    agent.replace_session_skills(&session, Vec::new()).unwrap();
    assert_eq!(session.visible_skill_names(), baseline);
    assert!(session
        .extension_runtime_snapshot()
        .skills
        .iter()
        .any(|row| row.name == "package-skill" && row.source.kind == ExtensionSourceKind::Plugin));
    session.shutdown_and_wait().await.unwrap();
}

struct UpdatingProvider {
    inner: RecordingProvider,
    binding: Mutex<Option<(std::sync::Weak<Agent>, std::sync::Weak<Session>)>>,
    changed: std::sync::atomic::AtomicBool,
}

impl ModelProvider for UpdatingProvider {
    fn info(&self) -> &ProviderInfo {
        self.inner.info()
    }
    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        self.inner.capabilities(model)
    }
    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        if !self.changed.swap(true, std::sync::atomic::Ordering::SeqCst) {
            self.inner.requests.lock().unwrap().push(request);
            let binding = self.binding.lock().unwrap();
            let (agent, session) = binding.as_ref().unwrap();
            agent
                .upgrade()
                .unwrap()
                .replace_session_skills(&session.upgrade().unwrap(), vec![skill("NEW_TOOL_BODY")])
                .unwrap();
            return Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::MessageStart {
                    model: "live-skills-test".into(),
                },
                ModelStreamEvent::ToolUseStart {
                    id: "load-skill".into(),
                    name: "skill".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "load-skill".into(),
                    partial_json: r#"{"name":"draft"}"#.into(),
                },
                ModelStreamEvent::ToolUseStop {
                    id: "load-skill".into(),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::ToolUse,
                    usage: Usage {
                        input_tokens: 10,
                        output_tokens: 2,
                        cache_read_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        total_tokens: Some(12),
                    },
                },
            ]));
        }
        self.inner.stream(request, signal)
    }
}

#[tokio::test]
async fn running_tool_uses_captured_body_and_the_next_turn_uses_the_replacement() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let provider = Arc::new(UpdatingProvider {
        inner: RecordingProvider {
            info: ProviderInfo {
                name: "live-skills-test".into(),
                base_url: None,
                env_key: None,
                default_model: "live-skills-test".into(),
            },
            requests: Mutex::new(Vec::new()),
        },
        binding: Mutex::new(None),
        changed: std::sync::atomic::AtomicBool::new(false),
    });
    let mut options = agent_options(process);
    options.provider = provider.clone();
    options.tools = None;
    options.include_default_agents = Some(false);
    options.config.as_mut().unwrap()["model"] = json!("live-skills-test");
    let agent = Arc::new(Agent::new(options));
    let initial = vec![skill("OLD_TOOL_BODY")];
    let session = agent.start_session(StartSessionOptions {
        skills: Some(Arc::new(move || initial.clone())),
        ..Default::default()
    });
    *provider.binding.lock().unwrap() = Some((Arc::downgrade(&agent), Arc::downgrade(&session)));
    run(&session, "Load the draft skill.").await;
    let captured = format!(
        "{:?}",
        provider.inner.requests.lock().unwrap().last().unwrap()
    );
    assert!(captured.contains("OLD_TOOL_BODY"), "{captured}");
    assert!(!captured.contains("NEW_TOOL_BODY"), "{captured}");
    run(&session, "/draft").await;
    let next = format!(
        "{:?}",
        provider.inner.requests.lock().unwrap().last().unwrap()
    );
    assert!(next.contains("NEW_TOOL_BODY"), "{next}");
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn a_new_skill_fork_uses_the_latest_published_body() {
    use klaus_agent_core::extensions::skills::loader::SkillContext;
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let provider = Arc::new(RecordingProvider {
        info: ProviderInfo {
            name: "live-skills-test".into(),
            base_url: None,
            env_key: None,
            default_model: "live-skills-test".into(),
        },
        requests: Mutex::new(Vec::new()),
    });
    let mut options = agent_options(process);
    options.provider = provider.clone();
    options.tools = None;
    options.config.as_mut().unwrap()["model"] = json!("live-skills-test");
    let agent = Agent::new(options);
    let initial = vec![skill("OLD_FORK_BODY")];
    let session = agent.start_session(StartSessionOptions {
        skills: Some(Arc::new(move || initial.clone())),
        ..Default::default()
    });
    agent
        .replace_session_skills(
            &session,
            vec![Skill {
                context: Some(SkillContext::Fork),
                ..skill("LATEST_FORK_BODY")
            }],
        )
        .unwrap();
    run(&session, "/draft").await;
    let requests = format!("{:?}", provider.requests.lock().unwrap());
    assert!(requests.contains("LATEST_FORK_BODY"), "{requests}");
    assert!(!requests.contains("OLD_FORK_BODY"), "{requests}");
    session.shutdown_and_wait().await.unwrap();
}

struct CatalogDroppingCompactor;

#[async_trait::async_trait]
impl klaus_agent_core::engine::deps::Compactor for CatalogDroppingCompactor {
    async fn maybe_compact(
        &self,
        messages: &[klaus_agent_core::schemas::message::Message],
        _: u64,
        _: &klaus_agent_core::schemas::config::ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> klaus_agent_core::engine::deps::CompactionResult {
        klaus_agent_core::engine::deps::CompactionResult {
            messages: messages.to_vec(),
            freed_tokens: 0,
            compacted: false,
            reason: None,
            diagnostics: Vec::new(),
        }
    }
    async fn reactive_compact(
        &self,
        _: &[klaus_agent_core::schemas::message::Message],
        _: &klaus_agent_core::schemas::config::ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> klaus_agent_core::engine::deps::CompactionResult {
        klaus_agent_core::engine::deps::CompactionResult {
            messages: vec![
                klaus_agent_core::schemas::message::Message::CompactBoundary(
                    klaus_agent_core::schemas::message::CompactBoundaryMessage {
                        native_compaction: None,
                        uuid: "catalog-compacted".into(),
                        summary: "Continue the existing task.".into(),
                    },
                ),
            ],
            freed_tokens: 100,
            compacted: true,
            reason: None,
            diagnostics: Vec::new(),
        }
    }
}

fn retained_catalog_count(messages: &[klaus_agent_core::schemas::message::Message]) -> usize {
    use klaus_agent_core::schemas::content::ContentBlock;
    use klaus_agent_core::schemas::message::Message;
    messages.iter().filter(|message| match message {
        Message::User(user) if user.is_meta == Some(true) => user.content.iter().any(|block| matches!(block, ContentBlock::Text(text) if text.text.starts_with("<skill-catalog:"))),
        _ => false,
    }).count()
}

#[tokio::test]
async fn catalog_context_is_retained_deduplicated_and_restored_after_compaction_and_resume() {
    use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
    let directory =
        std::env::temp_dir().join(format!("live-skill-catalog-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(
            directory
                .as_path()
                .join("sessions")
                .to_string_lossy()
                .into_owned(),
        ),
        archive_dir: None,
    }));
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let provider = Arc::new(RecordingProvider {
        info: ProviderInfo {
            name: "live-skills-test".into(),
            base_url: None,
            env_key: None,
            default_model: "live-skills-test".into(),
        },
        requests: Mutex::new(Vec::new()),
    });
    let mut options = agent_options(process);
    options.provider = provider.clone();
    options.tools = None;
    options.include_default_agents = Some(false);
    options.compactor = Some(Arc::new(CatalogDroppingCompactor));
    options.config.as_mut().unwrap()["model"] = json!("live-skills-test");
    let agent = Agent::new(options);
    let session = agent
        .open_session(
            store.clone(),
            StartSessionOptions {
                cwd: Some(directory.to_string_lossy().into_owned().into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    agent
        .replace_session_skills(&session, vec![skill("RETAINED_SKILL_BODY")])
        .unwrap();
    run(&session, "first turn").await;
    assert_eq!(retained_catalog_count(&session.history()), 1);
    run(&session, "stable second turn").await;
    assert_eq!(retained_catalog_count(&session.history()), 1);
    assert_eq!(
        retained_catalog_count(&provider.requests.lock().unwrap().last().unwrap().messages),
        1
    );
    let compacted = session.compact(None).await.unwrap();
    assert!(compacted.compacted);
    assert_eq!(retained_catalog_count(&session.history()), 1);
    run(&session, "after compaction").await;
    assert_eq!(retained_catalog_count(&session.history()), 1);
    let id = session.id.try_to_utf8().unwrap();
    session.shutdown_and_wait().await.unwrap();
    drop(session);
    let resumed = agent
        .resume_session(
            store,
            &id,
            StartSessionOptions {
                skills: Some(Arc::new(|| vec![skill("RETAINED_SKILL_BODY")])),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    run(&resumed, "after resume").await;
    assert_eq!(retained_catalog_count(&resumed.history()), 1);
    assert_eq!(
        retained_catalog_count(&provider.requests.lock().unwrap().last().unwrap().messages),
        1
    );
    resumed.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

struct OneSkillRoundProvider {
    inner: RecordingProvider,
    requested_skill: std::sync::atomic::AtomicBool,
}

impl ModelProvider for OneSkillRoundProvider {
    fn info(&self) -> &ProviderInfo {
        self.inner.info()
    }
    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        self.inner.capabilities(model)
    }
    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        if !self
            .requested_skill
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            self.inner.requests.lock().unwrap().push(request);
            return Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::MessageStart {
                    model: "live-skills-test".into(),
                },
                ModelStreamEvent::ToolUseStart {
                    id: "one-skill-round".into(),
                    name: "skill".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "one-skill-round".into(),
                    partial_json: r#"{"name":"draft"}"#.into(),
                },
                ModelStreamEvent::ToolUseStop {
                    id: "one-skill-round".into(),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::ToolUse,
                    usage: Usage {
                        input_tokens: 10,
                        output_tokens: 2,
                        cache_read_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        total_tokens: Some(12),
                    },
                },
            ]));
        }
        self.inner.stream(request, signal)
    }
}

#[tokio::test]
async fn meta_continuation_advances_catalog_pages_once_per_full_turn() {
    use klaus_agent_core::engine::message_queue::QueuePriority;
    use klaus_agent_core::engine::session::EnqueueOptions;
    use klaus_agent_core::schemas::message::Message;
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let provider = Arc::new(OneSkillRoundProvider {
        inner: RecordingProvider {
            info: ProviderInfo {
                name: "live-skills-test".into(),
                base_url: None,
                env_key: None,
                default_model: "live-skills-test".into(),
            },
            requests: Mutex::new(Vec::new()),
        },
        requested_skill: std::sync::atomic::AtomicBool::new(false),
    });
    let mut options = agent_options(process);
    options.provider = provider.clone();
    options.tools = None;
    options.include_default_agents = Some(false);
    options.config.as_mut().unwrap()["model"] = json!("live-skills-test");
    options.config.as_mut().unwrap()["agentMemory"] = json!(false);
    options.config.as_mut().unwrap()["autoMemory"] = json!(false);
    options.config.as_mut().unwrap()["autoDream"] = json!(false);
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions::default());
    let mut catalog = vec![skill("DRAFT_BODY")];
    catalog.extend((0..500).map(|index| Skill {
        name: format!("overflow-skill-{index:04}-{}", "x".repeat(30)).into(),
        ..skill("OVERFLOW_BODY")
    }));
    agent.replace_session_skills(&session, catalog).unwrap();
    run(&session, "Use the draft skill.").await;
    assert_eq!(provider.inner.requests.lock().unwrap().len(), 2);
    assert_eq!(
        retained_catalog_count(&session.history()),
        1,
        "tool rounds within one turn must not advance catalog pagination"
    );
    let user_ids = |messages: &[Message]| {
        messages
            .iter()
            .filter_map(|message| match message {
                Message::User(user) if user.is_meta != Some(true) => Some(user.uuid.clone()),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    let original_users = user_ids(&session.history());
    session
        .enqueue(
            Input::Text("Continue the existing work.".into()),
            EnqueueOptions {
                mode: Some("prompt".into()),
                is_meta: Some(true),
                priority: Some(QueuePriority::Next),
                ..Default::default()
            },
        )
        .unwrap();
    let continuation = session
        .resume_pending_streamed(TurnOptions::default())
        .await
        .unwrap()
        .unwrap();
    let events = continuation.events.collect::<Vec<_>>().await;
    assert!(
        !events.iter().any(|event| matches!(
            event,
            klaus_agent_core::types::events::EngineEvent::TurnFailed { .. }
        )),
        "{events:?}"
    );
    assert_eq!(
        user_ids(&session.history()),
        original_users,
        "a meta continuation does not add a genuine user message"
    );
    assert_eq!(
        retained_catalog_count(&session.history()),
        2,
        "a new full turn must advance uncovered catalog pages even without a new user UUID"
    );
    assert_eq!(provider.inner.requests.lock().unwrap().len(), 3);
    session.shutdown_and_wait().await.unwrap();
}
