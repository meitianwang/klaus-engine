use super::agent_memory_snapshot_reconcile_error;
use super::bare_tool_context;
use super::base_hook_input;
use super::compose_provider_request_admitted;
use super::effort_to_subagent;
use super::filter_tools_by_name;
use super::forwarded_agent_type;
use super::forwarded_resume;
use super::internal_err;
use super::internal_runtime_err;
use super::intersect_subagent_runtime_policy;
use super::join_runtime_strings;
use super::memory_gates_from;
use super::persistent_resume_key;
use super::project_subagent_permission;
use super::resolve_abs;
use super::runtime_forwarded_slot_to_typed_host;
use super::runtime_path_to_agent_memory_host;
use super::runtime_path_to_skill_shell_host;
use super::runtime_session_id_to_session_store_host;
use super::session_subagent_targets_part::SubagentInferenceError;
use super::subagent_permission_ceiling;
use super::subagent_plan_permission_floor;
use super::teammate_prompt;
use super::to_store_compaction;
use super::tool_lifecycle_rollout_writer;
use super::validate_native_pdf_provider;
use super::AgentMemorySnapshotMergeDelivery;
use super::AnyTool;
use super::CacheTtl;
use super::EngineEventStream;
use super::InferenceActivityGuard;
use super::JsForwardedRawSlot;
use super::MemoryBundle;
use super::QueryCompactionRecord;
use super::RunSubAgentOpts;
use super::Session;
use super::SkillShellAuthority;
use super::SlotGuard;
use super::SubAgentMapKey;
use super::SubagentCapabilityCapture;
use super::SubagentPermissionOrigin;
use super::SystemPromptResolvable;
use super::SystemPromptThunk;
use super::ThinkingConfig;
use super::MEMORY_FORK_STREAM_IDLE_TIMEOUT_MS;
use crate::context::date_change_reminder::create_date_change_reminder_provider;
use crate::context::edited_files_reminder::create_edited_files_reminder_provider;
use crate::context::env_info::environment_block;
use crate::context::env_info::EnvBlockOptions;
use crate::context::nested_memory_reminder::create_nested_memory_reminder_provider;
use crate::context::nested_memory_reminder::NestedMemoryReminderDeps;
use crate::engine::agent_definitions::AgentDefinition;
use crate::engine::builtin_agents::GENERAL_PURPOSE_AGENT;
use crate::engine::fork::build_fork_transcript;
use crate::engine::fork::FORK_AGENT_TYPE;
use crate::engine::query::QueryParams;
use crate::engine::query::ToolContextFactory;
use crate::engine::subagent_lifecycle::SubagentContext;
use crate::engine::subagent_lifecycle::SubagentKind;
use crate::engine::subagent_lifecycle::SubagentLifecycleError;
use crate::engine::subagent_lifecycle::SubagentLifecycleRecord;
use crate::engine::subagent_lifecycle::SubagentLifecycleRegistry;
use crate::engine::subagent_lifecycle::SubagentStatus;
use crate::engine::web_search_dependencies::retain_browser_search_dependency;
use crate::engine::web_search_select::select_web_search;
use crate::engine::web_search_select::CLIENT_WEB_SEARCH_TOOL;
use crate::extensions::skills::skill_listing_reminder::create_skill_listing_reminder_provider;
use crate::hooks::types::HookEvent;
use crate::mcp::client::ManagedClient;
use crate::memory::agent_memory::build_agent_memory_prompt_text;
use crate::memory::agent_memory::memory_tool_names;
use crate::memory::agent_memory::AgentMemoryScope;
use crate::memory::memory_recall_reminder::create_memory_recall_reminder_provider;
use crate::memory::memory_recall_reminder::MemoryRecallDeps;
use crate::memory::memory_recall_reminder::RecallContext;
use crate::memory::memory_recall_reminder::RecallError;
use crate::memory::store::RecallSemanticOptions;
use crate::memory::store::TypedMemoryStore;
use crate::memory::store::TypedMemoryStoreOptions;

use crate::permission::rules::parse_rule_strings;
use crate::runtime_values::RuntimePath;
use crate::runtime_values::RuntimeString;
use crate::runtime_values::{RuntimeJson, RuntimeNumber, RuntimeObject};
use crate::schemas::content::ContentBlock;
use crate::schemas::content::TextBlock;
use crate::schemas::message::Message;
use crate::schemas::message::UserMessage;
use crate::store::session_store::LiveRollout;
use crate::sync_ext::{MutexExt, RwLockExt};
use crate::tasks::swarm_store::create_swarm_reminder_provider;
use crate::tasks::swarm_store::register_peer as swarm_register_peer;
use crate::tasks::swarm_store::Identity as SwarmStoreIdentity;
use crate::tasks::task_store::get_task_list_id;
use crate::tools::builtins::tool_search::ensure_tool_search;
use crate::tools::pool::to_tool_specs;
use crate::types::abort::AbortSignal;
use crate::types::errors::EngineError;
use crate::types::errors::TerminalReason;
use crate::types::events::EngineEvent;
use crate::types::events::ThreadItem;
use crate::types::file_state::FileStateCache;
use crate::types::permission::PermissionContext;
use crate::types::permission::PermissionUpdate;
use crate::types::provider::SystemPromptPart;
use crate::types::reminder::ReminderProvider;
use crate::types::tool::RunBashOptions;
use crate::types::tool::SpawnSubAgentOptions;
use crate::types::tool::SwarmController;
use crate::types::tool::SwarmIdentity;
use crate::types::tool::WorktreeController;
use async_stream::stream;
use futures::future::BoxFuture;
use futures::stream::StreamExt;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

fn validate_child_effort(
    bundle: &super::InferenceBundle,
    effort: Option<&crate::schemas::effort::EffortValue>,
) -> Result<(), String> {
    let Some(effort) = effort else { return Ok(()) };
    let metadata = bundle.provider.model_metadata(&bundle.config.model);
    let supported = if metadata.supported_reasoning_efforts.is_empty() {
        metadata
            .effort
            .as_ref()
            .map(|capability| {
                capability
                    .levels
                    .iter()
                    .map(|level| RuntimeString::from(level.as_str()))
                    .collect()
            })
            .unwrap_or_default()
    } else {
        metadata.supported_reasoning_efforts
    };
    let requested = effort.raw_name();
    if supported.contains(&requested) {
        return Ok(());
    }
    Err(format!(
        "Reasoning effort `{}` is not supported for model `{}`. Supported reasoning efforts: {}",
        requested.try_to_utf8().unwrap_or_default(),
        bundle.config.model.try_to_utf8().unwrap_or_default(),
        join_runtime_strings(&supported, ", ")
            .try_to_utf8()
            .unwrap_or_default()
    ))
}

/// The retained child operation awaits terminal recording before publication.
/// Observation can disappear independently; it never owns this finalization.
struct SubagentTurnGuard {
    session: Arc<Session>,
    agent_id: String,
    turn_id: RuntimeString,
    started_at_ms: i64,
    rollout: Option<Arc<dyn LiveRollout>>,
    observable: bool,
    /// Visible ordinary and persistent children keep independent event streams.
    host_observable: bool,
    settled: bool,
}

impl SubagentTurnGuard {
    async fn observe(
        &mut self,
        event: &EngineEvent,
        final_message_id: Option<&RuntimeString>,
    ) -> Result<(), RuntimeString> {
        let status = match event {
            EngineEvent::TurnCompleted {
                stop_reason: TerminalReason::Completed,
                ..
            } => "completed",
            EngineEvent::TurnCompleted {
                stop_reason: TerminalReason::Aborted,
                ..
            } => "interrupted",
            EngineEvent::TurnCompleted { .. } | EngineEvent::TurnFailed { .. } => "failed",
            _ => return Ok(()),
        };
        self.session
            .await_cancel_durability(Some(&self.agent_id))
            .await?;
        let admission = match &self.rollout {
            Some(rollout) => Some(
                rollout
                    .prepare_append()
                    .await
                    .map_err(|error| RuntimeString::from(error.to_string()))?,
            ),
            None => None,
        };
        let review_floor = self.session.capture_ordinary_review_floor(&self.agent_id);
        let finished = (self.session.deps.now)();
        let mut payload = RuntimeObject::new();
        if let Some(floor) = review_floor {
            payload.insert("canonicalReviewFloor", floor);
        }
        payload.insert("status", RuntimeJson::String(status.into()));
        payload.insert(
            "startedAtMs",
            RuntimeJson::Number(RuntimeNumber::from(self.started_at_ms)),
        );
        payload.insert(
            "finishedAtMs",
            RuntimeJson::Number(RuntimeNumber::from(finished)),
        );
        payload.insert(
            "durationMs",
            RuntimeJson::Number(RuntimeNumber::from(
                finished.saturating_sub(self.started_at_ms).max(0),
            )),
        );
        if status == "completed" {
            if let Some(id) = final_message_id {
                payload.insert("finalMessageId", RuntimeJson::String(id.clone()));
            }
        }
        if let EngineEvent::TurnFailed { error, .. } = event {
            payload.insert("error", RuntimeJson::String(error.message.clone()));
            if let Some(kind) = error.provider_kind {
                payload.insert("kind", RuntimeJson::String(kind.as_str().into()));
            }
        }
        if let Some(admission) = admission {
            admission.commit(crate::schemas::rollout::RolloutItem::Event(
                crate::schemas::rollout::EventItem {
                    event: "turn_lifecycle".into(),
                    turn_id: Some(self.turn_id.clone()),
                    payload: Some(RuntimeJson::Object(payload)),
                },
            ));
        }
        self.settled = true;
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), RuntimeString> {
        if self.settled {
            return Ok(());
        }
        let event = EngineEvent::TurnCompleted {
            turn_id: self.turn_id.clone(),
            usage: None,
            stop_reason: TerminalReason::Aborted,
        };
        self.observe(&event, None).await?;
        if self.observable {
            self.session.mark_subagent_cancelled(&self.agent_id).await?;
            if self.host_observable {
                let sink = self.session.subagents.event_sink.lock_recover().clone();
                if let Some(sink) = sink {
                    sink(&self.agent_id, super::SubagentStreamEvent::Event(&event));
                }
            }
        }
        Ok(())
    }
}

impl Session {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::engine::session) async fn prepare_subagent_config(
        self: &Arc<Self>,
        opts: &SpawnSubAgentOptions,
        capture: Arc<InferenceActivityGuard>,
        fork_path: bool,
        resumed_agent_type: Option<&RuntimeString>,
        creation_snapshot: Option<&super::SubagentCreationSnapshot>,
        signal: &AbortSignal,
    ) -> Result<super::PreparedSubagentConfig, RuntimeString> {
        let registry = self.current_agent_registry();
        let agent_type_slot = forwarded_agent_type(opts);
        let agent_type_is_raw = agent_type_slot.is_raw();
        let requested_type = agent_type_slot.runtime_string_value();
        let type_name = if let Some(snapshot) = creation_snapshot {
            snapshot.agent_type.clone()
        } else if fork_path && !agent_type_slot.is_present() {
            Some(RuntimeString::from(FORK_AGENT_TYPE))
        } else {
            requested_type
                .clone()
                .or_else(|| resumed_agent_type.cloned())
                .or_else(|| {
                    let general_purpose = RuntimeString::from(GENERAL_PURPOSE_AGENT);
                    registry.has(&general_purpose).then_some(general_purpose)
                })
        };
        let resuming_fork =
            !fork_path && resumed_agent_type.is_some_and(|value| value == FORK_AGENT_TYPE);
        let trusted_fork_type = !agent_type_is_raw
            && creation_snapshot.is_some_and(|snapshot| {
                snapshot.in_fork_child
                    && snapshot
                        .agent_type
                        .as_ref()
                        .is_some_and(|name| name == FORK_AGENT_TYPE)
                    && requested_type.as_ref() == snapshot.agent_type.as_ref()
            });
        let mut unknown_type = requested_type
            .as_ref()
            .filter(|name| {
                !trusted_fork_type && (agent_type_is_raw || registry.get(name).is_none())
            })
            .cloned();
        let mut def = None;
        if let Some(snapshot) = creation_snapshot {
            def = snapshot.definition.clone();
        } else if (!fork_path || agent_type_slot.is_present()) && !resuming_fork {
            if let Some(tn) = &type_name {
                def = if agent_type_is_raw {
                    None
                } else {
                    registry.get(tn).cloned()
                };
                if def.is_none() {
                    unknown_type = Some(tn.clone());
                }
            }
        }
        let role_error = unknown_type.map(|name| {
            let known = registry
                .list()
                .iter()
                .map(|definition| definition.name.clone())
                .collect::<Vec<_>>();
            let known = if known.is_empty() {
                RuntimeString::from("none")
            } else {
                join_runtime_strings(&known, ", ")
            };
            RuntimeString::from("unknown agent type \"")
                .concat(&name)
                .concat(&RuntimeString::from("\" (available: "))
                .concat(&known)
                .concat(&RuntimeString::from(")"))
        });
        let bundle = capture.bundle.clone();
        let mut child_effort = creation_snapshot
            .map(|snapshot| snapshot.effort.clone())
            .unwrap_or_else(|| opts.effective_effort(None, bundle.config.effort.clone()));
        let mut child_bundle = bundle.clone();
        if creation_snapshot.is_none() {
            let selection = opts
                .model
                .as_ref()
                .or(bundle.config.default_subagent_model.as_ref());
            let requested_effort = opts
                .effort
                .clone()
                .or_else(|| bundle.config.default_subagent_reasoning_effort.clone());
            if let Some(selection) = selection {
                let tier = ["low", "medium", "high"]
                    .iter()
                    .any(|tier| selection == *tier);
                let selection_policy = if opts.model.is_some() {
                    if bundle.config.subagent_model_catalog {
                        if tier || bundle.config.subagent_model_options.is_empty() {
                            return Err("Model-directory mode requires an available concrete model from the captured directory.".into());
                        }
                        if !bundle
                            .config
                            .subagent_model_options
                            .iter()
                            .any(|option| &option.value == selection)
                        {
                            Err(SubagentInferenceError::Unavailable("The selected subagent model is not in the captured model directory.".into()))
                        } else {
                            Ok(())
                        }
                    } else if !tier {
                        return Err(
                            "Concrete subagent models are disabled. Select low, medium, or high."
                                .into(),
                        );
                    } else {
                        Ok(())
                    }
                } else {
                    Ok(())
                };
                child_bundle = match if let Err(error) = selection_policy {
                    Err(error)
                } else {
                    self.prepare_subagent_inference(&bundle, selection, signal)
                        .await
                } {
                    Ok(selected) => selected,
                    Err(SubagentInferenceError::Unavailable(error))
                        if !signal.is_cancelled() && !self.is_disposed() =>
                    {
                        tracing::warn!(
                            selection = ?selection,
                            reason = %error,
                            parent_model = ?bundle.config.model,
                            "Subagent model selection unavailable; using captured parent model"
                        );
                        bundle.clone()
                    }
                    Err(error) => return Err(error.to_string().into()),
                };
                let model_changed = child_bundle.config.model != bundle.config.model
                    || child_bundle.source != bundle.source
                    || !Arc::ptr_eq(&child_bundle.provider, &bundle.provider);
                if model_changed {
                    child_effort = requested_effort.clone().or_else(|| {
                        let metadata = child_bundle
                            .provider
                            .model_metadata(&child_bundle.config.model);
                        metadata
                            .default_reasoning_effort
                            .as_ref()
                            .and_then(crate::schemas::effort::EffortValue::from_raw)
                            .or_else(|| {
                                metadata
                                    .effort
                                    .as_ref()
                                    .and_then(|capability| capability.default)
                                    .map(crate::schemas::effort::EffortValue::Level)
                            })
                    });
                } else if requested_effort.is_some() {
                    child_effort = requested_effort.clone();
                }
            } else if requested_effort.is_some() {
                child_effort = requested_effort.clone();
            }
            if requested_effort.is_some() {
                validate_child_effort(&child_bundle, child_effort.as_ref())
                    .map_err(RuntimeString::from)?;
            }
        }
        if let Some(error) = role_error {
            return Err(error);
        }
        if creation_snapshot.is_none() {
            let previous_role_model = child_bundle.config.model.clone();
            let previous_role_effort = child_effort.clone();
            let previous_role_source = child_bundle.source.clone();
            let previous_role_provider = child_bundle
                .provider
                .review_target_provider()
                .unwrap_or_else(|| child_bundle.provider.clone());
            if let Some(role_model) = def
                .as_ref()
                .filter(|_| opts.model.is_none())
                .and_then(|definition| definition.model.as_ref())
                .filter(|model| *model != "inherit")
            {
                child_bundle = self
                    .resolve_subagent_inference(&bundle, role_model, signal)
                    .await
                    .map_err(RuntimeString::from)?;
            }
            if let Some(role_effort) = def
                .as_ref()
                .and_then(|definition| definition.effort.clone())
            {
                child_effort = Some(role_effort);
            }
            let role_provider = child_bundle
                .provider
                .review_target_provider()
                .unwrap_or_else(|| child_bundle.provider.clone());
            if child_bundle.config.model != previous_role_model
                || child_bundle.source != previous_role_source
                || !Arc::ptr_eq(&role_provider, &previous_role_provider)
                || child_effort != previous_role_effort
            {
                validate_child_effort(&child_bundle, child_effort.as_ref())
                    .map_err(RuntimeString::from)?;
            }
        }
        Ok(super::PreparedSubagentConfig {
            capture: capture.with_bundle(child_bundle),
            fork_path,
            resuming_fork,
            type_name,
            definition: def,
            effort: child_effort,
        })
    }

    pub(in crate::engine::session) async fn prepare_fresh_subagent(
        self: &Arc<Self>,
        opts: &SpawnSubAgentOptions,
        run_opts: &mut RunSubAgentOpts,
        signal: &AbortSignal,
    ) -> Result<(), RuntimeString> {
        if run_opts.prepared.is_some()
            || opts.memory_dir.is_some()
            || forwarded_resume(opts).is_present()
            || run_opts.creation_snapshot.is_some()
            || run_opts.message_continuation
        {
            return Ok(());
        }
        let capture = run_opts
            .inference_capture
            .clone()
            .map(Ok)
            .unwrap_or_else(|| self.begin_inference_activity().map(Arc::new))
            .map_err(RuntimeString::from)?;
        let fork_path = run_opts.fork_conversation.is_some();
        if fork_path
            && (run_opts.parent_agent_id.is_some() || run_opts.initiating_turn_id.is_some())
            && opts.inherited_context.is_none()
        {
            return Err("The initiating step's inherited context is unavailable.".into());
        }
        run_opts.prepared = Some(
            self.prepare_subagent_config(opts, capture.clone(), fork_path, None, None, signal)
                .await?,
        );
        run_opts.inference_capture = Some(capture);
        Ok(())
    }

    pub fn has_plugin_captured_contexts(&self) -> bool {
        self.turn.is_active()
            || self
                .subagents
                .lifecycle
                .lock_recover()
                .snapshot()
                .agents
                .iter()
                .any(|record| !record.status.is_terminal())
    }

    pub async fn wait_for_plugin_captured_contexts(&self) {
        self.wait_for_active_turns().await;
        while self.has_plugin_captured_contexts() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    pub(crate) async fn transition_subagent_lifecycle(
        &self,
        agent_id: &str,
        transition: impl FnOnce(&mut SubagentLifecycleRegistry) -> Result<(), SubagentLifecycleError>
            + Send,
    ) -> Result<(), RuntimeString> {
        self.await_cancel_durability(Some(agent_id)).await?;
        let rollout = self.lifecycle_rollout_for(&agent_id.into());
        let admission = match &rollout {
            Some(writer) => Some(
                writer
                    .prepare_append()
                    .await
                    .map_err(|error| RuntimeString::from(error.to_string()))?,
            ),
            None => None,
        };
        let record = {
            let mut lifecycle = self.subagents.lifecycle.lock_recover();
            transition(&mut lifecycle)
                .ok()
                .and_then(|()| lifecycle.get(agent_id).cloned())
        };
        if let (Some(record), Some(admission)) = (record, admission) {
            admission.commit(crate::schemas::rollout::RolloutItem::SubagentLifecycle(
                crate::schemas::rollout::SubagentLifecycleItem { record },
            ));
        }
        Ok(())
    }

    pub(crate) async fn record_subagent_started(
        &self,
        agent_id: &str,
        kind: SubagentKind,
        context: SubagentContext,
    ) -> Result<(), RuntimeString> {
        let rollout = if context.depth > 1 {
            context
                .parent_agent_id
                .as_ref()
                .and_then(|id| self.subagents.parent_rollouts.writer(id))
        } else {
            self.persistence.rollout().cloned()
        };
        let admission = match &rollout {
            Some(writer) => Some(
                writer
                    .prepare_batch(2)
                    .await
                    .map_err(|error| RuntimeString::from(error.to_string()))?,
            ),
            None => None,
        };
        let records = self.stage_subagent_started(agent_id, kind, context);
        if let Some(admission) = admission {
            admission.commit(
                records
                    .into_iter()
                    .map(|record| {
                        crate::schemas::rollout::RolloutItem::SubagentLifecycle(
                            crate::schemas::rollout::SubagentLifecycleItem { record },
                        )
                    })
                    .collect(),
            );
        }
        Ok(())
    }

    pub(crate) fn stage_subagent_started(
        &self,
        agent_id: &str,
        kind: SubagentKind,
        context: SubagentContext,
    ) -> Vec<SubagentLifecycleRecord> {
        let now_ms = (self.deps.now)();
        {
            let mut lifecycle = self.subagents.lifecycle.lock_recover();
            let record = SubagentLifecycleRecord::new(agent_id.to_string(), kind, context, now_ms);
            match lifecycle.spawn(record) {
                Ok(()) => {
                    let mut records: Vec<SubagentLifecycleRecord> =
                        lifecycle.get(agent_id).cloned().into_iter().collect();
                    if lifecycle.mark_running(agent_id, now_ms).is_ok() {
                        records.extend(lifecycle.get(agent_id).cloned());
                    }
                    records
                }
                Err(SubagentLifecycleError::AlreadyExists { .. }) => {
                    if lifecycle.mark_running(agent_id, now_ms).is_ok() {
                        lifecycle.get(agent_id).cloned().into_iter().collect()
                    } else {
                        Vec::new()
                    }
                }
                Err(SubagentLifecycleError::NotFound { .. }) => Vec::new(),
            }
        }
    }

    /// Stamp the concrete model a child run resolved to onto its lifecycle
    /// record. Called right after the bundle selection so the row the host
    /// reads names the model the child actually runs on, not the tier token
    /// the parent passed.
    pub(crate) async fn record_subagent_model(
        &self,
        agent_id: &str,
        model: &RuntimeString,
        source: Option<RuntimeString>,
    ) -> Result<(), RuntimeString> {
        self.transition_subagent_lifecycle(agent_id, |lifecycle| {
            lifecycle.set_model(agent_id, model.clone(), source)
        })
        .await
    }

    pub(crate) async fn mark_subagent_completed(
        &self,
        agent_id: &str,
        result: Option<RuntimeString>,
    ) -> Result<(), RuntimeString> {
        let now_ms = (self.deps.now)();
        self.transition_subagent_lifecycle(agent_id, |lifecycle| {
            lifecycle.mark_completed(agent_id, now_ms, result)
        })
        .await
    }

    pub(crate) async fn mark_subagent_failed(
        &self,
        agent_id: &str,
        error: RuntimeString,
    ) -> Result<(), RuntimeString> {
        let now_ms = (self.deps.now)();
        self.transition_subagent_lifecycle(agent_id, |lifecycle| {
            lifecycle.mark_failed(agent_id, now_ms, error)
        })
        .await
    }

    pub(crate) async fn mark_subagent_budget_limited(
        &self,
        agent_id: &str,
        result: Option<RuntimeString>,
        reason: RuntimeString,
    ) -> Result<(), RuntimeString> {
        let now_ms = (self.deps.now)();
        self.transition_subagent_lifecycle(agent_id, |lifecycle| {
            lifecycle.mark_budget_limited(agent_id, now_ms, result, reason)
        })
        .await
    }

    pub(crate) async fn mark_subagent_cancelled(
        &self,
        agent_id: &str,
    ) -> Result<(), RuntimeString> {
        let now_ms = (self.deps.now)();
        self.transition_subagent_lifecycle(agent_id, |lifecycle| lifecycle.cancel(agent_id, now_ms))
            .await
    }

    /// Stop changes physical and live cancellation state synchronously. Its
    /// retained cancellation operation owns this record's later persistence.
    pub(crate) fn cancel_subagent_state(&self, agent_id: &str) -> Option<SubagentLifecycleRecord> {
        let mut lifecycle = self.subagents.lifecycle.lock_recover();
        lifecycle.cancel(agent_id, (self.deps.now)()).ok()?;
        lifecycle.get(agent_id).cloned()
    }

    pub(crate) async fn mark_subagent_idle(&self, agent_id: &str) -> Result<(), RuntimeString> {
        let now_ms = (self.deps.now)();
        self.transition_subagent_lifecycle(agent_id, |lifecycle| {
            // Mailbox idleness must not erase the preceding run's failure or
            // cancellation. Check and transition under the same registry lock.
            if lifecycle.get(agent_id).is_some_and(|record| {
                matches!(
                    record.status,
                    SubagentStatus::Failed
                        | SubagentStatus::Cancelled
                        | SubagentStatus::Interrupted
                )
            }) {
                return Ok(());
            }
            lifecycle.mark_idle(agent_id, now_ms)
        })
        .await
    }

    /// Where this child's lifecycle rows settle: the direct parent's sidechain
    /// for a nested child, this session's rollout for a direct one. Rows are
    /// one hop from their spawner, so a transcript only ever carries rows
    /// whose spawn turn it owns.
    pub(crate) fn lifecycle_rollout_for(
        &self,
        agent_id: &RuntimeString,
    ) -> Option<Arc<dyn LiveRollout>> {
        let record = self
            .subagents
            .lifecycle
            .lock_recover()
            .get(agent_id)
            .cloned();
        let nested = record
            .as_ref()
            .map(|record| record.context.depth > 1)
            .unwrap_or(false);
        if nested {
            // One hop: the row settles in its DIRECT parent's sidechain, so a
            // transcript only carries rows whose spawn turn it owns.
            let parent = record.and_then(|record| record.context.parent_agent_id);
            parent.and_then(|parent| self.subagents.parent_rollouts.writer(&parent))
        } else {
            self.persistence.rollout().cloned()
        }
    }

    pub(crate) async fn acquire_parent_subagent_rollout(
        &self,
        parent_agent_id: Option<&str>,
    ) -> Result<Option<crate::engine::subagent_rollouts::SubagentRolloutLease>, RuntimeString> {
        let (Some(parent), Some(store)) = (parent_agent_id, self.persistence.store().cloned())
        else {
            return Ok(None);
        };
        let parent: RuntimeString = parent.into();
        let record = self
            .subagents
            .lifecycle
            .lock_recover()
            .get(&parent)
            .cloned()
            .ok_or_else(|| RuntimeString::from("The direct parent's lifecycle is unavailable."))?;
        let metadata = crate::store::session_store::SubAgentMeta {
            agent_type: record.context.agent_type,
            model: record.model.unwrap_or_else(|| self.config.model.clone()),
            permission_mode: None,
            effort: None,
            created_at: record.started_at_ms,
        };
        let session_id = runtime_session_id_to_session_store_host(&self.id);
        let agent_id = parent
            .try_to_utf8()
            .map_err(|_| RuntimeString::from("Invalid parent agent identity."))?;
        self.subagents
            .parent_rollouts
            .acquire(parent, move || async move {
                store
                    .open_sub_agent(&session_id, &agent_id, metadata)
                    .await
                    .map(Arc::from)
                    .map_err(|error| RuntimeString::from(error.to_string()))
            })
            .await
            .map(Some)
    }

    pub fn set_agent_notification_sink(&self, sink: Option<super::AgentNotificationSink>) {
        *self.subagents.notification_sink.lock_recover() = sink;
    }

    /// A receipt is admitted by the receiving session exactly once, including
    /// mailbox redelivery after a restart. The rollout event never enters a prompt.
    pub(crate) async fn publish_agent_notification(
        &self,
        mut notification: crate::schemas::agent_notification::AgentNotification,
    ) -> Result<(), RuntimeString> {
        if self.lifecycle.is_disposed() {
            return Ok(());
        }
        // A stamped owner wins: the receipt settles into the turn that
        // spawned the child even after that turn closed. Mailbox receipts may
        // bind to the receiving turn; an unknown child origin remains unknown.
        if notification.turn_id.is_none() && notification.source != "background-task" {
            notification.turn_id = self
                .turn
                .active_stack
                .lock_recover()
                .last()
                .and_then(|turn| turn.turn_id.clone());
        }
        let payload = crate::runtime_values::runtime_codec::encode_value(&notification)
            .map_err(|error| RuntimeString::from(error.to_string()))?;
        let rollout = self.persistence.rollout();
        let admission = match rollout {
            Some(writer) => Some(
                writer
                    .prepare_append()
                    .await
                    .map_err(|error| RuntimeString::from(error.to_string()))?,
            ),
            None => None,
        };
        {
            let mut index = self.turn_index.lock_recover();
            if index
                .agent_notifications
                .iter()
                .any(|old| old.notification_id == notification.notification_id)
            {
                return Ok(());
            }
            if let Some(admission) = admission {
                admission.commit(crate::schemas::rollout::RolloutItem::Event(
                    crate::schemas::rollout::EventItem {
                        event: "agent_notification".into(),
                        turn_id: notification.turn_id.clone(),
                        payload: Some(payload),
                    },
                ));
            }
            index.agent_notifications.push(notification.clone());
        }
        let sink = self.subagents.notification_sink.lock_recover().clone();
        if let Some(sink) = sink {
            sink(&notification);
        }
        Ok(())
    }

    pub(crate) fn restore_subagent_notification_ids(
        &self,
        agent_id: &str,
        index: Option<&super::TurnIndex>,
    ) {
        let Some(index) = index else {
            return;
        };
        self.subagents
            .notification_ids_by_child
            .lock_recover()
            .entry(agent_id.to_owned())
            .or_default()
            .extend(
                index
                    .agent_notifications
                    .iter()
                    .map(|receipt| receipt.notification_id.clone()),
            );
    }

    /// A child receipt belongs exclusively to its transcript and live observer.
    /// In particular, it must never consult the root turn or notification sink.
    pub(crate) async fn publish_subagent_notification(
        &self,
        agent_id: &str,
        mut receipt: crate::schemas::agent_notification::AgentNotification,
        turn_id: Option<&RuntimeString>,
        rollout: Option<&Arc<dyn LiveRollout>>,
        host_observable: bool,
    ) -> Result<(), RuntimeString> {
        if self.lifecycle.is_disposed() {
            return Ok(());
        }
        receipt.turn_id = turn_id.cloned();
        let payload = crate::runtime_values::runtime_codec::encode_value(&receipt)
            .map_err(|error| RuntimeString::from(error.to_string()))?;
        let admission = match rollout {
            Some(writer) => Some(
                writer
                    .prepare_append()
                    .await
                    .map_err(|error| RuntimeString::from(error.to_string()))?,
            ),
            None => None,
        };
        {
            let mut ids = self.subagents.notification_ids_by_child.lock_recover();
            if !ids
                .entry(agent_id.to_owned())
                .or_default()
                .insert(receipt.notification_id.clone())
            {
                return Ok(());
            }
            if let Some(admission) = admission {
                admission.commit(crate::schemas::rollout::RolloutItem::Event(
                    crate::schemas::rollout::EventItem {
                        event: "agent_notification".into(),
                        turn_id: receipt.turn_id.clone(),
                        payload: Some(payload),
                    },
                ));
            }
        }
        let sink = self.subagents.event_sink.lock_recover().clone();
        if host_observable {
            if let Some(sink) = sink {
                sink(agent_id, super::SubagentStreamEvent::Notification(&receipt));
            }
        }
        Ok(())
    }

    /// Install the app-server's ordered child-conversation observer. The callback
    /// only enqueues a frame; detached children outlive their spawning tool call.
    pub fn set_subagent_event_sink(&self, sink: Option<super::SubagentEventSink>) {
        *self.subagents.event_sink.lock_recover() = sink;
    }

    pub(crate) fn is_ordinary_communication(message: &Message) -> bool {
        matches!(message, Message::User(user) if user.content.iter().any(|block| {
            matches!(block, ContentBlock::Text(text) if text.harness_envelope == Some(crate::schemas::content::HarnessEnvelope::SubagentMessage))
        }))
    }

    pub(crate) fn publish_subagent_prompt(&self, agent_id: &str, message: &Message) {
        if !matches!(message, Message::User(_)) {
            return;
        }
        let sink = self.subagents.event_sink.lock_recover().clone();
        if let Some(sink) = sink {
            sink(agent_id, super::SubagentStreamEvent::Prompt(message));
        }
    }

    fn observe_subagent_stream(
        self: &Arc<Self>,
        agent_id: String,
        mut events: EngineEventStream,
    ) -> EngineEventStream {
        let this = self.clone();
        Box::pin(stream! {
            while let Some(event) = events.next().await {
                let sink = this.subagents.event_sink.lock_recover().clone();
                if let Some(sink) = sink {
                    sink(&agent_id, super::SubagentStreamEvent::Event(&event));
                }
                yield event;
            }
        })
    }

    pub(super) fn capture_subagent_goal_lease(
        &self,
        agent_id: &str,
        run_opts: &mut RunSubAgentOpts,
    ) {
        if run_opts.goal_lease.is_none() {
            run_opts.goal_lease =
                Some(self.goal_runtime().map(|goal| {
                    Arc::new(goal.register_descendant(
                        agent_id.to_owned(),
                        run_opts.parent_agent_id.as_deref(),
                    ))
                }));
        }
    }

    #[cfg(feature = "testing")]
    pub fn testing_run_uncaptured_subagent(
        self: &Arc<Self>,
        opts: SpawnSubAgentOptions,
        agent_id: String,
        isolated: bool,
    ) -> EngineEventStream {
        let run_opts = RunSubAgentOpts::default();
        if isolated {
            self.run_sub_agent_isolated(
                opts,
                1,
                AbortSignal::new(),
                self.permission_context(),
                agent_id,
                run_opts,
            )
        } else {
            self.run_sub_agent(
                opts,
                1,
                AbortSignal::new(),
                self.permission_context(),
                agent_id,
                run_opts,
            )
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn run_sub_agent(
        self: &Arc<Self>,
        opts: SpawnSubAgentOptions,
        depth: u32,
        caller_signal: AbortSignal,
        permission: PermissionContext,
        agent_id: String,
        mut run_opts: RunSubAgentOpts,
    ) -> EngineEventStream {
        self.capture_subagent_goal_lease(&agent_id, &mut run_opts);
        let runtime_owned = run_opts.creation_admission.is_some()
            || run_opts.message_continuation
            || run_opts.lifecycle_kind == Some(SubagentKind::Persistent);
        let detached = opts.memory_dir.is_none() && !runtime_owned;
        let worker_signal = if detached {
            AbortSignal::new()
        } else {
            caller_signal.clone()
        };
        let identity = forwarded_resume(&opts)
            .string_value()
            .unwrap_or_else(|| agent_id.clone());
        let events = self.run_sub_agent_execution(
            opts,
            depth,
            worker_signal.clone(),
            permission,
            agent_id,
            run_opts,
        );
        if detached {
            self.own_subagent_execution(identity, caller_signal, worker_signal, events)
        } else {
            events
        }
    }

    /// Keep execution alive after an accepted caller stops observing it.
    /// Before the real creation commit, cancellation still drops its pending
    /// creation guard and returns all admission resources.
    pub(super) fn own_subagent_execution(
        self: &Arc<Self>,
        identity: String,
        caller_signal: AbortSignal,
        worker_signal: AbortSignal,
        mut events: EngineEventStream,
    ) -> EngineEventStream {
        let this = self.clone();
        Box::pin(stream! {
            let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
            let observation = caller_signal.child_token();
            let registered = {
                let mut registry = this.background_tasks.lock_recover();
                if registry.closing || this.lifecycle.is_disposed() {
                    false
                } else {
                let owner = this.clone();
                registry.handles.push(tokio::spawn(async move {
                    let mut creating = true;
                    loop {
                        let next = if creating {
                            tokio::select! {
                                biased;
                                _ = caller_signal.cancelled() => {
                                    if !owner.subagent_creation_is_committed(&identity) {
                                        worker_signal.cancel();
                                        break;
                                    }
                                    creating = false;
                                    continue;
                                },
                                _ = sender.closed() => {
                                    if !owner.subagent_creation_is_committed(&identity) {
                                        worker_signal.cancel();
                                        break;
                                    }
                                    creating = false;
                                    continue;
                                },
                                event = events.next() => event,
                            }
                        } else {
                            events.next().await
                        };
                        let Some(event) = next else { break };
                        let _ = sender.send(event);
                    }
                }));
                    true
                }
            };
            if !registered {
                yield EngineEvent::TurnFailed {
                    inference: None,
                    error: internal_err("The parent session is shutting down."),
                };
                return;
            }
            let mut observation = Self::subagent_observation_stream(receiver, observation);
            while let Some(event) = observation.next().await {
                yield event;
            }
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn run_sub_agent_execution(
        self: &Arc<Self>,
        opts: SpawnSubAgentOptions,
        depth: u32,
        parent_signal: AbortSignal,
        permission: PermissionContext,
        agent_id: String,
        mut run_opts: RunSubAgentOpts,
    ) -> EngineEventStream {
        self.capture_subagent_goal_lease(&agent_id, &mut run_opts);
        let observable = opts.memory_dir.is_none();
        if !observable {
            // Private memory maintenance keeps its independent admission lifetime.
            return self.run_sub_agent_inner(
                opts,
                depth,
                parent_signal,
                permission,
                agent_id,
                run_opts,
            );
        }
        if run_opts.inference_capture.is_none()
            && !forwarded_resume(&opts).is_present()
            && run_opts.creation_snapshot.is_none()
            && !run_opts.message_continuation
        {
            match self.begin_inference_activity() {
                Ok(capture) => run_opts.inference_capture = Some(Arc::new(capture)),
                Err(error) => {
                    return Box::pin(futures::stream::once(async move {
                        EngineEvent::TurnFailed {
                            inference: None,
                            error: internal_err(&error),
                        }
                    }));
                }
            }
        }
        let observed_id = forwarded_resume(&opts)
            .string_value()
            .unwrap_or_else(|| agent_id.clone());
        let this = self.clone();
        let identity = observed_id.clone();
        let caller = run_opts
            .parent_agent_id
            .as_ref()
            .map(|id| RuntimeString::from(id.clone()));
        let resume_context = self.capture_subagent_resume_context(
            caller.as_ref(),
            run_opts.inference_capture.as_ref(),
            &permission,
            run_opts.runtime_capabilities.as_ref(),
        );
        let residency_config = resume_context
            .as_ref()
            .map(|context| context.bundle.config.clone())
            .unwrap_or_else(|_| {
                run_opts
                    .inference_capture
                    .as_ref()
                    .map(|capture| capture.bundle.config.clone())
                    .or_else(|| {
                        run_opts
                            .creation_snapshot
                            .as_ref()
                            .map(|snapshot| snapshot.config.clone())
                    })
                    .unwrap_or_else(|| self.inference_config())
            });
        let runtime_owned = run_opts.creation_admission.is_some()
            || run_opts.message_continuation
            || run_opts.lifecycle_kind == Some(SubagentKind::Persistent);
        let events = Box::pin(stream! {
            if let Err(error) = this.prepare_fresh_subagent(&opts, &mut run_opts, &parent_signal).await {
                yield EngineEvent::TurnFailed { inference: None, error: internal_runtime_err(error) };
                return;
            }
            let mut local = None;
            let mut local_pending = None;
            let mut _local_owner = None;
            let mut _existing_pin = None;
            if !runtime_owned && (this.has_ordinary_runtime_identity(&identity)
                || (forwarded_resume(&opts).is_present() && !forwarded_resume(&opts).is_raw()))
            {
                match this.ensure_ordinary_loaded_in_context(&identity, true, resume_context.as_ref().ok()).await {
                    Ok(pin) => {
                        _existing_pin = pin;
                        run_opts.execution_checked = true;
                        run_opts.residency_config = this.ordinary_residency_config(&identity);
                    },
                    Err(error) => {
                        let error = if error == "The sub-agent recovery context is unavailable." {
                            resume_context.as_ref().err().cloned().unwrap_or(error)
                        } else { error };
                        yield EngineEvent::TurnFailed { inference: None, error: internal_runtime_err(error) };
                        return;
                    }
                }
            }
            if run_opts.creation_admission.is_none()
                && !this.has_ordinary_runtime_identity(&identity)
                && !forwarded_resume(&opts).is_present()
            {
                match this.prepare_external_subagent(
                    &opts, depth, run_opts.parent_agent_id.clone(), run_opts.initiating_turn_id.clone(),
                    parent_signal.clone(), Some(identity.clone()),
                    &residency_config,
                ).await {
                    Ok((_, admission, pending, _)) => {
                        _local_owner = Some(this.creation_owner(&identity, &admission));
                        run_opts.creation_admission = Some(admission.clone());
                        run_opts.execution_checked = true;
                        run_opts.local_creation = true;
                        local = Some(admission);
                        local_pending = Some(pending);
                    },
                    Err(error) => {
                        yield EngineEvent::TurnFailed { inference: None, error: internal_runtime_err(error) };
                        return;
                    }
                }
            }
            let _direct_turn = (!runtime_owned).then(|| this.own_direct_subagent_turn(&identity));
            let mut events = this.run_sub_agent_inner(opts, depth, parent_signal, permission, agent_id, run_opts);
            while let Some(event) = events.next().await {
                this.note_ordinary_runtime_event(&identity, &event);
                if let Some(pending) = local_pending.as_mut() {
                    if local.as_ref().is_some_and(|admission| admission.is_committed()) {
                        pending.disarm();
                    }
                }
                yield event;
            }
            if let Some(admission) = local {
                this.finish_external_subagent(&identity, &admission).await;
            }
        });
        if observable {
            self.observe_subagent_stream(observed_id, events)
        } else {
            events
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn run_sub_agent_inner(
        self: &Arc<Self>,
        opts: SpawnSubAgentOptions,
        depth: u32,
        parent_signal: AbortSignal,
        permission: PermissionContext,
        agent_id: String,
        mut run_opts: RunSubAgentOpts,
    ) -> EngineEventStream {
        let child_collaboration_mode = run_opts
            .collaboration_mode
            .unwrap_or_else(|| self.planning.mode());
        let task_plan = run_opts.task_plan.clone().unwrap_or_else(|| {
            crate::tasks::task_store::TaskPlanBinding::capture(&self.task_list_id())
        });
        // Restricted maintenance forks have no earlier root request scope.
        // Capture their existing host view at this direct admission only.
        let direct_memory_capabilities =
            (opts.memory_dir.is_some() && run_opts.runtime_capabilities.is_none()).then(|| {
                SubagentCapabilityCapture::capture(
                    self.runtime_capability_snapshot_for_turn(&super::TurnOptions::default()),
                )
            });
        let observable = opts.memory_dir.is_none();
        let goal_lease = run_opts.goal_lease.take().unwrap_or_else(|| {
            self.goal_runtime().map(|goal| {
                Arc::new(
                    goal.register_descendant(agent_id.clone(), run_opts.parent_agent_id.as_deref()),
                )
            })
        });
        let goal_usage = self.goal_runtime().zip(
            goal_lease
                .as_ref()
                .and_then(|lease| lease.generation.clone()),
        );
        let this = self.clone();
        let inference = run_opts
            .inference_capture
            .clone()
            .map(Ok)
            .unwrap_or_else(|| this.begin_inference_activity().map(Arc::new));
        Box::pin(stream! {
            let _goal_lease = goal_lease;
            let capture = match inference {
                Ok(lease) => lease,
                Err(_) => match this.capture_inference_for(&parent_signal, None).await {
                    Ok(capture) => capture,
                    Err(error) => {
                        yield EngineEvent::TurnFailed { inference: None, error: internal_err(&error) };
                        return;
                    }
                },
            };
            #[cfg(any(test, feature = "testing"))]
            if let Some(goal) = this.goal_observation_runtime() {
                // Observe preparation before reading mutable parent policy.
                goal.testing_note_phase("subagent.identity");
            }
            let creation_snapshot = if let Some(snapshot) = &run_opts.creation_snapshot {
                Some(snapshot.clone())
            } else if opts.swarm.is_none() && opts.memory_dir.is_none() {
                if let Some(resume) = &opts.resume {
                    let id = match resume.try_to_utf8() {
                        Ok(id) => id,
                        Err(_) => {
                            yield EngineEvent::TurnFailed { inference: None, error: internal_err("Invalid sub-agent resume identity") };
                            return;
                        }
                    };
                    let restored = tokio::select! {
                        biased;
                        _ = parent_signal.cancelled() => Err(RuntimeString::from("Sub-agent recovery cancelled.")),
                        restored = this.ordinary_snapshot_for_resume(&id) => restored,
                    };
                    match restored {
                        Ok(snapshot) => Some(snapshot),
                        Err(error) => {
                            yield EngineEvent::TurnFailed { inference: None, error: internal_runtime_err(error) };
                            return;
                        }
                    }
                } else { None }
            } else { None };
            let child_runtime_capabilities = creation_snapshot.as_ref()
                .map(|snapshot| snapshot.runtime_capabilities.clone())
                .or_else(|| run_opts.runtime_capabilities.clone())
                .or_else(|| direct_memory_capabilities.clone())
                .unwrap_or_default();
            let capture = match &creation_snapshot {
                Some(snapshot) => capture.with_bundle(snapshot.bundle.clone()),
                None => capture,
            };
            if observable && !run_opts.execution_checked {
                if let Err(error) = this.governor.check_execution_capacity(Some(&parent_signal)) {
                    yield EngineEvent::TurnFailed { inference: None, error: internal_err(&error.to_string()) };
                    return;
                }
            }
            let resume_slot = forwarded_resume(&opts);
            let resume_key = resume_slot.string_value();
            let resume_map_key = resume_slot.in_memory_key(&agent_id);

            // Load any resume history FIRST.
            let (resume_history, resumed_agent_type, resumed_surfaced_tools) = match this
                .load_sub_agent_resume(&resume_slot, &resume_key, &resume_map_key,
                    creation_snapshot.as_ref().map(|snapshot| snapshot.fork_seed.as_slice()).unwrap_or_default())
                .await
            {
                Ok(loaded) => loaded,
                Err(error) => {
                    yield EngineEvent::TurnFailed { error, inference: None };
                    return;
                }
            };

            // Full-history children retain the captured conversation with explicit role overrides.
            let fork_conversation = run_opts.fork_conversation.clone();
            let fork_path = run_opts.prepared.as_ref().map(|prepared| prepared.fork_path)
                .unwrap_or_else(|| fork_conversation.is_some() && !resume_slot.is_present());
            let inherited_context = opts.inherited_context.as_ref().filter(|_| fork_path);
            if fork_path && creation_snapshot.is_none()
                && (run_opts.parent_agent_id.is_some() || run_opts.initiating_turn_id.is_some())
                && inherited_context.is_none() {
                yield EngineEvent::TurnFailed { inference: None,
                    error: internal_err("The initiating step's inherited context is unavailable."),
                };
                return;
            }

            let prepared = match run_opts.prepared.take() {
                Some(prepared) => prepared,
                None => match this.prepare_subagent_config(
                    &opts, capture, fork_path, resumed_agent_type.as_ref(),
                    creation_snapshot.as_deref(), &parent_signal,
                ).await {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        yield EngineEvent::TurnFailed { inference: None, error: internal_runtime_err(error) };
                        return;
                    }
                },
            };
            let type_name = prepared.type_name;
            let resuming_fork = prepared.resuming_fork;
            let def: Option<AgentDefinition> = prepared.definition;
            let child_effort = prepared.effort;
            let capture = prepared.capture;
            let child_bundle = capture.bundle.clone();
            let bundle = child_bundle;
            let child_model = bundle.config.model.clone();

            // A requested child policy can only narrow the captured parent authority.
            let parent_runtime_policy = this.runtime_policy_snapshot();
            let captured_untrusted_review = parent_runtime_policy.state.approval_policy
                == Some(crate::types::permission::ApprovalPolicy::UnlessTrusted)
                || this.ordinary_live_approval_policy(run_opts.parent_agent_id.as_deref())
                == crate::types::permission::ApprovalPolicy::UnlessTrusted
                || [
                    def.as_ref().and_then(|definition| definition.runtime_policy.as_ref()),
                    opts.runtime_policy.as_ref(),
                ].into_iter().flatten().any(|update| {
                    update.approval_policy == Some(crate::types::permission::ApprovalPolicy::UnlessTrusted)
                });
            let child_runtime_policy = if let Some(snapshot) = &creation_snapshot {
                snapshot.runtime_policy.clone()
            } else {
                let mut policy = parent_runtime_policy;
                for update in [
                    def.as_ref().and_then(|definition| definition.runtime_policy.as_ref()),
                    opts.runtime_policy.as_ref(),
                ].into_iter().flatten() {
                    match this.subagent_runtime_policy_override(&policy, update) {
                        Ok(restricted) => policy = restricted,
                        Err(error) => {
                            yield EngineEvent::TurnFailed { inference: None, error: internal_runtime_err(error) };
                            return;
                        }
                    }
                }
                policy
            };

            // A final raw selection determines the default collaboration policy.
            let child_ultra_mode = child_effort.as_ref().is_some_and(crate::schemas::effort::EffortValue::is_ultra);

            // Per-agent memory.
            let memory_bundle = match this
                .build_sub_agent_memory_bundle(&def, &type_name, &parent_signal)
                .await
            {
                Ok(bundle) => bundle,
                Err(error) => {
                    yield EngineEvent::TurnFailed { error, inference: None };
                    return;
                }
            };

            let effective_agent_id = resume_key.clone().unwrap_or_else(|| agent_id.clone());
            let effective_agent_map_key = resume_map_key
                .clone()
                .unwrap_or_else(|| SubAgentMapKey::String(agent_id.clone().into()));
            let lifecycle_kind = run_opts.lifecycle_kind.unwrap_or(SubagentKind::Foreground);
            let parent_agent_id = run_opts.parent_agent_id.clone();
            // This sub-agent's discovered-tools set (reuse live, else seed from resume).
            let agent_surfaced: Arc<Mutex<HashSet<RuntimeString>>> = {
                let existing = this
                    .subagents.surfaced
                    .lock_recover()
                    .get(&effective_agent_map_key)
                    .cloned();
                match existing {
                    Some(s) => s,
                    None => {
                        let s = Arc::new(Mutex::new(
                            resumed_surfaced_tools
                                .iter()
                                .cloned()
                                .collect::<HashSet<RuntimeString>>(),
                        ));
                        this.subagents.surfaced
                            .lock_recover()
                            .insert(effective_agent_map_key.clone(), s.clone());
                        s
                    }
                }
            };
            let mut sub_rollout: Option<Arc<dyn LiveRollout>> = None;
            let mut own_rollout_lease = None;
            let child_abort = parent_signal.child_token();
            let role_mcp = def.as_ref().and_then(|definition| definition.mcp_servers.as_deref())
                .unwrap_or_default();
            let mcp_runtime = if let Some(snapshot) = &creation_snapshot {
                snapshot.mcp_runtime.clone()
            } else {
                let mut parent_mcp = match opts.inherited_context.as_ref() {
                    Some(context) => context.mcp.clone(),
                    None => match this.ordinary_resume_context(run_opts.parent_agent_id.as_ref().map(|id| RuntimeString::from(id.clone())).as_ref()) {
                        Ok(context) => context.mcp,
                        Err(error) => {
                            yield EngineEvent::TurnFailed { inference: None, error: internal_runtime_err(error) };
                            return;
                        }
                    },
                };
                if let Some(tools) = &opts.tools {
                    parent_mcp.include_tools(tools);
                }
                this.connect_subagent_mcp(
                    parent_mcp,
                    role_mcp,
                    run_opts.cwd_override.clone().unwrap_or_else(|| this.cwd_now()),
                    child_abort.clone(),
                    bundle.config.tool_timeout_ms,
                    child_runtime_capabilities.capabilities().browser_tool_mapping.clone(),
                ).await
            };
            let _mcp_query_lease = mcp_runtime.begin_query(child_abort.clone());
            let mcp_clients = mcp_runtime.clients.clone();

            // Resolve the child's tool set.
            let (registered_tools, skill_catalog) = this.capture_tool_generation();
            let mut base_tools = inherited_context.map(|context| context.tools.clone()).unwrap_or(registered_tools);
            base_tools.retain(|tool| tool.native_mcp_binding().is_none());
            base_tools.extend(mcp_runtime.inherited_tools());
            let mut child_tools: Vec<AnyTool> = if fork_path && def.is_none() && opts.tools.is_none() {
                base_tools.clone()
            } else {
                let mut ct = match opts.tools.clone() {
                    Some(t) => t,
                    None => match def.as_ref().and_then(|d| d.tools.as_ref()) {
                        Some(names) => {
                            filter_tools_by_name(&base_tools, names)
                        }
                        None => base_tools.clone(),
                    },
                };
                if let Some(dt) = def.as_ref().and_then(|d| d.disallowed_tools.as_ref()) {
                    if !dt.is_empty() {
                        ct.retain(|tool| !dt.iter().any(|name| &tool.name() == name));
                    }
                }
                if memory_bundle.is_some() && def.as_ref().and_then(|d| d.tools.as_ref()).is_some() {
                    let present: HashSet<RuntimeString> =
                        ct.iter().map(|tool| tool.name()).collect();
                    for name in memory_tool_names() {
                        if present.contains(&RuntimeString::from(name)) {
                            continue;
                        }
                        if let Some(tool) = base_tools.iter().find(|t| t.name() == name) {
                            ct.push(tool.clone());
                        }
                    }
                }
                ct
            };

            if creation_snapshot.is_none() {
                child_tools.extend(mcp_runtime.role_tools(role_mcp));
            }

            // Consultation belongs to the parent query, whose live snapshot the child cannot own.
            child_tools.retain(|tool| tool.name() != "advisor");

            // Web-search selection for the sub-agent.
            let child_restricted = opts.tools.is_some() || def.as_ref().and_then(|d| d.tools.as_ref()).is_some();
            let child_capable = bundle.provider.capabilities(&child_model).web_search;
            let child_has_ws = child_tools.iter().any(|t| t.name() == CLIENT_WEB_SEARCH_TOOL);
            let child_web_selection = select_web_search(
                ensure_tool_search(child_tools),
                Some(&bundle.config.web_search),
                if child_restricted {
                    child_has_ws && child_capable
                } else {
                    child_capable
                },
            );
            child_tools = child_web_selection.tools;
            let browser_capabilities = child_runtime_capabilities.capabilities().clone();

            if let Some(snapshot) = &creation_snapshot {
                child_tools = snapshot.tools.clone();
                // Each new query captures the current catalog. Its immutable
                // snapshot remains stable until this query completes.
                if let Some(tool) = child_tools.iter_mut().find(|tool| tool.name() == "skill") {
                    if let Some(current) = base_tools.iter().find(|tool| tool.name() == "skill") {
                        *tool = current.clone();
                    }
                }
            }
            child_tools = match mcp_runtime.rebind_tools(child_tools) {
                Ok(tools) => tools,
                Err(error) => {
                    yield EngineEvent::TurnFailed { inference: None, error: internal_runtime_err(error) };
                    return;
                }
            };
            let available_dependencies: Vec<AnyTool> = match mcp_runtime.rebind_available(
                base_tools.clone(), &child_tools, browser_capabilities.browser_tool_mapping.as_ref(),
            ) {
                Ok(tools) => tools,
                Err(error) => {
                    yield EngineEvent::TurnFailed { inference: None, error: internal_runtime_err(error) };
                    return;
                }
            };

            if creation_snapshot.is_none() && browser_capabilities.browser_runtime.is_none() {
                crate::engine::browser_tool_dependencies::retain_browser_tool_dependencies(
                    &mut child_tools,
                    available_dependencies.iter().cloned(),
                    browser_capabilities.browser_tool_mapping.as_ref(),
                    def.as_ref().and_then(|definition| definition.disallowed_tools.as_deref()).unwrap_or_default(),
                    &this.config.disabled_tools,
                );
            }

            if creation_snapshot.is_none() && browser_capabilities.browser_runtime.is_none() {
                retain_browser_search_dependency(
                    &mut child_tools,
                    available_dependencies.iter().cloned(),
                    browser_capabilities.browser_tool_mapping.as_ref(),
                    def.as_ref()
                        .and_then(|definition| definition.disallowed_tools.as_deref())
                        .unwrap_or_default(),
                    &this.config.disabled_tools,
                );
            }

            // A restored authority snapshot is the ceiling. Apply this query's
            // captured collaboration view after restoring that original tool set.
            crate::planning::tools::retain_for_mode(
                &mut child_tools, child_collaboration_mode, false, false,
            );

            // Role prompt + optional critical reminder + memory + teammate addendum.
            let role_instructions = def
                .as_ref()
                .and_then(|d| d.prompt.clone().or_else(|| d.system_prompt.clone()));
            let critical_reminder = def.as_ref().and_then(|d| d.critical_system_reminder.clone());
            let memory_text = memory_bundle.as_ref().map(|m| m.text.clone());
            let child_cwd = run_opts.cwd_override.clone().unwrap_or_else(|| this.cwd_now());
            // The child's own turn id doubles as the spawn-turn attribution its
            // children will stamp: mint it before any lifecycle row so every
            // record carries the turn that owns it.
            let child_turn_id: RuntimeString = (this.deps.uuid)().into();
            let child_turn_started_at_ms = (this.deps.now)();
            let env_rebuild = this.system_prompt_is_default || inherited_context.is_some();
            let teammate_of: Option<SwarmIdentity> =
                match (&opts.swarm, this.swarm_identity.lock_recover().clone()) {
                    (Some(s), Some(id)) => Some(SwarmIdentity {
                        team: id.team,
                        peer: s.peer.clone(),
                    }),
                    _ => None,
                };

            let mut child_system_prompt: SystemPromptThunk = this.build_sub_agent_system_prompt(
                &def,
                &role_instructions,
                &critical_reminder,
                &memory_text,
                &teammate_of,
                env_rebuild,
                &child_cwd,
                &child_model,
                &bundle.config,
                inherited_context.map(|context| context.system_prompt.clone()),
            );

            // Open this sub-agent's OWN on-disk rollout when persisting.
            let mut subagent_dir_resolved = true;
            if let Some(store) = this.persistence.store().cloned() {
                let meta = crate::store::session_store::SubAgentMeta {
                    agent_type: type_name.clone(),
                    model: child_model.clone(),
                    permission_mode: None,
                    effort: child_effort.as_ref().map(effort_to_subagent),
                    created_at: (this.deps.now)(),
                };
                let session_id = runtime_session_id_to_session_store_host(&this.id);
                let writer_agent_id = effective_agent_id.clone();
                match this.subagents.parent_rollouts.acquire(
                    effective_agent_id.clone().into(), move || async move {
                        store.open_sub_agent(&session_id, &writer_agent_id, meta).await
                            .map(Arc::from).map_err(|error| RuntimeString::from(error.to_string()))
                    },
                ).await
                {
                    Ok(lease) => {
                        sub_rollout = Some(lease.writer());
                        own_rollout_lease = Some(Arc::new(lease));
                    }
                    // A child without its sidechain keeps running, but its
                    // transcript then lives only in memory. Record that here
                    // so the stream's teardown mirrors it for the read path
                    // instead of letting the gap surface as an empty history.
                    Err(error) => {
                        if run_opts.creation_admission.is_some() {
                            yield EngineEvent::TurnFailed { inference: None, error: internal_runtime_err(error) };
                            return;
                        }
                        subagent_dir_resolved = false;
                        tracing::warn!(
                            agent_id = %effective_agent_id,
                            error = ?error,
                            "sub-agent sidechain open failed; transcript stays in memory only"
                        );
                    }
                }
            }

            // The correlated merge prefix must be present on the exact first
            // provider request whose admission acknowledges the snapshot. Fork
            // transcripts and ordinary child transcripts share this prompt.
            let prompt_text = match memory_bundle.as_ref().and_then(|m| m.merge_prefix.as_ref()) {
                Some(prefix) => RuntimeString::from(prefix.clone())
                    .concat(&RuntimeString::from("\n\n"))
                    .concat(&opts.prompt),
                None => opts.prompt.clone(),
            };

            if let Some(rollout) = &sub_rollout {
                rollout.append_turn_context(crate::schemas::rollout::TurnContextSnapshot {
                    cwd: child_cwd.clone().into_runtime_string(), model: child_model.clone(),
                    permission_mode: crate::types::permission::ExternalPermissionMode::Default,
                    turn_id: Some(child_turn_id.clone()),
                    started_at_ms: Some(child_turn_started_at_ms),
                    approval_policy: child_runtime_policy.state.approval_policy,
                    sandbox_mode: child_runtime_policy.state.sandbox_mode,
                    approvals_reviewer: child_runtime_policy.state.approvals_reviewer,
                    network_allowed: Some(child_runtime_policy.state.network_allowed),
                    writable_roots: Some(child_runtime_policy.state.sandbox_workspace_write.writable_roots.clone()),
                }).await;
            }

            // Build the transcript (seed + this prompt).
            let transcript: Arc<Mutex<Vec<Message>>> =
                if fork_path && fork_conversation.is_some() {
                    let fc = fork_conversation.clone().unwrap();
                    let uuidfn = {
                        let d = this.deps.uuid.clone();
                        move || d()
                    };
                    let t = build_fork_transcript(&fc, prompt_text.clone(), &uuidfn);
                    if let Some(directive) = t.last() {
                        if let Some(r) = &sub_rollout {
                            r.append_message(directive.clone()).await;
                        }
                    }
                    Arc::new(Mutex::new(t))
                } else if run_opts.message_continuation {
                    Arc::new(Mutex::new(resume_history.clone()))
                } else if !run_opts.received_messages.is_empty() {
                    let mut v = resume_history.clone();
                    for message in &run_opts.received_messages {
                        if v.iter().any(|existing| super::message_uuid(existing) == super::message_uuid(message)) { continue; }
                        if let Some(r) = &sub_rollout { r.append_message(message.clone()).await; }
                        v.push(message.clone());
                    }
                    Arc::new(Mutex::new(v))
                } else {
                    let user_msg = Message::User(UserMessage {
            question_answer: None,
                        uuid: (this.deps.uuid)().into(),
                        content: vec![ContentBlock::Text(TextBlock {
                            text: prompt_text,
                            citations: None,
                            harness_envelope: None,
                            message_id: None,
                            phase: None,
                        })],
                        is_meta: None,
                    });
                    if let Some(r) = &sub_rollout {
                        r.append_message(user_msg.clone()).await;
                    }
                    let mut v = resume_history.clone();
                    v.push(user_msg);
                    Arc::new(Mutex::new(v))
                };

            // This immutable prefix belongs to the child. Its runtime-only
            // messages and resolved parent calls must survive independently of
            // later changes to the parent's rollout.
            let fork_seed = if fork_path {
                let history = transcript.lock_recover();
                history[..history.len().saturating_sub(1)].to_vec()
            } else {
                Vec::new()
            };

            // Only the new directive belongs to this run. A continuation starts
            // with old history and publishes its received input after handoff.
            let initial_prompts = if run_opts.message_continuation { Vec::new() } else if !run_opts.received_messages.is_empty() {
                transcript.lock_recover()[resume_history.len()..].to_vec()
            } else { transcript.lock_recover().last().cloned().into_iter().collect() };

            // Child rules and task-scoped writable roots.
            let mut grant_strings: Vec<RuntimeString> =
                opts.auto_allow_tools.clone().unwrap_or_default();
            if opts.memory_dir.is_some() {
                grant_strings.extend(memory_tool_names().iter().map(|name| (*name).into()));
            }
            let child_permission = if let Some(snapshot) = &creation_snapshot {
                snapshot.permission.clone()
            } else {
                let mut p = permission.clone();
                if !grant_strings.is_empty() {
                    p.allow_rules.extend(parse_rule_strings(&grant_strings));
                }
                if let Some(mb) = &memory_bundle {
                    p.additional_writable_roots.push(mb.dir.clone());
                }
                if let Some(md) = &opts.memory_dir {
                    p.additional_writable_roots.push(md.clone());
                }
                p
            };

            let sub_worktree = this.worktree_controller();
            // Sub-agent swarm identity cell.
            let child_swarm: Arc<Mutex<Option<SwarmIdentity>>> =
                Arc::new(Mutex::new(teammate_of.clone()));
            let child_swarm_id = child_swarm.lock_recover().clone();
            if let Some(id) = child_swarm_id {
                let _ = swarm_register_peer(
                    &id.team,
                    &id.peer,
                    Some(&this.id),
                    (this.deps.now)(),
                )
                .await;
            }
            let child_swarm_controller = {
                let g = child_swarm.clone();
                let s = child_swarm.clone();
                this.swarm_controller((
                    Arc::new(move || g.lock_recover().clone()),
                    Arc::new(move |id| {
                        *s.lock_recover() = id;
                    }),
                ))
            };

            // SubagentStart hook.
            if let Some(bus) = this.current_hook_bus() {
                if bus.has(HookEvent::SubagentStart) {
                    let mut hinput =
                        base_hook_input(HookEvent::SubagentStart, &this.cwd_now(), &this.id);
                    hinput.agent_type = type_name.clone();
                    hinput.agent_id = Some(effective_agent_id.clone().into());
                    let outcome = bus.dispatch(HookEvent::SubagentStart, &hinput).await;
                    if !outcome.additional_context.is_empty() {
                        let ctx_msg = Message::User(UserMessage {
            question_answer: None,
                            uuid: (this.deps.uuid)().into(),
                            content: vec![ContentBlock::Text(TextBlock {
                                text: this.render_notice_for_model(
                                    &join_runtime_strings(
                                        &outcome.additional_context,
                                        "\n",
                                    ),
                                    &child_model,
                                )
                                ,
                                citations: None,
                                harness_envelope: None,
                                message_id: None,
                                phase: None,
                            })],
                            is_meta: None,
                        });
                        transcript.lock_recover().push(ctx_msg.clone());
                        if let Some(r) = &sub_rollout {
                            r.append_message(ctx_msg).await;
                        }
                    }
                }
            }
            // Worktree notice (first turn of a worktree-isolated spawn).
            if let Some(notice) = &run_opts.worktree_notice {
                let wt_msg = Message::User(UserMessage {
            question_answer: None,
                    uuid: (this.deps.uuid)().into(),
                    content: vec![ContentBlock::Text(TextBlock {
                        text: notice.clone(),
                        citations: None,
                        harness_envelope: None,
                        message_id: None,
                        phase: None,
                    })],
                    is_meta: None,
                });
                transcript.lock_recover().push(wt_msg.clone());
                if let Some(r) = &sub_rollout {
                    r.append_message(wt_msg).await;
                }
            }

            // Per-turn reminders for the child.
            let child_nested_memory_triggers: Arc<Mutex<HashSet<RuntimePath>>> =
                Arc::new(Mutex::new(HashSet::new()));
            let child_nested_memory_loaded_paths: Arc<Mutex<HashSet<RuntimePath>>> =
                Arc::new(Mutex::new(HashSet::new()));
            // The sub-agent's OWN file-state cache.
            let child_file_state: Arc<dyn FileStateCache> = if fork_path {
                Arc::from(this.file_state.clone_box())
            } else {
                Arc::new(this.file_state.child_with_fresh_read_state())
            };
            let child_reminder_providers = this.build_sub_agent_reminder_providers(
                &run_opts,
                &memory_bundle,
                &effective_agent_id,
                &child_model,
                &bundle,
                &child_swarm,
                fork_path,
                opts.memory_dir.is_none(),
                opts.memory_dir.is_none(),
                &child_tools,
                skill_catalog.clone(),
                &child_file_state,
                &child_nested_memory_triggers,
                &child_nested_memory_loaded_paths,
                &child_turn_id,
                &sub_rollout,
            );

            // The worker's sandbox and network authority are immutable across follow-ups.
            let mut config = creation_snapshot.as_ref().map(|snapshot| snapshot.config.clone())
                .unwrap_or_else(|| bundle.config_for_model(&child_model));
            if let Some(residency_config) = &run_opts.residency_config {
                config.max_concurrent_sub_agents = residency_config.max_concurrent_sub_agents;
                config.max_concurrent_threads_per_session = residency_config.max_concurrent_threads_per_session;
            }
            if creation_snapshot.is_none() {
                // Config retains the captured canonical review floor; runtime_policy
                // separately retains the effective approval flow, including Never.
                config.approval_policy = if captured_untrusted_review {
                    Some(crate::types::permission::ApprovalPolicy::UnlessTrusted)
                } else {
                    child_runtime_policy.state.approval_policy
                };
            }
            config.ultra_mode = child_ultra_mode;
            config.effort = child_effort.clone();
            if opts.memory_dir.is_some() {
                config.stream_idle_timeout_ms = MEMORY_FORK_STREAM_IDLE_TIMEOUT_MS;
            }
            // Keep transport, pricing and identity services captured at child
            // admission while giving every child independent compaction state.
            let compaction_setup_failure = |message: String| {
                let this = this.clone();
                let agent_id = effective_agent_id.clone();
                let turn_id = child_turn_id.clone();
                let rollout = sub_rollout.clone();
                async move {
                let mut message = RuntimeString::from(message);
                if observable {
                    if let Err(error) = this.mark_subagent_failed(&agent_id, message.clone()).await {
                        message = message.concat(&RuntimeString::from("; lifecycle persistence failed: ")).concat(&error);
                    }
                }
                let mut event = EngineEvent::TurnFailed {
                    inference: None,
                    error: internal_runtime_err(message),
                };
                let mut failed_turn = SubagentTurnGuard {
                    session: this, agent_id,
                    turn_id, started_at_ms: child_turn_started_at_ms,
                    rollout, observable,
                    host_observable: observable, settled: false,
                };
                if let Err(error) = failed_turn.observe(&event, None).await {
                    event = EngineEvent::TurnFailed { inference: None, error: internal_runtime_err(error) };
                }
                event
                }
            };
            let mut child_bundle = (*bundle).clone();
            child_bundle.config = config.clone();
            child_bundle.deps.compactor = match bundle.deps.compactor.fork_isolated(
                bundle.deps.call_model_owned(),
                &config,
            ) {
                Ok(compactor) if !Arc::ptr_eq(&compactor, &bundle.deps.compactor) => compactor,
                Ok(_) => {
                    yield compaction_setup_failure("Cannot initialize child compaction: the configured compactor returned shared state".to_owned()).await;
                    return;
                }
                Err(error) => {
                    yield compaction_setup_failure(format!("Cannot initialize child compaction: {error}")).await;
                    return;
                }
            };
            // Rebind only the isolated instance. This preserves the captured
            // summary target and usage sink without retiring parent work.
            child_bundle.deps = match child_bundle.summary_deps(this.cost.clone(), this.subagents.parent_rollouts.usage_receipts(&RuntimeString::from(effective_agent_id.clone()))) {
                Ok(deps) => deps,
                Err(error) => {
                    yield compaction_setup_failure(format!("Cannot bind child summary transport: {error}")).await;
                    return;
                }
            };
            let bundle = Arc::new(child_bundle);
            let capture = capture.with_bundle(bundle.clone());
            let permission_is_frozen = creation_snapshot.is_some()
                || (opts.swarm.is_none() && opts.memory_dir.is_none());
            let child_permission_origin = creation_snapshot
                .as_ref()
                .map(|snapshot| snapshot.permission_origin.clone())
                .or_else(|| run_opts.permission_origin.clone())
                .flatten();
            let live_approval_policy = super::subagent_approval_policy_reader(
                Arc::downgrade(&this),
                run_opts.parent_agent_id.clone(),
            );
            let boundary_approval_policy = creation_snapshot.as_ref().and_then(|snapshot| snapshot.boundary_approval_policy);
            let child_permission_engine = if permission_is_frozen {
                subagent_permission_ceiling(&config, this.permission_engine.clone(), Some(live_approval_policy), boundary_approval_policy)
            } else {
                subagent_plan_permission_floor(&config, this.permission_engine.clone(), Some(live_approval_policy), boundary_approval_policy)
            };
            // Build the tool-context factory (mints a fresh ToolContext per tool call).
            let tool_context_base: ToolContextFactory = this
                .build_sub_agent_tool_context_factory(
                    depth,
                    &child_abort,
                    &child_file_state,
                    &child_permission,
                    child_permission_origin.clone(),
                    child_runtime_capabilities.clone(),
                    &child_model,
                    bundle.provider.clone(),
                    &run_opts.cwd_override,
                    &child_nested_memory_triggers,
                    &transcript,
                    &child_swarm,
                    &sub_worktree,
                    &child_swarm_controller,
                    opts.memory_dir.is_none(),
                    fork_path || run_opts.in_fork_child == Some(true)
                        || creation_snapshot.as_ref().is_some_and(|snapshot| snapshot.in_fork_child),
                    effective_agent_id.clone(),
                    task_plan.for_agent(&RuntimeString::from(effective_agent_id.clone())),
                    capture.clone(),
                    &child_turn_id,
                    child_runtime_policy.clone(),
                    child_permission_engine.clone(),
                    &child_tools,
                    permission_is_frozen,
                    child_collaboration_mode,
                    mcp_runtime.clone(),
                );
            let tool_context_base: ToolContextFactory = {
                let rollout_owner = own_rollout_lease.clone();
                Arc::new(move || {
                    let _owner = &rollout_owner;
                    tool_context_base()
                })
            };

            // Assemble the query params.
            // Inheriting history does not add a turn ceiling. Preserve only an
            // explicitly configured role or internal maintenance budget.
            let resolved_max_turns = run_opts
                .max_turns_override
                .or_else(|| def.as_ref().and_then(|definition| definition.max_turns));
            if opts.swarm.is_none() && opts.memory_dir.is_none() {
                let parts = match &creation_snapshot {
                    Some(snapshot) => snapshot.system_prompt.clone(),
                    None => {
                        let mut parts = child_system_prompt().await;
                        parts.retain(|part| part.name != "collaboration-mode");
                        parts.push(crate::planning::instructions::instruction_part(&config, child_collaboration_mode));
                        parts
                    }
                };
                let parts = mcp_runtime.rebind_instruction_parts(parts);
                let frozen_parts = parts.clone();
                child_system_prompt = Arc::new(move || {
                    let parts = frozen_parts.clone();
                    Box::pin(async move { parts })
                });
                if let Err(error) = this.freeze_ordinary_snapshot(&effective_agent_id, super::SubagentCreationSnapshot {
                    tools: child_tools.clone(), permission: child_permission.clone(), permission_origin: child_permission_origin.clone(), runtime_capabilities: child_runtime_capabilities.clone(), config: config.clone(),
                    definition: def.clone(), agent_type: type_name.clone(), bundle: bundle.clone(), runtime_policy: child_runtime_policy.clone(),
                    boundary_approval_policy,
                    collaboration_origin: creation_snapshot.as_ref()
                        .map(|snapshot| snapshot.collaboration_origin)
                        .unwrap_or_else(|| Some(super::SubagentCollaborationOrigin::capture(child_collaboration_mode))),
                    system_prompt: parts, cwd: child_cwd.clone(), max_turns: resolved_max_turns, depth,
                    effort: child_effort.clone(), ultra_mode: child_ultra_mode, output_schema: opts.output_schema.clone(), web_search: child_web_selection.request.clone(),
                    in_fork_child: fork_path || run_opts.in_fork_child == Some(true),
                    fork_seed,
                    name: opts.name.clone(), one_shot: crate::engine::builtin_agents::is_one_shot_builtin_role(
                        def.as_ref().and_then(|definition| definition.builtin_identity.as_ref())),
                    mcp_clients: mcp_clients.clone(),
                    mcp_runtime: mcp_runtime.clone(),
                    parent_agent_id: run_opts.parent_agent_id.clone(), task_plan: task_plan.clone(), task_list_id: this.task_list_id(),
                }, sub_rollout.clone()).await {
                    yield EngineEvent::TurnFailed { inference: None, error: internal_runtime_err(error) };
                    return;
                }
            }
            // Preserve the validated creation prompt, then derive exactly one
            // instruction view for this newly admitted query.
            {
                let base = child_system_prompt.clone();
                let config = config.clone();
                let mcp_runtime = mcp_runtime.clone();
                child_system_prompt = Arc::new(move || {
                    let base = base.clone();
                    let config = config.clone();
                    let mcp_runtime = mcp_runtime.clone();
                    Box::pin(async move {
                        let mut parts = mcp_runtime.rebind_instruction_parts(base().await);
                        parts.retain(|part| part.name != "collaboration-mode" && part.name != "advisor");
                        parts.push(crate::planning::instructions::instruction_part(&config, child_collaboration_mode));
                        parts
                    })
                });
            }
            if let Some(rollout) = &sub_rollout {
                if let Err(error) = rollout.flush().await {
                    yield EngineEvent::TurnFailed { inference: None, error: internal_err(&format!("Sub-agent creation metadata could not be persisted: {error}")) };
                    return;
                }
            }
            let context = SubagentContext {
                session_id: this.id.clone(),
                parent_agent_id: parent_agent_id.map(RuntimeString::from),
                parent_thread_id: Some(this.id.clone()), agent_type: type_name.clone(),
                cwd: child_cwd.as_runtime_string().clone(), description: opts.prompt.clone(), depth,
                resume_from: resume_key.clone().map(RuntimeString::from),
                spawn_turn_id: run_opts.initiating_turn_id.clone(),
            };
            let model_source = bundle.config.model_source.as_ref()
                .and_then(|attribution| attribution.source.as_ref()).map(RuntimeString::from);
            if let Some(admission) = &run_opts.creation_admission {
                this.stage_ordinary_rollout(&effective_agent_id, sub_rollout.clone());
                admission.stage(lifecycle_kind, context, child_model.clone(), model_source);
                let accepted = if run_opts.local_creation {
                    this.accept_local_subagent_creation(&effective_agent_id, admission).await
                } else {
                    this.await_ordinary_creation_acceptance(&effective_agent_id, admission).await
                };
                if let Err(error) = accepted {
                    yield EngineEvent::TurnFailed { inference: None, error: internal_runtime_err(error) };
                    return;
                }
            } else if observable {
                if let Err(error) = this.record_subagent_started(&effective_agent_id, lifecycle_kind, context).await {
                    yield EngineEvent::TurnFailed { inference: None, error: internal_runtime_err(error) };
                    return;
                }
                if let Err(error) = this.record_subagent_model(&effective_agent_id, &child_model, model_source).await {
                    yield EngineEvent::TurnFailed { inference: None, error: internal_runtime_err(error) };
                    return;
                }
            }
            // Execution ownership begins with the running task, independently
            // from the earlier read-only creation capacity check.
            let _release_guard = SlotGuard(observable.then(|| this.governor.admit_turn()));
            if opts.memory_dir.is_none() {
                for prompt in &initial_prompts {
                    this.publish_subagent_prompt(&effective_agent_id, prompt);
                }
            }
            if run_opts.message_continuation {
                let mut history = transcript.lock_recover().clone();
                let previous_len = history.len();
                if let Err(error) = this.append_ordinary_continuation(&effective_agent_id, &mut history, sub_rollout.as_ref()).await {
                    yield EngineEvent::TurnFailed { inference: None, error: internal_runtime_err(error) };
                    return;
                }
                if opts.memory_dir.is_none() {
                    for message in &history[previous_len..] {
                        this.publish_subagent_prompt(&effective_agent_id, message);
                    }
                }
                *transcript.lock_recover() = history;
            }
            let messages_snapshot = transcript.lock_recover().clone();
            let cost_so_far: crate::engine::query::CostSoFar = {
                let c = this.cost.clone();
                Box::new(move || c.lock_recover().total_cost_usd())
            };
            let on_message: crate::engine::query::OnMessage = {
                let this_m = this.clone();
                let transcript_m = transcript.clone();
                let sub_rollout_m = sub_rollout.clone();
                let message_observer = run_opts.message_observer.clone();
                let input_agent_id = effective_agent_id.clone();
                let publish_input = opts.memory_dir.is_none();
                let output_turn_id = child_turn_id.clone();
                Box::new(move |m: Message| {
                    let this_m = this_m.clone();
                    let transcript_m = transcript_m.clone();
                    let sub_rollout_m = sub_rollout_m.clone();
                    let message_observer = message_observer.clone();
                    let input_agent_id = input_agent_id.clone();
                    let output_turn_id = output_turn_id.clone();
                    Box::pin(async move {
                        let mut records = vec![crate::schemas::rollout::RolloutItem::Message { message: m.clone() }];
                            // Inspection output must survive child history replay even
                            // when its provider cannot receive the original image.
                            if let Message::ToolResult(result) = &m {
                                if let Some(output) = &result.output {
                                    let mut payload = RuntimeObject::new();
                                    payload.insert("toolUseId", RuntimeJson::String(result.tool_use_id.clone()));
                                    payload.insert("output", output.clone());
                                    records.push(crate::schemas::rollout::RolloutItem::Event(crate::schemas::rollout::EventItem {
                                        event: "tool_result_output".into(),
                                        turn_id: Some(output_turn_id.clone()),
                                        payload: Some(RuntimeJson::Object(payload)),
                                    }));
                                }
                            }
                        let admission = match &sub_rollout_m {
                            Some(writer) => Some(writer.prepare_batch(records.len()).await.map_err(|error| RuntimeString::from(error.to_string()))?),
                            None => None,
                        };
                        {
                            let mut transcript = transcript_m.lock_recover();
                            transcript.push(m.clone());
                            if let Some(admission) = admission {
                                admission.commit(records);
                            }
                        }
                        if let Some(observer) = &message_observer {
                            observer(&m);
                        }
                        if publish_input && Self::is_ordinary_communication(&m) {
                            this_m.publish_subagent_prompt(&input_agent_id, &m);
                        }
                        Ok(())
                    })
                })
            };
            let on_compaction: crate::engine::query::OnCompaction = {
                let transcript_c = transcript.clone();
                let sub_rollout_c = sub_rollout.clone();
                let file_state_c = child_file_state.clone();
                let nested_loaded_c = child_nested_memory_loaded_paths.clone();
                let nested_triggers_c = child_nested_memory_triggers.clone();
                Box::new(move |rec: QueryCompactionRecord| {
                    let transcript_c = transcript_c.clone();
                    let sub_rollout_c = sub_rollout_c.clone();
                    let file_state_c = file_state_c.clone();
                    let nested_loaded_c = nested_loaded_c.clone();
                    let nested_triggers_c = nested_triggers_c.clone();
                    Box::pin(async move {
                        let admission = match &sub_rollout_c {
                            Some(writer) => Some(writer.prepare_append().await.map_err(|error| RuntimeString::from(error.to_string()))?),
                            None => None,
                        };
                        let new_summary = rec.retires_context && rec.replacement_history.iter().any(|message| {
                            matches!(message, Message::CompactBoundary(boundary)
                                if !rec.source_message_ids.contains(&boundary.uuid))
                        });
                        if new_summary {
                            file_state_c.clear();

                            nested_loaded_c.lock_recover().clear();
                            nested_triggers_c.lock_recover().clear();
                        }
                        let mut transcript = transcript_c.lock_recover();
                        *transcript = rec.replacement_history.clone();
                        if let Some(admission) = admission {
                            let record = to_store_compaction(&rec);
                            admission.commit(crate::schemas::rollout::RolloutItem::Compacted(crate::schemas::rollout::CompactedItem {
                                summary: record.summary,
                                replaced_range: record.replaced_range,
                                replacement_history: record.replacement_history,
                                source_message_ids: record.source_message_ids,
                                approval_parent_history: record.approval_parent_history,
                            }));
                        }
                        Ok(())
                    })
                })
            };
            let on_permission_update: crate::engine::query::OnPermissionUpdate = {
                let this_p = this.clone();
                Box::new(move |updates: Vec<PermissionUpdate>| {
                    for u in updates {
                        this_p.apply_permission_update(u);
                    }
                })
            };
            let on_surface_tools: crate::engine::query::OnSurfaceTools = {
                let this_s = this.clone();
                let surfaced = agent_surfaced.clone();
                let sink = sub_rollout.clone();
                Box::new(move |names: Vec<RuntimeString>| {
                    let this_s = this_s.clone();
                    let surfaced = surfaced.clone();
                    let sink = sink.clone();
                    Box::pin(async move {
                        this_s.record_surfaced_tools(&surfaced, sink.as_ref(), names).await
                    })
                })
            };
            let edited_files_reminder: Option<Arc<dyn ReminderProvider>> =
                if this.edited_files_reminder.get().and_then(|o| o.as_ref()).is_some() {
                    let fs = child_file_state.clone();
                    Some(Arc::new(create_edited_files_reminder_provider(move || {
                        fs.clone()
                    })))
                } else {
                    None
                };

            let sub_thread_suffix = RuntimeString::from(":sub:")
                .concat(&RuntimeString::from((this.deps.uuid)()));
            let snapshot_provider_request_admitted = memory_bundle
                .as_ref()
                .and_then(|bundle| {
                    debug_assert_eq!(
                        bundle.merge_prefix.is_some(),
                        bundle.merge_request_id.is_some(),
                        "snapshot merge prefix and admission token must travel together"
                    );
                    bundle.merge_request_id.clone()
                })
                .map(|request_id| {
                    let session = this.clone();
                    let agent_type = type_name
                        .as_ref()
                        .map(runtime_forwarded_slot_to_typed_host)
                        .unwrap_or_default();
                    let applied = Arc::new(AtomicBool::new(false));
                    Arc::new(move || {
                        let session = session.clone();
                        let request_id = request_id.clone();
                        let agent_type = agent_type.clone();
                        let applied = applied.clone();
                        Box::pin(async move {
                            if applied.load(Ordering::Acquire) {
                                return Ok(());
                            }
                            session
                                .apply_agent_memory_snapshot_merge_after_provider_admission(
                                    &request_id,
                                )
                                .await
                                .map_err(|error| {
                                    agent_memory_snapshot_reconcile_error(
                                        "provider-admission-apply",
                                        &agent_type,
                                        &error,
                                    )
                                })?;
                            applied.store(true, Ordering::Release);
                            Ok(())
                        }) as BoxFuture<'static, Result<(), RuntimeString>>
                    }) as crate::engine::query::OnProviderRequestAdmitted
                });
            // A planning sub-agent's own plan document under the shared slug.
            let child_plan_file_path = this
                .plan_file_context(Some(&RuntimeString::from(effective_agent_id.clone())))
                .map(|p| p.path);
            // A child run owns its own budget memory (its history is its own
            // snapshot), so freezing ids here cannot leak into the parent's
            // decisions and vice versa.
            let child_budget_state = Some(Arc::new(Mutex::new(
                crate::context::tool_result_budget::ReplacementState::reconstruct(
                    &messages_snapshot,
                    Vec::new(),
                ),
            )));
            // Wire-level cache routing for a spawned child: siblings of one
            // delegation share a parent-scoped domain so their common prefix
            // caches once; fork continuations ride the parent's own key.
            let child_cache_domain = if fork_path || resuming_fork {
                None
            } else {
                let domain = subagent_cache_domain(&this.id, type_name.as_ref());
                tracing::debug!(
                    parent_thread = %this.id.try_to_utf8().unwrap_or_default(),
                    cache_domain = %domain.try_to_utf8().unwrap_or_default(),
                    "derived subagent prompt-cache domain from parent thread"
                );
                Some(domain)
            };
            let mut params = QueryParams {
                retained_tool_tasks: Some(this.persistence.tool_tasks.clone()),
                prepare_request_usage: Some(this.request_usage_recorder({
                    let bundle = bundle.clone();
                    Arc::new(move || bundle.clone())
                })),
                multi_agent_instructions: observable,
                thread_id: this.id.concat(&sub_thread_suffix),
                turn_id: child_turn_id.clone(),
                model: child_model.clone(),
                provider_info: Some(bundle.provider.info().clone()),
                messages: messages_snapshot,
                compaction_protected_message_ids: HashSet::new(),
                context_window: None,
                precompute: None,
                compaction_history: Some({
                    let transcript = transcript.clone();
                    Arc::new(move || transcript.lock_recover().clone())
                }),
                recover_compaction_history: None,
                compaction_output: Some({
                    let transcript = transcript.clone();
                    let rollout = sub_rollout.clone();
                    Arc::new(move |message: Message| {
                        let transcript = transcript.clone();
                        let rollout = rollout.clone();
                        Box::pin(async move {
                            let prepared = match &rollout {
                                Some(rollout) => match rollout.prepare_append().await {
                                    Ok(prepared) => Some(prepared),
                                    Err(error) => { tracing::warn!(%error, "Child compaction output persistence failed"); return; }
                                },
                                None => None,
                            };
                            transcript.lock_recover().push(message.clone());
                            if let Some(prepared) = prepared {
                                prepared.commit(crate::schemas::rollout::RolloutItem::Message { message });
                            }
                        })
                    })
                }),
                tools: child_tools.clone(),
                tool_specs: to_tool_specs(&child_tools),
                refresh_tools: None,
                refresh_tools_async: None,
                agent_tool_policy: this.root_agent_definition.clone(),
                system_prompt: SystemPromptResolvable::Fn(child_system_prompt),
                config,
                max_turns: resolved_max_turns,
                cost_so_far: Some(cost_so_far),
                tool_context_base,
                permission_engine: child_permission_engine.clone(),
                hook_bus: this.current_hook_bus(),
                signal: child_abort.clone(),
                // Memory forks keep their pinned file boundary, but a sensitive
                // host-selected root still needs the session's approval channel.
                // Ordinary sub-agents do not inherit the parent's approval UI.
                approval_handler: if opts.memory_dir.is_some() {
                    this.approval_handler.clone()
                } else {
                    None
                },
                approval_reviewer: Some(this.approval_reviewer_for_turn(
                    bundle.clone(),
                    if opts.memory_dir.is_some() { this.approval_reviewer.clone() } else { None },
                )),
                output_schema: creation_snapshot.as_ref().map(|snapshot| snapshot.output_schema.clone())
                    .unwrap_or_else(|| opts.output_schema.clone()).map(RuntimeJson::Object),
                token_budget: None,
                goal: None,
                goal_request_identity: None,
                goal_pending_input: None,
                goal_execution_enabled: None,
                goal_usage: goal_usage.clone(),
                web_search: creation_snapshot.as_ref().map(|snapshot| snapshot.web_search.clone())
                    .unwrap_or(child_web_selection.request),
                on_message: Some(on_message),
                message_rollout: sub_rollout.clone(),
                on_permission_update: Some(on_permission_update),
                on_compaction: Some(on_compaction),
                surfaced_tools: Some(agent_surfaced.lock_recover().iter().cloned().collect()),
                on_surface_tools: Some(on_surface_tools),
                on_tool_lifecycle: tool_lifecycle_rollout_writer(sub_rollout.clone()),
                provider_call_admission: None,
                on_provider_request_admitted: None,
                emit_thread_started: false,
                reminder_providers: child_reminder_providers,
                edited_files_reminder,
                effort: child_effort,
                ultra_mode: child_ultra_mode,
                thinking: if (fork_path || resuming_fork) && bundle.config.thinking {
                    Some(ThinkingConfig::Adaptive)
                } else {
                    None
                },
                skip_cache_write: if fork_path || resuming_fork { Some(true) } else { None },
                cache_ttl: if this.config.cache1h_ttl {
                    Some(CacheTtl::OneHour)
                } else {
                    None
                },
                cache_key: Some(if fork_path || resuming_fork {
                    this.id.clone()
                } else {
                    effective_agent_id.clone().into()
                }),
                cache_domain: child_cache_domain,
                plan_file_path: child_plan_file_path.clone(),
                message_queue: Some(this.conversation.queue().clone()),
                input_pdf_bytes: 0,
                input_pdf_documents: 0,
                validate_native_pdf: {
                    let provider = bundle.provider.clone();
                    Some(Arc::new(move |model| {
                        validate_native_pdf_provider(provider.as_ref(), model)
                    }))
                },
                agent_id: Some(effective_agent_id.clone().into()),
                agent_type: type_name.clone(),
                // Sub-agents never run the idle pass: their lifetime is one short
                // stretch of work, so a gap long enough to cool the prompt cache
                // means the sub-agent has already finished.
                context_hint_state: None,
                file_state: None,
                tool_result_budget_state: child_budget_state,
                on_tool_result_budget_records: None,
                // Sub-agents consult the main thread's cache-cold decision but never
                // advance it.
                thinking_context_cold: this.turn.thinking_context_cold(),
                padded_countdown_spend: this.turn.padded_countdown_spend(),
                task_countdown_anchor: None,
                live_model_override: None,
            };
            params.on_provider_request_admitted = compose_provider_request_admitted(
                params.on_provider_request_admitted.take(),
                snapshot_provider_request_admitted,
            );

            let mut turn_guard = SubagentTurnGuard {
                session: this.clone(), agent_id: effective_agent_id.clone(),
                turn_id: child_turn_id.clone(), started_at_ms: child_turn_started_at_ms,
                rollout: sub_rollout.clone(), observable,
                host_observable: observable, settled: false,
            };
            let mut gen = crate::engine::query::query_with_inference(params, bundle.deps.to_engine_deps(),
                crate::engine::query::InferenceCapture { slot: Arc::new(Mutex::new(Some(capture))), next: None, model_override: None, effort_override: None, ultra_override: None });
            let mut final_agent_text: Option<RuntimeString> = None;
            let mut final_agent_message_id: Option<RuntimeString> = None;
            let mut lifecycle_terminal: Option<SubagentStatus> = None;
            let mut lifecycle_error: Option<RuntimeString> = None;
            let _skill_receipts = this.invoked_skill_scope(effective_agent_id.clone().into());
            while let Some(ev) = gen.next().await {
                let mut lifecycle_publication = Ok(());
                match &ev {
                    EngineEvent::ItemCompleted {
                        inference: _,
                        item: ThreadItem::AgentMessage { id, text, phase },
                    } => {
                        if phase.as_ref().is_none_or(|phase| phase != "commentary") {
                            final_agent_message_id = Some(id.clone());
                        }
                        final_agent_text = Some(text.clone());
                    }
                    EngineEvent::TurnFailed { error, .. } => {
                        lifecycle_terminal = Some(SubagentStatus::Failed);
                        let error = error.message.clone();
                        lifecycle_error = Some(error.clone());
                        if observable {
                            lifecycle_publication = this.mark_subagent_failed(&effective_agent_id, error).await;
                        }
                    }
                    EngineEvent::TurnCompleted { stop_reason, .. } => match stop_reason {
                        TerminalReason::Completed => {
                            lifecycle_terminal = Some(SubagentStatus::Completed);
                            if observable {
                                lifecycle_publication = this.mark_subagent_completed(
                                    &effective_agent_id,
                                    final_agent_text.clone(),
                                ).await;
                            }
                        }
                        TerminalReason::Aborted => {
                            lifecycle_terminal = Some(SubagentStatus::Cancelled);
                            if observable {
                                lifecycle_publication = this.mark_subagent_cancelled(&effective_agent_id).await;
                            }
                        }
                        other @ (TerminalReason::TurnLimitExhausted
                        | TerminalReason::SpendLimitExhausted) => {
                            lifecycle_terminal = Some(SubagentStatus::BudgetLimited);
                            let reason = RuntimeString::from(match other {
                                TerminalReason::TurnLimitExhausted => {
                                    "sub-agent reached its configured turn limit"
                                }
                                _ => "sub-agent reached the session spending limit",
                            });
                            if observable {
                                lifecycle_publication = this.mark_subagent_budget_limited(
                                    &effective_agent_id,
                                    final_agent_text.clone(),
                                    reason,
                                ).await;
                            }
                        }
                        other => {
                            lifecycle_terminal = Some(SubagentStatus::Failed);
                            let error = lifecycle_error.clone().unwrap_or_else(|| {
                                RuntimeString::from(format!("sub-agent ended with {other:?}"))
                            });
                            if observable {
                                lifecycle_publication = this.mark_subagent_failed(&effective_agent_id, error).await;
                            }
                        }
                    },
                    _ => {}
                }
                if let Err(error) = lifecycle_publication {
                    lifecycle_terminal = Some(SubagentStatus::Failed);
                    let failed = EngineEvent::TurnFailed { inference: None, error: internal_runtime_err(error) };
                    let terminal = turn_guard.observe(&failed, None).await;
                    yield failed;
                    if let Err(error) = terminal {
                        yield EngineEvent::TurnFailed { inference: None, error: internal_runtime_err(error) };
                    }
                    break;
                }
                if let Err(error) = turn_guard.observe(&ev, final_agent_message_id.as_ref()).await {
                    lifecycle_terminal = Some(SubagentStatus::Failed);
                    yield EngineEvent::TurnFailed { inference: None, error: internal_runtime_err(error) };
                    break;
                }
                yield ev;
            }
            if !turn_guard.settled {
                if let Err(error) = turn_guard.finish().await {
                    yield EngineEvent::TurnFailed { inference: None, error: internal_runtime_err(error) };
                } else if lifecycle_terminal.is_none() {
                    lifecycle_terminal = Some(SubagentStatus::Cancelled);
                }
            }
            if let Err(error) = this.finish_sub_agent_stream(
                &lifecycle_terminal,
                observable,
                &child_abort,
                &parent_signal,
                &effective_agent_id,
                effective_agent_map_key,
                &transcript,
                &sub_rollout,
                subagent_dir_resolved,
                &mcp_clients,
            )
            .await {
                yield EngineEvent::TurnFailed { inference: None, error: internal_runtime_err(error) };
            }
            drop(own_rollout_lease);
        })
    }

    /// Build the sub-agent's system-prompt thunk: role prompt + optional
    /// critical reminder + memory + teammate addendum, with an env-block
    /// rebuild when the child diverges in model or cwd. Falls back to the
    /// parent's provider when nothing child-specific applies.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build_sub_agent_system_prompt(
        self: &Arc<Self>,
        def: &Option<AgentDefinition>,
        role_instructions: &Option<RuntimeString>,
        critical_reminder: &Option<RuntimeString>,
        memory_text: &Option<String>,
        teammate_of: &Option<SwarmIdentity>,
        env_rebuild: bool,
        child_cwd: &RuntimePath,
        child_model: &RuntimeString,
        inference_config: &crate::schemas::config::ResolvedConfig,
        inherited_system: Option<Vec<SystemPromptPart>>,
    ) -> SystemPromptThunk {
        if role_instructions.is_some()
            || critical_reminder.is_some()
            || memory_text.is_some()
            || teammate_of.is_some()
            || env_rebuild
        {
            let role_name = def
                .as_ref()
                .map(|d| d.name.clone())
                .unwrap_or_else(|| RuntimeString::from("agent"));
            let base_provider: SystemPromptThunk = match inherited_system {
                Some(parts) => Arc::new(move || {
                    let parts = parts.clone();
                    Box::pin(async move { parts })
                }),
                None => self.system_prompt_provider.read_recover().clone(),
            };
            let config = inference_config.clone();
            let child_cwd2 = child_cwd.clone();
            let child_model2 = child_model.clone();
            let ri = role_instructions.clone();
            let cr = critical_reminder.clone();
            let mt = memory_text.clone();
            let to = teammate_of.clone();
            Arc::new(move || {
                let base_provider = base_provider.clone();
                let config = config.clone();
                let child_cwd2 = child_cwd2.clone();
                let child_model2 = child_model2.clone();
                let ri = ri.clone();
                let cr = cr.clone();
                let mt = mt.clone();
                let to = to.clone();
                let role_name = role_name.clone();
                Box::pin(async move {
                    let mut parts = (base_provider)().await;
                    if env_rebuild {
                        for p in parts.iter_mut() {
                            if p.name == "env" {
                                p.text = environment_block(
                                    &child_cwd2,
                                    &config,
                                    EnvBlockOptions {
                                        plat: None,
                                        shell: None,
                                        model: Some(child_model2.clone()),
                                        is_worktree: false,
                                    },
                                );
                            }
                        }
                    }
                    if let Some(ri) = &ri {
                        parts.retain(|part| !part.name.to_node_utf8().starts_with(b"agent:"));
                        let part_name = RuntimeString::from("agent:").concat(&role_name);
                        parts.push(SystemPromptPart {
                            name: part_name,
                            text: ri.clone(),
                            cacheable: Some(true),
                            cache_scope: None,
                        });
                    }
                    if let Some(mt) = &mt {
                        let part_name = RuntimeString::from("agent:")
                            .concat(&role_name)
                            .concat(&RuntimeString::from(":memory"));
                        parts.push(SystemPromptPart {
                            name: part_name,
                            text: mt.clone().into(),
                            cacheable: Some(true),
                            cache_scope: None,
                        });
                    }
                    if let Some(to) = &to {
                        let part_name = RuntimeString::from("agent:")
                            .concat(&role_name)
                            .concat(&RuntimeString::from(":teammate"));
                        parts.push(SystemPromptPart {
                            name: part_name,
                            text: teammate_prompt(&to.team, &to.peer),
                            cacheable: Some(true),
                            cache_scope: None,
                        });
                    }
                    if let Some(cr) = &cr {
                        let part_name = RuntimeString::from("agent:")
                            .concat(&role_name)
                            .concat(&RuntimeString::from(":critical"));
                        parts.push(SystemPromptPart {
                            name: part_name,
                            text: RuntimeString::from("IMPORTANT: ").concat(cr),
                            cacheable: Some(true),
                            cache_scope: None,
                        });
                    }
                    parts
                })
            })
        } else {
            self.system_prompt_provider.read_recover().clone()
        }
    }

    /// Build the sub-agent's tool-context factory (mints a fresh ToolContext
    /// per tool call) with child-scoped permission, abort, file-state, swarm,
    /// worktree, and spawn wiring.
    #[allow(clippy::too_many_arguments)]
    fn build_sub_agent_tool_context_factory(
        self: &Arc<Self>,
        depth: u32,
        child_abort: &AbortSignal,
        child_file_state: &Arc<dyn FileStateCache>,
        child_permission: &PermissionContext,
        permission_origin: Option<SubagentPermissionOrigin>,
        runtime_capabilities: SubagentCapabilityCapture,
        child_model: &RuntimeString,
        provider: Arc<dyn crate::types::provider::ModelProvider>,
        cwd_override: &Option<RuntimePath>,
        child_nested_memory_triggers: &Arc<Mutex<HashSet<RuntimePath>>>,
        transcript: &Arc<Mutex<Vec<Message>>>,
        child_swarm: &Arc<Mutex<Option<SwarmIdentity>>>,
        sub_worktree: &Option<Arc<dyn WorktreeController>>,
        child_swarm_controller: &Option<Arc<dyn SwarmController>>,
        memory_dir_is_none: bool,
        grandchild_in_fork: bool,
        parent_agent_for_children: String,
        task_plan: crate::tasks::task_store::TaskPlanBinding,
        // This child's own frozen capture. A grandchild it spawns must inherit
        // the same bundle rather than re-capturing from the live session, so the
        // "step A's spawn keeps A" invariant holds at every nesting depth.
        child_capture: Arc<InferenceActivityGuard>,
        // This child's own turn: the initiating turn for anything IT spawns.
        child_turn_id: &RuntimeString,
        runtime_policy: Arc<super::RuntimePolicySnapshot>,
        permission_engine: Arc<dyn crate::permission::engine::PermissionEngine>,
        child_tools: &[AnyTool],
        permission_is_frozen: bool,
        collaboration_mode: crate::planning::CollaborationMode,
        mcp_runtime: Arc<super::SubagentMcpRuntime>,
    ) -> ToolContextFactory {
        let this = self.clone();
        let child_turn_id = child_turn_id.clone();
        let bash_tool = child_tools
            .iter()
            .find(|tool| tool.name() == "bash")
            .cloned();
        let child_file_state = child_file_state.clone();
        let child_permission = child_permission.clone();
        let child_model = child_model.clone();
        let cwd_override = cwd_override.clone();
        let child_abort = child_abort.clone();
        let child_nested_memory_triggers = child_nested_memory_triggers.clone();
        let transcript = transcript.clone();
        let child_swarm = child_swarm.clone();
        let sub_worktree = sub_worktree.clone();
        let child_swarm_controller = child_swarm_controller.clone();
        let lsp_manager = this.current_lsp_manager();
        let browser = runtime_capabilities.capabilities().clone();
        let planning_read_only = collaboration_mode == crate::planning::CollaborationMode::Plan;
        let child_mcp_capture = mcp_runtime.capture(child_tools);
        Arc::new(move || {
            let cwd = cwd_override.clone().unwrap_or_else(|| this.cwd_now());
            let current = this.runtime_policy_snapshot();
            let policy = intersect_subagent_runtime_policy(&runtime_policy, &current);
            let current_permission = this.permission_context_for_policy(&current);
            let descendant_permission_origin =
                SubagentPermissionOrigin::capture(&current_permission);
            let mut permission = project_subagent_permission(
                &child_permission,
                &current_permission,
                permission_origin.as_ref(),
                permission_is_frozen,
            );
            super::constrain_subagent_canonical_roots(
                &mut permission,
                &runtime_policy.state,
                &policy.state,
                permission_origin.as_ref(),
            );
            let mut ctx = bare_tool_context(
                this.process_context.clone(),
                this.background_processes.clone(),
                cwd,
                child_abort.clone(),
                child_file_state.clone(),
                permission,
                policy.sandbox_backend.clone(),
            );
            {
                let session = this.clone();
                let turn = child_turn_id.clone();
                let agent = RuntimeString::from(parent_agent_for_children.clone());
                ctx.apply_skill_context = Some(Box::new(move |change| {
                    if let Some(path) = change.invoked_skill_path {
                        if let Some(content) = change.invoked_skill_content {
                            session.record_invoked_skill_content(
                                Some(agent.clone()),
                                &path,
                                &content,
                            );
                        }
                        session.record_invoked_skill(Some(agent.clone()), &turn, &path);
                    }
                }));
            }
            ctx.planning_read_only = planning_read_only;
            super::apply_subagent_runtime_policy(&policy, &mut ctx, !memory_dir_is_none);
            let child_caps = provider.capabilities(&child_model);
            ctx.native_pdf = Some(child_caps.native_pdf);
            ctx.vision = Some(child_caps.vision);
            if memory_dir_is_none {
                let this_a = this.clone();
                let cwd_ov = cwd_override.clone();
                let triggers = child_nested_memory_triggers.clone();
                ctx.activate_skill_paths = Some(Box::new(move |paths: Vec<RuntimePath>| {
                    let child_cwd = cwd_ov.clone().unwrap_or_else(|| this_a.cwd_now());
                    this_a.activate_skill_paths(&paths, &child_cwd);
                    this_a.feed_nested_memory_triggers(&triggers, &paths, &child_cwd);
                }));
            }
            {
                let this_b = this.clone();
                let perm = ctx.permission.clone();
                let shell_policy = policy.clone();
                let shell_permission_engine = permission_engine.clone();
                let shell_permission_origin = permission_origin.clone();
                let bash_tool = bash_tool.clone();
                let cwd_ov = cwd_override.clone();
                let sig = child_abort.clone();
                ctx.run_bash = Some(Arc::new(
                    move |command: String, bash_opts: Option<RunBashOptions>| {
                        let this_b = this_b.clone();
                        let current = this_b.runtime_policy_snapshot();
                        let mut perm = project_subagent_permission(
                            &perm,
                            &this_b.permission_context_for_policy(&current),
                            shell_permission_origin.as_ref(),
                            permission_is_frozen,
                        );
                        let captured_shell_policy = shell_policy.clone();
                        let shell_policy =
                            intersect_subagent_runtime_policy(&shell_policy, &current);
                        super::constrain_subagent_canonical_roots(
                            &mut perm,
                            &captured_shell_policy.state,
                            &shell_policy.state,
                            shell_permission_origin.as_ref(),
                        );
                        let bash_opts = bash_opts.map(|mut options| {
                            if permission_is_frozen || planning_read_only {
                                options.allow_tools = None;
                            }
                            options
                        });
                        let shell_permission_engine = shell_permission_engine.clone();
                        let bash_tool = bash_tool.clone();
                        let sig = sig.clone();
                        let cwd = cwd_ov.clone().unwrap_or_else(|| this_b.cwd_now());
                        Box::pin(async move {
                            this_b
                                .run_skill_bash_with_policy(
                                    command,
                                    perm,
                                    runtime_path_to_skill_shell_host(&cwd),
                                    sig,
                                    bash_opts,
                                    SkillShellAuthority {
                                        planning_read_only,
                                        bash_tool,
                                        runtime_policy: shell_policy,
                                        permission_engine: shell_permission_engine,
                                    },
                                )
                                .await
                        })
                    },
                ));
            }
            ctx.depth = Some(depth);
            if this.config.forward_subagent_text {
                ctx.forward_subagent_text = Some(true);
            }
            ctx.spawn_sub_agent = Some(this.make_spawn_sub_agent_with_permission_origin(
                depth,
                child_abort.clone(),
                ctx.permission.clone(),
                {
                    let t = transcript.clone();
                    Arc::new(move || t.lock_recover().clone())
                },
                grandchild_in_fork,
                Some(parent_agent_for_children.clone()),
                Some(task_plan.fork()),
                Some(child_capture.clone()),
                Some(child_turn_id.clone()),
                Some(descendant_permission_origin.clone()),
                Some(runtime_capabilities.clone()),
                Some(collaboration_mode),
            ));
            ctx.spawn_background_agent =
                Some(this.make_spawn_background_agent_with_permission_origin(
                    depth,
                    child_abort.clone(),
                    Some(child_turn_id.clone()),
                    ctx.permission.clone(),
                    {
                        let transcript = transcript.clone();
                        Arc::new(move || transcript.lock_recover().clone())
                    },
                    grandchild_in_fork,
                    Some(parent_agent_for_children.clone()),
                    Some(task_plan.fork()),
                    Some(child_capture.clone()),
                    Some(descendant_permission_origin),
                    Some(runtime_capabilities.clone()),
                    Some(collaboration_mode),
                ));
            if this.has_workflow_tool {
                ctx.spawn_workflow_agent = Some(this.make_spawn_workflow_agent(
                    depth,
                    child_abort.clone(),
                    ctx.permission.clone(),
                    Some(parent_agent_for_children.clone()),
                    Some(task_plan.fork()),
                    Some(child_capture.clone()),
                    Some(child_turn_id.clone()),
                    runtime_capabilities.clone(),
                    collaboration_mode,
                ));
            }
            ctx.workflow_journal = this.workflow_journal();
            ctx.swarm = child_swarm_controller.clone();
            if let Some(lsp) = &lsp_manager {
                ctx.lsp = Some(lsp.controller());
            }
            ctx.browser = browser.browser_runtime.clone();
            ctx.browser_tool_mapping = browser.browser_tool_mapping.clone();
            {
                let clients = mcp_runtime.resources();
                ctx.mcp_clients = Some(Box::new(move || clients.clone()));
            }
            ctx.background_tasks = Some(this.background_task_registry_in_context(
                Some(parent_agent_for_children.clone().into()),
                Some(super::SubagentResumeContext {
                    mcp: child_mcp_capture.clone(),
                    bundle: child_capture.bundle.clone(),
                    permission: ctx.permission.clone(),
                    runtime_policy: policy.clone(),
                    cwd: ctx.cwd.clone(),
                    runtime_capabilities: runtime_capabilities.clone(),
                }),
            ));
            let team = {
                let cs = child_swarm.lock_recover().as_ref().map(|i| i.team.clone());
                match cs {
                    Some(t) => Some(t),
                    None => this
                        .swarm_identity
                        .lock_recover()
                        .as_ref()
                        .map(|i| i.team.clone()),
                }
            };
            ctx.task_list_id = Some(get_task_list_id(Some(&this.id), team.as_ref()));
            task_plan.observe(ctx.task_list_id.as_ref().unwrap());
            ctx.task_plan = Some(task_plan.clone());
            ctx.run_hook = this.make_run_hook();
            ctx.worktree = sub_worktree.clone();
            // Historical plan documents retain the child's scoped path.
            ctx.plan_file = this.plan_file_context(Some(&RuntimeString::from(
                parent_agent_for_children.clone(),
            )));
            ctx
        })
    }

    /// Assemble the sub-agent's per-turn reminder providers: date change,
    /// plan mode, swarm, skill listing, memory recall, and nested memory.
    #[allow(clippy::too_many_arguments, clippy::ptr_arg)]
    fn build_sub_agent_reminder_providers(
        self: &Arc<Self>,
        run_opts: &RunSubAgentOpts,
        memory_bundle: &Option<MemoryBundle>,
        effective_agent_id: &String,
        child_model: &RuntimeString,
        inference_bundle: &Arc<super::InferenceBundle>,
        child_swarm: &Arc<Mutex<Option<SwarmIdentity>>>,
        _fork_path: bool,
        memory_dir_is_none: bool,
        host_observable: bool,
        child_tools: &[AnyTool],
        skill_catalog: super::SessionSkillSnapshot,
        child_file_state: &Arc<dyn FileStateCache>,
        child_nested_memory_triggers: &Arc<Mutex<HashSet<RuntimePath>>>,
        child_nested_memory_loaded_paths: &Arc<Mutex<HashSet<RuntimePath>>>,
        child_turn_id: &RuntimeString,
        sub_rollout: &Option<Arc<dyn LiveRollout>>,
    ) -> Vec<Arc<dyn ReminderProvider>> {
        let mut child_reminder_providers: Vec<Arc<dyn ReminderProvider>> = Vec::new();
        {
            let now = self.deps.now.clone();
            child_reminder_providers.push(Arc::new(create_date_change_reminder_provider(
                move || now(),
            )));
        }
        if self.config.swarm && run_opts.suppress_swarm_reminder != Some(true) {
            let cs = child_swarm.clone();
            let now = self.deps.now.clone();
            let id = self.id.clone();
            let receiver = Arc::downgrade(self);
            let agent_id = effective_agent_id.clone();
            let turn_id = child_turn_id.clone();
            let rollout = sub_rollout.clone();
            let provider = create_swarm_reminder_provider(
                move || {
                    cs.lock_recover().clone().map(|i| SwarmStoreIdentity {
                        team: i.team,
                        peer: i.peer,
                    })
                },
                move || now(),
                Some(id),
            )
            .with_delivery_state(self.swarm_delivery_state.clone());
            let provider = if memory_dir_is_none {
                provider.with_delivery_sink(Arc::new(move |receipt| {
                    let receiver = receiver.clone();
                    let agent_id = agent_id.clone();
                    let turn_id = turn_id.clone();
                    let rollout = rollout.clone();
                    Box::pin(async move {
                        if let Some(session) = receiver.upgrade() {
                            session
                                .publish_subagent_notification(
                                    &agent_id,
                                    receipt,
                                    Some(&turn_id),
                                    rollout.as_ref(),
                                    host_observable,
                                )
                                .await?;
                        }
                        Ok(())
                    })
                }))
            } else {
                provider
            };
            child_reminder_providers.push(Arc::new(provider));
        }
        if skill_catalog.skills.is_some() && child_tools.iter().any(|tool| tool.name() == "skill") {
            let cw = inference_bundle
                .config_for_model(child_model)
                .context_window;
            child_reminder_providers.push(Arc::new(create_skill_listing_reminder_provider(
                move || skill_catalog.visible(),
                Some(cw as i64),
            )) as Arc<dyn ReminderProvider>);
        }
        if let Some(memory_bundle) = memory_bundle.as_ref().filter(|_| self.config.memory_recall) {
            let mem_dir = memory_bundle.dir.clone();
            // A resumed agent gets a newly captured provider, while an already
            // running worker keeps its own reminder and dependency binding.
            let provider = {
                let recall_deps = inference_bundle.deps.clone();
                let child_model2 = child_model.clone();
                let now = self.deps.now.clone();
                let p: Arc<dyn ReminderProvider> =
                    Arc::new(create_memory_recall_reminder_provider(MemoryRecallDeps {
                        recall: Box::new(move |query: RuntimeString, ctx: RecallContext| {
                            let mem_dir = mem_dir.clone();
                            let call_model = recall_deps.call_model_owned();
                            let model = child_model2.clone();
                            Box::pin(async move {
                                let store = TypedMemoryStore::new(TypedMemoryStoreOptions {
                                    dir: Some(mem_dir),
                                    redact_secrets: None,
                                });
                                store
                                    .recall_semantic(
                                        &query,
                                        RecallSemanticOptions {
                                            call_model: &call_model,
                                            model,
                                            limit: None,
                                            recent_tools: Some(&ctx.recent_tools),
                                            exclude: Some(&ctx.exclude),
                                            signal: ctx.signal,
                                        },
                                    )
                                    .await
                                    .map_err(|e| Box::new(e) as RecallError)
                            })
                        }),
                        now: Box::new(move || now()),
                        recent_tool_window: None,
                    }));
                self.subagents
                    .memory_recall_providers
                    .lock_recover()
                    .insert(effective_agent_id.clone(), p.clone());
                p
            };
            child_reminder_providers.push(provider);
        }
        if self.config.memory.enabled && memory_dir_is_none {
            let triggers = child_nested_memory_triggers.clone();
            child_reminder_providers.push(Arc::new(create_nested_memory_reminder_provider(
                NestedMemoryReminderDeps {
                    drain_triggers: Box::new(move || {
                        Session::drain_nested_memory_triggers(&triggers)
                    }),
                    loaded_paths: child_nested_memory_loaded_paths.clone(),
                    file_state: child_file_state.clone(),
                    gates: Some(memory_gates_from(&self.config.memory)),
                    user_config_dir: None,
                },
            )));
        }
        child_reminder_providers
    }

    /// Load a resuming sub-agent's transcript seed — the persisted store
    /// transcript (with meta/surfaced tools) or the in-memory map. Returns `(resume_history,
    /// resumed_agent_type, resumed_surfaced_tools)`; a raw persistent resume
    /// key fails with the preserved built-in error.
    async fn load_sub_agent_resume(
        self: &Arc<Self>,
        resume_slot: &JsForwardedRawSlot,
        resume_key: &Option<String>,
        resume_map_key: &Option<SubAgentMapKey>,
        fork_seed: &[Message],
    ) -> Result<(Vec<Message>, Option<RuntimeString>, Vec<RuntimeString>), EngineError> {
        let mut resume_history: Vec<Message> = Vec::new();
        let mut resumed_agent_type: Option<RuntimeString> = None;
        let mut resumed_surfaced_tools: Vec<RuntimeString> = Vec::new();
        if resume_key.is_some() {
            if let Some(store) = self.persistence.store() {
                let persistent_resume = match persistent_resume_key(resume_slot) {
                    Ok(Some(resume)) => resume,
                    Ok(None) => unreachable!("resume key is present"),
                    Err(error) => {
                        return Err(internal_err(error));
                    }
                };
                if fork_seed.is_empty() {
                    let loaded = store
                        .load_sub_agent(
                            &runtime_session_id_to_session_store_host(&self.id),
                            &persistent_resume,
                        )
                        .await
                        .map_err(|error| {
                            internal_err(&format!("Loading the sub-agent history failed: {error}"))
                        })?;
                    self.restore_subagent_notification_ids(
                        &persistent_resume,
                        loaded.turn_index.as_ref(),
                    );
                    resume_history = loaded.messages;
                    resumed_agent_type = loaded.meta.and_then(|m| m.agent_type);
                    resumed_surfaced_tools = loaded.surfaced_tools.unwrap_or_default();
                } else {
                    let loaded = store
                        .read_sub_agent(
                            &runtime_session_id_to_session_store_host(&self.id),
                            &persistent_resume,
                        )
                        .await
                        .map_err(|error| {
                            internal_err(&format!("Loading the sub-agent context failed: {error}"))
                        })?;
                    let (_, _, turn_index, _) =
                        crate::store::jsonl_store::rollout_metadata(&loaded.raw_items);
                    self.restore_subagent_notification_ids(&persistent_resume, Some(&turn_index));
                    resume_history =
                        crate::store::projection::project_for_prompt_with_initial_history(
                            &loaded.raw_items,
                            fork_seed.to_vec(),
                        );
                    resumed_agent_type = loaded.meta.and_then(|m| m.agent_type);
                    resumed_surfaced_tools = loaded.surfaced_tools;
                }
            } else {
                resume_history = self
                    .subagents
                    .transcripts
                    .lock_recover()
                    .get(
                        resume_map_key
                            .as_ref()
                            .expect("present resume has a map key"),
                    )
                    .cloned()
                    .ok_or_else(|| internal_err("The sub-agent history is unavailable."))?;
            }
        }
        Ok((resume_history, resumed_agent_type, resumed_surfaced_tools))
    }

    /// Build the per-agent memory bundle: host sync, optional snapshot-merge
    /// reconciliation, prompt text, and the memory directory. Returns `Err`
    /// when snapshot reconciliation failed the spawn.
    async fn build_sub_agent_memory_bundle(
        self: &Arc<Self>,
        def: &Option<AgentDefinition>,
        type_name: &Option<RuntimeString>,
        parent_signal: &AbortSignal,
    ) -> Result<Option<MemoryBundle>, EngineError> {
        let mut memory_bundle: Option<MemoryBundle> = None;
        if let (Some(host), Some(def_ref), Some(tn)) = (
            self.agent_memory_host.as_ref(),
            def.as_ref(),
            type_name.as_ref(),
        ) {
            if let Some(scope) = def_ref.memory {
                let tn_host = runtime_forwarded_slot_to_typed_host(tn);
                let layout = host.layout();
                let dir = resolve_abs(&RuntimePath::from(layout.dir_for(&tn_host, scope)));
                let dir_host = runtime_path_to_agent_memory_host(&dir);
                // Host sync may mutate local memory. Snapshot identity and
                // the correlated decision must be based on that post-sync
                // state; otherwise the provider could accept a merge prompt
                // before admission apply discovers a stale localStateId.
                self.sync_agent_memory(host.as_ref(), &tn_host, scope, &dir_host)
                    .await;
                let mut merge_delivery: Option<AgentMemorySnapshotMergeDelivery> = None;
                if self.config.agent_memory_snapshot && scope == AgentMemoryScope::User {
                    merge_delivery = match self
                        .reconcile_agent_memory_snapshot(host.as_ref(), &tn_host, parent_signal)
                        .await
                    {
                        Ok(delivery) => delivery,
                        Err(error) => {
                            return Err(internal_runtime_err(error));
                        }
                    };
                }
                let text = build_agent_memory_prompt_text(&tn_host, scope, &dir_host).await;
                let (merge_prefix, merge_request_id) = match merge_delivery {
                    Some(delivery) => (Some(delivery.prefix), Some(delivery.request_id)),
                    None => (None, None),
                };
                memory_bundle = Some(MemoryBundle {
                    text,
                    dir,
                    merge_prefix,
                    merge_request_id,
                });
            }
        }
        Ok(memory_bundle)
    }

    /// Sub-agent stream finalizer: apply the lifecycle fallback for a stream
    /// that ended without a terminal event, persist the transcript into the
    /// in-memory map when no store is configured, then flush the sub-rollout
    /// and close per-agent MCP clients (the concurrency slot is released by
    /// its guard's Drop).
    #[allow(clippy::too_many_arguments)]
    async fn finish_sub_agent_stream(
        self: &Arc<Self>,
        lifecycle_terminal: &Option<SubagentStatus>,
        observable: bool,
        child_abort: &AbortSignal,
        parent_signal: &AbortSignal,
        effective_agent_id: &str,
        effective_agent_map_key: SubAgentMapKey,
        transcript: &Arc<Mutex<Vec<Message>>>,
        sub_rollout: &Option<Arc<dyn LiveRollout>>,
        subagent_dir_resolved: bool,
        mcp_clients: &[ManagedClient],
    ) -> Result<(), RuntimeString> {
        let mut persistence_error = None;
        if observable && lifecycle_terminal.is_none() {
            if child_abort.is_cancelled() || parent_signal.is_cancelled() {
                if let Err(error) = self.mark_subagent_cancelled(effective_agent_id).await {
                    persistence_error = Some(error);
                }
            } else if let Err(error) = self
                .mark_subagent_failed(
                    effective_agent_id,
                    RuntimeString::from("sub-agent stream ended without a terminal event"),
                )
                .await
            {
                persistence_error = Some(error);
            }
        }
        // Without a store the in-memory map is the only transcript home.
        // A store whose sidechain never opened is the same situation one
        // failure later: the child ran and produced messages that no file
        // holds, so the mirror keeps them reachable for the read path.
        if self.persistence.store().is_none() || !subagent_dir_resolved {
            self.subagents
                .transcripts
                .lock_recover()
                .insert(effective_agent_map_key, transcript.lock_recover().clone());
        }
        // finally: flush the sub-rollout + close per-agent MCP clients (release
        // is handled by `_release_guard`'s Drop).
        if let Some(r) = sub_rollout {
            // The flush result is the only proof the child's transcript is
            // durable. Swallowing it left a poisoned writer looking like a
            // persisted child, and the read path then served an empty file as
            // its whole history.
            if let Err(error) = r.flush().await {
                tracing::warn!(
                    agent_id = %effective_agent_id,
                    "sub-agent sidechain flush failed; transcript may be incomplete on disk: {error}"
                );
                persistence_error = Some(error.to_string().into());
            }
        }
        // Producer leases serialize the final close after every descendant's
        // lifecycle and receipt append, including results arriving after this turn.
        if !self.ordinary_retains_clients(effective_agent_id) {
            for client in mcp_clients {
                client.close().await;
            }
        }
        match persistence_error {
            Some(error) => {
                self.note_subagent_persistence_error(effective_agent_id, error.clone());
                Err(error)
            }
            None => Ok(()),
        }
    }
}

/// A UNC / network path (`\\server\share`, `//host/…`), except a WSL path.
pub(crate) fn is_unc_path(p: &str) -> bool {
    let b = p.as_bytes();
    let is_sep = |c: &u8| *c == b'/' || *c == b'\\';
    if b.len() < 2 || !is_sep(&b[0]) || !is_sep(&b[1]) {
        return false;
    }
    let after = p[2..].to_ascii_lowercase();
    let ab = after.as_bytes();
    let is_wsl = (after.starts_with("wsl$") && ab.get(4).map(is_sep).unwrap_or(false))
        || (after.starts_with("wsl.localhost") && ab.get(13).map(is_sep).unwrap_or(false));
    !is_wsl
}

/// Derive the shared wire-level prompt-cache domain for a spawned internal
/// session: scoped to the parent thread and the agent source so sibling
/// sessions of one delegation reuse one cached prefix instead of each
/// fragmenting the domain under its own per-child identity. The unique
/// per-child `cache_key` remains the identity used for trace attribution.
pub(crate) fn subagent_cache_domain(
    parent_id: &RuntimeString,
    agent_type: Option<&RuntimeString>,
) -> RuntimeString {
    let source = agent_type
        .cloned()
        .unwrap_or_else(|| RuntimeString::from("subagent"));
    parent_id.concat(&RuntimeString::from(":")).concat(&source)
}

#[cfg(test)]
mod cache_domain_tests {
    use super::subagent_cache_domain;
    use crate::runtime_values::RuntimeString;

    #[test]
    fn siblings_of_one_parent_and_source_share_a_domain() {
        let parent = RuntimeString::from("thread-1");
        let kind = Some(RuntimeString::from("explore"));
        assert_eq!(
            subagent_cache_domain(&parent, kind.as_ref())
                .try_to_utf8()
                .unwrap(),
            "thread-1:explore"
        );
        assert_eq!(
            subagent_cache_domain(&parent, kind.as_ref()),
            subagent_cache_domain(&parent, kind.as_ref())
        );
    }

    #[test]
    fn domains_stay_scoped_per_parent_and_source() {
        let kind = Some(RuntimeString::from("explore"));
        let other_kind = Some(RuntimeString::from("plan"));
        assert_ne!(
            subagent_cache_domain(&RuntimeString::from("thread-1"), kind.as_ref()),
            subagent_cache_domain(&RuntimeString::from("thread-2"), kind.as_ref())
        );
        assert_ne!(
            subagent_cache_domain(&RuntimeString::from("thread-1"), kind.as_ref()),
            subagent_cache_domain(&RuntimeString::from("thread-1"), other_kind.as_ref())
        );
    }

    #[test]
    fn missing_source_falls_back_to_a_stable_label() {
        assert_eq!(
            subagent_cache_domain(&RuntimeString::from("thread-1"), None)
                .try_to_utf8()
                .unwrap(),
            "thread-1:subagent"
        );
    }
}
