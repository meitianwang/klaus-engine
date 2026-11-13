//! Active context accounting and canonical request snapshots.

use super::Session;
use crate::context::skill_receipts::InvokedSkillContent;
use crate::runtime_values::RuntimeString;
use crate::sync_ext::MutexExt;

impl Session {
    pub(crate) fn compaction_output_sink(
        &self,
    ) -> Option<crate::context::compact::full_compact::CompactionOutputSink> {
        let session = std::sync::Arc::downgrade(&self.snapshot.resolve_self()?);
        Some(std::sync::Arc::new(move |message| {
            let session = session.clone();
            Box::pin(async move {
                if let Some(session) = session.upgrade() {
                    let prepared = match session.persistence.rollout() {
                        Some(rollout) => match rollout.prepare_append().await {
                            Ok(prepared) => Some(prepared),
                            Err(error) => {
                                tracing::warn!(%error, "Compaction output persistence failed");
                                return;
                            }
                        },
                        None => None,
                    };
                    session.push_history(message.clone());
                    if let Some(prepared) = prepared {
                        prepared.commit(crate::schemas::rollout::RolloutItem::Message { message });
                    }
                }
            })
        }))
    }

    pub(crate) fn context_window(&self) -> crate::context::window_usage::ContextWindowUsage {
        let rollout = self.persistence.rollout().cloned();
        crate::context::window_usage::ContextWindowUsage::with_update_gate(
            self.turn.context_window_usage.clone(),
            std::sync::Arc::new(move |state| {
                let rollout = rollout.clone();
                Box::pin(async move {
                    if let Some(rollout) = rollout {
                        rollout
                            .append_event(
                                crate::context::window_usage::WINDOW_USAGE_EVENT.into(),
                                Some(crate::store::session_store::AppendEventOptions {
                                    turn_id: None,
                                    payload: Some(crate::context::window_usage::encode(&state)),
                                }),
                            )
                            .await;
                    }
                })
            }),
            self.turn.context_window_update_gate.clone(),
        )
    }

    pub(crate) async fn reset_context_window(&self, active_tokens: u64) {
        self.context_window().reset(active_tokens).await;
    }

    pub(crate) async fn persist_context_usage_checkpoint(&self) {
        if let Some(rollout) = self.persistence.rollout() {
            rollout
                .append_event(
                    crate::context::usage_checkpoint::CONTEXT_USAGE_ROLLOUT_EVENT.into(),
                    Some(crate::store::session_store::AppendEventOptions {
                        turn_id: None,
                        payload: Some(crate::context::usage_checkpoint::encode_checkpoint(
                            self.turn.padded_countdown_spend(),
                        )),
                    }),
                )
                .await;
        }
    }

    pub(crate) fn restore_context_usage_checkpoint(
        &self,
        items: &[crate::schemas::rollout::RolloutItem],
    ) {
        *self.turn.context_window_usage.lock().unwrap() = crate::context::window_usage::restore(
            items,
            crate::context::compact::policy::hard_block_threshold(&self.config) as u64,
        );
        self.turn
            .restore_context_usage(crate::context::usage_checkpoint::checkpoint_from_rollout(
                items,
            ));
    }

    pub(crate) async fn rebuild_compaction_context(
        &self,
        config: &crate::schemas::config::ResolvedConfig,
        mut history: Vec<crate::schemas::message::Message>,
        signal: &crate::types::abort::AbortSignal,
    ) -> Vec<crate::schemas::message::Message> {
        use crate::schemas::{
            content::{ContentBlock, HarnessEnvelope, TextBlock},
            message::{Message, UserMessage},
        };
        let request = self.manual_compaction_request(config, &history).await;
        history = request.messages;
        let task_list_id = self.task_list_id();
        if let Some(providers) = self.reminder_providers.get() {
            for provider in providers
                .iter()
                .filter(|provider| provider.retained_context())
            {
                let context = crate::types::reminder::ReminderContext {
                    messages: history.clone(),
                    cwd: self.cwd_now(),
                    task_list_id: Some(task_list_id.clone()),
                    task_plan: Some(crate::tasks::task_store::TaskPlanBinding::capture(
                        &task_list_id,
                    )),
                    signal: Some(signal.clone()),
                    plan_file_path: self.plan_file_context(None).map(|plan| plan.path),
                    plan_file_exists: None,
                    has_task_tool: self
                        .current_tools()
                        .iter()
                        .any(|tool| tool.name() == "task"),
                };
                if let Some(messages) = provider.retained_messages(&context).await {
                    for message in messages {
                        if !history.iter().any(|existing| {
                            super::message_uuid(existing) == super::message_uuid(&message)
                        }) {
                            history.push(message);
                        }
                    }
                } else if let Some(text) = provider
                    .reminder_with_context_window(&context, config.context_window)
                    .await
                {
                    history.push(Message::User(UserMessage {
                        uuid: (self.deps.uuid)().into(),
                        is_meta: Some(true),
                        question_answer: None,
                        content: vec![ContentBlock::Text(TextBlock {
                            text,
                            citations: None,
                            harness_envelope: Some(HarnessEnvelope::SystemReminder),
                            message_id: None,
                            phase: None,
                        })],
                    }));
                }
            }
        }
        crate::context::compact::full_compact::place_retained_context(&mut history);
        history
    }

    pub(crate) async fn manual_compaction_request(
        &self,
        config: &crate::schemas::config::ResolvedConfig,
        history: &[crate::schemas::message::Message],
    ) -> crate::types::provider::ModelRequest {
        use crate::sync_ext::RwLockExt;
        use crate::types::provider::{
            ModelRequest, SystemPromptPart, ThinkingConfig, ThinkingContextPolicy,
        };

        self.refresh_workspace_instruction_cache();
        let provider = self.system_prompt_provider.read_recover().clone();
        let mut parts = provider().await;
        let tools = self.current_tools();
        let surfaced = self.surfaced_tools.lock_recover().clone();
        let tools = crate::engine::query::visible_tool_specs(
            &tools,
            &surfaced,
            &crate::tools::pool::to_tool_specs(&tools),
        );
        if tools.iter().any(|tool| tool.name == "advisor") {
            let boundary = parts
                .iter()
                .position(|part| {
                    part.cache_scope != Some(crate::types::provider::CacheScope::Global)
                })
                .unwrap_or(parts.len());
            parts.insert(boundary, crate::context::system_prompt::advisor_guidance());
        }
        if self.system_prompt_is_default {
            for part in &mut parts {
                if part.name == "env" {
                    part.text = crate::context::env_info::environment_block(
                        &self.cwd_now(),
                        config,
                        crate::context::env_info::EnvBlockOptions {
                            plat: None,
                            shell: None,
                            model: Some(config.model.clone()),
                            is_worktree: self.workspace.is_in_worktree(),
                            windows_shell: None,
                        },
                    );
                }
            }
        }

        if let Some(part) = self.plugin_mcp_instruction_snapshot() {
            parts.push(part);
        }
        if let Some(definition) = &self.root_agent_definition {
            for (name, text) in [
                (
                    "agent-role",
                    definition
                        .prompt
                        .as_ref()
                        .or(definition.system_prompt.as_ref()),
                ),
                (
                    "agent-critical",
                    definition.critical_system_reminder.as_ref(),
                ),
            ] {
                if let Some(text) = text {
                    parts.push(SystemPromptPart {
                        name: name.into(),
                        text: text.clone(),
                        cacheable: Some(true),
                        cache_scope: None,
                    });
                }
            }
        }
        if let Some(dir) = self.memory_roots.main() {
            if let Some(host) = &self.agent_memory_host {
                self.sync_agent_memory(
                    host.as_ref(),
                    "main",
                    crate::memory::agent_memory::AgentMemoryScope::Project,
                    dir,
                )
                .await;
            }
            let text = crate::memory::agent_memory::build_agent_memory_prompt_text(
                "main",
                crate::memory::agent_memory::AgentMemoryScope::Project,
                dir,
            )
            .await;
            if !text.is_empty() {
                parts.push(SystemPromptPart {
                    name: "memory".into(),
                    text: text.into(),
                    cacheable: None,
                    cache_scope: None,
                });
            }
        }
        if self.meta.role.as_ref() == Some(&crate::schemas::rollout::SessionRole::SideTask) {
            parts.push(SystemPromptPart {
                name: "side-task".into(),
                text: super::SIDE_TASK_SYSTEM_PROMPT.into(),
                cacheable: None,
                cache_scope: None,
            });
        }
        parts.retain(|part| part.name != "collaboration-mode");
        parts.push(crate::planning::instructions::instruction_part(
            config,
            self.collaboration_mode(),
        ));
        let (system, messages) =
            crate::context::instruction_policy::project_workspace_instructions(&parts, history);
        let policy = self.runtime_policy_snapshot();
        let permission = self.permission_context_for_policy(&policy);
        let mut context = super::bare_tool_context(
            self.process_context.clone(),
            self.background_processes.clone(),
            self.cwd_now(),
            crate::types::abort::AbortSignal::new(),
            self.file_state.clone(),
            permission,
            policy.sandbox_backend.clone(),
        );
        policy.apply_to_context(&mut context);
        context.planning_read_only =
            self.collaboration_mode() == crate::planning::CollaborationMode::Plan;
        let bundle = self.inference_snapshot();
        let profile = crate::provider::behavior::resolve_provider_behavior_profile(
            Some(bundle.provider.info()),
            &config.model,
        );
        let system = crate::engine::query::finalize_instruction_system(
            system,
            &context,
            self.approval_handler.is_some(),
            config.ultra_mode,
            self.meta.role != Some(crate::schemas::rollout::SessionRole::SideTask),
            &tools,
            None,
            &profile,
        );
        let tools = crate::provider::behavior::render_tool_specs_for_behavior(&tools, &profile);
        ModelRequest {
            supports_image_detail_original: false,
            model: config.model.clone(),
            image_limits: None,
            max_media_blocks: None,
            system,
            messages,
            tools,
            native_deferred_tools: None,
            deferred_tools: Vec::new(),
            tool_choice: None,
            max_output_tokens: config.max_output_tokens.unwrap_or(20_000),
            task_budget: None,
            output_schema: None,
            web_search: None,
            thinking: config.thinking.then_some(ThinkingConfig::Adaptive),
            effort: config.effort.clone(),
            model_verbosity: config.model_verbosity,
            reasoning_summary: config.reasoning_summary,
            skip_cache_write: Some(true),
            cache_ttl: None,
            cache_domain: None,
            cache_key: Some(self.id.clone()),
            context_management: config.context_management.clone(),
            context_hints: None,
            // Read (never advance) the cold latch so this request declares the
            // same thinking policy the main thread is caching against.
            thinking_context: (config.thinking && config.manage_thinking_context).then_some(
                if self.turn.thinking_context_cold() {
                    ThinkingContextPolicy::KeepMostRecentTurn
                } else {
                    ThinkingContextPolicy::KeepAll
                },
            ),
        }
    }

    pub(crate) fn record_invoked_skill_content(
        &self,
        agent: Option<RuntimeString>,
        path: &RuntimeString,
        content: &RuntimeString,
    ) {
        let mut cache = self.memory_roots.invoked_skill_content.lock_recover();
        let entries = cache.entry(agent).or_default();
        entries.retain(|entry| entry.path != *path);
        entries.push(InvokedSkillContent {
            path: path.clone(),
            content: content.clone(),
        });
        if entries.len() > 32 {
            entries.drain(..entries.len() - 32);
        }
    }
}
