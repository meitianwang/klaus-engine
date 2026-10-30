//! Loads and unloads ordinary child runtimes while retaining their identities.

use super::*;
use crate::engine::session::SubAgentMapKey;

impl Session {
    pub(crate) fn ordinary_residency_config(&self, id: &str) -> Option<ResolvedConfig> {
        self.ordinary_runtime()
            .state
            .lock_recover()
            .workers
            .get(id)
            .and_then(|worker| worker.residency_config.clone())
    }

    pub(crate) fn has_ordinary_runtime_identity(&self, id: &str) -> bool {
        self.ordinary_runtime()
            .state
            .lock_recover()
            .workers
            .contains_key(id)
    }

    pub(crate) fn note_ordinary_runtime_event(
        &self,
        id: &str,
        event: &crate::types::events::EngineEvent,
    ) {
        use crate::types::events::EngineEvent;
        let status = match event {
            EngineEvent::TurnStarted { .. } => None,
            EngineEvent::TurnCompleted {
                stop_reason: TerminalReason::Completed,
                ..
            } => Some(SubagentStatus::Completed),
            EngineEvent::TurnCompleted {
                stop_reason: TerminalReason::Aborted,
                ..
            } => Some(SubagentStatus::Interrupted),
            EngineEvent::TurnCompleted { .. } | EngineEvent::TurnFailed { .. } => {
                Some(SubagentStatus::Failed)
            }
            _ => return,
        };
        if let Some(worker) = self
            .ordinary_runtime()
            .state
            .lock_recover()
            .workers
            .get_mut(id)
        {
            worker.terminal_status = status;
        }
    }

    pub(crate) async fn accept_local_subagent_creation(
        &self,
        id: &str,
        admission: &Arc<OrdinaryCreationAdmission>,
    ) -> Result<(), RuntimeString> {
        self.persist_ordinary_creation_commit(id).await?;
        self.commit_ordinary_creation(id, admission).await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn prepare_external_subagent(
        self: &Arc<Self>,
        opts: &SpawnSubAgentOptions,
        depth: u32,
        parent_agent_id: Option<String>,
        spawn_turn_id: Option<RuntimeString>,
        abort: CancellationToken,
        reserved_id: Option<String>,
        load_config: &ResolvedConfig,
    ) -> Result<
        (
            String,
            Arc<OrdinaryCreationAdmission>,
            PendingCreation,
            tokio::sync::oneshot::Receiver<Result<(), RuntimeString>>,
        ),
        RuntimeString,
    > {
        self.governor
            .check_execution_capacity(Some(&abort))
            .map_err(|error| RuntimeString::from(error.to_string()))?;
        let slot = self
            .reserve_subagent_residency(parent_agent_id.as_deref(), load_config)
            .await?;
        let id = reserved_id.unwrap_or_else(|| (self.deps.uuid)());
        let (response, accepted) = tokio::sync::oneshot::channel();
        let runtime = self.ordinary_runtime();
        let admission = Arc::new(OrdinaryCreationAdmission {
            response: Mutex::new(Some(response)),
            slot: Mutex::new(Some(slot)),
            committed: AtomicBool::new(false),
            abort: abort.clone(),
            description: opts.prompt.clone(),
            persistent: opts.persistent == Some(true),
            activation: Notify::new(),
            publication: Mutex::new(None),
            owner_started: AtomicBool::new(false),
            observable: opts.memory_dir.is_none(),
            cleanup_token: Mutex::new(Some(runtime.pending_cleanup.token())),
        });
        runtime.state.lock_recover().workers.insert(
            id.clone(),
            OrdinaryWorker {
                committed: false,
                loaded: Some(Arc::new(tokio::sync::RwLock::new(()))),
                locally_evicted: false,
                evicted_capabilities: None,
                observable: opts.memory_dir.is_none(),
                terminal_status: None,
                residency_config: Some(load_config.clone()),

                bindings: OrdinaryBindings {
                    depth,
                    spawn_turn_id,
                    parent_agent_id,
                },
                snapshot: None,
                boundary_approval_policy: None,
                running: true,
                user_cancelled: false,
                one_shot: false,
                abort,
                exited: Arc::new(Notify::new()),
                rollout: None,
                error: None,
                creation_metadata: None,
                cancel_generation: 0,
                cancel_receipts: Vec::new(),
                notify_completion: true,
                event_followers: Vec::new(),
                worktree: None,
            },
        );
        let pending = PendingCreation {
            admission: admission.clone(),
            accepted: false,
            session: Arc::downgrade(self),
            id: id.clone(),
        };
        Ok((id, admission, pending, accepted))
    }

    pub(crate) async fn finish_external_subagent(
        &self,
        id: &str,
        admission: &OrdinaryCreationAdmission,
    ) {
        if !admission.committed.load(Ordering::Acquire) {
            admission.reject("Sub-agent initialization ended before input acceptance.".into());
            self.cleanup_pending_ordinary(id).await;
        } else if let Some(worker) = self
            .ordinary_runtime()
            .state
            .lock_recover()
            .workers
            .get_mut(id)
        {
            worker.running = false;
            worker.exited.notify_waiters();
        }
    }

    pub(crate) async fn reserve_subagent_residency(
        self: &Arc<Self>,
        protected: Option<&str>,
        load_config: &ResolvedConfig,
    ) -> Result<ResidencySlot, RuntimeString> {
        let capacity = load_config.effective_subagent_capacity();
        let mut failures = Vec::new();
        loop {
            if let Some(slot) = self.governor.try_reserve_pending_slot(capacity) {
                return Ok(slot);
            }
            let runtime = self.ordinary_runtime();
            let mut evicted = false;
            for id in self.governor.resident_ids() {
                if protected == Some(id.as_str()) {
                    continue;
                }
                let loaded = {
                    let state = runtime.state.lock_recover();
                    state
                        .workers
                        .get(&id)
                        .and_then(|worker| worker.loaded.clone())
                };
                let Some(loaded) = loaded else {
                    self.governor.remove_resident(&id);
                    evicted = true;
                    break;
                };
                let Ok(pin) = loaded.clone().try_write_owned() else {
                    continue;
                };
                let eligible = {
                    let state = runtime.state.lock_recover();
                    state.workers.get(&id).is_some_and(|worker| {
                        worker.committed
                            && !worker.running
                            && worker
                                .loaded
                                .as_ref()
                                .is_some_and(|current| Arc::ptr_eq(current, &loaded))
                            && !self.has_ordinary_trigger_mail(&id)
                            && self
                                .subagents
                                .lifecycle
                                .lock_recover()
                                .get(id.as_str())
                                .map(|record| record.status)
                                .or_else(|| {
                                    (!worker.observable)
                                        .then_some(worker.terminal_status)
                                        .flatten()
                                })
                                .is_some_and(|status| {
                                    matches!(
                                        status,
                                        SubagentStatus::Completed
                                            | SubagentStatus::Failed
                                            | SubagentStatus::Interrupted
                                    )
                                })
                    })
                };
                if !eligible {
                    continue;
                }
                // The independent owner keeps the write pin until persistence and
                // resource shutdown finish, even if this admission is cancelled.
                let this = self.clone();
                let target = id.clone();
                let result = tokio::spawn(async move {
                    let _pin = pin;
                    this.unload_ordinary_runtime(&target, &loaded).await
                })
                .await;
                match result {
                    Ok(Ok(true)) => {
                        evicted = true;
                        break;
                    }
                    Ok(Ok(false)) => {}
                    Ok(Err(error)) => {
                        tracing::warn!(agent_id = %id, reason = ?error, "Resident unload failed; retaining the resident and trying another candidate");
                        failures.push(format!("{id}: {}", runtime_string_diagnostic(&error)));
                    }
                    Err(error) => {
                        tracing::warn!(agent_id = %id, reason = %error, "Resident eviction owner failed; trying another candidate");
                        failures.push(format!("{id}: {error}"));
                    }
                }
            }
            if !evicted {
                let mut error =
                    SlotAdmissionError::ResidencyCapacity { maximum: capacity }.to_string();
                if !failures.is_empty() {
                    error.push_str(" Resident unload failures: ");
                    error.push_str(&failures.join("; "));
                }
                return Err(error.into());
            }
        }
    }

    fn has_ordinary_trigger_mail(&self, id: &str) -> bool {
        self.conversation
            .queue()
            .admitted_snapshot()
            .iter()
            .any(|message| {
                message.agent_id.as_ref() == Some(&RuntimeString::from(id))
                    && message.triggers_turn()
            })
    }

    async fn unload_ordinary_runtime(
        &self,
        id: &str,
        loaded: &Arc<tokio::sync::RwLock<()>>,
    ) -> Result<bool, RuntimeString> {
        // A persistent rollout is required before dropping executable state.
        // Memory-only workers remain resident rather than losing their history.
        let Some(store) = self.persistence.store() else {
            return Ok(false);
        };
        #[cfg(any(test, feature = "testing"))]
        if let Some(goal) = self.goal_runtime() {
            goal.testing_note_phase("subagent.residency.flush");
        }
        self.conversation.queue().await_pending_durability().await?;
        self.subagents
            .parent_rollouts
            .flush_for_unload(&RuntimeString::from(id))
            .await?;
        let session_id = self
            .id
            .try_to_utf8()
            .map_err(|_| RuntimeString::from("Invalid session identity."))?;
        store
            .read_sub_agent(&session_id, id)
            .await
            .map_err(|error| RuntimeString::from(error.to_string()))?;
        let runtime = self.ordinary_runtime();
        let clients = {
            let state = runtime.state.lock_recover();
            let Some(worker) = state.workers.get(id) else {
                return Ok(false);
            };
            if worker.running
                || self.has_ordinary_trigger_mail(id)
                || !worker
                    .loaded
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, loaded))
            {
                return Ok(false);
            }
            worker
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.mcp_clients.clone())
                .unwrap_or_default()
        };
        for client in clients {
            client.close().await;
        }
        {
            let mut state = runtime.state.lock_recover();
            let Some(worker) = state.workers.get_mut(id) else {
                return Ok(false);
            };
            worker.evicted_capabilities = worker
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.runtime_capabilities.clone());
            worker.snapshot = None;
            worker.rollout = None;
            worker.loaded = None;
            worker.residency_config = None;
            worker.locally_evicted = true;
        }
        let key = SubAgentMapKey::String(id.into());
        self.subagents.transcripts.lock_recover().remove(&key);
        self.subagents.surfaced.lock_recover().remove(&key);
        self.subagents
            .memory_recall_providers
            .lock_recover()
            .remove(id);
        self.governor.remove_resident(id);
        #[cfg(any(test, feature = "testing"))]
        if let Some(goal) = self.goal_runtime() {
            goal.testing_note_phase("subagent.residency.unloaded");
        }
        Ok(true)
    }

    /// Compatibility entry point for callers without a captured query context.
    #[allow(dead_code)]
    pub(crate) async fn ensure_ordinary_loaded(
        self: &Arc<Self>,
        id: &str,
        trigger: bool,
        load_config: &ResolvedConfig,
    ) -> Result<Option<tokio::sync::OwnedRwLockReadGuard<()>>, RuntimeString> {
        let mut context = self.ordinary_resume_context(None)?;
        let mut bundle = (*context.bundle).clone();
        bundle.config.max_concurrent_sub_agents = load_config.max_concurrent_sub_agents;
        bundle.config.max_concurrent_threads_per_session =
            load_config.max_concurrent_threads_per_session;
        context.bundle = Arc::new(bundle);
        self.ensure_ordinary_loaded_in_context(id, trigger, Some(&context))
            .await
    }

    pub(crate) async fn ensure_ordinary_loaded_in_context(
        self: &Arc<Self>,
        id: &str,
        trigger: bool,
        resume_context: Option<&SubagentResumeContext>,
    ) -> Result<Option<tokio::sync::OwnedRwLockReadGuard<()>>, RuntimeString> {
        let runtime = self.ordinary_runtime();
        self.hydrate_ordinary_workers(&runtime).await?;
        // Serialize a cold load with other cold loads, without waiting for a
        // running child to release execution capacity.
        let _load = runtime.loading.lock().await;
        loop {
            let (loaded, locally_evicted) = {
                let state = runtime.state.lock_recover();
                let worker = state
                    .workers
                    .get(id)
                    .ok_or_else(|| RuntimeString::from("Unknown sub-agent identity."))?;
                (worker.loaded.clone(), worker.locally_evicted)
            };
            if let Some(loaded) = loaded {
                let pin = loaded.clone().read_owned().await;
                let current = runtime
                    .state
                    .lock_recover()
                    .workers
                    .get(id)
                    .is_some_and(|worker| {
                        worker
                            .loaded
                            .as_ref()
                            .is_some_and(|instance| Arc::ptr_eq(instance, &loaded))
                    });
                if !current {
                    continue;
                }
                self.governor.touch_resident(id);
                if trigger
                    && runtime
                        .state
                        .lock_recover()
                        .workers
                        .get(id)
                        .is_some_and(|worker| !worker.running)
                {
                    self.governor
                        .check_execution_capacity(None)
                        .map_err(|error| RuntimeString::from(error.to_string()))?;
                }
                return Ok(Some(pin));
            }
            // Passive mail for an evicted local identity stays in the tree mailbox.
            if !trigger && locally_evicted {
                return Ok(None);
            }
            let resume_context =
                resume_context.ok_or_else(|| RuntimeString::from(MISSING_RESUME_CONTEXT))?;
            let load_config = &resume_context.bundle.config;
            let slot = self
                .reserve_subagent_residency(Some(id), load_config)
                .await?;
            self.ordinary_snapshot_for_recovery_in_context(id, true, true, Some(resume_context))
                .await?;
            let loaded = Arc::new(tokio::sync::RwLock::new(()));
            let pin = loaded.clone().read_owned().await;
            {
                let mut state = runtime.state.lock_recover();
                let worker = state
                    .workers
                    .get_mut(id)
                    .ok_or_else(|| RuntimeString::from("Unknown sub-agent identity."))?;
                worker.loaded = Some(loaded);
                worker.locally_evicted = false;
                worker.evicted_capabilities = None;
                worker.residency_config = Some(load_config.clone());
            }
            slot.commit(id);
            if trigger
                && runtime
                    .state
                    .lock_recover()
                    .workers
                    .get(id)
                    .is_some_and(|worker| !worker.running)
            {
                self.governor
                    .check_execution_capacity(None)
                    .map_err(|error| RuntimeString::from(error.to_string()))?;
            }
            return Ok(Some(pin));
        }
    }

    pub(crate) async fn await_ordinary_creation_acceptance(
        &self,
        id: &str,
        admission: &Arc<OrdinaryCreationAdmission>,
    ) -> Result<(), RuntimeString> {
        let sender = admission
            .response
            .lock_recover()
            .take()
            .ok_or_else(|| RuntimeString::from("Sub-agent creation was cancelled."))?;
        if sender.is_closed() || admission.abort.is_cancelled() || self.lifecycle.is_disposed() {
            return Err("Sub-agent creation was cancelled.".into());
        }
        sender
            .send(Ok(()))
            .map_err(|_| RuntimeString::from("Sub-agent creation was cancelled."))?;
        tokio::select! {
            _ = admission.activation.notified() => {},
            _ = admission.abort.cancelled() => return Err("Sub-agent creation was cancelled.".into()),
        }
        if !admission.committed.load(Ordering::Acquire) {
            return Err("Sub-agent creation was cancelled.".into());
        }
        self.governor.touch_resident(id);
        Ok(())
    }

    pub(crate) fn stage_ordinary_rollout(&self, id: &str, rollout: Option<Arc<dyn LiveRollout>>) {
        if let Some(worker) = self
            .ordinary_runtime()
            .state
            .lock_recover()
            .workers
            .get_mut(id)
        {
            worker.rollout = rollout;
        }
    }

    pub(super) async fn persist_ordinary_creation_commit(
        &self,
        id: &str,
    ) -> Result<(), RuntimeString> {
        let rollout = self
            .ordinary_runtime()
            .state
            .lock_recover()
            .workers
            .get(id)
            .and_then(|worker| worker.rollout.clone());
        if let Some(rollout) = rollout {
            let prepared = rollout
                .prepare_append()
                .await
                .map_err(|error| RuntimeString::from(error.to_string()))?;
            prepared.commit(RolloutItem::Event(crate::schemas::rollout::EventItem {
                event: CREATION_COMMIT_EVENT.into(),
                turn_id: None,
                payload: Some(RuntimeJson::Bool(true)),
            }));
            rollout
                .flush()
                .await
                .map_err(|error| RuntimeString::from(error.to_string()))?;
        }
        Ok(())
    }

    pub(super) async fn commit_ordinary_creation(
        &self,
        id: &str,
        admission: &Arc<OrdinaryCreationAdmission>,
    ) -> Result<(), RuntimeString> {
        let context = admission
            .publication
            .lock_recover()
            .as_ref()
            .map(|publication| publication.context.clone())
            .ok_or_else(|| RuntimeString::from("Missing sub-agent creation metadata."))?;
        let lifecycle_rollout = if !admission.observable {
            None
        } else if context.depth > 1 {
            context
                .parent_agent_id
                .as_ref()
                .and_then(|parent| self.subagents.parent_rollouts.writer(parent))
        } else {
            self.persistence.rollout().cloned()
        };
        let prepared = match &lifecycle_rollout {
            Some(writer) => Some(
                writer
                    .prepare_batch(3)
                    .await
                    .map_err(|error| RuntimeString::from(error.to_string()))?,
            ),
            None => None,
        };
        if admission.abort.is_cancelled() || self.lifecycle.is_disposed() {
            return Err("Sub-agent creation was cancelled.".into());
        }
        let slot = admission
            .slot
            .lock_recover()
            .take()
            .ok_or_else(|| RuntimeString::from("Missing pending residency reservation."))?;
        let publication = admission
            .publication
            .lock_recover()
            .take()
            .ok_or_else(|| RuntimeString::from("Missing sub-agent creation metadata."))?;
        let runtime = self.ordinary_runtime();
        let bindings = {
            let mut state = runtime.state.lock_recover();
            let worker = state
                .workers
                .get_mut(id)
                .ok_or_else(|| RuntimeString::from("Missing pending sub-agent."))?;
            worker.committed = true;
            let name = worker
                .snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.name.clone());
            let bindings = worker.bindings.clone();
            bind_name(&mut state, id, name.as_ref());
            bindings
        };
        slot.commit(id);
        if admission.observable {
            self.publish_ordinary_handle(
                id,
                admission.abort.clone(),
                BackgroundTaskStatus::Running,
                admission.description.clone(),
                &bindings,
            );
            if admission.persistent {
                if let Some(entry) = self.subagents.background_agents.lock_recover().get_mut(id) {
                    entry.teammate = true;
                    entry.mailbox_live = true;
                }
            }
            let mut records =
                self.stage_subagent_started(id, publication.kind, publication.context);
            {
                let mut lifecycle = self.subagents.lifecycle.lock_recover();
                if lifecycle
                    .set_model(id, publication.model, publication.model_source)
                    .is_ok()
                {
                    records.extend(lifecycle.get(id).cloned());
                }
            }
            if let Some(prepared) = prepared {
                prepared.commit(
                    records
                        .into_iter()
                        .map(|record| {
                            RolloutItem::SubagentLifecycle(
                                crate::schemas::rollout::SubagentLifecycleItem { record },
                            )
                        })
                        .collect(),
                );
            }
        }
        admission.committed.store(true, Ordering::Release);
        admission.cleanup_token.lock_recover().take();
        Ok(())
    }

    pub(super) async fn cleanup_pending_ordinary(&self, id: &str) {
        let runtime = self.ordinary_runtime();
        let worker = runtime.state.lock_recover().workers.remove(id);
        if let Some(worker) = worker {
            if let Some(snapshot) = worker.snapshot {
                for client in &snapshot.mcp_clients {
                    client.close().await;
                }
            }
            if let Some(rollout) = worker.rollout {
                match rollout.prepare_append().await {
                    Ok(prepared) => {
                        prepared.commit(RolloutItem::Event(crate::schemas::rollout::EventItem {
                            event: CREATION_COMMIT_EVENT.into(),
                            payload: Some(RuntimeJson::Bool(false)),
                            turn_id: None,
                        }))
                    }
                    Err(error) => {
                        self.note_pending_ordinary_cleanup_error(error.to_string().into())
                    }
                }
                if let Err(error) = rollout.flush().await {
                    self.note_pending_ordinary_cleanup_error(error.to_string().into());
                }
                if let Err(error) = rollout.shutdown().await {
                    self.note_pending_ordinary_cleanup_error(error.to_string().into());
                }
            }
            self.teardown_agent_isolation(&worker.worktree).await;
        }
        self.subagents.background_agents.lock_recover().remove(id);
        self.governor.remove_resident(id);
        let key = SubAgentMapKey::String(id.into());
        self.subagents.transcripts.lock_recover().remove(&key);
        self.subagents.surfaced.lock_recover().remove(&key);
        runtime
            .state
            .lock_recover()
            .names
            .values_mut()
            .for_each(|ids| {
                ids.remove(id);
            });
    }
}
