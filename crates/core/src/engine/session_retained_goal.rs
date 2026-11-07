//! Record genuine human goal controls without creating a turn or queued input.

use super::Session;
use crate::goal::retained_context::{self, GoalInstruction};
use crate::runtime_values::RuntimeString;
use crate::schemas::rollout::RolloutItem;
use crate::sync_ext::MutexExt;

impl Session {
    pub(crate) async fn restore_retained_goal_instructions(
        &self,
        items: &[RolloutItem],
    ) -> std::io::Result<()> {
        let thread = self.id.try_to_utf8().map_err(|error| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
        })?;
        let retained =
            retained_context::instructions_from_rollout_with_receipts(&thread, items).await?;
        let mut accepted = self.conversation.retained_goal_instructions.lock_recover();
        let mut history = self.conversation.history.lock_recover();
        *history =
            retained_context::preserve_instructions(std::mem::take(&mut *history), &retained);
        *accepted = retained;
        let submitted = items
            .iter()
            .filter_map(|item| {
                let RolloutItem::Event(event) = item else {
                    return None;
                };
                if event.event != "goal_submission" {
                    return None;
                }
                event
                    .payload
                    .as_ref()?
                    .get("clientRequestId")?
                    .as_string()
                    .cloned()
            })
            .collect();
        *self.conversation.goal_submission_ids.lock_recover() = submitted;
        *self.conversation.goal_completions.lock_recover() =
            retained_context::completions_from_rollout(items);
        Ok(())
    }

    pub async fn record_user_goal_instruction(
        &self,
        objective: Option<&RuntimeString>,
        status: Option<&RuntimeString>,
    ) -> Result<(), RuntimeString> {
        if let Some(instruction) =
            retained_context::instruction_for_set((self.deps.uuid)().into(), objective, status)
                .map_err(|error| RuntimeString::from(error.to_string()))?
        {
            self.retain_goal_instruction(instruction).await?;
        }
        Ok(())
    }

    pub async fn record_user_goal_clear(&self) -> Result<(), RuntimeString> {
        self.retain_goal_instruction(retained_context::clear_instruction(
            (self.deps.uuid)().into(),
        ))
        .await
    }

    pub async fn record_user_goal_submission_instruction(
        &self,
        request_id: &RuntimeString,
        objective: Option<&RuntimeString>,
        status: Option<&RuntimeString>,
    ) -> Result<(), RuntimeString> {
        use sha2::{Digest, Sha256};
        let id = format!(
            "goal-instruction-{:x}",
            Sha256::digest(request_id.to_node_utf8())
        )
        .into();
        if let Some(instruction) = retained_context::instruction_for_set(id, objective, status)
            .map_err(|error| RuntimeString::from(error.to_string()))?
        {
            self.retain_goal_instruction(instruction).await?;
        }
        Ok(())
    }

    async fn retain_goal_instruction(
        &self,
        instruction: GoalInstruction,
    ) -> Result<(), RuntimeString> {
        if self.conversation.retained_goal_instructions.lock_recover().iter().any(|message| {
            matches!(message, crate::schemas::message::Message::User(user) if user.uuid == instruction.id)
        }) { return Ok(()); }
        let rollout = self.persistence.rollout().ok_or_else(|| {
            RuntimeString::from("user goal instructions require a persistent thread")
        })?;
        retained_context::append_instruction(rollout.as_ref(), &instruction)
            .await
            .map_err(|error| {
                RuntimeString::from(format!("goal instruction could not be retained: {error}"))
            })?;
        let message = retained_context::instruction_message(&instruction);
        let mut accepted = self.conversation.retained_goal_instructions.lock_recover();
        let mut history = self.conversation.history.lock_recover();
        accepted.push(message.clone());
        history.push(message);
        Ok(())
    }
}

impl Session {
    /// Persist the explicit composer objective once, independently of automatic input.
    pub async fn record_submitted_goal_objective(
        &self,
        client_request_id: RuntimeString,
        objective: RuntimeString,
        display_text: Option<RuntimeString>,
    ) -> Result<crate::goal::retained_context::GoalSubmission, RuntimeString> {
        use crate::runtime_values::{RuntimeJson, RuntimeObject};
        use crate::schemas::content::{ContentBlock, TextBlock};
        use crate::schemas::message::{Message, UserMessage};
        use crate::store::session_store::AppendEventOptions;
        use sha2::{Digest, Sha256};
        let rollout = self
            .persistence
            .rollout()
            .ok_or_else(|| RuntimeString::from("goal submission requires a persistent thread"))?;
        let uuid: RuntimeString = format!(
            "goal-user-{:x}",
            Sha256::digest(client_request_id.to_node_utf8())
        )
        .into();
        let display_uuid = uuid.concat(&"-display".into());
        let mut history = self.conversation.history_snapshot();
        if self
            .conversation
            .goal_submission_ids
            .lock_recover()
            .contains(&client_request_id)
            && !history
                .iter()
                .any(|message| matches!(message, Message::User(user) if user.uuid == uuid))
        {
            let store = self
                .persistence
                .store()
                .ok_or_else(|| RuntimeString::from("goal submission history is unavailable"))?;
            history = store
                .read_thread(
                    &self
                        .id
                        .try_to_utf8()
                        .map_err(|error| RuntimeString::from(error.to_string()))?,
                )
                .await
                .map_err(|error| RuntimeString::from(error.to_string()))?
                .raw_items
                .into_iter()
                .filter_map(|item| match item {
                    crate::schemas::rollout::RolloutItem::Message { message } => Some(message),
                    _ => None,
                })
                .collect();
        }
        let saved_objective = history.iter().find_map(|message| match message {
            Message::User(user) if user.uuid == uuid => {
                user.content.iter().find_map(|block| match block {
                    ContentBlock::Text(text) => Some(text.text.clone()),
                    _ => None,
                })
            }
            _ => None,
        });
        let saved_display = history.iter().find_map(|message| match message {
            Message::System(system) if system.uuid == display_uuid => {
                crate::runtime_values::runtime_json::parse(&system.text)
                    .ok()?
                    .get("localRender")?
                    .get("displayText")?
                    .as_str()
                    .cloned()
            }
            _ => None,
        });
        let already_written = saved_objective.is_some();
        let presentation = crate::goal::retained_context::GoalSubmission {
            message_uuid: uuid.clone(),
            display_uuid: display_uuid.clone(),
            text: if already_written {
                saved_display
                    .filter(|text| !text.is_empty())
                    .or(saved_objective)
                    .expect("saved objective exists")
            } else {
                display_text
                    .clone()
                    .filter(|text| !text.is_empty())
                    .unwrap_or_else(|| objective.clone())
            },
        };
        if self
            .conversation
            .goal_submission_ids
            .lock_recover()
            .contains(&client_request_id)
        {
            return Ok(presentation);
        }
        let message = Message::User(UserMessage {
            question_answer: None,
            uuid: uuid.clone(),
            content: vec![ContentBlock::Text(TextBlock {
                text: objective,
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
        });
        let mut metadata = RuntimeObject::new();
        metadata.insert("origin", "goal".into());
        if let Some(display_text) = display_text {
            metadata.insert("displayText", display_text.into());
        }
        let display = super::session_run_part::user_display_message(
            uuid.clone(),
            &RuntimeJson::Object(metadata),
            display_uuid,
        );
        if !already_written {
            rollout.append_message(message.clone()).await;
            rollout.append_message(display.clone()).await;
        }
        let mut receipt = RuntimeObject::new();
        receipt.insert("clientRequestId", client_request_id.clone().into());
        receipt.insert("messageUuid", uuid.clone().into());
        rollout
            .append_event(
                "goal_submission".into(),
                Some(AppendEventOptions {
                    turn_id: None,
                    payload: Some(RuntimeJson::Object(receipt)),
                }),
            )
            .await;
        rollout
            .flush()
            .await
            .map_err(|error| RuntimeString::from(error.to_string()))?;
        if let Some(goal) = self.goal_runtime() {
            goal.remember_submission_message(uuid.clone());
        }
        if !already_written {
            self.conversation.push_history(message);
            self.conversation.push_history(display);
        }
        self.conversation
            .goal_submission_ids
            .lock_recover()
            .insert(client_request_id);
        Ok(presentation)
    }
}

impl Session {
    /// Flush accounting before capturing an explicitly inherited persistent fork.
    pub async fn persistent_goal_fork_snapshot(
        &self,
        before_message_uuid: Option<&str>,
    ) -> Result<super::LiveForkSnapshot, String> {
        if self.persistence.store().is_none() || self.persistence.rollout().is_none() {
            return Err("ephemeral sessions cannot inherit a deferred goal".to_owned());
        }
        let thread = self.id.try_to_utf8().map_err(|error| error.to_string())?;
        let _mutation = crate::goal::service::thread_mutation_guard(&thread).await;
        self.flush_goal_progress()
            .await
            .map_err(|error| error.try_to_utf8().unwrap_or_default())?;
        let inherited_goal = crate::goal::store::capture_goal_fork_snapshot(&thread)
            .await
            .map_err(|error| error.to_string())?;
        let mut snapshot = self.persistent_fork_snapshot(before_message_uuid)?;
        snapshot.inherited_goal = Some(inherited_goal);
        Ok(snapshot)
    }
}

impl Session {
    pub fn goal_completions(&self) -> Vec<crate::goal::retained_context::GoalCompletion> {
        self.conversation.goal_completions.lock_recover().clone()
    }

    pub async fn record_goal_completion(
        &self,
        goal: &crate::goal::ThreadGoal,
        revision: i64,
    ) -> Result<(), RuntimeString> {
        use crate::goal::retained_context::GoalCompletion;
        let source_turn_id = self
            .turn
            .active_stack
            .lock_recover()
            .last()
            .and_then(|turn| turn.turn_id.clone());
        let completion = GoalCompletion {
            goal: goal.clone(),
            goal_revision: revision,
            source_turn_id,
        };
        if self
            .conversation
            .goal_completions
            .lock_recover()
            .iter()
            .any(|entry| {
                entry.goal.goal_id == completion.goal.goal_id
                    && entry.goal_revision >= completion.goal_revision
            })
        {
            return Ok(());
        }
        let rollout = self
            .persistence
            .rollout()
            .ok_or_else(|| RuntimeString::from("goal completion requires a persistent thread"))?;
        retained_context::append_completion(rollout.as_ref(), &completion)
            .await
            .map_err(|error| RuntimeString::from(error.to_string()))?;
        let mut completions = self.conversation.goal_completions.lock_recover();
        if let Some(previous) = completions
            .iter_mut()
            .find(|entry| entry.goal.goal_id == completion.goal.goal_id)
        {
            *previous = completion;
        } else {
            completions.push(completion);
        }
        Ok(())
    }
}

impl Session {
    /// Retain scoped intent before SQL, and grant it only through the exact committed receipt.
    pub async fn pause_goal_by_user_guarded(
        &self,
        control_guard: Option<crate::goal::store::GoalControlGuard>,
        foreground_submission_id: Option<RuntimeString>,
    ) -> Result<(), RuntimeString> {
        let thread = self
            .id
            .try_to_utf8()
            .map_err(|error| RuntimeString::from(error.to_string()))?;
        #[cfg(any(test, feature = "testing"))]
        if let Some(runtime) = self.goal_observation_runtime() {
            runtime.testing_wait_stop_snapshot().await;
        }
        let snapshot = self
            .goal_snapshot()
            .await
            .map_err(|error| RuntimeString::from(error.to_string()))?;
        let Some(goal) = snapshot.goal.filter(|goal| goal.status.is_active()) else {
            return Ok(());
        };
        let _guard = crate::goal::service::thread_mutation_guard(&thread).await;
        let mutation = crate::goal::service::HostGoalMutation {
            status: Some("paused".into()),
            expected_goal_id: Some(goal.goal_id),
            expected_revision: Some(snapshot.revision),
            control_guard,
            ..Default::default()
        };
        let attempt = retained_context::pause_control_attempt(
            format!("goal-control-{}", (self.deps.uuid)()).into(),
            foreground_submission_id,
            &mutation,
        )
        .map_err(|error| RuntimeString::from(error.to_string()))?;
        let rollout = self
            .persistence
            .rollout()
            .ok_or_else(|| RuntimeString::from("user goal controls require a persistent thread"))?;
        retained_context::append_control_attempt(rollout.as_ref(), &attempt)
            .await
            .map_err(|error| RuntimeString::from(error.to_string()))?;
        let request_id = attempt
            .request_id
            .try_to_utf8()
            .map_err(|error| RuntimeString::from(error.to_string()))?;
        let runtime = self
            .goal_observation_runtime()
            .ok_or_else(|| RuntimeString::from("goal controls require a persistent main thread"))?;
        // A guarded control must not synchronize mode or clocks before its
        // reservation check; only the committed transition owns those effects.
        let applied = runtime
            .apply_host_update_once(mutation, &request_id)
            .await
            .map(|_| ())
            .map_err(|error| RuntimeString::from(error.to_string()));
        let accepted = retained_context::control_attempt_was_applied(&thread, &attempt)
            .await
            .map_err(|error| RuntimeString::from(error.to_string()))?;
        retained_context::append_control_disposition(
            rollout.as_ref(),
            &attempt,
            accepted,
            applied.as_ref().err(),
        )
        .await
        .map_err(|error| RuntimeString::from(error.to_string()))?;
        if accepted {
            let message = retained_context::instruction_message(
                &retained_context::applied_control_instruction(&attempt),
            );
            self.conversation
                .retained_goal_instructions
                .lock_recover()
                .push(message.clone());
            self.conversation.history.lock_recover().push(message);
        } else if applied.is_ok() {
            return Err("goal control committed without an exact receipt".into());
        }
        applied
    }
}
