//! The session-owned sub-agent lifecycle registry. The persisted record
//! schema (record/kind/status/context/snapshot and their codecs) lives in
//! [`crate::schemas::subagent_lifecycle`] and is re-exported here so existing
//! `engine::subagent_lifecycle::*` paths keep working.

use crate::runtime_values::RuntimeString;
pub use crate::schemas::subagent_lifecycle::{
    SubagentContext, SubagentKind, SubagentLifecycleRecord, SubagentSnapshot, SubagentStatus,
    RECOVERY_INTERRUPTED_REASON,
};
use std::collections::BTreeMap;

/// A session-owned sub-agent lifecycle index, rebuilt from rollout records on
/// cold resume.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SubagentLifecycleRegistry {
    records: BTreeMap<RuntimeString, SubagentLifecycleRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubagentLifecycleError {
    AlreadyExists { agent_id: RuntimeString },
    NotFound { agent_id: RuntimeString },
}

impl SubagentLifecycleRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn spawn(&mut self, record: SubagentLifecycleRecord) -> Result<(), SubagentLifecycleError> {
        let agent_id = record.agent_id.clone();
        if self.records.contains_key(&agent_id) {
            return Err(SubagentLifecycleError::AlreadyExists { agent_id });
        }
        self.records.insert(agent_id, record);
        Ok(())
    }

    pub fn get(&self, agent_id: impl Into<RuntimeString>) -> Option<&SubagentLifecycleRecord> {
        self.records.get(&agent_id.into())
    }

    pub fn snapshot(&self) -> SubagentSnapshot {
        SubagentSnapshot {
            agents: self.records.values().cloned().collect(),
        }
    }

    pub fn recovery_records(&self) -> Vec<SubagentLifecycleRecord> {
        self.records.values().cloned().collect()
    }

    pub fn from_recovery_records(
        records: Vec<SubagentLifecycleRecord>,
    ) -> Result<Self, SubagentLifecycleError> {
        let mut registry = Self::new();
        for record in records {
            registry.spawn(record)?;
        }
        Ok(registry)
    }

    /// Rebuild from an append-only transition stream. Later records for the
    /// same agent are authoritative, matching rollout replay semantics.
    pub fn restore_latest(records: Vec<SubagentLifecycleRecord>) -> Self {
        let mut registry = Self::new();
        for record in records {
            registry.records.insert(record.agent_id.clone(), record);
        }
        registry
    }

    pub fn children_of(
        &self,
        parent_agent_id: impl Into<RuntimeString>,
    ) -> Vec<SubagentLifecycleRecord> {
        let parent_agent_id = parent_agent_id.into();
        self.records
            .values()
            .filter(|record| record.context.parent_agent_id.as_ref() == Some(&parent_agent_id))
            .cloned()
            .collect()
    }

    pub fn descendants_of(
        &self,
        parent_agent_id: impl Into<RuntimeString>,
    ) -> Result<Vec<SubagentLifecycleRecord>, SubagentLifecycleError> {
        let parent_agent_id = parent_agent_id.into();
        if !self.records.contains_key(&parent_agent_id) {
            return Err(SubagentLifecycleError::NotFound {
                agent_id: parent_agent_id,
            });
        }
        let mut descendants = Vec::new();
        self.collect_descendants(&parent_agent_id, &mut descendants);
        Ok(descendants)
    }

    pub fn mark_running(
        &mut self,
        agent_id: impl Into<RuntimeString>,
        now_ms: i64,
    ) -> Result<(), SubagentLifecycleError> {
        self.transition(agent_id, now_ms, SubagentStatus::Running, None, None, false)
    }

    /// Stamp the concrete model a child run resolved to, with the model
    /// source (the product's model mode) it belongs to. Spawn resolves the
    /// tier token against the session's tier targets only after the lifecycle
    /// row exists, so the model lands as its own transition and persists with
    /// the next rolled-out record.
    pub fn set_model(
        &mut self,
        agent_id: impl Into<RuntimeString>,
        model: impl Into<RuntimeString>,
        source: Option<RuntimeString>,
    ) -> Result<(), SubagentLifecycleError> {
        let agent_id = agent_id.into();
        let record =
            self.records
                .get_mut(&agent_id)
                .ok_or_else(|| SubagentLifecycleError::NotFound {
                    agent_id: agent_id.clone(),
                })?;
        record.model = Some(model.into());
        record.model_source = source;
        Ok(())
    }

    pub fn mark_idle(
        &mut self,
        agent_id: impl Into<RuntimeString>,
        now_ms: i64,
    ) -> Result<(), SubagentLifecycleError> {
        self.transition(agent_id, now_ms, SubagentStatus::Idle, None, None, false)
    }

    pub fn mark_interrupted(
        &mut self,
        agent_id: impl Into<RuntimeString>,
        now_ms: i64,
        reason: impl Into<RuntimeString>,
    ) -> Result<(), SubagentLifecycleError> {
        self.transition(
            agent_id,
            now_ms,
            SubagentStatus::Interrupted,
            None,
            Some(reason.into()),
            false,
        )
    }

    pub fn mark_completed(
        &mut self,
        agent_id: impl Into<RuntimeString>,
        now_ms: i64,
        result: Option<RuntimeString>,
    ) -> Result<(), SubagentLifecycleError> {
        self.transition(
            agent_id,
            now_ms,
            SubagentStatus::Completed,
            result,
            None,
            false,
        )
    }

    pub fn mark_failed(
        &mut self,
        agent_id: impl Into<RuntimeString>,
        now_ms: i64,
        error: impl Into<RuntimeString>,
    ) -> Result<(), SubagentLifecycleError> {
        self.transition(
            agent_id,
            now_ms,
            SubagentStatus::Failed,
            None,
            Some(error.into()),
            false,
        )
    }

    /// A budget ceiling (turn or spend) ended the child's turn. The child's
    /// partial result rides along as `result`, exactly like a completed run.
    pub fn mark_budget_limited(
        &mut self,
        agent_id: impl Into<RuntimeString>,
        now_ms: i64,
        result: Option<RuntimeString>,
        reason: impl Into<RuntimeString>,
    ) -> Result<(), SubagentLifecycleError> {
        self.transition(
            agent_id,
            now_ms,
            SubagentStatus::BudgetLimited,
            result,
            Some(reason.into()),
            false,
        )
    }

    pub fn cancel(
        &mut self,
        agent_id: impl Into<RuntimeString>,
        now_ms: i64,
    ) -> Result<(), SubagentLifecycleError> {
        self.transition(
            agent_id,
            now_ms,
            SubagentStatus::Cancelled,
            None,
            None,
            true,
        )
    }

    pub fn cancel_descendants_of(
        &mut self,
        parent_agent_id: impl Into<RuntimeString>,
        now_ms: i64,
    ) -> Result<Vec<RuntimeString>, SubagentLifecycleError> {
        let descendants = self.descendants_of(parent_agent_id)?;
        let mut cancelled = Vec::new();
        for record in descendants {
            if record.status.is_terminal() {
                continue;
            }
            self.cancel(&record.agent_id, now_ms)?;
            cancelled.push(record.agent_id);
        }
        Ok(cancelled)
    }

    /// Convert process-local states that cannot survive a cold restart into
    /// explicit terminal records and return the synthesized transitions.
    pub fn interrupt_nonterminal_for_recovery(
        &mut self,
        now_ms: i64,
    ) -> Vec<SubagentLifecycleRecord> {
        let agent_ids = self
            .records
            .values()
            .filter(|record| !record.status.is_terminal())
            .map(|record| record.agent_id.clone())
            .collect::<Vec<_>>();
        agent_ids
            .into_iter()
            .filter_map(|agent_id| {
                self.mark_interrupted(&agent_id, now_ms, RECOVERY_INTERRUPTED_REASON)
                    .ok()
                    .and_then(|()| self.get(&agent_id).cloned())
            })
            .collect()
    }

    fn transition(
        &mut self,
        agent_id: impl Into<RuntimeString>,
        now_ms: i64,
        status: SubagentStatus,
        result: Option<RuntimeString>,
        error: Option<RuntimeString>,
        cancellation_requested: bool,
    ) -> Result<(), SubagentLifecycleError> {
        let agent_id = agent_id.into();
        let Some(record) = self.records.get_mut(&agent_id) else {
            return Err(SubagentLifecycleError::NotFound { agent_id });
        };
        record.status = status;
        record.updated_at_ms = now_ms;
        if status.is_terminal() {
            record.completed_at_ms = Some(now_ms);
        } else {
            record.completed_at_ms = None;
        }
        if result.is_some() {
            record.last_result = result;
        }
        if error.is_some() {
            record.last_error = error;
        }
        if cancellation_requested {
            record.cancellation_requested = true;
        }
        Ok(())
    }

    fn collect_descendants(
        &self,
        parent_agent_id: &RuntimeString,
        descendants: &mut Vec<SubagentLifecycleRecord>,
    ) {
        for child in self.children_of(parent_agent_id) {
            descendants.push(child.clone());
            self.collect_descendants(&child.agent_id, descendants);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::runtime_codec;

    fn child_context() -> SubagentContext {
        SubagentContext {
            session_id: "session-1".into(),
            parent_agent_id: Some("parent".into()),
            parent_thread_id: Some("thread-parent".into()),
            agent_type: Some("general-purpose".into()),
            cwd: "/workspace/project".into(),
            description: "inspect engine state".into(),
            depth: 2,
            resume_from: Some("old-child".into()),
            spawn_turn_id: None,
        }
    }

    #[test]
    fn spawn_status_and_context_are_visible_in_snapshot() {
        let mut registry = SubagentLifecycleRegistry::new();

        registry
            .spawn(SubagentLifecycleRecord::new(
                "child-1",
                SubagentKind::Foreground,
                child_context(),
                100,
            ))
            .expect("spawn should register child");
        registry
            .mark_running("child-1", 110)
            .expect("running transition should succeed");

        let snapshot = registry.snapshot();
        assert_eq!(snapshot.agents.len(), 1);
        let child = &snapshot.agents[0];
        assert_eq!(child.agent_id, "child-1");
        assert_eq!(child.kind, SubagentKind::Foreground);
        assert_eq!(child.status, SubagentStatus::Running);
        assert_eq!(
            child.context.parent_agent_id.as_ref(),
            Some(&RuntimeString::from("parent"))
        );
        assert_eq!(
            child.context.parent_thread_id.as_ref(),
            Some(&RuntimeString::from("thread-parent"))
        );
        assert_eq!(child.context.depth, 2);
        assert_eq!(
            child.context.resume_from.as_ref(),
            Some(&RuntimeString::from("old-child"))
        );
        assert_eq!(child.started_at_ms, 100);
        assert_eq!(child.updated_at_ms, 110);
    }

    #[test]
    fn cancelling_parent_cancels_descendants_only() {
        let mut registry = SubagentLifecycleRegistry::new();

        registry
            .spawn(SubagentLifecycleRecord::new(
                "parent",
                SubagentKind::Foreground,
                SubagentContext {
                    parent_agent_id: None,
                    depth: 1,
                    ..child_context()
                },
                10,
            ))
            .expect("spawn parent");
        registry
            .spawn(SubagentLifecycleRecord::new(
                "child",
                SubagentKind::Background,
                SubagentContext {
                    parent_agent_id: Some("parent".into()),
                    depth: 2,
                    ..child_context()
                },
                20,
            ))
            .expect("spawn child");
        registry
            .spawn(SubagentLifecycleRecord::new(
                "grandchild",
                SubagentKind::Workflow,
                SubagentContext {
                    parent_agent_id: Some("child".into()),
                    depth: 3,
                    ..child_context()
                },
                30,
            ))
            .expect("spawn grandchild");
        registry
            .spawn(SubagentLifecycleRecord::new(
                "unrelated",
                SubagentKind::Persistent,
                SubagentContext {
                    parent_agent_id: None,
                    depth: 1,
                    ..child_context()
                },
                40,
            ))
            .expect("spawn unrelated");

        registry.mark_running("parent", 11).expect("parent running");
        registry.mark_running("child", 21).expect("child running");
        registry
            .mark_running("grandchild", 31)
            .expect("grandchild running");
        registry
            .mark_running("unrelated", 41)
            .expect("unrelated running");

        let cancelled = registry
            .cancel_descendants_of("parent", 99)
            .expect("cancel descendants");

        assert_eq!(
            cancelled,
            vec![
                RuntimeString::from("child"),
                RuntimeString::from("grandchild")
            ]
        );
        assert_eq!(
            registry.get("parent").unwrap().status,
            SubagentStatus::Running
        );
        assert_eq!(
            registry.get("child").unwrap().status,
            SubagentStatus::Cancelled
        );
        assert_eq!(
            registry.get("grandchild").unwrap().status,
            SubagentStatus::Cancelled
        );
        assert_eq!(
            registry.get("unrelated").unwrap().status,
            SubagentStatus::Running
        );
    }

    #[test]
    fn recovery_records_rebuild_registry_state() {
        let mut registry = SubagentLifecycleRegistry::new();
        registry
            .spawn(SubagentLifecycleRecord::new(
                "child-1",
                SubagentKind::Foreground,
                child_context(),
                100,
            ))
            .expect("spawn child");
        registry
            .mark_completed("child-1", 150, Some("done".into()))
            .expect("complete child");

        let recovered =
            SubagentLifecycleRegistry::from_recovery_records(registry.recovery_records())
                .expect("recovery should rebuild");

        assert_eq!(recovered.snapshot(), registry.snapshot());
        let child = recovered.get("child-1").expect("child recovered");
        assert_eq!(child.status, SubagentStatus::Completed);
        assert_eq!(
            child.last_result.as_ref(),
            Some(&RuntimeString::from("done"))
        );
        assert_eq!(child.context.session_id, "session-1");
        assert_eq!(child.context.cwd, "/workspace/project");
    }

    #[test]
    fn cold_recovery_keeps_terminal_records_and_interrupts_live_ones() {
        let mut completed = SubagentLifecycleRecord::new(
            "completed",
            SubagentKind::Foreground,
            child_context(),
            10,
        );
        completed.status = SubagentStatus::Completed;
        completed.updated_at_ms = 20;
        completed.completed_at_ms = Some(20);
        completed.last_result = Some("done".into());

        let mut running =
            SubagentLifecycleRecord::new("running", SubagentKind::Background, child_context(), 30);
        running.status = SubagentStatus::Running;
        running.updated_at_ms = 40;
        let mut idle =
            SubagentLifecycleRecord::new("idle", SubagentKind::Persistent, child_context(), 50);
        idle.status = SubagentStatus::Idle;
        idle.updated_at_ms = 60;
        idle.last_result = Some("previous turn".into());

        let mut registry =
            SubagentLifecycleRegistry::restore_latest(vec![completed.clone(), running, idle]);
        let transitions = registry.interrupt_nonterminal_for_recovery(100);

        assert_eq!(transitions.len(), 2);
        assert_eq!(registry.get("completed"), Some(&completed));
        for agent_id in ["running", "idle"] {
            let record = registry.get(agent_id).unwrap();
            assert_eq!(record.status, SubagentStatus::Interrupted);
            assert_eq!(record.updated_at_ms, 100);
            assert_eq!(record.completed_at_ms, Some(100));
            assert_eq!(
                record.last_error.as_ref(),
                Some(&RuntimeString::from(RECOVERY_INTERRUPTED_REASON))
            );
        }
        assert_eq!(
            registry.get("idle").unwrap().last_result.as_ref(),
            Some(&RuntimeString::from("previous turn"))
        );
    }

    #[test]
    fn append_only_recovery_uses_the_last_record_per_agent() {
        let spawned =
            SubagentLifecycleRecord::new("child-1", SubagentKind::Workflow, child_context(), 10);
        let mut failed = spawned.clone();
        failed.status = SubagentStatus::Failed;
        failed.updated_at_ms = 20;
        failed.completed_at_ms = Some(20);
        failed.last_error = Some("boom".into());

        let registry = SubagentLifecycleRegistry::restore_latest(vec![spawned, failed.clone()]);
        assert_eq!(registry.snapshot().agents, vec![failed]);
    }

    #[test]
    fn lifecycle_codec_and_registry_preserve_exact_utf16_ids_and_text() {
        let agent_id = RuntimeString::from_utf16([b'a' as u16, 0xd800]);
        let description = RuntimeString::from_utf16([0xdc00, b'd' as u16]);
        let mut context = child_context();
        context.description = description.clone();
        let mut record =
            SubagentLifecycleRecord::new(agent_id.clone(), SubagentKind::Background, context, 10);
        record.last_result = Some(description.clone());

        let encoded = runtime_codec::encode_value(&record).unwrap();
        let decoded = runtime_codec::decode_value::<SubagentLifecycleRecord>(&encoded).unwrap();
        assert_eq!(decoded, record);

        let mut registry = SubagentLifecycleRegistry::new();
        registry.spawn(record).unwrap();
        assert_eq!(registry.get(&agent_id).unwrap().agent_id, agent_id);
    }

    #[test]
    fn set_model_stamps_the_resolved_child_model_onto_the_record() {
        let mut registry = SubagentLifecycleRegistry::new();
        registry
            .spawn(SubagentLifecycleRecord::new(
                "child-1",
                SubagentKind::Background,
                child_context(),
                10,
            ))
            .expect("spawn child");
        assert_eq!(registry.get("child-1").unwrap().model, None);

        registry
            .set_model("child-1", "tier-resolved-model", Some("codex".into()))
            .expect("stamp model");
        let stamped = registry.get("child-1").expect("stamped record");
        assert_eq!(
            stamped.model.as_ref(),
            Some(&RuntimeString::from("tier-resolved-model"))
        );
        assert_eq!(
            stamped.model_source.as_ref(),
            Some(&RuntimeString::from("codex"))
        );

        // The stamp rides the wire shape the host reads, and survives a
        // rollout round trip so a reopened session still names the model.
        let encoded = runtime_codec::encode_value(stamped).unwrap();
        let decoded = runtime_codec::decode_value::<SubagentLifecycleRecord>(&encoded).unwrap();
        assert_eq!(decoded.model, stamped.model);
        assert_eq!(registry.snapshot().agents[0].model, stamped.model);

        assert!(matches!(
            registry.set_model("missing", "any-model", None),
            Err(SubagentLifecycleError::NotFound { .. })
        ));
    }
}
