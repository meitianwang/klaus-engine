//! Collaboration views restrict a new child query without replacing its permission preset.

use super::*;
use crate::planning::CollaborationMode;
use std::sync::atomic::{AtomicUsize, Ordering};

struct PlanMutationProbe(Arc<AtomicUsize>);
#[async_trait::async_trait]
impl Tool for PlanMutationProbe {
    fn static_name(&self) -> &str {
        "write"
    }
    fn static_description(&self) -> &str {
        "A mutation that planning must not execute."
    }
    fn input_schema(&self) -> RuntimeJson {
        exact_json!({"type":"object","properties":{},"additionalProperties":false})
    }
    fn persistence_identity(&self) -> Option<RuntimeString> {
        Some("fixture:plan-mutation-boundary:1".into())
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_read_only(&self, _: &RuntimeJson) -> bool {
        false
    }
    fn is_concurrency_safe(&self, _: &RuntimeJson) -> bool {
        false
    }
    async fn call(
        &self,
        _: RuntimeJson,
        _: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, crate::types::errors::ToolError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("A planning query must reject the actual mutation before execution")
    }
}

pub(super) fn mutation_probe() -> (AnyTool, Arc<AtomicUsize>) {
    let count = Arc::new(AtomicUsize::new(0));
    (Arc::new(PlanMutationProbe(count.clone())), count)
}

pub(super) fn mutation_attempt_turn() -> Vec<crate::types::provider::ModelStreamEvent> {
    let mut events = vec![ev::message_start(None)];
    events.extend(exact_tool_use_events(
        "forbidden-plan-mutation",
        "write",
        &exact_json!({}),
    ));
    events.extend(exact_tool_use_events(
        "policy-probe-call",
        "child_policy_probe",
        &exact_json!({}),
    ));
    events.push(ev::stop(
        Some(ModelStopReason::ToolUse),
        Some(make_usage(None, None)),
    ));
    events
}

pub(super) fn assert_query_view(
    session: &Session,
    request: &ModelRequest,
    mode: CollaborationMode,
) {
    let parts = request
        .system
        .iter()
        .filter(|part| part.name == "collaboration-mode")
        .collect::<Vec<_>>();
    assert_eq!(
        parts.len(),
        1,
        "one complete instruction view at the actual query boundary"
    );
    let expected = crate::planning::instructions::instruction_part(&session.config, mode);
    assert_eq!(
        crate::runtime_values::runtime_codec::encode_value(parts[0]).unwrap(),
        crate::runtime_values::runtime_codec::encode_value(&expected).unwrap(),
        "the complete rendered collaboration part must match this admitted query"
    );
    assert_eq!(
        request
            .tools
            .iter()
            .any(|tool| tool.name == "write" || tool.name == "Write"),
        mode == CollaborationMode::Default
    );
    if mode == CollaborationMode::Plan {
        assert!(!request.tools.iter().any(|tool| matches!(
            tool.name.try_to_utf8().ok().as_deref(),
            Some("edit" | "multi_edit" | "notebook_edit" | "EnterWorktree" | "ExitWorktree")
        )));
    }
}

pub(super) fn assert_mutation_rejected(request: &ModelRequest, count: &AtomicUsize) {
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert!(request.messages.iter().any(|message| matches!(message, Message::ToolResult(result) if result.tool_use_id == "forbidden-plan-mutation" && result.is_error)), "the next actual provider request must carry the rejected mutator result");
}

pub(super) fn assert_plan_permissions(observation: &ObservedChildPolicy) {
    use crate::permission::rules::parse_rule_strings;
    assert!(
        observation.planning_read_only,
        "the admitted planning effect ceiling survives ordinary policy changes"
    );
    assert_eq!(
        observation.permission.allow_rules,
        parse_rule_strings(&["shared".into()])
    );
    assert_eq!(
        observation.permission.deny_rules,
        parse_rule_strings(&["current_deny".into()])
    );
    assert_eq!(
        observation.permission.ask_rules,
        parse_rule_strings(&["current_ask".into()])
    );
}
