//! Request-scoped orchestration derived from the raw reasoning selection.
//! Policies are rebuilt for every model call, never persisted in conversation.

use crate::schemas::effort::{EffortLevel, EffortValue};
use crate::types::provider::{ModelCapabilities, SystemPromptPart};
use crate::types::tool::{SpawnSubAgentOptions, ToolContext};
use std::sync::Arc;

/// Resolve the request's local collaboration preset against its captured model.
/// Saved native preferences remain untouched so leaving the mode restores them.
pub(super) fn ultra_effort(metadata: &ModelCapabilities) -> EffortValue {
    let levels: Vec<_> = if metadata.supported_reasoning_efforts.is_empty() {
        metadata
            .effort
            .as_ref()
            .map(|cap| cap.levels.iter().copied().map(EffortValue::Level).collect())
            .unwrap_or_default()
    } else {
        metadata
            .supported_reasoning_efforts
            .iter()
            .filter_map(|value| {
                value
                    .try_to_utf8()
                    .ok()
                    .as_deref()
                    .and_then(|value| EffortValue::from_raw(&value.into()))
            })
            .collect()
    };
    let native = |effort: &EffortValue| !effort.is_ultra();
    let preferred = metadata
        .multi_agent_reasoning_effort
        .as_ref()
        .and_then(|value| {
            value
                .try_to_utf8()
                .ok()
                .as_deref()
                .and_then(|value| EffortValue::from_raw(&value.into()))
        })
        .filter(|value| native(value) && levels.contains(value));
    preferred
        .or_else(|| {
            levels
                .contains(&EffortValue::Level(EffortLevel::Max))
                .then_some(EffortValue::Level(EffortLevel::Max))
        })
        .or_else(|| levels.into_iter().rev().find(native))
        .unwrap_or(EffortValue::Level(EffortLevel::Medium))
}

/// Resolve raw special values once; an Ultra result is already a wire choice.
pub(super) fn resolve_effort(
    effort: Option<EffortValue>,
    metadata: &ModelCapabilities,
) -> Option<EffortValue> {
    match effort {
        Some(EffortValue::Level(EffortLevel::Ultra)) => Some(ultra_effort(metadata)),
        Some(EffortValue::Level(EffortLevel::Persistent)) => {
            Some(EffortValue::Custom("disabled".into()))
        }
        other => other,
    }
}

#[cfg(test)]
pub(super) fn request_policy(
    canonical: &[SystemPromptPart],
    ultra_mode: bool,
    tools: &[crate::types::provider::ToolSpec],
    agent_id: Option<&crate::runtime_values::RuntimeString>,
) -> Vec<SystemPromptPart> {
    request_policy_with_model_messages(canonical, ultra_mode, tools, agent_id, true, None, None)
}

pub(super) fn request_policy_with_model_messages(
    canonical: &[SystemPromptPart],
    ultra_mode: bool,
    tools: &[crate::types::provider::ToolSpec],
    agent_id: Option<&crate::runtime_values::RuntimeString>,
    include_instructions: bool,
    model_messages: Option<&crate::runtime_values::RuntimeJson>,
    configured_hint: Option<&crate::runtime_values::RuntimeString>,
) -> Vec<SystemPromptPart> {
    let mut parts: Vec<_> = canonical
        .iter()
        .filter(|part| !crate::context::multi_agent::is_instruction_part(&part.name))
        .cloned()
        .collect();
    if !include_instructions {
        return parts;
    }
    let index = parts
        .iter()
        .position(|part| part.name == "total-tokens" || part.name.starts_with("provider-behavior:"))
        .unwrap_or(parts.len());
    let instructions = if model_messages.is_none() && configured_hint.is_none() {
        crate::context::multi_agent::instructions(tools, agent_id, ultra_mode)
    } else {
        crate::context::multi_agent::instructions_with_model_messages(
            tools,
            agent_id,
            ultra_mode,
            model_messages,
            configured_hint,
        )
    };
    parts.splice(index..index, instructions);
    parts
}

/// Snapshot the effective parent-turn effort and orchestration bool into all
/// three spawn paths. This is separate from a child's explicit effort and role
/// defaults, which retain precedence.
pub(super) fn inherit_parent_effort(
    ctx: &mut ToolContext,
    effort: Option<EffortValue>,
    ultra_mode: bool,
) {
    let background_effort = effort.clone();
    let workflow_effort = effort.clone();
    if let Some(spawn) = ctx.spawn_sub_agent.take() {
        ctx.spawn_sub_agent = Some(Arc::new(move |mut opts: SpawnSubAgentOptions| {
            opts.inherited_effort = Some(effort.clone());
            opts.inherited_ultra_mode = Some(ultra_mode);
            spawn(opts)
        }));
    }
    if let Some(spawn) = ctx.spawn_background_agent.take() {
        ctx.spawn_background_agent = Some(Arc::new(move |mut opts: SpawnSubAgentOptions| {
            opts.inherited_effort = Some(background_effort.clone());
            opts.inherited_ultra_mode = Some(ultra_mode);
            spawn(opts)
        }));
    }
    if let Some(spawn) = ctx.spawn_workflow_agent.take() {
        ctx.spawn_workflow_agent = Some(Arc::new(move |mut opts: SpawnSubAgentOptions| {
            opts.inherited_effort = Some(workflow_effort.clone());
            opts.inherited_ultra_mode = Some(ultra_mode);
            spawn(opts)
        }));
    }
}

pub(super) fn inherit_parent_context(
    ctx: &mut ToolContext,
    context: Option<Arc<crate::types::tool::InheritedSubagentContext>>,
) {
    if let Some(context) = &context {
        if let Some(registry) = ctx
            .background_tasks
            .as_ref()
            .and_then(|registry| registry.with_inherited_mcp(context))
        {
            ctx.background_tasks = Some(registry);
        }
    }
    if let Some(spawn) = ctx.spawn_sub_agent.take() {
        let context = context.clone();
        ctx.spawn_sub_agent = Some(Arc::new(move |mut opts: SpawnSubAgentOptions| {
            opts.inherited_context = context.clone();
            spawn(opts)
        }));
    }
    if let Some(spawn) = ctx.spawn_background_agent.take() {
        let context = context.clone();
        ctx.spawn_background_agent = Some(Arc::new(move |mut opts: SpawnSubAgentOptions| {
            opts.inherited_context = context.clone();
            spawn(opts)
        }));
    }
    if let Some(spawn) = ctx.spawn_workflow_agent.take() {
        ctx.spawn_workflow_agent = Some(Arc::new(move |mut opts: SpawnSubAgentOptions| {
            opts.inherited_context = context.clone();
            spawn(opts)
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_special_values_resolve_once_without_dropping_custom_catalog_entries() {
        let metadata = ModelCapabilities {
            supported_reasoning_efforts: ["ultra", "custom/exact", "persistent"]
                .into_iter()
                .map(Into::into)
                .collect(),
            multi_agent_reasoning_effort: Some("custom/exact".into()),
            ..Default::default()
        };
        assert_eq!(
            ultra_effort(&metadata),
            EffortValue::Custom("custom/exact".into())
        );
        let metadata = ModelCapabilities {
            multi_agent_reasoning_effort: None,
            ..metadata
        };
        assert_eq!(
            resolve_effort(Some(EffortValue::Level(EffortLevel::Ultra)), &metadata),
            Some(EffortValue::Level(EffortLevel::Persistent))
        );
        assert_eq!(
            resolve_effort(Some(EffortValue::Level(EffortLevel::Persistent)), &metadata),
            Some(EffortValue::Custom("disabled".into()))
        );
    }

    fn native_tools() -> Vec<crate::types::provider::ToolSpec> {
        vec![crate::types::provider::ToolSpec {
            name: "task".into(),
            description: "Start a child.".into(),
            input_schema: crate::runtime_values::RuntimeJson::Object(
                crate::runtime_values::RuntimeObject::new(),
            ),
            kind: crate::types::provider::ToolSpecKind::Function,
            format: None,
        }]
    }

    #[test]
    fn role_and_mode_follow_stable_context_and_precede_the_token_countdown() {
        use crate::context::multi_agent::{EXPLICIT_MODE, MODE_PART, PROACTIVE_MODE, ROLE_PART};
        use crate::types::provider::CacheScope;
        let canonical = vec![
            SystemPromptPart {
                name: "base".into(),
                text: "Cached instructions".into(),
                cacheable: Some(true),
                cache_scope: Some(CacheScope::Global),
            },
            SystemPromptPart {
                name: "total-tokens".into(),
                text: "123 tokens left".into(),
                cacheable: None,
                cache_scope: None,
            },
        ];
        let tools = native_tools();
        for (ultra_mode, text) in [(false, EXPLICIT_MODE), (true, PROACTIVE_MODE)] {
            let parts = request_policy(&canonical, ultra_mode, &tools, None);
            assert_eq!(parts[0], canonical[0]);
            assert_eq!(parts[1].name, ROLE_PART);
            assert_eq!(parts[2].name, MODE_PART);
            assert_eq!(parts[2].text, text);
            assert_eq!(parts[3], canonical[1]);
            assert_eq!(parts[1].cacheable, Some(false));
            assert_eq!(parts[2].cacheable, Some(false));
            assert_eq!(request_policy(&parts, ultra_mode, &tools, None), parts);
            assert_eq!(canonical.len(), 2);
        }
    }

    #[test]
    fn current_mode_and_query_origin_replace_stale_delegation_authorization() {
        use crate::context::multi_agent::{EXPLICIT_MODE, MODE_PART, PROACTIVE_MODE};
        let tools = native_tools();
        let ultra = request_policy(&[], true, &tools, None);
        assert_eq!(ultra[1].text, PROACTIVE_MODE);
        let ordinary = request_policy(&ultra, false, &tools, None);
        assert_eq!(ordinary.len(), 2);
        assert_eq!(ordinary[1].name, MODE_PART);
        assert_eq!(ordinary[1].text, EXPLICIT_MODE);
        assert_eq!(
            request_policy(&ordinary, true, &[], None)[1].text,
            PROACTIVE_MODE
        );
        assert!(request_policy_with_model_messages(
            &ordinary, true, &tools, None, false, None, None
        )
        .is_empty());
        let child = request_policy(&ordinary, true, &tools, Some(&"worker".into()));
        assert!(child[0].text.contains("child agent"));
        assert!(!child[0].text.contains("primary agent coordinating"));
    }

    /// Ordinary non-Ultra selections use the explicit-request policy.
    #[test]
    fn native_effort_levels_do_not_authorize_proactive_delegation() {
        use crate::context::multi_agent::EXPLICIT_MODE;
        let tools = native_tools();
        let parts = request_policy(&[], false, &tools, None);
        assert_eq!(parts[1].text, EXPLICIT_MODE);
    }

    #[test]
    fn workflow_spawn_snapshots_explicit_parent_none() {
        use std::sync::Mutex;
        let seen = Arc::new(Mutex::new(None));
        let capture = seen.clone();
        let mut ctx = crate::tools::builtins::test_ctx::make_ctx(".");
        ctx.spawn_workflow_agent = Some(Arc::new(move |opts| {
            *capture.lock().unwrap() = Some(opts.inherited_effort);
            Box::pin(futures::stream::empty())
        }));
        inherit_parent_effort(&mut ctx, None, false);
        drop((ctx.spawn_workflow_agent.unwrap())(
            SpawnSubAgentOptions::default(),
        ));
        assert_eq!(*seen.lock().unwrap(), Some(Some(None)));
    }
}
