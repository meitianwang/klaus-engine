//! Request-scoped collaboration roles and delegation policy.
//!
//! The role describes the current agent's responsibilities. The mode decides
//! when delegation is authorized; execution details belong to tool contracts.

use crate::runtime_values::RuntimeString;
use crate::types::provider::{SystemPromptPart, ToolSpec};

pub(crate) const ROLE_PART: &str = "multi-agent-role";
pub(crate) const MODE_PART: &str = "orchestration-mode";

pub(crate) const ROOT_ROLE: &str =
    "You are the primary agent coordinating the user's requested outcome. \
    Delegate independent, bounded work through the native collaboration tools declared for this \
    request, continue complementary work yourself, and integrate the returned evidence into your \
    final answer. Messages sent to other agents may be read by the user, so make them legible. \
    Always put proper spaces between words and/or numbers. Agents may delegate further only \
    through capabilities actually available to them. \
    A child owns its assigned scope. Reuse an existing child when it already owns useful context. \
    Do not duplicate an unresolved assignment or redo completed child work without a concrete \
    unresolved risk. Review evidence when that risk warrants it; additional reviewers or review \
    rounds are not a completion requirement.";

pub(crate) const CHILD_ROLE: &str = "You are a child agent collaborating with your parent on a bounded \
    assignment. Complete the assigned scope, report the evidence and any remaining uncertainty in \
    your final answer, and return it to your parent. Your final answer and messages to other agents \
    may also be read by the user, so make them legible. Always put proper spaces between words \
    and/or numbers. Delegate further only through the native \
    collaboration tools actually declared for this request and only when the current delegation \
    mode and assigned scope permit it. Do not reassign the whole task, duplicate another child's \
    unresolved work, or repeat completed research without a concrete unresolved risk. Additional \
    reviewers or review rounds are not a completion requirement.";

pub(crate) const PROACTIVE_MODE: &str = "Proactive multi-agent delegation is active. Any earlier \
    developer instruction requiring an explicit user request before spawning sub-agents no longer \
    applies. This mode remains active until a later multi-agent mode developer message changes it. \
    User requests override this hint.\n\nIf at any point you can parallelize work by delegating tasks \
    to another agent (no matter if you are root or subagent), you should do so using collaboration \
    tools if it could save time or improve quality.";

pub(crate) const EXPLICIT_MODE: &str = "Any earlier instruction enabling proactive multi-agent \
    delegation no longer applies. Do not spawn sub-agents unless the user or applicable \
    AGENTS.md/skill instructions explicitly ask for sub-agents, delegation, or parallel agent work.";

/// Delegation role/mode parts resolved through the model prompt catalog.
///
/// The resolved fragments bind to this inference: engine defaults at the
/// bottom, catalog packs above them, and the source declaration riding with
/// the model metadata as the top layer. A slot resolved to disabled (or an
/// explicitly empty text) omits its part, preserving the pre-catalog
/// suppression semantics.
pub(crate) fn instructions_with_fragments(
    _tools: &[ToolSpec],
    agent_id: Option<&RuntimeString>,
    ultra_mode: bool,
    fragments: &crate::prompt_catalog::ModelFragments,
    configured_hint: Option<&RuntimeString>,
) -> Vec<SystemPromptPart> {
    let role_text = fragments.agent_role(agent_id.is_some()).cloned();
    let mode_text = fragments.agent_mode(ultra_mode, configured_hint);
    let mut parts = Vec::new();
    for (name, text) in [(ROLE_PART, role_text), (MODE_PART, mode_text)] {
        if let Some(text) = text.filter(|text| !text.is_empty()) {
            parts.push(SystemPromptPart {
                name: name.into(),
                text,
                cacheable: Some(false),
                cache_scope: None,
            });
        }
    }
    parts
}

pub(crate) fn is_instruction_part(name: &RuntimeString) -> bool {
    name == ROLE_PART || name == MODE_PART
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::{runtime_json, RuntimeJson, RuntimeObject};
    use crate::types::provider::ToolSpecKind;

    fn tools() -> Vec<ToolSpec> {
        vec![ToolSpec {
            name: "task".into(),
            description: "Start a child".into(),
            input_schema: RuntimeJson::Object(RuntimeObject::new()),
            kind: ToolSpecKind::Function,
            format: None,
            namespace: None,
        }]
    }

    fn declared(text: &str) -> RuntimeJson {
        runtime_json::parse(&RuntimeString::from(text)).unwrap()
    }

    fn mode(parts: &[SystemPromptPart]) -> Option<&RuntimeString> {
        parts
            .iter()
            .find(|part| part.name == MODE_PART)
            .map(|part| &part.text)
    }

    #[test]
    fn configured_hint_precedes_model_hint_and_raw_selection() {
        let messages = declared(
            r#"{"multi_agent":{"mode":{"hint_text":"catalog","proactive":"active","explicit":"inactive"}}}"#,
        );
        let hint = RuntimeString::from("configured");
        for enabled in [false, true] {
            let parts = instructions_with_fragments(
                &tools(),
                None,
                enabled,
                &crate::prompt_catalog::ModelFragments::for_family(None, Some(&messages)),
                Some(&hint),
            );
            assert_eq!(mode(&parts), Some(&hint));
        }
    }

    #[test]
    fn empty_configured_hint_suppresses_a_nonempty_catalog_hint() {
        let messages = declared(r#"{"multi_agent":{"mode":{"hint_text":"catalog"}}}"#);
        let hint = RuntimeString::default();
        let parts = instructions_with_fragments(
            &tools(),
            None,
            true,
            &crate::prompt_catalog::ModelFragments::for_family(None, Some(&messages)),
            Some(&hint),
        );
        assert!(mode(&parts).is_none());
        assert!(parts.iter().any(|part| part.name == ROLE_PART));
    }

    #[test]
    fn model_hint_precedes_both_mode_alternatives() {
        let messages = declared(
            r#"{"multi_agent":{"mode":{"hint_text":"catalog","proactive":"active","explicit":"inactive"}}}"#,
        );
        for enabled in [false, true] {
            let parts = instructions_with_fragments(
                &tools(),
                None,
                enabled,
                &crate::prompt_catalog::ModelFragments::for_family(None, Some(&messages)),
                None,
            );
            assert_eq!(mode(&parts), Some(&RuntimeString::from("catalog")));
        }
    }

    #[test]
    fn absent_mode_uses_bundled_text_but_empty_mode_suppresses_it() {
        let messages = declared(r#"{"multi_agent":{"mode":{"proactive":""}}}"#);
        let enabled = instructions_with_fragments(
            &tools(),
            None,
            true,
            &crate::prompt_catalog::ModelFragments::for_family(None, Some(&messages)),
            None,
        );
        assert!(mode(&enabled).is_none());
        let disabled = instructions_with_fragments(
            &tools(),
            None,
            false,
            &crate::prompt_catalog::ModelFragments::for_family(None, Some(&messages)),
            None,
        );
        assert_eq!(mode(&disabled), Some(&RuntimeString::from(EXPLICIT_MODE)));
    }

    #[test]
    fn role_and_mode_catalog_texts_preserve_their_exact_contents() {
        let messages = declared(
            r#"{"multi_agent":{"role":{"root":"root base","subagent":"child base"},"mode":{"proactive":" custom\nmode ","explicit":"off"}}}"#,
        );
        let child = RuntimeString::from("child-id");
        let parts = instructions_with_fragments(
            &tools(),
            Some(&child),
            true,
            &crate::prompt_catalog::ModelFragments::for_family(None, Some(&messages)),
            None,
        );
        assert_eq!(parts[0].text, "child base");
        assert_eq!(mode(&parts), Some(&RuntimeString::from(" custom\nmode ")));
        assert!(parts.iter().all(|part| part.cacheable == Some(false)));
    }
}
