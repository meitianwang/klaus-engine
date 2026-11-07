//! Inline skill effort could not replace an explicit turn orchestration preset.
//! Root cause: a new raw effort retained the earlier explicit preset override.
//! Work item: 2026-10-24 inline skill effort override.

use super::r20261005_ultra_orchestration::harness_with_child_options;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::schemas::content::OutputBlock;
use klaus_agent_core::schemas::effort::{EffortLevel, EffortValue};
use klaus_agent_core::schemas::message::Message;
use klaus_agent_core::types::provider::ModelCapabilities;
use serde_json::json;

#[tokio::test]
async fn inline_skill_raw_effort_replaces_an_active_turn_preset_only_when_supplied() {
    for (scenario, expected, proactive) in [
        ("skill-effort-low", EffortLevel::Low, false),
        ("skill-effort-unchanged", EffortLevel::High, true),
    ] {
        let h = harness_with_child_options(
            Some("medium"),
            Some(scenario),
            ModelCapabilities {
                supported_reasoning_efforts: ["low", "medium", "high", "ultra"]
                    .into_iter()
                    .map(Into::into)
                    .collect(),
                multi_agent_reasoning_effort: Some("high".into()),
                ..Default::default()
            },
            json!({}),
            None,
            Some(json!({"name":"step-effort"})),
        )
        .await;
        let result = h
            .session
            .run(
                Input::Text("Apply the inline skill and answer.".into()),
                TurnOptions {
                    ultra_mode: Some(true),
                    ..Default::default()
                },
            )
            .await;
        assert!(result.error.is_none(), "{:?}", result.error);
        {
            let requests = h.provider.requests.lock().unwrap();
            assert_eq!(
                requests.len(),
                2,
                "the skill must trigger the next model step"
            );
            let first = requests.first().unwrap();
            assert_eq!(first.effort, Some(EffortValue::Level(EffortLevel::High)));
            assert!(first.system.iter().any(|part| part
                .text
                .contains("Proactive multi-agent delegation is active")));
            let after_skill = requests.last().unwrap();
            assert!(
                after_skill.messages.iter().any(|message| match message {
                    Message::ToolResult(result) => !result.is_error
                        && result.content.iter().any(|block| matches!(block,
                            OutputBlock::Text(text) if text.text.contains("SKILL_SESSION_SENTINEL")
                        )),
                    _ => false,
                }),
                "the real skill tool must execute successfully"
            );
            assert_eq!(after_skill.effort, Some(EffortValue::Level(expected)));
            assert_eq!(
                after_skill.system.iter().any(|part| part
                    .text
                    .contains("Proactive multi-agent delegation is active")),
                proactive,
            );
        }
        assert_eq!(
            h.session.model_runtime().1.effort,
            Some(EffortValue::Level(EffortLevel::Medium)),
            "the skill selection belongs to this turn",
        );
        h.teardown().await;
    }
}
