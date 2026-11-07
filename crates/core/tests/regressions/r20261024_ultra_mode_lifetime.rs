//! Delegation policy follows the selected reasoning value and query origin.
//! Its lifetime is replaced by a later mode message, even without spawn tools.
//! Work item: 2026-10-24 delegation mode lifetime.

use super::r20261005_ultra_orchestration::{harness, harness_with_child_options};
use klaus_agent_core::engine::session::{Input, TurnOptions};

#[tokio::test]
async fn selected_delegation_mode_has_explicit_lifetime_and_replacement() {
    let harness = harness(Some("low"), None).await;
    for enabled in [true, false, true] {
        let result = harness
            .session
            .run(
                Input::Text("Answer directly.".into()),
                TurnOptions {
                    ultra_mode: Some(enabled),
                    ..Default::default()
                },
            )
            .await;
        assert!(result.error.is_none(), "{:?}", result.error);
        let requests = harness.provider.requests.lock().unwrap();
        let request = requests.last().unwrap();
        let modes: Vec<_> = request
            .system
            .iter()
            .filter(|part| part.name == "orchestration-mode")
            .collect();
        assert_eq!(modes.len(), 1);
        let text = &modes[0].text;
        if enabled {
            assert!(text.contains("until a later multi-agent mode developer message changes it"));
            assert!(text.contains("User requests override"));
            assert!(text.contains("no matter if you are root or subagent"));
            assert!(!text.contains("active for this request"));
        } else {
            assert!(text.contains("Any earlier instruction enabling proactive multi-agent delegation no longer applies"));
            assert!(text.contains("explicitly ask for sub-agents"));
            assert!(!text.contains("remains active"));
        }
    }
    harness.teardown().await;
}

#[tokio::test]
async fn ordinary_turn_mode_is_not_suppressed_by_a_missing_spawn_tool() {
    let harness = harness_with_child_options(
        Some("low"),
        None,
        Default::default(),
        serde_json::json!({"disabledTools":["task"]}),
        None,
        None,
    )
    .await;
    let result = harness
        .session
        .run(
            Input::Text("Answer directly.".into()),
            TurnOptions {
                ultra_mode: Some(true),
                ..Default::default()
            },
        )
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    {
        let requests = harness.provider.requests.lock().unwrap();
        let request = requests.last().unwrap();
        assert!(!request.tools.iter().any(|tool| tool.name == "task"));
        assert_eq!(
            request
                .system
                .iter()
                .filter(|part| part.name == "orchestration-mode")
                .count(),
            1
        );
        assert!(request
            .system
            .iter()
            .any(|part| part.name == "orchestration-mode"
                && part
                    .text
                    .contains("Proactive multi-agent delegation is active")));
    }
    harness.teardown().await;
}
