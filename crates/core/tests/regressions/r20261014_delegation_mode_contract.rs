//! Delegation policy mixed tool instructions with a request mode and retained
//! authorization after capability filtering. Role and mode now follow the live
//! native tool catalog and remain separate request-scoped instruction segments.
//! Work item: 2026-10-14 delegation mode contract.

use super::r20261005_ultra_orchestration::harness;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::schemas::effort::{EffortLevel, EffortValue};

#[tokio::test]
async fn delegation_role_and_mode_are_separate_and_follow_the_current_turn() {
    let harness = harness(Some("low"), None).await;
    for (enabled, expected) in [
        (true, "Proactive multi-agent delegation is active"),
        (
            false,
            "Any earlier instruction enabling proactive multi-agent delegation no longer applies",
        ),
        (true, "Proactive multi-agent delegation is active"),
    ] {
        let result = harness
            .session
            .run(
                Input::Text("Give a short direct answer.".into()),
                TurnOptions {
                    effort: Some(EffortValue::Level(EffortLevel::Low)),
                    ultra_mode: Some(enabled),
                    ..Default::default()
                },
            )
            .await;
        assert!(result.error.is_none(), "{:?}", result.error);
        let requests = harness.provider.requests.lock().unwrap();
        let request = requests.last().unwrap();
        assert_eq!(
            request.effort,
            Some(EffortValue::Level(if enabled {
                EffortLevel::Medium
            } else {
                EffortLevel::Low
            }))
        );
        assert!(request.tools.iter().any(|tool| tool.name == "task"));
        let roles: Vec<_> = request
            .system
            .iter()
            .enumerate()
            .filter(|(_, part)| part.name == "multi-agent-role")
            .collect();
        let modes: Vec<_> = request
            .system
            .iter()
            .enumerate()
            .filter(|(_, part)| part.name == "orchestration-mode")
            .collect();
        assert_eq!(roles.len(), 1);
        assert_eq!(modes.len(), 1);
        assert_eq!(roles[0].0 + 1, modes[0].0);
        assert!(roles[0].1.text.contains("primary agent coordinating"));
        assert!(roles[0].1.text.contains("not a completion requirement"));
        assert!(modes[0].1.text.contains(expected));
        assert_eq!(roles[0].1.cacheable, Some(false));
        assert_eq!(modes[0].1.cacheable, Some(false));
        assert!(!modes[0].1.text.contains("export const meta"));
        assert!(!modes[0].1.text.contains("two to four"));
        if enabled {
            assert!(modes[0]
                .1
                .text
                .contains("could save time or improve quality"));
        } else {
            assert!(modes[0].1.text.contains("explicitly ask for sub-agents"));
        }
        if let Some(countdown) = request
            .system
            .iter()
            .position(|part| part.name == "total-tokens")
        {
            assert!(modes[0].0 < countdown);
        }
        assert!(request.system[..roles[0].0]
            .iter()
            .any(|part| part.name == "env"));
    }
    harness.teardown().await;
}
