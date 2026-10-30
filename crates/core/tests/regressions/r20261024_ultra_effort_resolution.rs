//! Ultra left the provider's native effort unchanged instead of resolving the preset.
//! Resolve each request against its captured model without rewriting saved choices.
//! Work item: 2026-10-24 Ultra request effort.

use super::r20261005_ultra_orchestration::harness_with_metadata;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::schemas::effort::{EffortLevel, EffortValue};
use klaus_agent_core::types::provider::{
    EffortCapability, ModelCapabilities, ThinkingConfig, ThinkingContextPolicy,
};

#[tokio::test]
async fn ultra_resolves_model_preference_then_max_then_last_native_then_medium() {
    use EffortLevel::{High, Low, Max, Medium, Ultra, Xhigh};
    for (levels, preferred, expected) in [
        (vec![Low, High, Xhigh, Max], Some("high"), High),
        (vec![Low, High, Max, Xhigh], None, Max),
        (vec![Low, Xhigh], None, Xhigh),
        (vec![High, Low], None, Low),
        (vec![Low, High], Some("xhigh"), High),
        (vec![Low, Ultra], Some("ultra"), Low),
        (vec![Ultra], None, Medium),
        (vec![], None, Medium),
    ] {
        let metadata = ModelCapabilities {
            effort: Some(EffortCapability {
                levels,
                default: Some(Low),
            }),
            multi_agent_reasoning_effort: preferred.map(Into::into),
            ..Default::default()
        };
        let h = harness_with_metadata(Some("low"), None, metadata).await;
        for enabled in [true, false] {
            let result = h
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
            let requests = h.provider.requests.lock().unwrap();
            let request = requests.last().unwrap();
            assert_eq!(
                request.effort,
                Some(EffortValue::Level(if enabled { expected } else { Low }))
            );
            if enabled {
                assert_eq!(request.thinking, Some(ThinkingConfig::Adaptive));
                assert_eq!(
                    request.thinking_context,
                    Some(ThinkingContextPolicy::KeepAll)
                );
            }
        }
        assert_eq!(
            h.session.model_runtime().1.effort,
            Some(EffortValue::Level(Low))
        );
        h.teardown().await;
    }
}

#[tokio::test]
async fn raw_catalog_strings_and_persistent_are_resolved_exactly_once() {
    for (raw, supported, preferred, expected) in [
        (
            "ultra",
            vec!["ultra", "custom/exact", "persistent"],
            Some("custom/exact"),
            EffortValue::Custom("custom/exact".into()),
        ),
        (
            "ultra",
            vec!["ultra", "persistent"],
            None,
            EffortValue::Level(EffortLevel::Persistent),
        ),
        (
            "persistent",
            vec!["persistent"],
            None,
            EffortValue::Custom("disabled".into()),
        ),
        (
            "minimal",
            vec!["none", "minimal"],
            None,
            EffortValue::Level(EffortLevel::Minimal),
        ),
    ] {
        let h = harness_with_metadata(
            Some(raw),
            None,
            ModelCapabilities {
                supported_reasoning_efforts: supported.into_iter().map(Into::into).collect(),
                multi_agent_reasoning_effort: preferred.map(Into::into),
                ..Default::default()
            },
        )
        .await;
        let result = h
            .session
            .run(
                Input::Text("Answer directly.".into()),
                TurnOptions::default(),
            )
            .await;
        assert!(result.error.is_none(), "{:?}", result.error);
        assert_eq!(
            h.provider.requests.lock().unwrap().last().unwrap().effort,
            Some(expected)
        );
        h.teardown().await;
    }
}
