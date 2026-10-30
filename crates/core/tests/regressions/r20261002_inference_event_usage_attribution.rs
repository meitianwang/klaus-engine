//! In-flight responses used the latest configuration's attribution and pricing.
//! Request events must freeze provenance before enqueue; persisted spend must not
//! be repriced when the same model name moves to another source.

use klaus_agent_core::observability::cost_tracker::{
    CostTracker, ModelPricing, PricingTable, UsageAttribution,
};
use klaus_agent_core::provider::observability::{self, ProviderRequestMetadata};
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::schemas::message::Usage;
use klaus_agent_core::schemas::rollout::{RolloutItem, UsageLedgerItem};
use klaus_agent_core::types::errors::{EngineError, EngineErrorCode, TerminalReason};
use klaus_agent_core::types::events::{EngineEvent, InferenceIdentity, ThreadItem};

fn usage(input_tokens: u64) -> Usage {
    Usage {
        input_tokens,
        output_tokens: 0,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: None,
    }
}

fn identity(revision: u64, source: &str) -> InferenceIdentity {
    InferenceIdentity {
        revision,
        change_id: Some(format!("change-{revision}").into()),
        model: "same-model".into(),
        source: Some(source.into()),
        tier: Some("high".into()),
        selection: Some("explicit".into()),
    }
}

fn attribute(event: &mut EngineEvent, identity: InferenceIdentity) {
    match event {
        EngineEvent::StreamRequestStart { inference }
        | EngineEvent::TextDelta { inference, .. }
        | EngineEvent::ThinkingDelta { inference, .. }
        | EngineEvent::ItemStarted { inference, .. }
        | EngineEvent::ItemUpdated { inference, .. }
        | EngineEvent::ItemCompleted { inference, .. }
        | EngineEvent::TurnFailed { inference, .. }
        | EngineEvent::ProviderDecision { inference, .. } => *inference = Some(identity),
        _ => panic!("expected an attributable model event"),
    }
}

#[test]
fn event_codecs_accept_old_history_and_preserve_request_identity() {
    let item = || ThreadItem::AgentMessage {
        id: "assistant".into(),
        text: "answer".into(),
        phase: None,
    };
    let metadata = ProviderRequestMetadata {
        provider_id: "test-provider".into(),
        model: "same-model".into(),
        service_tier: None,
        tool_trace_id: None,
    };
    let events = vec![
        EngineEvent::StreamRequestStart { inference: None },
        EngineEvent::TextDelta {
            text: "answer".into(),
            message_id: None,
            phase: None,
            inference: None,
        },
        EngineEvent::ThinkingDelta {
            message_id: None,
            text: "reasoning".into(),
            inference: None,
        },
        EngineEvent::ItemStarted {
            item: item(),
            inference: None,
        },
        EngineEvent::ItemUpdated {
            item: item(),
            inference: None,
        },
        EngineEvent::ItemCompleted {
            item: item(),
            inference: None,
        },
        EngineEvent::TurnFailed {
            error: EngineError {
                code: EngineErrorCode::ProviderError,
                message: "failed".into(),
                cause: None,
                request_id: None,
                provider_kind: None,
                retry_after_ms: None,
            },
            inference: None,
        },
        EngineEvent::ProviderDecision {
            event: observability::request_completed(&metadata, 1, &usage(0)),
            inference: None,
        },
    ];
    for mut event in events {
        let legacy = runtime_codec::encode_value(&event).unwrap();
        assert!(!runtime_json::stringify(&legacy)
            .try_to_utf8()
            .unwrap()
            .contains("inference"));
        let decoded: EngineEvent = runtime_codec::decode_value(&legacy).unwrap();
        assert!(decoded.inference().is_none());
        assert_eq!(runtime_codec::encode_value(&decoded).unwrap(), legacy);
        attribute(&mut event, identity(17, "custom"));
        let attributed = runtime_codec::encode_value(&event).unwrap();
        let decoded: EngineEvent = runtime_codec::decode_value(&attributed).unwrap();
        assert_eq!(decoded.inference(), Some(&identity(17, "custom")));
        assert_eq!(runtime_codec::encode_value(&decoded).unwrap(), attributed);
    }
    let aggregate = EngineEvent::TurnCompleted {
        turn_id: "turn".into(),
        usage: Some(usage(0)),
        stop_reason: TerminalReason::Completed,
    };
    assert!(aggregate.inference().is_none());
}

fn pricing(input: f64) -> PricingTable {
    vec![(
        "same-model".to_string(),
        ModelPricing {
            input_per_m_tok: input,
            output_per_m_tok: 0.0,
            cache_read_per_m_tok: 0.0,
            cache_write_per_m_tok: 0.0,
        },
    )]
}

#[test]
fn same_model_different_sources_keep_request_prices_and_original_rollout_cost() {
    let a_prices = pricing(1.0);
    let b_prices = pricing(10.0);
    let usage = usage(1_000_000);
    let mut tracker = CostTracker::with_pricing(b_prices.clone());
    let a_ledger = CostTracker::source_qualified_ledger_model(Some(&(
        "custom".to_string(),
        "same-model".to_string(),
    )));
    let a = tracker.record_attributed(
        "request-a",
        "assistant-a",
        "same-model",
        &usage,
        &a_prices,
        UsageAttribution {
            inference: Some(identity(1, "custom")),
            ledger_model: a_ledger.as_deref(),
        },
    );
    let b_ledger = CostTracker::source_qualified_ledger_model(Some(&(
        "codex".to_string(),
        "same-model".to_string(),
    )));
    let b = tracker.record_attributed(
        "request-b",
        "assistant-b",
        "same-model",
        &usage,
        &b_prices,
        UsageAttribution {
            inference: Some(identity(2, "codex")),
            ledger_model: b_ledger.as_deref(),
        },
    );
    assert_eq!(a.cost_usd, 1.0);
    assert_eq!(b.cost_usd, 10.0);
    assert_eq!(tracker.total_cost_usd(), 11.0);
    // Same wire model id, two sources: each books under its own source-qualified
    // ledger name, so neither aggregates into the other's row, and the price
    // lookup still ran against the wire model name.
    assert_eq!(a.model.try_to_utf8().unwrap(), "custom:same-model");
    assert_eq!(b.model.try_to_utf8().unwrap(), "codex:same-model");
    let mut restored = CostTracker::with_pricing(pricing(999.0));
    for original in [a, b] {
        let encoded =
            runtime_codec::encode_value(&RolloutItem::UsageLedger(original.clone())).unwrap();
        let RolloutItem::UsageLedger(decoded) = runtime_codec::decode_value(&encoded).unwrap()
        else {
            panic!("expected usage ledger");
        };
        assert_eq!(decoded, original);
        restored.restore(&decoded);
    }
    assert_eq!(restored.total_cost_usd(), 11.0);
}

#[test]
fn old_usage_ledger_remains_optional_and_never_reprices() {
    let record: UsageLedgerItem = runtime_codec::decode_text(&RuntimeString::from(
        r#"{"requestId":"old","assistantMessageId":"old-assistant","model":"same-model","usage":{"inputTokens":1000000,"outputTokens":0,"cacheReadInputTokens":0,"cacheCreationInputTokens":0},"costUsd":0.125}"#,
    )).unwrap();
    assert!(record.inference.is_none());
    let mut restored = CostTracker::with_pricing(pricing(999.0));
    restored.restore(&record);
    assert_eq!(restored.total_cost_usd(), 0.125);
}
