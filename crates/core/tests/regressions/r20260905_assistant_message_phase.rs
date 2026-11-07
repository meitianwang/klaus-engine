//! Defect: the host could not distinguish streaming commentary from a final reply.
//! Root cause: normalized deltas and persisted text discarded message identity and phase.
//! Fixed by: 2026-09-05 desktop transcript parity work item.

use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::types::events::{EngineEvent, ThreadItem};
use klaus_agent_core::types::provider::ModelStreamEvent;

#[test]
fn assistant_message_phase_survives_provider_event_and_history_roundtrips() {
    let provider: ModelStreamEvent = runtime_codec::decode_text(&RuntimeString::from(
        r#"{"type":"text_message","id":"provider-final","phase":"final_answer"}"#,
    ))
    .unwrap();
    assert!(
        matches!(provider, ModelStreamEvent::TextMessage { phase: Some(phase), .. } if phase == "final_answer")
    );

    let delta: EngineEvent = runtime_codec::decode_text(&RuntimeString::from(
        r#"{"type":"text.delta","text":"Answer","messageId":"stable-final","phase":"final_answer"}"#,
    ))
    .unwrap();
    assert!(
        matches!(&delta, EngineEvent::TextDelta { message_id: Some(id), phase: Some(phase), .. }
        if id == "stable-final" && phase == "final_answer")
    );
    let wire = runtime_codec::encode_value(&delta).unwrap();
    assert_eq!(
        wire["messageId"].as_str(),
        Some(&RuntimeString::from("stable-final"))
    );
    assert_eq!(
        wire["phase"].as_str(),
        Some(&RuntimeString::from("final_answer"))
    );

    let text: ContentBlock = runtime_codec::decode_text(&RuntimeString::from(
        r#"{"type":"text","text":"Checking","messageId":"stable-commentary","phase":"commentary"}"#,
    ))
    .unwrap();
    let wire = runtime_codec::encode_value(&text).unwrap();
    let recovered: ContentBlock = runtime_codec::decode_value(&wire).unwrap();
    assert_eq!(recovered, text);
    assert_eq!(
        wire["phase"].as_str(),
        Some(&RuntimeString::from("commentary"))
    );
}

#[test]
fn legacy_unclassified_text_remains_unclassified() {
    let delta: EngineEvent = runtime_codec::decode_text(&RuntimeString::from(
        r#"{"type":"text.delta","text":"Checking"}"#,
    ))
    .unwrap();
    assert!(matches!(
        &delta,
        EngineEvent::TextDelta {
            message_id: None,
            phase: None,
            ..
        }
    ));
    assert_eq!(
        runtime_json::stringify(&runtime_codec::encode_value(&delta).unwrap()),
        r#"{"type":"text.delta","text":"Checking"}"#
    );
    let item: ThreadItem = runtime_codec::decode_text(&RuntimeString::from(
        r#"{"type":"agent_message","id":"legacy","text":"Checking"}"#,
    ))
    .unwrap();
    assert!(matches!(item, ThreadItem::AgentMessage { phase: None, .. }));
}
