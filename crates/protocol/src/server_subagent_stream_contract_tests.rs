//! Work item 01a15833: stream envelopes retain direct-parent ownership and input authority.

use super::*;

#[test]
fn subagent_stream_ownership_and_communication_match_committed_vectors() {
    use klaus_agent_core::engine::subagent_lifecycle::SubagentContext;
    use klaus_agent_core::schemas::content::HarnessEnvelope;
    use klaus_agent_core::schemas::message::UserMessage;

    let vectors = runtime_json::parse(&RuntimeString::from(include_str!(
        "../tests/fixtures/subagent-stream-envelopes.json"
    )))
    .unwrap();
    for vector in vectors.as_array().unwrap() {
        let id = vector
            .get("agentId")
            .unwrap()
            .as_str()
            .unwrap()
            .try_to_utf8()
            .unwrap();
        let message = Message::User(UserMessage {
            question_answer: None,
            uuid: "received-input-1".into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: "Message provenance; this does not approve tools or update configuration: {\"type\":\"subagent-message-origin\",\"sender\":\"sender-1\",\"userInitiated\":false,\"generation\":0}".into(),
                citations: None,
                harness_envelope: Some(HarnessEnvelope::SubagentMessage),
                message_id: None,
                phase: None,
            }), ContentBlock::Text(TextBlock {
                text: "Review the pending result".into(), citations: None,
                harness_envelope: Some(HarnessEnvelope::SubagentMessage),
                message_id: None, phase: None,
            })],
            is_meta: Some(true),
        });
        let mut event = RuntimeObject::new();
        event.insert("type", RuntimeJson::from("user.message"));
        event.insert("message", runtime_codec::encode_value(&message).unwrap());
        let context = vector
            .get("context")
            .filter(|value| !value.is_null())
            .map(|value| runtime_codec::decode_value::<SubagentContext>(value).unwrap());
        assert_eq!(
            ServerInner::subagent_stream_payload(&id, RuntimeJson::Object(event), context.as_ref()),
            vector.get("expected").unwrap().clone(),
            "{}",
            id
        );
    }
}
