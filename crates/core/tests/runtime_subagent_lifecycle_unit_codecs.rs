use klaus_agent_core::engine::subagent_lifecycle::{SubagentKind, SubagentStatus};
use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeCodecErrorKind, RuntimeJson, RuntimeObject, RuntimeString,
};

macro_rules! assert_unit_enum_contract {
    ($type:ty; $( $variant:path => $tag:literal ),+ $(,)?) => {{
        let cases: &[($type, &str)] = &[$(($variant, $tag)),+];
        for (value, tag) in cases {
            let expected_text = RuntimeString::from(format!("\"{tag}\""));
            let runtime_text = runtime_codec::encode_text(value).unwrap();
            assert_eq!(runtime_text, expected_text);
            assert_eq!(
                runtime_codec::decode_text::<$type>(&runtime_text).unwrap(),
                *value,
            );
        }

        let unknown_tag = "__runtime_unknown_variant__";
        let unknown_text = RuntimeString::from(format!("\"{unknown_tag}\""));
        let unknown = runtime_codec::decode_text::<$type>(&unknown_text).unwrap_err();
        assert_eq!(unknown.pointer(), RuntimeString::default());
        assert_eq!(unknown.kind(), &RuntimeCodecErrorKind::UnknownEnumVariant);

        for wrong_shape in [RuntimeJson::Null, RuntimeJson::Bool(true)] {
            let error = runtime_codec::decode_value::<$type>(&wrong_shape).unwrap_err();
            assert_eq!(error.pointer(), RuntimeString::default());
            assert_eq!(error.kind(), &RuntimeCodecErrorKind::InvalidEnumShape);
        }

        let first_tag = cases[0].1;
        let mut payload = RuntimeObject::new();
        payload.insert(first_tag, RuntimeJson::Null);
        let payload = runtime_codec::decode_value::<$type>(&RuntimeJson::Object(payload))
            .unwrap_err();
        assert_eq!(payload.pointer(), RuntimeString::from(format!("/{first_tag}")));
        assert_eq!(payload.kind(), &RuntimeCodecErrorKind::InvalidEnumShape);

        let mut unknown_payload = RuntimeObject::new();
        unknown_payload.insert(unknown_tag, RuntimeJson::Null);
        let unknown_payload =
            runtime_codec::decode_value::<$type>(&RuntimeJson::Object(unknown_payload))
                .unwrap_err();
        assert_eq!(
            unknown_payload.pointer(),
            RuntimeString::from(format!("/{unknown_tag}")),
        );
        assert_eq!(
            unknown_payload.kind(),
            &RuntimeCodecErrorKind::UnknownEnumVariant,
        );

        let lone_surrogate = RuntimeString::from_utf16([b'"' as u16, 0xd800, b'"' as u16]);
        let lone_surrogate = runtime_codec::decode_text::<$type>(&lone_surrogate).unwrap_err();
        assert_eq!(lone_surrogate.pointer(), RuntimeString::default());
        assert_eq!(
            lone_surrogate.kind(),
            &RuntimeCodecErrorKind::UnknownEnumVariant,
        );
    }};
}

#[test]
fn subagent_lifecycle_unit_runtime_codecs_cover_the_complete_family() {
    assert_unit_enum_contract!(SubagentKind;
        SubagentKind::Foreground => "foreground",
        SubagentKind::Background => "background",
        SubagentKind::Persistent => "persistent",
        SubagentKind::Workflow => "workflow",
    );
    assert_unit_enum_contract!(SubagentStatus;
        SubagentStatus::Spawned => "spawned",
        SubagentStatus::Running => "running",
        SubagentStatus::Idle => "idle",
        SubagentStatus::Interrupted => "interrupted",
        SubagentStatus::Completed => "completed",
        SubagentStatus::BudgetLimited => "budget_limited",
        SubagentStatus::Failed => "failed",
        SubagentStatus::Cancelled => "cancelled",
    );
}
