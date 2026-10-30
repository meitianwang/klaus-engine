use klaus_agent_core::memory::agent_memory::{AgentMemoryMergeChoice, AgentMemoryScope};
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

        let unknown_text = "\"__runtime_unknown_variant__\"";
        let unknown =
            runtime_codec::decode_text::<$type>(&RuntimeString::from(unknown_text)).unwrap_err();
        assert_eq!(unknown.kind(), &RuntimeCodecErrorKind::UnknownEnumVariant);
        assert_eq!(unknown.pointer(), RuntimeString::default());

        for wrong_shape in [RuntimeJson::Null, RuntimeJson::Bool(true)] {
            let error = runtime_codec::decode_value::<$type>(&wrong_shape).unwrap_err();
            assert_eq!(error.kind(), &RuntimeCodecErrorKind::InvalidEnumShape);
            assert_eq!(error.pointer(), RuntimeString::default());
        }

        let first_tag = cases[0].1;
        let mut payload = RuntimeObject::new();
        payload.insert(first_tag, RuntimeJson::Null);
        let payload =
            runtime_codec::decode_value::<$type>(&RuntimeJson::Object(payload)).unwrap_err();
        assert_eq!(payload.kind(), &RuntimeCodecErrorKind::InvalidEnumShape);
        assert_eq!(payload.pointer(), RuntimeString::from(format!("/{first_tag}")));

        let mut unknown_payload = RuntimeObject::new();
        unknown_payload.insert("__runtime_unknown_variant__", RuntimeJson::Null);
        let unknown_payload =
            runtime_codec::decode_value::<$type>(&RuntimeJson::Object(unknown_payload))
                .unwrap_err();
        assert_eq!(
            unknown_payload.kind(),
            &RuntimeCodecErrorKind::UnknownEnumVariant,
        );
        assert_eq!(
            unknown_payload.pointer(),
            RuntimeString::from("/__runtime_unknown_variant__"),
        );

        let lone_surrogate = RuntimeString::from_utf16([b'"' as u16, 0xd800, b'"' as u16]);
        let lone_surrogate = runtime_codec::decode_text::<$type>(&lone_surrogate).unwrap_err();
        assert_eq!(
            lone_surrogate.kind(),
            &RuntimeCodecErrorKind::UnknownEnumVariant,
        );
        assert_eq!(lone_surrogate.pointer(), RuntimeString::default());
    }};
}

#[test]
fn agent_memory_unit_runtime_codecs_match_native_contract_and_fail_closed() {
    assert_unit_enum_contract!(AgentMemoryScope;
        AgentMemoryScope::User => "user",
        AgentMemoryScope::Project => "project",
        AgentMemoryScope::Local => "local",
    );
    assert_unit_enum_contract!(AgentMemoryMergeChoice;
        AgentMemoryMergeChoice::Merge => "merge",
        AgentMemoryMergeChoice::Keep => "keep",
        AgentMemoryMergeChoice::Replace => "replace",
    );
}
