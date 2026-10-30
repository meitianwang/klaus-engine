use klaus_agent_core::hooks::types::{HookDecision, HookEvent, HookResult, HookTrigger};
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

        let unknown = runtime_codec::decode_text::<$type>(&RuntimeString::from(
            "\"__runtime_unknown_variant__\"",
        ))
        .unwrap_err();
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
        unknown_payload.insert("__runtime_unknown_variant__", RuntimeJson::Null);
        let unknown_payload =
            runtime_codec::decode_value::<$type>(&RuntimeJson::Object(unknown_payload))
                .unwrap_err();
        assert_eq!(
            unknown_payload.pointer(),
            RuntimeString::from("/__runtime_unknown_variant__"),
        );
        assert_eq!(
            unknown_payload.kind(),
            &RuntimeCodecErrorKind::UnknownEnumVariant,
        );

        let surrogate = RuntimeString::from_utf16([b'"' as u16, 0xd800, b'"' as u16]);
        let surrogate = runtime_codec::decode_text::<$type>(&surrogate).unwrap_err();
        assert_eq!(surrogate.pointer(), RuntimeString::default());
        assert_eq!(surrogate.kind(), &RuntimeCodecErrorKind::UnknownEnumVariant);
    }};
}

#[test]
fn hooks_unit_runtime_codecs_match_native_contract_and_fail_closed() {
    assert_unit_enum_contract!(HookDecision;
        HookDecision::Approve => "approve",
        HookDecision::Block => "block",
        HookDecision::Ask => "ask",
    );
    assert_unit_enum_contract!(HookEvent;
        HookEvent::SessionStart => "SessionStart",
        HookEvent::SessionEnd => "SessionEnd",
        HookEvent::UserPromptSubmit => "UserPromptSubmit",
        HookEvent::PreToolUse => "PreToolUse",
        HookEvent::PostToolUse => "PostToolUse",
        HookEvent::PostToolUseFailure => "PostToolUseFailure",
        HookEvent::Stop => "Stop",
        HookEvent::SubagentStop => "SubagentStop",
        HookEvent::PreCompact => "PreCompact",
        HookEvent::PostCompact => "PostCompact",
        HookEvent::Notification => "Notification",
        HookEvent::SubagentStart => "SubagentStart",
        HookEvent::Setup => "Setup",
        HookEvent::TaskCreated => "TaskCreated",
        HookEvent::TaskCompleted => "TaskCompleted",
        HookEvent::WorktreeCreate => "WorktreeCreate",
        HookEvent::WorktreeRemove => "WorktreeRemove",
    );
    assert_unit_enum_contract!(HookResult;
        HookResult::Success => "success",
        HookResult::FailedContinue => "failed_continue",
        HookResult::FailedAbort => "failed_abort",
    );
    assert_unit_enum_contract!(HookTrigger;
        HookTrigger::Init => "init",
        HookTrigger::Maintenance => "maintenance",
        HookTrigger::Manual => "manual",
        HookTrigger::Auto => "auto",
    );
}
