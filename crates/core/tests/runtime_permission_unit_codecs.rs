use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeCodecErrorKind, RuntimeJson, RuntimeObject, RuntimeString,
};
use klaus_agent_core::types::permission::{
    ApprovalKind, ExternalPermissionMode, PermissionBehavior, PermissionDestination, PermissionMode,
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
        let error = runtime_codec::decode_value::<$type>(&RuntimeJson::Object(payload))
            .unwrap_err();
        assert_eq!(error.kind(), &RuntimeCodecErrorKind::InvalidEnumShape);
        assert_eq!(error.pointer(), RuntimeString::from(format!("/{first_tag}")));

        let surrogate = RuntimeString::from_utf16([b'"' as u16, 0xd800, b'"' as u16]);
        let error = runtime_codec::decode_text::<$type>(&surrogate).unwrap_err();
        assert_eq!(error.kind(), &RuntimeCodecErrorKind::UnknownEnumVariant);
        assert_eq!(error.pointer(), RuntimeString::default());
    }};
}

#[test]
fn permission_unit_runtime_codecs_cover_the_complete_family() {
    assert_unit_enum_contract!(ExternalPermissionMode;
        ExternalPermissionMode::Default => "default",
        ExternalPermissionMode::Plan => "plan",
        ExternalPermissionMode::AcceptEdits => "acceptEdits",
        ExternalPermissionMode::Auto => "auto",
        ExternalPermissionMode::BypassPermissions => "bypassPermissions",
        ExternalPermissionMode::DontAsk => "dontAsk",
    );
    assert_unit_enum_contract!(PermissionMode;
        PermissionMode::Default => "default",
        PermissionMode::Plan => "plan",
        PermissionMode::AcceptEdits => "acceptEdits",
        PermissionMode::Auto => "auto",
        PermissionMode::BypassPermissions => "bypassPermissions",
        PermissionMode::DontAsk => "dontAsk",
        PermissionMode::Bubble => "bubble",
    );
    assert_unit_enum_contract!(PermissionBehavior;
        PermissionBehavior::Allow => "allow",
        PermissionBehavior::Deny => "deny",
        PermissionBehavior::Ask => "ask",
    );
    assert_unit_enum_contract!(PermissionDestination;
        PermissionDestination::Session => "session",
        PermissionDestination::LocalSettings => "localSettings",
        PermissionDestination::ProjectSettings => "projectSettings",
        PermissionDestination::UserSettings => "userSettings",
    );
    assert_unit_enum_contract!(ApprovalKind;
        ApprovalKind::Tool => "tool",
        ApprovalKind::Exec => "exec",
        ApprovalKind::Patch => "patch",
        ApprovalKind::Network => "network",
    );
}
