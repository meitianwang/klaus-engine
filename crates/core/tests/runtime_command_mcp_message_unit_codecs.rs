use klaus_agent_core::command_runtime::CommandProcessStatus;
use klaus_agent_core::mcp::oauth_provider::McpAuthStatus;
use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeCodecErrorKind, RuntimeJson, RuntimeObject, RuntimeString,
};
use klaus_agent_core::schemas::message::SystemSubtype;

macro_rules! assert_unit_enum_contract {
    ($type:ty; $( $variant:path => $tag:literal ),+ $(,)?) => {{
        let cases: &[($type, &str)] = &[$(($variant, $tag)),+];
        for (value, tag) in cases {
            let runtime_text = runtime_codec::encode_text(value).unwrap();
            assert_eq!(runtime_text, RuntimeString::from(format!("\"{tag}\"")));
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

        let mut unknown_payload = RuntimeObject::new();
        unknown_payload.insert("__runtime_unknown_variant__", RuntimeJson::Null);
        let error = runtime_codec::decode_value::<$type>(&RuntimeJson::Object(unknown_payload))
            .unwrap_err();
        assert_eq!(error.kind(), &RuntimeCodecErrorKind::UnknownEnumVariant);
        assert_eq!(
            error.pointer(),
            RuntimeString::from("/__runtime_unknown_variant__"),
        );

        let surrogate = RuntimeString::from_utf16([b'"' as u16, 0xd800, b'"' as u16]);
        let error = runtime_codec::decode_text::<$type>(&surrogate).unwrap_err();
        assert_eq!(error.kind(), &RuntimeCodecErrorKind::UnknownEnumVariant);
        assert_eq!(error.pointer(), RuntimeString::default());
    }};
}

#[test]
fn command_mcp_message_unit_runtime_codecs_cover_the_complete_families() {
    assert_unit_enum_contract!(CommandProcessStatus;
        CommandProcessStatus::Running => "running",
        CommandProcessStatus::Exited => "exited",
        CommandProcessStatus::Killed => "killed",
        CommandProcessStatus::Failed => "failed",
    );
    assert_unit_enum_contract!(McpAuthStatus;
        McpAuthStatus::Unsupported => "unsupported",
        McpAuthStatus::NotLoggedIn => "notLoggedIn",
        McpAuthStatus::BearerToken => "bearerToken",
        McpAuthStatus::OAuth => "oAuth",
    );
    assert_unit_enum_contract!(SystemSubtype;
        SystemSubtype::Info => "info",
        SystemSubtype::Warning => "warning",
        SystemSubtype::CompactNotice => "compact_notice",
        SystemSubtype::LocalCommand => "local_command",
    );
}
