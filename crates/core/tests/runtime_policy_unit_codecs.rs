use klaus_agent_core::network_proxy::{NetworkMode, NetworkProtocol};
use klaus_agent_core::permission::path_rules::PathRuleKind;
use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeCodecErrorKind, RuntimeJson, RuntimeObject, RuntimeString,
};
use klaus_agent_core::types::sandbox::SandboxKind;

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
fn policy_unit_runtime_codecs_cover_the_complete_families() {
    assert_unit_enum_contract!(NetworkMode;
        NetworkMode::Limited => "limited",
        NetworkMode::Full => "full",
    );
    assert_unit_enum_contract!(NetworkProtocol;
        NetworkProtocol::Http => "http",
        NetworkProtocol::HttpsConnect => "httpsConnect",
        NetworkProtocol::Socks5Tcp => "socks5Tcp",
        NetworkProtocol::UnixSocket => "unixSocket",
    );
    assert_unit_enum_contract!(SandboxKind;
        SandboxKind::Process => "process",
        SandboxKind::Seatbelt => "seatbelt",
        SandboxKind::Bubblewrap => "bubblewrap",
        SandboxKind::NativeHelper => "native-helper",
    );
    assert_unit_enum_contract!(PathRuleKind;
        PathRuleKind::Read => "read",
        PathRuleKind::Edit => "edit",
    );
}
