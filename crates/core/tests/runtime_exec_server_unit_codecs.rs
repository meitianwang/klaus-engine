use klaus_agent_core::command_runtime::exec_server::protocol::{
    ExecOutputStream, HttpRedirectPolicy, ProcessSignal, ShellEnvironmentPolicyInherit, WriteStatus,
};
use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeCodecErrorKind, RuntimeJson, RuntimeObject, RuntimeString,
};

macro_rules! assert_unit_enum_contract {
    ($type:ty; $( $variant:path => $tag:literal ),+ $(,)?) => {{
        let cases: &[($type, &str)] = &[$(($variant, $tag)),+];
        for (value, tag) in cases {
            let expected = RuntimeString::from(format!("\"{tag}\""));
            let runtime_text = runtime_codec::encode_text(value).unwrap();
            assert_eq!(runtime_text, expected);
            assert_eq!(
                runtime_codec::decode_text::<$type>(&runtime_text).unwrap(),
                *value,
            );
        }

        let unknown = runtime_codec::decode_text::<$type>(&RuntimeString::from("\"unknown\""))
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
fn exec_server_unit_runtime_codecs_cover_the_complete_family() {
    assert_unit_enum_contract!(ShellEnvironmentPolicyInherit;
        ShellEnvironmentPolicyInherit::All => "all",
        ShellEnvironmentPolicyInherit::Core => "core",
        ShellEnvironmentPolicyInherit::None => "none",
    );
    assert_unit_enum_contract!(ExecOutputStream;
        ExecOutputStream::Stdout => "stdout",
        ExecOutputStream::Stderr => "stderr",
        ExecOutputStream::Pty => "pty",
    );
    assert_unit_enum_contract!(WriteStatus;
        WriteStatus::Accepted => "accepted",
        WriteStatus::UnknownProcess => "unknownProcess",
        WriteStatus::StdinClosed => "stdinClosed",
        WriteStatus::Starting => "starting",
    );
    assert_unit_enum_contract!(ProcessSignal;
        ProcessSignal::Interrupt => "interrupt",
    );
    assert_unit_enum_contract!(HttpRedirectPolicy;
        HttpRedirectPolicy::Follow => "follow",
        HttpRedirectPolicy::Stop => "stop",
    );
}
