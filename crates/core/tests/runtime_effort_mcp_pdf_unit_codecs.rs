use klaus_agent_core::fs::pdf::PdfErrorReason;
use klaus_agent_core::mcp::types::McpConnectionState;
use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeCodecErrorKind, RuntimeJson, RuntimeObject, RuntimeString,
};
use klaus_agent_core::schemas::effort::EffortLevel;

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

        let surrogate = RuntimeString::from_utf16([b'"' as u16, 0xd800, b'"' as u16]);
        let error = runtime_codec::decode_text::<$type>(&surrogate).unwrap_err();
        assert_eq!(error.kind(), &RuntimeCodecErrorKind::UnknownEnumVariant);
        assert_eq!(error.pointer(), RuntimeString::default());
    }};
}

#[test]
fn effort_mcp_pdf_unit_runtime_codecs_cover_the_complete_families() {
    assert_unit_enum_contract!(EffortLevel;
        EffortLevel::Low => "low",
        EffortLevel::Medium => "medium",
        EffortLevel::High => "high",
        EffortLevel::Xhigh => "xhigh",
        EffortLevel::Max => "max",
        EffortLevel::Ultra => "ultra",
    );
    assert_unit_enum_contract!(McpConnectionState;
        McpConnectionState::Connecting => "connecting",
        McpConnectionState::Ready => "ready",
        McpConnectionState::Failed => "failed",
        McpConnectionState::NeedsAuth => "needs-auth",
        McpConnectionState::Closed => "closed",
    );
    assert_unit_enum_contract!(PdfErrorReason;
        PdfErrorReason::Empty => "empty",
        PdfErrorReason::TooLarge => "too_large",
        PdfErrorReason::PasswordProtected => "password_protected",
        PdfErrorReason::Corrupted => "corrupted",
        PdfErrorReason::Unknown => "unknown",
        PdfErrorReason::Unavailable => "unavailable",
    );
}
