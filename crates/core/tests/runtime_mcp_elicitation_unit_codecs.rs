use klaus_agent_core::mcp::elicitation::{
    McpElicitationAction, McpElicitationArrayType, McpElicitationBooleanType,
    McpElicitationNumberType, McpElicitationObjectType, McpElicitationStringFormat,
    McpElicitationStringType,
};
use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeCodecErrorKind, RuntimeJson, RuntimeObject, RuntimeString,
};

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

        let mut unknown_payload = RuntimeObject::new();
        unknown_payload.insert("unknown", RuntimeJson::Null);
        let error =
            runtime_codec::decode_value::<$type>(&RuntimeJson::Object(unknown_payload))
                .unwrap_err();
        assert_eq!(error.kind(), &RuntimeCodecErrorKind::UnknownEnumVariant);
        assert_eq!(error.pointer(), RuntimeString::from("/unknown"));

        let surrogate = RuntimeString::from_utf16([b'"' as u16, 0xd800, b'"' as u16]);
        let error = runtime_codec::decode_text::<$type>(&surrogate).unwrap_err();
        assert_eq!(error.kind(), &RuntimeCodecErrorKind::UnknownEnumVariant);
        assert_eq!(error.pointer(), RuntimeString::default());
    }};
}

#[test]
fn mcp_elicitation_unit_runtime_codecs_cover_the_complete_family() {
    assert_unit_enum_contract!(McpElicitationAction;
        McpElicitationAction::Accept => "accept",
        McpElicitationAction::Decline => "decline",
        McpElicitationAction::Cancel => "cancel",
    );
    assert_unit_enum_contract!(McpElicitationObjectType;
        McpElicitationObjectType::Object => "object",
    );
    assert_unit_enum_contract!(McpElicitationStringType;
        McpElicitationStringType::String => "string",
    );
    assert_unit_enum_contract!(McpElicitationStringFormat;
        McpElicitationStringFormat::Email => "email",
        McpElicitationStringFormat::Uri => "uri",
        McpElicitationStringFormat::Date => "date",
        McpElicitationStringFormat::DateTime => "date-time",
    );
    assert_unit_enum_contract!(McpElicitationNumberType;
        McpElicitationNumberType::Number => "number",
        McpElicitationNumberType::Integer => "integer",
    );
    assert_unit_enum_contract!(McpElicitationBooleanType;
        McpElicitationBooleanType::Boolean => "boolean",
    );
    assert_unit_enum_contract!(McpElicitationArrayType;
        McpElicitationArrayType::Array => "array",
    );
}
