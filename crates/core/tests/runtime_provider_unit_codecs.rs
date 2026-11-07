use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeCodecErrorKind, RuntimeJson, RuntimeObject, RuntimeString,
};
use klaus_agent_core::types::provider::{
    CacheScope, CacheTtl, ModelStopReason, ProviderToolCallKind, ToolInputFormat,
    ToolSpecGrammarSyntax, ToolSpecKind, WebSearchContextSize,
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

        let unknown = runtime_codec::decode_text::<$type>(&RuntimeString::from("\"unknown\""))
            .unwrap_err();
        assert_eq!(unknown.pointer(), RuntimeString::default());
        assert_eq!(unknown.kind(), &RuntimeCodecErrorKind::UnknownEnumVariant);

        let non_string = runtime_codec::decode_value::<$type>(&RuntimeJson::Null).unwrap_err();
        assert_eq!(non_string.pointer(), RuntimeString::default());
        assert_eq!(non_string.kind(), &RuntimeCodecErrorKind::InvalidEnumShape);

        let first_tag = cases[0].1;
        let mut payload = RuntimeObject::new();
        payload.insert(first_tag, RuntimeJson::Null);
        let payload = runtime_codec::decode_value::<$type>(&RuntimeJson::Object(payload))
            .unwrap_err();
        assert_eq!(payload.pointer(), RuntimeString::from(format!("/{first_tag}")));
        assert_eq!(payload.kind(), &RuntimeCodecErrorKind::InvalidEnumShape);

        let mut unknown_payload = RuntimeObject::new();
        unknown_payload.insert("unknown", RuntimeJson::Null);
        let unknown_payload =
            runtime_codec::decode_value::<$type>(&RuntimeJson::Object(unknown_payload))
                .unwrap_err();
        assert_eq!(unknown_payload.pointer(), RuntimeString::from("/unknown"));
        assert_eq!(unknown_payload.kind(), &RuntimeCodecErrorKind::UnknownEnumVariant);

        let surrogate = RuntimeString::from_utf16([b'\"' as u16, 0xD800, b'\"' as u16]);
        let surrogate = runtime_codec::decode_text::<$type>(&surrogate).unwrap_err();
        assert_eq!(surrogate.pointer(), RuntimeString::default());
        assert_eq!(surrogate.kind(), &RuntimeCodecErrorKind::UnknownEnumVariant);
    }};
}

#[test]
fn provider_unit_runtime_codecs_match_native_contract_and_reject_non_unit_shapes() {
    assert_unit_enum_contract!(ToolSpecKind;
        ToolSpecKind::Function => "function",
        ToolSpecKind::Freeform => "freeform",
        ToolSpecKind::ToolSearch => "tool_search",
    );
    assert_unit_enum_contract!(ToolSpecGrammarSyntax;
        ToolSpecGrammarSyntax::Lark => "lark",
        ToolSpecGrammarSyntax::Regex => "regex",
    );
    assert_unit_enum_contract!(ProviderToolCallKind;
        ProviderToolCallKind::Function => "function",
        ProviderToolCallKind::Custom => "custom",
        ProviderToolCallKind::ToolSearch => "tool_search",
    );
    assert_unit_enum_contract!(ToolInputFormat;
        ToolInputFormat::Json => "json",
        ToolInputFormat::Raw => "raw",
    );
    assert_unit_enum_contract!(CacheScope;
        CacheScope::Global => "global",
    );
    assert_unit_enum_contract!(klaus_agent_core::schemas::config::WebSearchAccess;
        klaus_agent_core::schemas::config::WebSearchAccess::Disabled => "disabled",
        klaus_agent_core::schemas::config::WebSearchAccess::Cached => "cached",
        klaus_agent_core::schemas::config::WebSearchAccess::Indexed => "indexed",
        klaus_agent_core::schemas::config::WebSearchAccess::Live => "live",
    );
    assert_unit_enum_contract!(WebSearchContextSize;
        WebSearchContextSize::Low => "low",
        WebSearchContextSize::Medium => "medium",
        WebSearchContextSize::High => "high",
    );
    assert_unit_enum_contract!(CacheTtl;
        CacheTtl::OneHour => "1h",
    );
    assert_unit_enum_contract!(ModelStopReason;
        ModelStopReason::EndTurn => "end_turn",
        ModelStopReason::Continue => "continue",
        ModelStopReason::ToolUse => "tool_use",
        ModelStopReason::MaxTokens => "max_tokens",
        ModelStopReason::StopSequence => "stop_sequence",
        ModelStopReason::Refusal => "refusal",
    );
}
