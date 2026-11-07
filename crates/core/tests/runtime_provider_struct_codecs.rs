use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeCodecErrorKind, RuntimeJson, RuntimeNumber, RuntimeObject, RuntimeString,
};
use klaus_agent_core::types::provider::{ProviderCapabilities, ThinkingConfig};

fn capabilities() -> ProviderCapabilities {
    ProviderCapabilities {
        supports_image_detail_original: false,
        streaming: true,
        tools: false,
        thinking: true,
        structured_output: false,
        web_search: true,
        prompt_cache_control: false,
        context_window: 200_000,
        max_output_tokens: 32_768,
        native_pdf: true,
        vision: true,
        image_limits: None,
        max_media_blocks: None,
    }
}

#[test]
fn provider_capabilities_runtime_codec_matches_native_field_contract() {
    let value = capabilities();
    let expected = r#"{"streaming":true,"tools":false,"thinking":true,"structuredOutput":false,"webSearch":true,"promptCacheControl":false,"contextWindow":200000,"maxOutputTokens":32768,"nativePdf":true,"vision":true}"#;

    let encoded = runtime_codec::encode_value(&value).unwrap();
    assert_eq!(runtime_codec::encode_text(&value).unwrap(), expected);
    assert_eq!(
        runtime_codec::decode_value::<ProviderCapabilities>(&encoded).unwrap(),
        value,
    );

    let RuntimeJson::Object(object) = &encoded else {
        panic!("provider capabilities must encode as an object");
    };
    for field in [
        "streaming",
        "tools",
        "thinking",
        "structuredOutput",
        "webSearch",
        "promptCacheControl",
        "contextWindow",
        "maxOutputTokens",
        "nativePdf",
        "vision",
    ] {
        let mut missing = object.clone();
        missing.remove(field);
        let error =
            runtime_codec::decode_value::<ProviderCapabilities>(&RuntimeJson::Object(missing))
                .unwrap_err();
        assert_eq!(error.kind(), &RuntimeCodecErrorKind::MissingField);
        assert_eq!(error.pointer(), RuntimeString::from(format!("/{field}")));
    }

    let mut invalid_number = object.clone();
    invalid_number.insert(
        "contextWindow",
        RuntimeJson::String(RuntimeString::from("many")),
    );
    let error =
        runtime_codec::decode_value::<ProviderCapabilities>(&RuntimeJson::Object(invalid_number))
            .unwrap_err();
    assert_eq!(error.kind(), &RuntimeCodecErrorKind::ExpectedNumber);
    assert_eq!(error.pointer(), RuntimeString::from("/contextWindow"));

    let mut invalid_bool = object.clone();
    invalid_bool.insert("nativePdf", RuntimeJson::from(1));
    let error =
        runtime_codec::decode_value::<ProviderCapabilities>(&RuntimeJson::Object(invalid_bool))
            .unwrap_err();
    assert_eq!(error.kind(), &RuntimeCodecErrorKind::ExpectedBool);
    assert_eq!(error.pointer(), RuntimeString::from("/nativePdf"));

    let mut out_of_range = object.clone();
    out_of_range.insert(
        "contextWindow",
        RuntimeJson::Number(RuntimeNumber::from_f64(2_f64.powi(64))),
    );
    let error =
        runtime_codec::decode_value::<ProviderCapabilities>(&RuntimeJson::Object(out_of_range))
            .unwrap_err();
    assert_eq!(error.kind(), &RuntimeCodecErrorKind::IntegerOutOfRange);
    assert_eq!(error.pointer(), RuntimeString::from("/contextWindow"));

    let mut unknown = object.clone();
    unknown.insert(
        RuntimeString::from_utf16([0xd800]),
        RuntimeJson::String(RuntimeString::from_utf16([0xdc00])),
    );
    assert_eq!(
        runtime_codec::decode_value::<ProviderCapabilities>(&RuntimeJson::Object(unknown)).unwrap(),
        value,
    );
}

#[test]
fn thinking_config_runtime_codec_matches_internal_tag_and_optional_payload_contract() {
    let cases = [
        (ThinkingConfig::Adaptive, r#"{"type":"adaptive"}"#),
        (
            ThinkingConfig::Enabled {
                budget_tokens: None,
            },
            r#"{"type":"enabled"}"#,
        ),
        (
            ThinkingConfig::Enabled {
                budget_tokens: Some(4096),
            },
            r#"{"type":"enabled","budgetTokens":4096}"#,
        ),
        (ThinkingConfig::Disabled, r#"{"type":"disabled"}"#),
    ];
    for (value, expected) in cases {
        assert_eq!(runtime_codec::encode_text(&value).unwrap(), expected);
        assert_eq!(
            runtime_codec::decode_text::<ThinkingConfig>(&RuntimeString::from(expected)).unwrap(),
            value,
        );
    }

    let mut reversed = RuntimeObject::new();
    reversed.insert("budgetTokens", RuntimeJson::from(2048));
    reversed.insert("type", RuntimeJson::String(RuntimeString::from("enabled")));
    let decoded =
        runtime_codec::decode_value::<ThinkingConfig>(&RuntimeJson::Object(reversed)).unwrap();
    assert_eq!(
        decoded,
        ThinkingConfig::Enabled {
            budget_tokens: Some(2048),
        },
    );
    assert_eq!(
        runtime_codec::encode_text(&decoded)
            .unwrap()
            .try_to_utf8()
            .unwrap(),
        r#"{"type":"enabled","budgetTokens":2048}"#,
    );

    let explicit_null = RuntimeString::from(r#"{"type":"enabled","budgetTokens":null}"#);
    assert_eq!(
        runtime_codec::decode_text::<ThinkingConfig>(&explicit_null).unwrap(),
        ThinkingConfig::Enabled {
            budget_tokens: None,
        },
    );

    for (text, kind, pointer) in [
        (r#"{}"#, RuntimeCodecErrorKind::MissingField, "/type"),
        (
            r#"{"type":1}"#,
            RuntimeCodecErrorKind::ExpectedString,
            "/type",
        ),
        (
            r#"{"type":"unknown"}"#,
            RuntimeCodecErrorKind::UnknownEnumVariant,
            "/type",
        ),
        (
            r#"{"type":"enabled","budgetTokens":"many"}"#,
            RuntimeCodecErrorKind::ExpectedNumber,
            "/budgetTokens",
        ),
    ] {
        let error =
            runtime_codec::decode_text::<ThinkingConfig>(&RuntimeString::from(text)).unwrap_err();
        assert_eq!(error.kind(), &kind);
        assert_eq!(error.pointer(), RuntimeString::from(pointer));
    }

    let error = runtime_codec::decode_value::<ThinkingConfig>(&RuntimeJson::Null).unwrap_err();
    assert_eq!(error.kind(), &RuntimeCodecErrorKind::ExpectedObject);
    assert_eq!(error.pointer(), RuntimeString::default());

    let mut surrogate_tag = RuntimeObject::new();
    surrogate_tag.insert(
        "type",
        RuntimeJson::String(RuntimeString::from_utf16([0xd800])),
    );
    let error = runtime_codec::decode_value::<ThinkingConfig>(&RuntimeJson::Object(surrogate_tag))
        .unwrap_err();
    assert_eq!(error.kind(), &RuntimeCodecErrorKind::UnknownEnumVariant);
    assert_eq!(error.pointer(), RuntimeString::from("/type"));

    let mut unknown = RuntimeObject::new();
    unknown.insert("type", RuntimeJson::String(RuntimeString::from("adaptive")));
    unknown.insert(RuntimeString::from_utf16([0xd800]), RuntimeJson::Null);
    assert_eq!(
        runtime_codec::decode_value::<ThinkingConfig>(&RuntimeJson::Object(unknown)).unwrap(),
        ThinkingConfig::Adaptive,
    );
}
