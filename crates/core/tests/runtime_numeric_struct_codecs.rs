use klaus_agent_core::observability::cost_tracker::ModelPricing;
use klaus_agent_core::provider::observability::ProviderUsageSummary;
use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeCodecErrorKind, RuntimeJson, RuntimeObject, RuntimeString,
};
use klaus_agent_core::testing::provider_behavior::TaskEngineConfig;

fn assert_decode_error<T: klaus_agent_core::runtime_values::RuntimeDecode + std::fmt::Debug>(
    value: &RuntimeJson,
    kind: RuntimeCodecErrorKind,
    pointer: &str,
) {
    let error = runtime_codec::decode_value::<T>(value).unwrap_err();
    assert_eq!(error.kind(), &kind);
    assert_eq!(error.pointer(), RuntimeString::from(pointer));
}

#[test]
fn provider_usage_summary_runtime_codec_matches_open_numeric_object_contract() {
    let value = ProviderUsageSummary {
        input_tokens: 1,
        output_tokens: 2,
        cache_read_input_tokens: 3,
        cache_creation_input_tokens: 4,
        total_tokens: 10,
        cache_hit_rate: 0.3,
    };
    let expected = r#"{"inputTokens":1,"outputTokens":2,"cacheReadInputTokens":3,"cacheCreationInputTokens":4,"totalTokens":10,"cacheHitRate":0.3}"#;
    let encoded = runtime_codec::encode_text(&value).unwrap();
    assert_eq!(encoded, RuntimeString::from(expected));
    assert_eq!(
        runtime_codec::decode_text::<ProviderUsageSummary>(&encoded).unwrap(),
        value,
    );

    let mut open = runtime_codec::encode_value(&value)
        .unwrap()
        .into_object()
        .unwrap();
    open.insert(RuntimeString::from_utf16([0xd800]), RuntimeJson::Null);
    assert_eq!(
        runtime_codec::decode_value::<ProviderUsageSummary>(&RuntimeJson::Object(open)).unwrap(),
        value,
    );

    let mut negative = RuntimeObject::new();
    negative.insert("inputTokens", RuntimeJson::from(-1));
    negative.insert("outputTokens", RuntimeJson::from(2_u64));
    negative.insert("cacheReadInputTokens", RuntimeJson::from(3_u64));
    negative.insert("cacheCreationInputTokens", RuntimeJson::from(4_u64));
    negative.insert("totalTokens", RuntimeJson::from(10_u64));
    negative.insert("cacheHitRate", RuntimeJson::from(0.3));
    assert_decode_error::<ProviderUsageSummary>(
        &RuntimeJson::Object(negative),
        RuntimeCodecErrorKind::IntegerOutOfRange,
        "/inputTokens",
    );
}

#[test]
fn model_pricing_runtime_codec_preserves_camel_case_acronym_fields() {
    let value = ModelPricing {
        input_per_m_tok: 1.25,
        output_per_m_tok: 10.0,
        cache_read_per_m_tok: 0.31,
        cache_write_per_m_tok: 1.25,
    };
    let runtime_expected = r#"{"inputPerMTok":1.25,"outputPerMTok":10,"cacheReadPerMTok":0.31,"cacheWritePerMTok":1.25}"#;
    let encoded = runtime_codec::encode_text(&value).unwrap();
    assert_eq!(encoded, RuntimeString::from(runtime_expected));
    assert_eq!(
        runtime_codec::decode_text::<ModelPricing>(&encoded).unwrap(),
        value
    );

    let mut wrong = RuntimeObject::new();
    wrong.insert("inputPerMTok", RuntimeJson::Bool(true));
    wrong.insert("outputPerMTok", RuntimeJson::from(10.0));
    wrong.insert("cacheReadPerMTok", RuntimeJson::from(0.31));
    wrong.insert("cacheWritePerMTok", RuntimeJson::from(1.25));
    assert_decode_error::<ModelPricing>(
        &RuntimeJson::Object(wrong),
        RuntimeCodecErrorKind::ExpectedNumber,
        "/inputPerMTok",
    );
}

#[test]
fn task_engine_config_runtime_codec_matches_closed_optional_integer_contract() {
    let value = TaskEngineConfig {
        code_mode: true,
        edited_files: false,
        fork: true,
        deferred_threshold: None,
    };
    let expected = r#"{"codeMode":true,"editedFiles":false,"fork":true,"deferredThreshold":null}"#;
    let encoded = runtime_codec::encode_text(&value).unwrap();
    assert_eq!(encoded, RuntimeString::from(expected));
    assert_eq!(
        runtime_codec::decode_text::<TaskEngineConfig>(&encoded).unwrap(),
        value,
    );

    let mut omitted_optional = RuntimeObject::new();
    omitted_optional.insert("codeMode", RuntimeJson::Bool(false));
    omitted_optional.insert("editedFiles", RuntimeJson::Bool(true));
    omitted_optional.insert("fork", RuntimeJson::Bool(false));
    assert_eq!(
        runtime_codec::decode_value::<TaskEngineConfig>(&RuntimeJson::Object(omitted_optional))
            .unwrap(),
        TaskEngineConfig {
            code_mode: false,
            edited_files: true,
            fork: false,
            deferred_threshold: None,
        },
    );

    let mut unknown = RuntimeObject::new();
    unknown.insert("codeMode", RuntimeJson::Bool(true));
    unknown.insert("editedFiles", RuntimeJson::Bool(false));
    unknown.insert("fork", RuntimeJson::Bool(true));
    unknown.insert("extra", RuntimeJson::Null);
    assert_decode_error::<TaskEngineConfig>(
        &RuntimeJson::Object(unknown),
        RuntimeCodecErrorKind::UnknownField,
        "/extra",
    );

    let mut negative = RuntimeObject::new();
    negative.insert("codeMode", RuntimeJson::Bool(true));
    negative.insert("editedFiles", RuntimeJson::Bool(false));
    negative.insert("fork", RuntimeJson::Bool(true));
    negative.insert("deferredThreshold", RuntimeJson::from(-1));
    assert_decode_error::<TaskEngineConfig>(
        &RuntimeJson::Object(negative),
        RuntimeCodecErrorKind::IntegerOutOfRange,
        "/deferredThreshold",
    );
}
