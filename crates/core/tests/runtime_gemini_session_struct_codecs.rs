use klaus_agent_core::engine::session::{DreamNowOutcome, DreamNowReason};
use klaus_agent_core::provider::gemini::{GoogleSearch, GroundingSupport, Segment, UsageMetadata};
use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeCodecErrorKind, RuntimeJson, RuntimeNumber, RuntimeObject, RuntimeString,
};

fn assert_error<T: klaus_agent_core::runtime_values::RuntimeDecode + std::fmt::Debug>(
    value: &RuntimeJson,
    kind: RuntimeCodecErrorKind,
    pointer: &str,
) {
    let error = runtime_codec::decode_value::<T>(value).unwrap_err();
    assert_eq!(error.kind(), &kind);
    assert_eq!(error.pointer(), RuntimeString::from(pointer));
}

#[test]
fn google_search_empty_object_runtime_codec_is_exact_and_open_to_unknown_fields() {
    let value = GoogleSearch {};
    assert_eq!(
        runtime_codec::encode_text(&value).unwrap(),
        RuntimeString::from("{}"),
    );

    let mut unknown = RuntimeObject::new();
    unknown.insert(RuntimeString::from_utf16([0xd800]), RuntimeJson::Bool(true));
    assert_eq!(
        runtime_codec::decode_value::<GoogleSearch>(&RuntimeJson::Object(unknown)).unwrap(),
        value,
    );
    assert_error::<GoogleSearch>(
        &RuntimeJson::Null,
        RuntimeCodecErrorKind::ExpectedObject,
        "",
    );
}

#[test]
fn gemini_decode_only_numeric_dtos_preserve_default_null_and_integer_boundaries() {
    let segment = runtime_codec::decode_text::<Segment>(&RuntimeString::from(
        r#"{"startIndex":1,"endIndex":null,"extra":true}"#,
    ))
    .unwrap();
    assert_eq!(
        segment,
        Segment {
            start_index: Some(1),
            end_index: None,
        },
    );
    assert_eq!(
        runtime_codec::decode_text::<Segment>(&RuntimeString::from("{}")).unwrap(),
        Segment::default(),
    );

    let usage = runtime_codec::decode_text::<UsageMetadata>(&RuntimeString::from(
        r#"{"promptTokenCount":1,"cachedContentTokenCount":null,"candidatesTokenCount":2,"thoughtsTokenCount":3,"totalTokenCount":4,"extra":true}"#,
    ))
    .unwrap();
    assert_eq!(usage.prompt_token_count, Some(1));
    assert_eq!(usage.cached_content_token_count, None);
    assert_eq!(usage.candidates_token_count, Some(2));
    assert_eq!(usage.thoughts_token_count, Some(3));
    assert_eq!(usage.total_token_count, Some(4));

    let mut negative = RuntimeObject::new();
    negative.insert("startIndex", RuntimeJson::from(-1));
    assert_error::<Segment>(
        &RuntimeJson::Object(negative),
        RuntimeCodecErrorKind::IntegerOutOfRange,
        "/startIndex",
    );

    let mut fractional = RuntimeObject::new();
    fractional.insert(
        "totalTokenCount",
        RuntimeJson::Number(RuntimeNumber::from_f64(1.5)),
    );
    assert_error::<UsageMetadata>(
        &RuntimeJson::Object(fractional),
        RuntimeCodecErrorKind::NonIntegralInteger,
        "/totalTokenCount",
    );
}

#[test]
fn grounding_support_decode_codec_nests_segment_and_array_error_paths() {
    let decoded = runtime_codec::decode_text::<GroundingSupport>(&RuntimeString::from(
        r#"{"segment":{"startIndex":1},"groundingChunkIndices":[0,2],"extra":true}"#,
    ))
    .unwrap();
    assert_eq!(
        decoded,
        GroundingSupport {
            segment: Some(Segment {
                start_index: Some(1),
                end_index: None,
            }),
            grounding_chunk_indices: Some(vec![0, 2]),
        },
    );
    assert_eq!(
        runtime_codec::decode_text::<GroundingSupport>(&RuntimeString::from("{}")).unwrap(),
        GroundingSupport::default(),
    );

    let mut invalid = RuntimeObject::new();
    invalid.insert(
        "groundingChunkIndices",
        RuntimeJson::Array(vec![RuntimeJson::from(-1)]),
    );
    assert_error::<GroundingSupport>(
        &RuntimeJson::Object(invalid),
        RuntimeCodecErrorKind::IntegerOutOfRange,
        "/groundingChunkIndices/0",
    );
}

#[test]
fn dream_now_outcome_runtime_codec_omits_none_and_nests_reason_errors() {
    let success = DreamNowOutcome {
        ok: true,
        reason: None,
    };
    let encoded = runtime_codec::encode_text(&success).unwrap();
    assert_eq!(encoded, RuntimeString::from(r#"{"ok":true}"#));
    assert_eq!(
        runtime_codec::decode_text::<DreamNowOutcome>(&encoded).unwrap(),
        success,
    );

    let blocked = DreamNowOutcome {
        ok: false,
        reason: Some(DreamNowReason::GateClosed),
    };
    assert_eq!(
        runtime_codec::encode_text(&blocked).unwrap(),
        RuntimeString::from(r#"{"ok":false,"reason":"gate-closed"}"#),
    );

    let mut nullable = RuntimeObject::new();
    nullable.insert("ok", RuntimeJson::Bool(true));
    nullable.insert("reason", RuntimeJson::Null);
    nullable.insert(RuntimeString::from_utf16([0xd800]), RuntimeJson::Null);
    assert_eq!(
        runtime_codec::decode_value::<DreamNowOutcome>(&RuntimeJson::Object(nullable)).unwrap(),
        success,
    );

    let mut missing = RuntimeObject::new();
    missing.insert("reason", RuntimeJson::Null);
    assert_error::<DreamNowOutcome>(
        &RuntimeJson::Object(missing),
        RuntimeCodecErrorKind::MissingField,
        "/ok",
    );

    let mut bad_reason = RuntimeObject::new();
    bad_reason.insert("ok", RuntimeJson::Bool(false));
    bad_reason.insert("reason", RuntimeJson::from("not-a-reason"));
    assert_error::<DreamNowOutcome>(
        &RuntimeJson::Object(bad_reason),
        RuntimeCodecErrorKind::UnknownEnumVariant,
        "/reason",
    );
}
