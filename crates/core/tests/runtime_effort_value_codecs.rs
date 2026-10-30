use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeCodecErrorKind, RuntimeJson, RuntimeNumber, RuntimeString,
};
use klaus_agent_core::schemas::effort::{EffortLevel, EffortValue};

#[test]
fn effort_value_untagged_runtime_codec_preserves_level_and_integer_variants() {
    let cases = [
        (EffortValue::Level(EffortLevel::High), r#""high""#),
        (EffortValue::Number(-7), "-7"),
        (EffortValue::Level(EffortLevel::None), r#""none""#),
        (EffortValue::Level(EffortLevel::Minimal), r#""minimal""#),
        (
            EffortValue::Level(EffortLevel::Persistent),
            r#""persistent""#,
        ),
        (
            EffortValue::Custom("custom/exact".into()),
            r#""custom/exact""#,
        ),
    ];
    for (value, expected) in cases {
        let encoded = runtime_codec::encode_text(&value).unwrap();
        assert_eq!(encoded, RuntimeString::from(expected));
        assert_eq!(
            runtime_codec::decode_text::<EffortValue>(&encoded).unwrap(),
            value,
        );
    }

    let exact = RuntimeString::from_utf16([0xd800]);
    let custom =
        runtime_codec::decode_value::<EffortValue>(&RuntimeJson::String(exact.clone())).unwrap();
    assert_eq!(custom, EffortValue::Custom(exact));
    assert_eq!(
        runtime_codec::decode_text::<EffortValue>(&runtime_codec::encode_text(&custom).unwrap())
            .unwrap(),
        custom
    );

    let fractional = runtime_codec::decode_value::<EffortValue>(&RuntimeJson::Number(
        RuntimeNumber::from_f64(1.5),
    ))
    .unwrap_err();
    assert_eq!(
        fractional.kind(),
        &RuntimeCodecErrorKind::NonIntegralInteger
    );
    assert_eq!(fractional.pointer(), RuntimeString::default());

    assert_eq!(
        runtime_codec::decode_value::<EffortValue>(&RuntimeJson::Number(RuntimeNumber::from_f64(
            -2_f64.powi(63)
        ),))
        .unwrap(),
        EffortValue::Number(i64::MIN),
    );
    let out_of_range = runtime_codec::decode_value::<EffortValue>(&RuntimeJson::Number(
        RuntimeNumber::from_f64(2_f64.powi(63)),
    ))
    .unwrap_err();
    assert_eq!(
        out_of_range.kind(),
        &RuntimeCodecErrorKind::IntegerOutOfRange
    );
    assert_eq!(out_of_range.pointer(), RuntimeString::default());

    for wrong_shape in [
        RuntimeJson::Null,
        RuntimeJson::Bool(false),
        RuntimeJson::Array(Vec::new()),
        RuntimeJson::Object(Default::default()),
    ] {
        let error = runtime_codec::decode_value::<EffortValue>(&wrong_shape).unwrap_err();
        assert_eq!(
            error.kind(),
            &RuntimeCodecErrorKind::NoMatchingUntaggedVariant
        );
        assert_eq!(error.pointer(), RuntimeString::default());
    }
}
