use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder, RuntimeJson,
    RuntimeMap, RuntimeObject, RuntimeObjectEncoder, RuntimeString,
};

#[derive(Debug, Eq, PartialEq)]
struct FixtureDto {
    id: RuntimeString,
    note: Option<RuntimeString>,
    count: u64,
    extra: RuntimeObject,
}

impl RuntimeEncode for FixtureDto {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {
            id,
            note,
            count,
            extra,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("id", id)?;
        if let Some(note) = note {
            object.field("note", note)?;
        }
        object.field("count", count)?;
        object.flatten(extra)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for FixtureDto {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            let id = object.required("id", &["legacyId"])?;
            let note = object.optional("note", &[])?;
            let count = object.defaulted("count", &[], || 7_u64)?;
            let extra = object.take_remaining();
            Ok(Self {
                id,
                note,
                count,
                extra,
            })
        })
    }
}

#[test]
fn typed_codec_preserves_declaration_order_skip_default_alias_and_flatten() {
    let mut extra = RuntimeObject::new();
    extra.insert("0", RuntimeJson::from("index"));
    extra.insert("tail", RuntimeJson::from("extra"));
    let dto = FixtureDto {
        id: RuntimeString::from_utf16(vec![0xd800]),
        note: None,
        count: 9,
        extra,
    };

    let encoded = runtime_codec::encode_value(&dto).unwrap();
    assert_eq!(
        runtime_codec::encode_text(&dto).unwrap().as_utf16(),
        runtime_codec::encode_bytes(&dto)
            .unwrap()
            .iter()
            .map(|byte| u16::from(*byte))
            .collect::<Vec<_>>()
    );
    let RuntimeJson::Object(object) = &encoded else {
        panic!("fixture DTO encodes as object");
    };
    assert_eq!(
        object
            .entries()
            .map(|(key, _)| key.try_to_utf8().unwrap())
            .collect::<Vec<_>>(),
        ["0", "id", "count", "tail"]
    );
    assert_eq!(
        runtime_codec::decode_value::<FixtureDto>(&encoded).unwrap(),
        dto
    );

    let alias = runtime_codec::decode_text::<FixtureDto>(&RuntimeString::from(
        r#"{"legacyId":"x","note":null,"unknown":1}"#,
    ))
    .unwrap();
    assert_eq!(alias.id, "x");
    assert_eq!(alias.note, None);
    assert_eq!(alias.count, 7);
    assert_eq!(alias.extra.get("unknown"), Some(&RuntimeJson::from(1)));
}

#[test]
fn typed_codec_reports_semantic_alias_duplicates_with_exact_pointer() {
    let error = runtime_codec::decode_text::<FixtureDto>(&RuntimeString::from(
        r#"{"id":"canonical","legacyId":"alias"}"#,
    ))
    .unwrap_err();
    assert_eq!(error.pointer(), RuntimeString::from("/id"));
    assert!(format!("{:?}", error.kind()).contains("DuplicateField"));

    let mut surrogate_key = RuntimeObject::new();
    surrogate_key.insert(
        RuntimeString::from_utf16(vec![0xd800, u16::from(b'/'), u16::from(b'~')]),
        RuntimeJson::Null,
    );
    let surrogate_value = RuntimeJson::Object(surrogate_key);
    let error = runtime_codec::decode_object(&surrogate_value, true, |_object| Ok(())).unwrap_err();
    assert_eq!(
        error.pointer().as_utf16(),
        [
            u16::from(b'/'),
            0xd800,
            u16::from(b'~'),
            u16::from(b'1'),
            u16::from(b'~'),
            u16::from(b'0')
        ]
    );
}

#[test]
fn integer_decode_checks_finite_integral_and_exact_target_range() {
    for value in [f64::NAN, f64::INFINITY, 1.5, -1.0, 256.0] {
        assert!(runtime_codec::decode_value::<u8>(&RuntimeJson::from(value)).is_err());
    }
    assert_eq!(
        runtime_codec::decode_value::<u8>(&RuntimeJson::from(255.0)).unwrap(),
        255
    );
    assert!(runtime_codec::decode_value::<u128>(&RuntimeJson::from(2_f64.powi(128))).is_err());
    assert!(runtime_codec::decode_value::<i128>(&RuntimeJson::from(2_f64.powi(127))).is_err());
    assert_eq!(
        runtime_codec::decode_value::<i128>(&RuntimeJson::from(-2_f64.powi(127))).unwrap(),
        i128::MIN
    );
}

struct EncodeFailure;

impl RuntimeEncode for EncodeFailure {
    fn encode_runtime(&self, _out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        Err(RuntimeCodecError::new(
            klaus_agent_core::runtime_values::RuntimeCodecErrorKind::Custom(RuntimeString::from(
                "fixture failure",
            )),
        ))
    }
}

#[test]
fn object_encoder_prepends_the_canonical_field_to_nested_errors() {
    let mut object = RuntimeObjectEncoder::new();
    let error = object.field("outer", &EncodeFailure).unwrap_err();
    assert_eq!(error.pointer(), RuntimeString::from("/outer"));
}

struct SwallowedDuplicateEmit;

impl RuntimeEncode for SwallowedDuplicateEmit {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        out.emit(RuntimeJson::from(1))?;
        let _ignored = out.emit(RuntimeJson::from(2));
        Ok(())
    }
}

#[test]
fn duplicate_emit_poisoning_cannot_be_hidden_by_a_handwritten_codec() {
    let error = runtime_codec::encode_value(&SwallowedDuplicateEmit).unwrap_err();
    assert_eq!(error.pointer(), RuntimeString::from(""));
    assert_eq!(
        error.kind(),
        &klaus_agent_core::runtime_values::RuntimeCodecErrorKind::EncoderAlreadySet
    );
}

#[derive(Debug)]
struct DenyUnknownFixture;

impl RuntimeDecode for DenyUnknownFixture {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            let _id: RuntimeString = object.required("id", &[])?;
            Ok(Self)
        })
    }
}

#[test]
fn deny_unknown_finalization_cannot_be_omitted_by_a_handwritten_codec() {
    let error = runtime_codec::decode_text::<DenyUnknownFixture>(&RuntimeString::from(
        r#"{"id":"x","extra":1}"#,
    ))
    .unwrap_err();
    assert_eq!(error.pointer(), RuntimeString::from("/extra"));
    assert_eq!(
        error.kind(),
        &klaus_agent_core::runtime_values::RuntimeCodecErrorKind::UnknownField
    );
}

#[test]
fn one_property_cannot_be_consumed_by_two_logical_fields() {
    let value = runtime_json_value(r#"{"shared":"x"}"#);
    let error = runtime_codec::decode_object(&value, false, |object| {
        let _first: RuntimeString = object.required("first", &["shared"])?;
        let _second: RuntimeString = object.required("second", &["shared"])?;
        Ok(())
    })
    .unwrap_err();
    assert_eq!(error.pointer(), RuntimeString::from("/second"));
    assert_eq!(
        error.kind(),
        &klaus_agent_core::runtime_values::RuntimeCodecErrorKind::DuplicateField
    );
}

#[derive(Debug, Eq, PartialEq)]
enum ExternalEnumFixture {
    Unit,
    Payload(RuntimeString),
}

const EXTERNAL_VARIANTS: &[runtime_codec::RuntimeEnumVariant<'static>] = &[
    runtime_codec::RuntimeEnumVariant::new("unit", &["legacy_unit"]),
    runtime_codec::RuntimeEnumVariant::new("payload", &["legacy_payload"]),
];

impl RuntimeEncode for ExternalEnumFixture {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        match self {
            Self::Unit => runtime_codec::encode_external_unit(out, "unit"),
            Self::Payload(value) => runtime_codec::encode_external_payload(out, "payload", value),
        }
    }
}

impl RuntimeDecode for ExternalEnumFixture {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_external(value, EXTERNAL_VARIANTS, |variant| match variant {
            runtime_codec::RuntimeExternalVariant::Unit { variant: 0 } => Ok(Self::Unit),
            runtime_codec::RuntimeExternalVariant::Payload { variant: 1, value } => {
                RuntimeString::decode_runtime(value).map(Self::Payload)
            }
            _ => Err(RuntimeCodecError::new(
                klaus_agent_core::runtime_values::RuntimeCodecErrorKind::InvalidEnumShape,
            )),
        })
    }
}

#[test]
fn external_enum_helper_enforces_string_units_and_single_key_payloads() {
    assert_eq!(
        runtime_codec::encode_text(&ExternalEnumFixture::Unit).unwrap(),
        RuntimeString::from(r#""unit""#)
    );
    assert_eq!(
        runtime_codec::encode_text(&ExternalEnumFixture::Payload(RuntimeString::from("x")))
            .unwrap(),
        RuntimeString::from(r#"{"payload":"x"}"#)
    );
    assert_eq!(
        runtime_codec::decode_text::<ExternalEnumFixture>(
            &RuntimeString::from(r#""legacy_unit""#,)
        )
        .unwrap(),
        ExternalEnumFixture::Unit
    );
    assert_eq!(
        runtime_codec::decode_text::<ExternalEnumFixture>(&RuntimeString::from(
            r#"{"legacy_payload":"x"}"#,
        ))
        .unwrap(),
        ExternalEnumFixture::Payload(RuntimeString::from("x"))
    );

    let error = runtime_codec::decode_text::<ExternalEnumFixture>(&RuntimeString::from(
        r#"{"unit":null,"payload":"x"}"#,
    ))
    .unwrap_err();
    assert_eq!(error.pointer(), RuntimeString::default());
    assert_eq!(
        error.kind(),
        &klaus_agent_core::runtime_values::RuntimeCodecErrorKind::InvalidEnumShape
    );

    let error = runtime_codec::decode_text::<ExternalEnumFixture>(&RuntimeString::from(
        r#"{"unknown":null}"#,
    ))
    .unwrap_err();
    assert_eq!(error.pointer(), RuntimeString::from("/unknown"));
    assert_eq!(
        error.kind(),
        &klaus_agent_core::runtime_values::RuntimeCodecErrorKind::UnknownEnumVariant
    );

    let error = runtime_codec::decode_text::<ExternalEnumFixture>(&RuntimeString::from(
        r#"{"legacy_payload":null}"#,
    ))
    .unwrap_err();
    assert_eq!(error.pointer(), RuntimeString::from("/payload"));
    assert_eq!(
        error.kind(),
        &klaus_agent_core::runtime_values::RuntimeCodecErrorKind::ExpectedString
    );
}

#[derive(Debug, Eq, PartialEq)]
enum InternalEnumFixture {
    Text(RuntimeString),
    Empty,
}

const INTERNAL_VARIANTS: &[runtime_codec::RuntimeEnumVariant<'static>] = &[
    runtime_codec::RuntimeEnumVariant::new("text", &["legacy_text"]),
    runtime_codec::RuntimeEnumVariant::new("empty", &[]),
];

impl RuntimeEncode for InternalEnumFixture {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut fields = RuntimeObjectEncoder::new();
        let tag = match self {
            Self::Text(value) => {
                fields.field("value", value)?;
                "text"
            }
            Self::Empty => "empty",
        };
        runtime_codec::encode_internal(out, "type", &["kind"], tag, &fields.finish())
    }
}

impl RuntimeDecode for InternalEnumFixture {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            true,
            "type",
            &["kind"],
            INTERNAL_VARIANTS,
            |variant, object| match variant {
                0 => object.required("value", &[]).map(Self::Text),
                1 => Ok(Self::Empty),
                _ => unreachable!("variant index comes from INTERNAL_VARIANTS"),
            },
        )
    }
}

#[test]
fn internal_enum_helper_emits_tag_first_and_stabilizes_tag_errors() {
    let encoded =
        runtime_codec::encode_value(&InternalEnumFixture::Text(RuntimeString::from("x"))).unwrap();
    let RuntimeJson::Object(encoded) = &encoded else {
        panic!("internal enum must encode as an object");
    };
    assert_eq!(
        encoded
            .entries()
            .map(|(key, _)| key.try_to_utf8().unwrap())
            .collect::<Vec<_>>(),
        ["type", "value"]
    );
    assert_eq!(
        runtime_codec::decode_text::<InternalEnumFixture>(&RuntimeString::from(
            r#"{"kind":"legacy_text","value":"x"}"#,
        ))
        .unwrap(),
        InternalEnumFixture::Text(RuntimeString::from("x"))
    );

    let mut conflicting_fields = RuntimeObject::new();
    conflicting_fields.insert("kind", RuntimeJson::from("forged"));
    let error = runtime_codec::encode_internal(
        &mut RuntimeEncoder::default(),
        "type",
        &["kind"],
        "text",
        &conflicting_fields,
    )
    .unwrap_err();
    assert_eq!(error.pointer(), RuntimeString::from("/type"));
    assert_eq!(
        error.kind(),
        &klaus_agent_core::runtime_values::RuntimeCodecErrorKind::DuplicateField
    );

    for (text, kind) in [
        (
            r#"{"type":"text","kind":"text","value":"x"}"#,
            klaus_agent_core::runtime_values::RuntimeCodecErrorKind::DuplicateField,
        ),
        (
            r#"{"value":"x"}"#,
            klaus_agent_core::runtime_values::RuntimeCodecErrorKind::MissingField,
        ),
        (
            r#"{"type":1,"value":"x"}"#,
            klaus_agent_core::runtime_values::RuntimeCodecErrorKind::ExpectedString,
        ),
        (
            r#"{"type":"unknown","value":"x"}"#,
            klaus_agent_core::runtime_values::RuntimeCodecErrorKind::UnknownEnumVariant,
        ),
    ] {
        let error = runtime_codec::decode_text::<InternalEnumFixture>(&RuntimeString::from(text))
            .unwrap_err();
        assert_eq!(error.pointer(), RuntimeString::from("/type"), "{text}");
        assert_eq!(error.kind(), &kind, "{text}");
    }
}

#[derive(Debug, Eq, PartialEq)]
enum AdjacentEnumFixture {
    Payload(RuntimeString),
    Unit,
}

const ADJACENT_VARIANTS: &[runtime_codec::RuntimeEnumVariant<'static>] = &[
    runtime_codec::RuntimeEnumVariant::new("payload", &[]),
    runtime_codec::RuntimeEnumVariant::new("unit", &[]),
];

impl RuntimeEncode for AdjacentEnumFixture {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        match self {
            Self::Payload(value) => runtime_codec::encode_adjacent_payload(
                out,
                "kind",
                &[],
                "content",
                &[],
                "payload",
                value,
            ),
            Self::Unit => {
                runtime_codec::encode_adjacent_unit(out, "kind", &[], "content", &[], "unit")
            }
        }
    }
}

impl RuntimeDecode for AdjacentEnumFixture {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_adjacent(
            value,
            true,
            "kind",
            &[],
            "content",
            &[],
            ADJACENT_VARIANTS,
            &[1],
            |variant, content| match (variant, content) {
                (0, Some(content)) => RuntimeString::decode_runtime(content).map(Self::Payload),
                (1, None) => Ok(Self::Unit),
                _ => Err(RuntimeCodecError::new(
                    klaus_agent_core::runtime_values::RuntimeCodecErrorKind::InvalidEnumShape,
                )),
            },
        )
    }
}

#[test]
fn adjacent_enum_helper_requires_content_except_for_explicit_unit_variants() {
    assert_eq!(
        runtime_codec::encode_text(&AdjacentEnumFixture::Payload(RuntimeString::from("x")))
            .unwrap(),
        RuntimeString::from(r#"{"kind":"payload","content":"x"}"#)
    );
    assert_eq!(
        runtime_codec::encode_text(&AdjacentEnumFixture::Unit).unwrap(),
        RuntimeString::from(r#"{"kind":"unit"}"#)
    );
    assert_eq!(
        runtime_codec::decode_text::<AdjacentEnumFixture>(&RuntimeString::from(
            r#"{"kind":"unit"}"#,
        ))
        .unwrap(),
        AdjacentEnumFixture::Unit
    );

    let error = runtime_codec::decode_text::<AdjacentEnumFixture>(&RuntimeString::from(
        r#"{"kind":"payload"}"#,
    ))
    .unwrap_err();
    assert_eq!(error.pointer(), RuntimeString::from("/content"));
    assert_eq!(
        error.kind(),
        &klaus_agent_core::runtime_values::RuntimeCodecErrorKind::MissingField
    );

    let error = runtime_codec::decode_text::<AdjacentEnumFixture>(&RuntimeString::from(
        r#"{"kind":"payload","content":null}"#,
    ))
    .unwrap_err();
    assert_eq!(error.pointer(), RuntimeString::from("/content"));
    assert_eq!(
        error.kind(),
        &klaus_agent_core::runtime_values::RuntimeCodecErrorKind::ExpectedString
    );

    let error = runtime_codec::encode_adjacent_payload(
        &mut RuntimeEncoder::default(),
        "kind",
        &["content"],
        "content",
        &[],
        "payload",
        &RuntimeString::from("x"),
    )
    .unwrap_err();
    assert_eq!(error.pointer(), RuntimeString::from("/content"));
    assert_eq!(
        error.kind(),
        &klaus_agent_core::runtime_values::RuntimeCodecErrorKind::DuplicateField
    );

    let error = runtime_codec::decode_adjacent(
        &runtime_json_value(r#"{"kind":"payload","content":"x"}"#),
        true,
        "kind",
        &["content"],
        "content",
        &[],
        ADJACENT_VARIANTS,
        &[],
        |_variant, _content| Ok(()),
    )
    .unwrap_err();
    assert_eq!(error.pointer(), RuntimeString::from("/content"));
    assert_eq!(
        error.kind(),
        &klaus_agent_core::runtime_values::RuntimeCodecErrorKind::DuplicateField
    );
}

#[derive(Debug, Eq, PartialEq)]
enum UntaggedEnumFixture {
    First(RuntimeString),
    Second(RuntimeString),
}

fn decode_untagged_first(value: &RuntimeJson) -> Result<UntaggedEnumFixture, RuntimeCodecError> {
    runtime_codec::decode_object(value, true, |object| {
        let value = object.required("value", &[])?;
        let _complete: bool = object.required("complete", &[])?;
        Ok(UntaggedEnumFixture::First(value))
    })
}

fn decode_untagged_second(value: &RuntimeJson) -> Result<UntaggedEnumFixture, RuntimeCodecError> {
    runtime_codec::decode_object(value, true, |object| {
        object
            .required("value", &[])
            .map(UntaggedEnumFixture::Second)
    })
}

#[test]
fn untagged_enum_helper_retries_immutable_input_in_order_and_closes_all_failures() {
    let variants: &[runtime_codec::RuntimeUntaggedVariantDecoder<UntaggedEnumFixture>] =
        &[decode_untagged_first, decode_untagged_second];

    assert_eq!(
        runtime_codec::decode_untagged(
            &runtime_json_value(r#"{"value":"x","complete":true}"#),
            variants,
        )
        .unwrap(),
        UntaggedEnumFixture::First(RuntimeString::from("x"))
    );
    assert_eq!(
        runtime_codec::decode_untagged(&runtime_json_value(r#"{"value":"x"}"#), variants).unwrap(),
        UntaggedEnumFixture::Second(RuntimeString::from("x"))
    );

    let error = runtime_codec::decode_untagged(&RuntimeJson::Null, variants).unwrap_err();
    assert_eq!(error.pointer(), RuntimeString::default());
    assert_eq!(
        error.kind(),
        &klaus_agent_core::runtime_values::RuntimeCodecErrorKind::NoMatchingUntaggedVariant
    );
}

#[test]
fn runtime_key_map_recurses_in_runtime_object_order_with_exact_error_paths() {
    let mut map = RuntimeMap::new();
    map.insert(RuntimeString::from("10"), vec![10_u64]);
    map.insert(RuntimeString::from("2"), vec![2_u64]);
    map.insert(RuntimeString::from("tail"), vec![3_u64]);
    map.insert(RuntimeString::from("head"), vec![4_u64]);
    map.insert(RuntimeString::from("tail"), vec![5_u64]);

    assert_eq!(
        map.entries()
            .map(|(key, value)| (key.try_to_utf8().unwrap(), value.clone()))
            .collect::<Vec<_>>(),
        [
            ("2".to_string(), vec![2]),
            ("10".to_string(), vec![10]),
            ("tail".to_string(), vec![5]),
            ("head".to_string(), vec![4]),
        ]
    );
    assert_eq!(
        runtime_codec::encode_text(&map).unwrap(),
        RuntimeString::from(r#"{"2":[2],"10":[10],"tail":[5],"head":[4]}"#)
    );
    assert_eq!(
        runtime_codec::decode_text::<RuntimeMap<Vec<u64>>>(&RuntimeString::from(
            r#"{"tail":[5],"10":[10],"2":[2],"head":[4]}"#,
        ))
        .unwrap(),
        map
    );

    let error = runtime_codec::decode_text::<RuntimeMap<Vec<u64>>>(&RuntimeString::from(
        r#"{"ok":[1],"bad/~":[1,"x"]}"#,
    ))
    .unwrap_err();
    assert_eq!(error.pointer(), RuntimeString::from("/bad~1~0/1"));
    assert_eq!(
        error.kind(),
        &klaus_agent_core::runtime_values::RuntimeCodecErrorKind::ExpectedNumber
    );

    let mut failing_encode = RuntimeMap::new();
    failing_encode.insert(RuntimeString::from("bad/~"), EncodeFailure);
    let error = runtime_codec::encode_value(&failing_encode).unwrap_err();
    assert_eq!(error.pointer(), RuntimeString::from("/bad~1~0"));
    assert_eq!(
        error.kind(),
        &klaus_agent_core::runtime_values::RuntimeCodecErrorKind::Custom(RuntimeString::from(
            "fixture failure"
        ))
    );
}

#[derive(Debug, Eq, PartialEq)]
struct FlattenDecodeFixture {
    id: RuntimeString,
    nested: RuntimeString,
    extra: RuntimeMap<u64>,
}

impl RuntimeDecode for FlattenDecodeFixture {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            let id = object.required("id", &["legacyId"])?;
            let nested = object.flatten_dto(|object| object.required("nested", &[]))?;
            let extra = object.flatten_map()?;
            Ok(Self { id, nested, extra })
        })
    }
}

#[test]
fn flatten_dto_and_map_share_consumption_and_preserve_remaining_order() {
    let decoded = runtime_codec::decode_text::<FlattenDecodeFixture>(&RuntimeString::from(
        r#"{"tail":3,"10":10,"legacyId":"outer","2":2,"nested":"inner","head":4}"#,
    ))
    .unwrap();
    assert_eq!(decoded.id, "outer");
    assert_eq!(decoded.nested, "inner");
    assert_eq!(
        decoded
            .extra
            .entries()
            .map(|(key, value)| (key.try_to_utf8().unwrap(), *value))
            .collect::<Vec<_>>(),
        [
            ("2".to_string(), 2),
            ("10".to_string(), 10),
            ("tail".to_string(), 3),
            ("head".to_string(), 4),
        ]
    );
    assert!(decoded
        .extra
        .get(&RuntimeString::from("legacyId"))
        .is_none());
    assert!(decoded.extra.get(&RuntimeString::from("nested")).is_none());

    let value = runtime_json_value(r#"{"legacyId":"outer"}"#);
    let error = runtime_codec::decode_object(&value, false, |object| {
        let _id: RuntimeString = object.required("id", &["legacyId"])?;
        object.flatten_dto(|object| {
            let _shadow: RuntimeString = object.required("shadow", &["legacyId"])?;
            Ok(())
        })
    })
    .unwrap_err();
    assert_eq!(error.pointer(), RuntimeString::from("/shadow"));
    assert_eq!(
        error.kind(),
        &klaus_agent_core::runtime_values::RuntimeCodecErrorKind::DuplicateField
    );

    let error = runtime_codec::decode_text::<FlattenDecodeFixture>(&RuntimeString::from(
        r#"{"id":"outer","nested":"inner","bad/~":"x"}"#,
    ))
    .unwrap_err();
    assert_eq!(error.pointer(), RuntimeString::from("/bad~1~0"));
    assert_eq!(
        error.kind(),
        &klaus_agent_core::runtime_values::RuntimeCodecErrorKind::ExpectedNumber
    );
}

#[test]
fn flatten_encode_expands_at_call_position_with_javascript_collision_semantics() {
    let mut flattened = RuntimeMap::new();
    flattened.insert(RuntimeString::from("0"), 0_u64);
    flattened.insert(RuntimeString::from("before"), 2_u64);
    flattened.insert(RuntimeString::from("middle"), 3_u64);

    let mut object = RuntimeObjectEncoder::new();
    object.field("before", &1_u64).unwrap();
    object.flatten(&flattened).unwrap();
    object.field("before", &4_u64).unwrap();
    object.field("after", &5_u64).unwrap();
    let object = object.finish();

    assert_eq!(
        object
            .entries()
            .map(|(key, value)| (key.try_to_utf8().unwrap(), value.clone()))
            .collect::<Vec<_>>(),
        [
            ("0".to_string(), RuntimeJson::from(0)),
            ("before".to_string(), RuntimeJson::from(4)),
            ("middle".to_string(), RuntimeJson::from(3)),
            ("after".to_string(), RuntimeJson::from(5)),
        ]
    );
}

fn runtime_json_value(text: &str) -> RuntimeJson {
    klaus_agent_core::runtime_values::runtime_json::parse(&RuntimeString::from(text)).unwrap()
}
