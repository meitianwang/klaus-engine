use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode, RuntimeJson, RuntimeObject,
    RuntimeString,
};
use klaus_agent_protocol::types::{InitSessionResult, ProtocolSessionFileChange, SteerResult};

fn assert_wire_round_trip<T>(value: &T, expected: &str)
where
    T: RuntimeEncode + RuntimeDecode + PartialEq + std::fmt::Debug,
{
    assert_eq!(runtime_codec::encode_text(value).unwrap(), expected);
    assert_eq!(
        runtime_codec::decode_text::<T>(&RuntimeString::from(expected)).unwrap(),
        *value,
    );
}

fn assert_missing<T: RuntimeDecode + std::fmt::Debug>(field: &str) {
    let error =
        runtime_codec::decode_value::<T>(&RuntimeJson::Object(RuntimeObject::new())).unwrap_err();
    assert_eq!(error.kind(), &RuntimeCodecErrorKind::MissingField);
    assert_eq!(error.pointer(), RuntimeString::from(format!("/{field}")));
}

#[test]
fn steer_result_runtime_codec_matches_required_bool_contract() {
    let value = SteerResult { applied: true };
    assert_wire_round_trip(&value, r#"{"applied":true}"#);
    assert_missing::<SteerResult>("applied");

    let mut unknown = RuntimeObject::new();
    unknown.insert("applied", RuntimeJson::Bool(true));
    unknown.insert(RuntimeString::from_utf16([0xd800]), RuntimeJson::Null);
    assert_eq!(
        runtime_codec::decode_value::<SteerResult>(&RuntimeJson::Object(unknown)).unwrap(),
        value,
    );
}

#[test]
fn init_session_result_runtime_codec_matches_required_bool_contract() {
    let value = InitSessionResult { accepted: false };
    assert_wire_round_trip(&value, r#"{"accepted":false}"#);
    assert_missing::<InitSessionResult>("accepted");

    let mut wrong = RuntimeObject::new();
    wrong.insert("accepted", RuntimeJson::from(1));
    let error =
        runtime_codec::decode_value::<InitSessionResult>(&RuntimeJson::Object(wrong)).unwrap_err();
    assert_eq!(error.kind(), &RuntimeCodecErrorKind::ExpectedBool);
    assert_eq!(error.pointer(), RuntimeString::from("/accepted"));
}

#[test]
fn protocol_plain_struct_runtime_codecs_cover_both_roots() {
    steer_result_runtime_codec_matches_required_bool_contract();
    init_session_result_runtime_codec_matches_required_bool_contract();
}

#[test]
fn file_change_turn_diff_is_additive_and_preserves_exact_runtime_text() {
    let historical = RuntimeString::from(r#"{"path":"a.txt","kind":"modify"}"#);
    let mut row: ProtocolSessionFileChange = runtime_codec::decode_text(&historical).unwrap();
    assert_eq!(row.turn_diff, None);
    assert_eq!(runtime_codec::encode_text(&row).unwrap(), historical);
    row.turn_diff = Some(RuntimeString::from_utf16([0xd800, 0x000a]));
    let encoded = runtime_codec::encode_text(&row).unwrap();
    assert!(encoded.contains("turnDiff"));
    let decoded: ProtocolSessionFileChange = runtime_codec::decode_text(&encoded).unwrap();
    assert_eq!(decoded, row);
}
