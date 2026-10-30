use std::fmt::Debug;

use klaus_agent_core::command_runtime::exec_server::protocol::{
    FsCloseResponse, FsCopyResponse, FsCreateDirectoryResponse, FsRemoveResponse,
    FsWriteFileResponse, SignalResponse, WriteResponse, WriteStatus,
};
use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode, RuntimeJson, RuntimeObject,
    RuntimeString,
};

fn assert_decode_error<T: RuntimeDecode + Debug>(
    value: &RuntimeJson,
    kind: RuntimeCodecErrorKind,
    pointer: &str,
) {
    let error = runtime_codec::decode_value::<T>(value).unwrap_err();
    assert_eq!(error.kind(), &kind);
    assert_eq!(error.pointer(), RuntimeString::from(pointer));
}

fn assert_empty_response_contract<T>(expected: T)
where
    T: Clone + Debug + Eq + RuntimeDecode + RuntimeEncode,
{
    assert_eq!(runtime_codec::encode_text(&expected).unwrap(), "{}");
    assert_eq!(
        runtime_codec::decode_text::<T>(&RuntimeString::from("{}")).unwrap(),
        expected,
    );

    let mut unknown = RuntimeObject::new();
    unknown.insert("futureField", RuntimeJson::Bool(true));
    unknown.insert(RuntimeString::from_utf16([0xd800]), RuntimeJson::Null);
    assert_eq!(
        runtime_codec::decode_value::<T>(&RuntimeJson::Object(unknown)).unwrap(),
        expected,
    );

    for non_object in [RuntimeJson::Null, RuntimeJson::Bool(true)] {
        assert_decode_error::<T>(&non_object, RuntimeCodecErrorKind::ExpectedObject, "");
    }
}

fn assert_write_response_contract<T>(cases: &[(T, &str)])
where
    T: Clone + Debug + Eq + RuntimeDecode + RuntimeEncode,
{
    for (value, expected) in cases {
        assert_eq!(runtime_codec::encode_text(value).unwrap(), *expected);
        assert_eq!(
            runtime_codec::decode_text::<T>(&RuntimeString::from(*expected)).unwrap(),
            value.clone(),
        );
    }

    let accepted = cases[0].0.clone();
    let mut unknown = RuntimeObject::new();
    unknown.insert("status", RuntimeJson::from("accepted"));
    unknown.insert("futureField", RuntimeJson::Bool(true));
    assert_eq!(
        runtime_codec::decode_value::<T>(&RuntimeJson::Object(unknown)).unwrap(),
        accepted,
    );

    let mut surrogate_key = RuntimeObject::new();
    surrogate_key.insert("status", RuntimeJson::from("accepted"));
    surrogate_key.insert(RuntimeString::from_utf16([0xd800]), RuntimeJson::Null);
    assert_eq!(
        runtime_codec::decode_value::<T>(&RuntimeJson::Object(surrogate_key)).unwrap(),
        cases[0].0.clone(),
    );

    assert_decode_error::<T>(
        &RuntimeJson::Object(RuntimeObject::new()),
        RuntimeCodecErrorKind::MissingField,
        "/status",
    );
    for non_object in [RuntimeJson::Null, RuntimeJson::Bool(true)] {
        assert_decode_error::<T>(&non_object, RuntimeCodecErrorKind::ExpectedObject, "");
    }

    for invalid_status in [
        RuntimeJson::Null,
        RuntimeJson::Bool(true),
        RuntimeJson::from("__runtime_unknown_variant__"),
    ] {
        let expected_kind = if matches!(invalid_status, RuntimeJson::String(_)) {
            RuntimeCodecErrorKind::UnknownEnumVariant
        } else {
            RuntimeCodecErrorKind::InvalidEnumShape
        };
        let mut object = RuntimeObject::new();
        object.insert("status", invalid_status);
        assert_decode_error::<T>(&RuntimeJson::Object(object), expected_kind, "/status");
    }

    let mut status_payload = RuntimeObject::new();
    status_payload.insert("accepted", RuntimeJson::Null);
    let mut object = RuntimeObject::new();
    object.insert("status", RuntimeJson::Object(status_payload));
    assert_decode_error::<T>(
        &RuntimeJson::Object(object),
        RuntimeCodecErrorKind::InvalidEnumShape,
        "/status/accepted",
    );
}

#[test]
fn exec_server_empty_and_write_response_runtime_codecs_cover_the_complete_family() {
    assert_empty_response_contract(SignalResponse {});
    assert_empty_response_contract(FsCloseResponse {});
    assert_empty_response_contract(FsWriteFileResponse {});
    assert_empty_response_contract(FsCreateDirectoryResponse {});
    assert_empty_response_contract(FsRemoveResponse {});
    assert_empty_response_contract(FsCopyResponse {});
    assert_write_response_contract(&[
        (
            WriteResponse {
                status: WriteStatus::Accepted,
            },
            r#"{"status":"accepted"}"#,
        ),
        (
            WriteResponse {
                status: WriteStatus::UnknownProcess,
            },
            r#"{"status":"unknownProcess"}"#,
        ),
        (
            WriteResponse {
                status: WriteStatus::StdinClosed,
            },
            r#"{"status":"stdinClosed"}"#,
        ),
        (
            WriteResponse {
                status: WriteStatus::Starting,
            },
            r#"{"status":"starting"}"#,
        ),
    ]);
}
