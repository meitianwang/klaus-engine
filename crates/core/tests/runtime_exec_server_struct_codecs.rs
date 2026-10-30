use klaus_agent_core::command_runtime::exec_server::protocol::{
    FsGetMetadataResponse, TerminateResponse, WalkOptions,
};
use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeCodecErrorKind, RuntimeDecode, RuntimeJson, RuntimeNumber, RuntimeObject,
    RuntimeString,
};

fn assert_decode_error<T: RuntimeDecode + std::fmt::Debug>(
    value: &RuntimeJson,
    kind: RuntimeCodecErrorKind,
    pointer: &str,
) {
    let error = runtime_codec::decode_value::<T>(value).unwrap_err();
    assert_eq!(error.kind(), &kind);
    assert_eq!(error.pointer(), RuntimeString::from(pointer));
}

#[test]
fn terminate_response_runtime_codec_matches_required_bool_contract() {
    let value = TerminateResponse { running: true };
    let expected = r#"{"running":true}"#;
    assert_eq!(
        runtime_codec::encode_text(&value)
            .unwrap()
            .try_to_utf8()
            .unwrap(),
        expected,
    );
    assert_eq!(
        runtime_codec::decode_text::<TerminateResponse>(&RuntimeString::from(expected)).unwrap(),
        value,
    );

    assert_decode_error::<TerminateResponse>(
        &RuntimeJson::Object(RuntimeObject::new()),
        RuntimeCodecErrorKind::MissingField,
        "/running",
    );
    assert_decode_error::<TerminateResponse>(
        &RuntimeJson::Null,
        RuntimeCodecErrorKind::ExpectedObject,
        "",
    );

    let mut wrong_type = RuntimeObject::new();
    wrong_type.insert("running", RuntimeJson::from(1));
    assert_decode_error::<TerminateResponse>(
        &RuntimeJson::Object(wrong_type),
        RuntimeCodecErrorKind::ExpectedBool,
        "/running",
    );

    let mut unknown = RuntimeObject::new();
    unknown.insert("running", RuntimeJson::Bool(true));
    unknown.insert(RuntimeString::from_utf16([0xd800]), RuntimeJson::Null);
    assert_eq!(
        runtime_codec::decode_value::<TerminateResponse>(&RuntimeJson::Object(unknown)).unwrap(),
        value,
    );
}

#[test]
fn fs_get_metadata_response_runtime_codec_matches_required_numeric_contract() {
    let value = FsGetMetadataResponse {
        is_directory: true,
        is_file: false,
        is_symlink: true,
        size: 4_096,
        created_at_ms: -1_000,
        modified_at_ms: 2_000,
    };
    let expected = r#"{"isDirectory":true,"isFile":false,"isSymlink":true,"size":4096,"createdAtMs":-1000,"modifiedAtMs":2000}"#;
    let encoded = runtime_codec::encode_value(&value).unwrap();
    assert_eq!(
        runtime_codec::encode_text(&value)
            .unwrap()
            .try_to_utf8()
            .unwrap(),
        expected,
    );
    assert_eq!(
        runtime_codec::decode_value::<FsGetMetadataResponse>(&encoded).unwrap(),
        value,
    );

    let RuntimeJson::Object(object) = &encoded else {
        panic!("filesystem metadata must encode as an object");
    };
    for field in [
        "isDirectory",
        "isFile",
        "isSymlink",
        "size",
        "createdAtMs",
        "modifiedAtMs",
    ] {
        let mut missing = object.clone();
        missing.remove(field);
        assert_decode_error::<FsGetMetadataResponse>(
            &RuntimeJson::Object(missing),
            RuntimeCodecErrorKind::MissingField,
            &format!("/{field}"),
        );
    }

    let mut reversed = RuntimeObject::new();
    reversed.insert("modifiedAtMs", RuntimeJson::from(2_000));
    reversed.insert("createdAtMs", RuntimeJson::from(-1_000));
    reversed.insert("size", RuntimeJson::from(4_096));
    reversed.insert("isSymlink", RuntimeJson::Bool(true));
    reversed.insert("isFile", RuntimeJson::Bool(false));
    reversed.insert("isDirectory", RuntimeJson::Bool(true));
    let decoded =
        runtime_codec::decode_value::<FsGetMetadataResponse>(&RuntimeJson::Object(reversed))
            .unwrap();
    assert_eq!(decoded, value);
    assert_eq!(
        runtime_codec::encode_text(&decoded)
            .unwrap()
            .try_to_utf8()
            .unwrap(),
        expected,
    );

    let mut negative_size = object.clone();
    negative_size.insert("size", RuntimeJson::from(-1));
    assert_decode_error::<FsGetMetadataResponse>(
        &RuntimeJson::Object(negative_size),
        RuntimeCodecErrorKind::IntegerOutOfRange,
        "/size",
    );

    let mut fractional_time = object.clone();
    fractional_time.insert(
        "createdAtMs",
        RuntimeJson::Number(RuntimeNumber::from_f64(1.5)),
    );
    assert_decode_error::<FsGetMetadataResponse>(
        &RuntimeJson::Object(fractional_time),
        RuntimeCodecErrorKind::NonIntegralInteger,
        "/createdAtMs",
    );

    let mut overflow_time = object.clone();
    overflow_time.insert(
        "modifiedAtMs",
        RuntimeJson::Number(RuntimeNumber::from_f64(2_f64.powi(63))),
    );
    assert_decode_error::<FsGetMetadataResponse>(
        &RuntimeJson::Object(overflow_time),
        RuntimeCodecErrorKind::IntegerOutOfRange,
        "/modifiedAtMs",
    );

    let mut unknown = object.clone();
    unknown.insert(RuntimeString::from_utf16([0xd800]), RuntimeJson::Null);
    assert_eq!(
        runtime_codec::decode_value::<FsGetMetadataResponse>(&RuntimeJson::Object(unknown))
            .unwrap(),
        value,
    );
}

#[test]
fn walk_options_runtime_codec_matches_default_null_and_non_skipping_contract() {
    let empty = WalkOptions {
        max_depth: None,
        max_entries: None,
    };
    let empty_expected = r#"{"maxDepth":null,"maxEntries":null}"#;
    assert_eq!(
        runtime_codec::encode_text(&empty)
            .unwrap()
            .try_to_utf8()
            .unwrap(),
        empty_expected,
    );
    assert_eq!(
        runtime_codec::decode_value::<WalkOptions>(&RuntimeJson::Object(RuntimeObject::new()))
            .unwrap(),
        empty,
    );
    assert_eq!(
        runtime_codec::decode_text::<WalkOptions>(&RuntimeString::from(empty_expected)).unwrap(),
        empty,
    );

    let populated = WalkOptions {
        max_depth: Some(3),
        max_entries: Some(500),
    };
    let populated_expected = r#"{"maxDepth":3,"maxEntries":500}"#;
    assert_eq!(
        runtime_codec::encode_text(&populated)
            .unwrap()
            .try_to_utf8()
            .unwrap(),
        populated_expected,
    );
    assert_eq!(
        runtime_codec::decode_text::<WalkOptions>(&RuntimeString::from(populated_expected))
            .unwrap(),
        populated,
    );

    let mut partial = RuntimeObject::new();
    partial.insert("maxEntries", RuntimeJson::from(7));
    partial.insert(RuntimeString::from_utf16([0xd800]), RuntimeJson::Null);
    assert_eq!(
        runtime_codec::decode_value::<WalkOptions>(&RuntimeJson::Object(partial)).unwrap(),
        WalkOptions {
            max_depth: None,
            max_entries: Some(7),
        },
    );

    let mut wrong_type = RuntimeObject::new();
    wrong_type.insert("maxDepth", RuntimeJson::from("deep"));
    assert_decode_error::<WalkOptions>(
        &RuntimeJson::Object(wrong_type),
        RuntimeCodecErrorKind::ExpectedNumber,
        "/maxDepth",
    );

    let mut fractional = RuntimeObject::new();
    fractional.insert(
        "maxEntries",
        RuntimeJson::Number(RuntimeNumber::from_f64(1.5)),
    );
    assert_decode_error::<WalkOptions>(
        &RuntimeJson::Object(fractional),
        RuntimeCodecErrorKind::NonIntegralInteger,
        "/maxEntries",
    );

    assert_decode_error::<WalkOptions>(
        &RuntimeJson::Null,
        RuntimeCodecErrorKind::ExpectedObject,
        "",
    );
}

#[test]
fn exec_server_plain_struct_runtime_codecs_cover_the_complete_family() {
    terminate_response_runtime_codec_matches_required_bool_contract();
    fs_get_metadata_response_runtime_codec_matches_required_numeric_contract();
    walk_options_runtime_codec_matches_default_null_and_non_skipping_contract();
}
