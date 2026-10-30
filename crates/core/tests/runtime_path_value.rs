use klaus_agent_core::runtime_process::ProcessAtom;
use klaus_agent_core::runtime_values::{
    runtime_codec, Node20FsPathIngress, RuntimeCodecErrorKind, RuntimeJson, RuntimePath,
    RuntimeString,
};

const _: fn(RuntimePath) -> ProcessAtom = RuntimePath::into_runtime_process_atom;

#[test]
fn runtime_path_preserves_exact_string_identity_through_wire_and_process_atom() {
    let identity = RuntimeString::from_utf16([
        u16::from(b'/'),
        u16::from(b't'),
        u16::from(b'm'),
        u16::from(b'p'),
        u16::from(b'/'),
        0xd800,
        0xfffd,
        0xfdd0,
    ]);
    let path = RuntimePath::from_runtime_string(identity.clone());

    assert_eq!(path.as_runtime_string(), &identity);
    assert_eq!(path.clone().into_runtime_string(), identity);

    let encoded = runtime_codec::encode_value(&path).unwrap();
    assert_eq!(encoded, RuntimeJson::String(identity.clone()));
    assert_eq!(
        runtime_codec::decode_value::<RuntimePath>(&encoded).unwrap(),
        path,
    );

    assert_eq!(
        path.into_runtime_process_atom(),
        ProcessAtom::Runtime(identity),
    );
}

#[test]
fn runtime_path_process_atom_terminal_preserves_every_code_unit_and_never_returns_native() {
    let cases = [
        vec![0xd800],
        vec![0xdc00],
        vec![0],
        "路径/😀/e\u{301}".encode_utf16().collect(),
        vec![
            b'a' as u16,
            b'/' as u16,
            b'.' as u16,
            b'.' as u16,
            b'/' as u16,
            b'b' as u16,
            0,
            0xd800,
            0xdc00,
        ],
    ];

    for expected in cases {
        let atom = RuntimePath::from_runtime_string(RuntimeString::from_utf16(expected.clone()))
            .into_runtime_process_atom();
        match atom {
            ProcessAtom::Runtime(actual) => assert_eq!(actual.as_utf16(), expected),
            ProcessAtom::Native(_) => panic!("RuntimePath must never select the native terminal"),
        }
    }
}

#[test]
fn runtime_path_accepts_only_exact_json_strings() {
    let ordinary: RuntimePath = "relative/file.txt".into();
    assert_eq!(
        ordinary.as_runtime_string(),
        &RuntimeString::from("relative/file.txt"),
    );

    for invalid in [
        RuntimeJson::Null,
        RuntimeJson::Bool(false),
        RuntimeJson::Array(Vec::new()),
        RuntimeJson::Object(Default::default()),
    ] {
        let error = runtime_codec::decode_value::<RuntimePath>(&invalid).unwrap_err();
        assert_eq!(error.kind(), &RuntimeCodecErrorKind::ExpectedString);
        assert_eq!(error.pointer(), RuntimeString::default());
    }
}

#[test]
fn runtime_path_ingress_variants_are_closed_and_distinct() {
    let variants = [
        Node20FsPathIngress::OriginalLogicalOperand,
        Node20FsPathIngress::ProjectedLogicalOperandNodeUtf8Echo,
        Node20FsPathIngress::UnixNativeBytesToV8String,
        Node20FsPathIngress::WindowsWideViaLibuvUtf8ToV8String,
        Node20FsPathIngress::NodeBufferPathBytes,
    ];

    assert_eq!(variants.len(), 5);
    for (index, left) in variants.iter().enumerate() {
        for (other_index, right) in variants.iter().enumerate() {
            assert_eq!(left == right, index == other_index);
        }
    }
}
