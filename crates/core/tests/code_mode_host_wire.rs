//! Wire-contract tests for the code-mode host connection.
//!
//! These pin the connection's OBSERVABLE form, not just round-tripping: tag
//! spellings, the strict unknown-field policy, the exact-string identity of
//! payloads, and the framing boundaries.

use klaus_agent_core::engine::code_mode::host::{Capability, CapabilitySet};
use klaus_agent_core::engine::code_mode::host::{
    ClientHello, ClientHelloError, ClientToHost, DelegateRequest, DelegateRequestId,
    DelegateResponse, EncodedFrame, FramedReader, FramedWriter, HandshakeRejectReason, HostHello,
    HostRequest, HostResponse, HostToClient, ProtocolVersion, RequestId, SessionId,
    SupportedProtocolVersions, WireCellId, WireContentItem, WireExecuteRequest, WireImageDetail,
    WireNestedToolCall, WireResult, WireRuntimeResponse, WireToolDefinition, WireToolKind,
    WireWaitOutcome, WireWaitRequest, MAX_FRAME_BYTES,
};
use klaus_agent_core::engine::code_mode::protocol::{
    CellId, CodeModeNestedToolCall, CodeModeToolKind, ExecuteRequest, OutputContentItem,
    RuntimeResponse, ToolDefinition, WaitOutcome, WaitRequest,
};
use klaus_agent_core::runtime_values::{
    runtime_codec, runtime_json, RuntimeJson, RuntimeObject, RuntimeString,
};

fn rs(value: &str) -> RuntimeString {
    RuntimeString::from(value)
}

/// The exact JSON text one message encodes to.
fn wire_text<T: runtime_codec_encode::Encodable>(message: &T) -> String {
    message.encode_text()
}

/// A thin helper trait so both directions of the connection share one call
/// shape in these tests.
mod runtime_codec_encode {
    use super::*;

    pub trait Encodable {
        fn encode_text(&self) -> String;
    }

    impl<T: klaus_agent_core::runtime_values::RuntimeEncode> Encodable for T {
        fn encode_text(&self) -> String {
            runtime_codec::encode_text(self)
                .expect("host frames encode")
                .try_to_utf8()
                .expect("test fixtures are scalar text")
        }
    }
}

fn decode<T: klaus_agent_core::runtime_values::RuntimeDecode>(text: &str) -> T {
    runtime_codec::decode_text(&rs(text)).expect("host frame decodes")
}

fn versions_v1() -> SupportedProtocolVersions {
    SupportedProtocolVersions::try_new([ProtocolVersion::V1]).expect("versions")
}

fn capability(name: &str) -> Capability {
    Capability::new(rs(name)).expect("capability")
}

#[test]
fn client_frames_carry_their_documented_tags_and_round_trip() {
    let hello = ClientToHost::ClientHello(
        ClientHello::new(
            versions_v1(),
            CapabilitySet::try_new([capability("cells")]).expect("required"),
            CapabilitySet::empty(),
        )
        .expect("hello"),
    );
    let text = wire_text(&hello);
    assert!(text.contains(r#""type":"connection/hello""#), "{text}");
    assert!(text.contains(r#""supportedVersions":[1]"#), "{text}");
    assert_eq!(decode::<ClientToHost>(&text), hello);

    let request = ClientToHost::Request {
        id: RequestId::new(7),
        request: HostRequest::OpenSession {
            session_id: SessionId::new(rs("thread-1")).expect("session"),
        },
    };
    let text = wire_text(&request);
    assert!(text.contains(r#""type":"operation/request""#), "{text}");
    assert!(text.contains(r#""method":"session/open""#), "{text}");
    assert_eq!(decode::<ClientToHost>(&text), request);

    let cancel = ClientToHost::CancelRequest {
        id: RequestId::new(-3),
    };
    let text = wire_text(&cancel);
    assert!(text.contains(r#""type":"operation/cancel""#), "{text}");
    assert_eq!(decode::<ClientToHost>(&text), cancel);

    let delegate = ClientToHost::DelegateResponse {
        id: DelegateRequestId::new(2),
        result: WireResult::from_result(Ok(DelegateResponse::NotificationDelivered)),
    };
    let text = wire_text(&delegate);
    assert!(text.contains(r#""type":"delegate/response""#), "{text}");
    assert!(text.contains(r#""status":"ok""#), "{text}");
    assert!(
        text.contains(r#""type":"notification/delivered""#),
        "{text}"
    );
    assert_eq!(decode::<ClientToHost>(&text), delegate);
}

#[test]
fn host_frames_carry_their_documented_tags_and_round_trip() {
    let ready = HostToClient::HostHello(HostHello::new(
        ProtocolVersion::V1,
        CapabilitySet::try_new([capability("cells")]).expect("capabilities"),
    ));
    let text = wire_text(&ready);
    assert!(text.contains(r#""type":"connection/ready""#), "{text}");
    assert!(text.contains(r#""selectedVersion":1"#), "{text}");
    assert_eq!(decode::<HostToClient>(&text), ready);

    let rejected = HostToClient::HandshakeRejected {
        reason: HandshakeRejectReason::NoCompatibleVersion {
            supported_versions: versions_v1(),
        },
    };
    let text = wire_text(&rejected);
    assert!(text.contains(r#""type":"connection/rejected""#), "{text}");
    assert!(text.contains(r#""type":"noCompatibleVersion""#), "{text}");
    assert_eq!(decode::<HostToClient>(&text), rejected);

    let started = HostToClient::Response {
        id: RequestId::new(1),
        result: WireResult::from_result(Ok(HostResponse::ExecutionStarted {
            cell_id: WireCellId::new(rs("4")),
        })),
    };
    let text = wire_text(&started);
    assert!(text.contains(r#""type":"operation/response""#), "{text}");
    assert!(text.contains(r#""type":"execution/started""#), "{text}");
    assert_eq!(decode::<HostToClient>(&text), started);

    let initial = HostToClient::InitialResponse {
        id: RequestId::new(1),
        result: WireResult::from_result(Ok(WireRuntimeResponse::Yielded {
            cell_id: WireCellId::new(rs("4")),
            content_items: vec![WireContentItem::InputText { text: rs("hi") }],
        })),
    };
    let text = wire_text(&initial);
    assert!(
        text.contains(r#""type":"execute/initialResponse""#),
        "{text}"
    );
    assert!(text.contains(r#""type":"yielded""#), "{text}");
    assert_eq!(decode::<HostToClient>(&text), initial);

    let delegate = HostToClient::DelegateRequest {
        id: DelegateRequestId::new(5),
        session_id: SessionId::new(rs("thread-1")).expect("session"),
        request: DelegateRequest::Notify {
            call_id: rs("call-1"),
            cell_id: WireCellId::new(rs("4")),
            text: rs("halfway"),
        },
    };
    let text = wire_text(&delegate);
    assert!(text.contains(r#""type":"delegate/request""#), "{text}");
    assert!(text.contains(r#""type":"notification/send""#), "{text}");
    assert_eq!(decode::<HostToClient>(&text), delegate);

    let cancel = HostToClient::CancelDelegateRequest {
        id: DelegateRequestId::new(5),
    };
    let text = wire_text(&cancel);
    assert!(text.contains(r#""type":"delegate/cancel""#), "{text}");
    assert_eq!(decode::<HostToClient>(&text), cancel);

    let closed = HostToClient::CellClosed {
        session_id: SessionId::new(rs("thread-1")).expect("session"),
        cell_id: WireCellId::new(rs("4")),
    };
    let text = wire_text(&closed);
    assert!(text.contains(r#""type":"cell/closed""#), "{text}");
    assert_eq!(decode::<HostToClient>(&text), closed);
}

#[test]
fn a_failed_operation_travels_as_data_not_as_a_connection_fault() {
    let failed: WireResult<HostResponse> =
        WireResult::from_result(Err(rs("exec cell 9 not found")));
    let text = wire_text(&failed);
    assert!(text.contains(r#""status":"error""#), "{text}");
    let decoded: WireResult<HostResponse> = decode(&text);
    assert_eq!(
        decoded.into_result().expect_err("carries the failure"),
        "exec cell 9 not found"
    );
}

#[test]
fn unknown_fields_are_rejected_on_every_framed_message() {
    let extra = r#"{"type":"operation/cancel","id":1,"extra":true}"#;
    let error = runtime_codec::decode_text::<ClientToHost>(&rs(extra))
        .expect_err("unknown fields are refused");
    assert!(
        format!("{error}").contains("UnknownField at /extra"),
        "{error}"
    );

    let extra = r#"{"type":"cell/closed","sessionId":"s","cellId":"1","extra":1}"#;
    assert!(runtime_codec::decode_text::<HostToClient>(&rs(extra)).is_err());
}

#[test]
fn a_blank_session_id_or_zero_version_never_decodes() {
    let blank = r#"{"type":"cell/closed","sessionId":"  ","cellId":"1"}"#;
    let error =
        runtime_codec::decode_text::<HostToClient>(&rs(blank)).expect_err("blank id is refused");
    assert!(
        format!("{error}").contains("identifier must not be empty"),
        "{error}"
    );

    let zero = r#"{"type":"connection/ready","selectedVersion":0,"capabilities":[]}"#;
    let error =
        runtime_codec::decode_text::<HostToClient>(&rs(zero)).expect_err("version 0 is refused");
    assert!(
        format!("{error}").contains("protocol version must not be 0"),
        "{error}"
    );
}

#[test]
fn a_capability_cannot_be_both_required_and_optional() {
    let shared = capability("cells");
    let error = ClientHello::new(
        versions_v1(),
        CapabilitySet::try_new([shared.clone()]).expect("required"),
        CapabilitySet::try_new([shared.clone()]).expect("optional"),
    )
    .expect_err("overlap is refused");
    assert_eq!(error, ClientHelloError::OverlappingCapability(shared));

    let overlapping = r#"{"type":"connection/hello","supportedVersions":[1],"requiredCapabilities":["cells"],"optionalCapabilities":["cells"]}"#;
    let error = runtime_codec::decode_text::<ClientToHost>(&rs(overlapping))
        .expect_err("overlap is refused on decode too");
    assert!(
        format!("{error}").contains("cannot be both required and optional"),
        "{error}"
    );
}

#[test]
fn payload_conversions_are_lossless_in_both_directions() {
    let mut schema = RuntimeObject::new();
    schema.insert("type", RuntimeJson::String(rs("object")));
    let request = ExecuteRequest {
        tool_call_id: rs("exec"),
        enabled_tools: vec![ToolDefinition {
            name: rs("read_file"),
            tool_name: rs("read-file"),
            description: rs("read a file"),
            kind: CodeModeToolKind::Function,
            input_schema: Some(RuntimeJson::Object(schema)),
            output_schema: None,
        }],
        source: rs(r#"text("hi")"#),
        yield_time_ms: Some(5_000),
        max_output_tokens: Some(1_024),
    };
    let wire = WireExecuteRequest::try_from(request.clone()).expect("fits V1");
    assert_eq!(
        ExecuteRequest::try_from(wire.clone()).expect("fits the runtime"),
        request
    );
    assert_eq!(decode::<WireExecuteRequest>(&wire_text(&wire)), wire);

    let wait = WaitRequest {
        cell_id: CellId::new(rs("4")),
        yield_time_ms: None,
        max_output_tokens: Some(7),
        terminate: Some(true),
    };
    let wire = WireWaitRequest::try_from(wait.clone()).expect("fits V1");
    assert_eq!(WaitRequest::try_from(wire.clone()).expect("fits"), wait);
    assert_eq!(decode::<WireWaitRequest>(&wire_text(&wire)), wire);

    let outcome = WaitOutcome::LiveCell(RuntimeResponse::Result {
        cell_id: CellId::new(rs("4")),
        content_items: vec![
            OutputContentItem::InputText { text: rs("done") },
            OutputContentItem::InputImage {
                image_url: rs("data:image/png;base64,AAAA"),
                detail: Some(klaus_agent_core::engine::code_mode::protocol::ImageDetail::High),
            },
        ],
        error_text: Some(rs("boom")),
    });
    let wire = WireWaitOutcome::from(outcome.clone());
    assert_eq!(WaitOutcome::from(wire.clone()), outcome);
    let text = wire_text(&wire);
    assert!(text.contains(r#""liveCell""#), "{text}");
    assert!(text.contains(r#""detail":"high""#), "{text}");
    assert_eq!(decode::<WireWaitOutcome>(&text), wire);

    let call = CodeModeNestedToolCall {
        cell_id: CellId::new(rs("4")),
        runtime_tool_call_id: rs("call-1"),
        tool_name: rs("read-file"),
        tool_kind: CodeModeToolKind::Freeform,
        input: Some(RuntimeJson::String(rs("raw"))),
    };
    let wire = WireNestedToolCall::from(call.clone());
    assert_eq!(CodeModeNestedToolCall::from(wire.clone()), call);
    let text = wire_text(&wire);
    assert!(text.contains(r#""toolKind":"freeform""#), "{text}");
    assert_eq!(decode::<WireNestedToolCall>(&text), wire);
}

#[test]
fn an_output_budget_beyond_the_wire_width_fails_instead_of_truncating() {
    let request = ExecuteRequest {
        tool_call_id: rs("exec"),
        enabled_tools: Vec::new(),
        source: rs("1"),
        yield_time_ms: None,
        max_output_tokens: Some(usize::try_from(i32::MAX).expect("fits") + 1),
    };
    assert!(
        WireExecuteRequest::try_from(request).is_err(),
        "a budget wider than the V1 field must not be silently clamped"
    );
}

#[test]
fn frames_preserve_lone_surrogates_through_the_length_prefix() {
    let lone = RuntimeString::from_utf16(vec![u16::from(b'a'), 0xd800]);
    let message = HostToClient::CellClosed {
        session_id: SessionId::new(lone.clone()).expect("non-blank"),
        cell_id: WireCellId::new(lone.clone()),
    };
    let framed = EncodedFrame::encode(&message)
        .expect("encodes")
        .into_framed_bytes();
    let decoded: HostToClient = EncodedFrame::decode_framed(&framed).expect("decodes");
    let HostToClient::CellClosed {
        session_id,
        cell_id,
    } = decoded
    else {
        panic!("expected a cell/closed frame");
    };
    assert_eq!(session_id.as_runtime_string().as_utf16(), lone.as_utf16());
    assert_eq!(cell_id.as_runtime_string().as_utf16(), lone.as_utf16());
}

#[test]
fn framing_errors_are_precise_about_the_boundary() {
    let message = ClientToHost::CancelRequest {
        id: RequestId::new(1),
    };
    let framed = EncodedFrame::encode(&message)
        .expect("encodes")
        .into_framed_bytes();

    let error = EncodedFrame::decode_framed::<ClientToHost>(&framed[..2])
        .expect_err("a partial prefix is not a frame");
    assert!(
        format!("{error}").contains("missing its length prefix"),
        "{error}"
    );

    let mut trailing = framed.clone();
    trailing.push(b'!');
    let error = EncodedFrame::decode_framed::<ClientToHost>(&trailing)
        .expect_err("trailing bytes are a framing error");
    assert!(format!("{error}").contains("payload bytes"), "{error}");

    let mut oversized = (MAX_FRAME_BYTES as u32 + 1).to_le_bytes().to_vec();
    oversized.extend_from_slice(b"{}");
    let error = EncodedFrame::decode_framed::<ClientToHost>(&oversized)
        .expect_err("an oversized declaration is refused before allocating");
    assert!(format!("{error}").contains("exceeds"), "{error}");
}

#[tokio::test]
async fn a_framed_stream_reads_back_every_written_frame_and_reports_clean_eof() {
    let frames = vec![
        ClientToHost::CancelRequest {
            id: RequestId::new(1),
        },
        ClientToHost::Request {
            id: RequestId::new(2),
            request: HostRequest::Terminate {
                session_id: SessionId::new(rs("s")).expect("session"),
                cell_id: WireCellId::new(rs("4")),
            },
        },
    ];

    let mut buffer = Vec::new();
    {
        let mut writer = FramedWriter::new(&mut buffer);
        for frame in &frames {
            writer.write(frame).await.expect("writes");
        }
    }

    let mut reader = FramedReader::new(std::io::Cursor::new(buffer));
    for expected in &frames {
        let actual: ClientToHost = reader
            .read()
            .await
            .expect("reads")
            .expect("a frame is available");
        assert_eq!(&actual, expected);
    }
    assert!(
        reader
            .read::<ClientToHost>()
            .await
            .expect("clean eof")
            .is_none(),
        "EOF at a frame boundary is not an error"
    );
}

#[tokio::test]
async fn a_truncated_frame_is_an_error_not_an_orderly_close() {
    let framed = EncodedFrame::encode(&ClientToHost::CancelRequest {
        id: RequestId::new(1),
    })
    .expect("encodes")
    .into_framed_bytes();

    let mut reader = FramedReader::new(std::io::Cursor::new(framed[..framed.len() - 1].to_vec()));
    let error = reader
        .read::<ClientToHost>()
        .await
        .expect_err("a truncated payload fails");
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
}

#[test]
fn tool_kinds_and_image_details_use_their_lowercase_wire_spellings() {
    assert_eq!(wire_text(&WireToolKind::Function), r#""function""#);
    assert_eq!(wire_text(&WireToolKind::Freeform), r#""freeform""#);
    for (detail, expected) in [
        (WireImageDetail::Auto, r#""auto""#),
        (WireImageDetail::Low, r#""low""#),
        (WireImageDetail::High, r#""high""#),
        (WireImageDetail::Original, r#""original""#),
    ] {
        assert_eq!(wire_text(&detail), expected);
        assert_eq!(decode::<WireImageDetail>(expected), detail);
    }
}

#[test]
fn a_tool_definition_keeps_its_schemas_exactly() {
    let schema = runtime_json::parse(&rs(r#"{"type":"object","required":["a"]}"#)).expect("parses");
    let definition = WireToolDefinition {
        name: rs("read_file"),
        tool_name: rs("read-file"),
        description: rs("read"),
        kind: WireToolKind::Function,
        input_schema: Some(schema.clone()),
        output_schema: None,
    };
    let text = wire_text(&definition);
    assert!(
        text.contains(r#""inputSchema":{"type":"object","required":["a"]}"#),
        "{text}"
    );
    assert!(!text.contains("outputSchema"), "{text}");
    assert_eq!(decode::<WireToolDefinition>(&text), definition);
}
