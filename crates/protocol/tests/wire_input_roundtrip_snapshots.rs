//! Output-level round-trip snapshots for the protocol wire-input hybrid
//! adapter (`wire_input::serialize`/`deserialize` behind `OpKind::Run` and
//! `OpKind::Steer`) and the strictly-positive `numTurns` decode adapter behind
//! `OpKind::Rewind`. Samples cover the string form, every part variant with
//! optional fields absent/present, empty part lists, and special characters;
//! re-record via `cargo insta review`.

use klaus_agent_protocol::types::{parse_op, Input, Op, OpKind, UserContentPart};

use klaus_agent_core::runtime_values::{
    runtime_codec, runtime_json, RuntimeCodecError, RuntimeEncode, RuntimeString,
};

fn encoded_pretty<T: RuntimeEncode + ?Sized>(value: &T) -> String {
    let json = runtime_codec::encode_value(value).expect("sample encodes");
    runtime_json::stringify_pretty(&json)
        .try_to_utf8()
        .expect("encoded wire JSON is scalar text")
}

fn parse_from(text: &str) -> Result<Op, RuntimeCodecError> {
    let value = runtime_json::parse(&RuntimeString::from(text)).expect("sample JSON parses");
    parse_op(value)
}

/// Encode, snapshot-ready; parse back and assert the re-encoded text matches.
fn roundtrip(op: &Op) -> String {
    let encoded = encoded_pretty(op);
    let decoded = parse_from(&encoded).expect("encoded op parses back");
    assert_eq!(
        encoded_pretty(&decoded),
        encoded,
        "op encode/parse round-trip must be lossless"
    );
    encoded
}

#[test]
fn wire_input_hybrid_roundtrip_snapshots() {
    let text = Op {
        id: RuntimeString::from("run-text"),
        op: OpKind::Run {
            input: Input::Text(RuntimeString::from("héllo 世界")),
            options: None,
        },
    };
    let parts = Op {
        id: RuntimeString::from("run-parts"),
        op: OpKind::Run {
            input: Input::Parts(vec![
                UserContentPart::Text {
                    text: RuntimeString::from("look at this"),
                },
                UserContentPart::Image {
                    base64: RuntimeString::from("aGk="),
                    mime_type: RuntimeString::from("image/png"),
                    source_path: None,
                },
                UserContentPart::Document {
                    base64: RuntimeString::from("ZG9j"),
                    mime_type: RuntimeString::from("application/pdf"),
                    filename: None,
                },
                UserContentPart::Document {
                    base64: RuntimeString::from("ZG9j"),
                    mime_type: RuntimeString::from("application/pdf"),
                    filename: Some(RuntimeString::from("规格 v2.pdf")),
                },
            ]),
            options: None,
        },
    };
    let empty_parts = Op {
        id: RuntimeString::from("run-empty"),
        op: OpKind::Run {
            input: Input::Parts(Vec::new()),
            options: None,
        },
    };
    let steer = Op {
        id: RuntimeString::from("steer-1"),
        op: OpKind::Steer {
            input: Input::Text(RuntimeString::from("change course")),
            priority: None,
            delivery: None,
            display_metadata: None,
            expected_turn_id: None,
            queue_message_id: None,
        },
    };
    insta::assert_snapshot!(
        "wire_input_ops_encoded",
        format!(
            "{}\n{}\n{}\n{}",
            roundtrip(&text),
            roundtrip(&parts),
            roundtrip(&empty_parts),
            roundtrip(&steer)
        )
    );

    let bad_part = parse_from(
        "{\"id\":\"x\",\"op\":{\"type\":\"run\",\"input\":[{\"type\":\"video\",\"base64\":\"\"}]}}",
    )
    .unwrap_err();
    let bad_shape =
        parse_from("{\"id\":\"x\",\"op\":{\"type\":\"run\",\"input\":42}}").unwrap_err();
    insta::assert_snapshot!(
        "wire_input_decode_rejections",
        format!("{bad_part}\n{bad_shape}")
    );
}

#[test]
fn rewind_positive_int_adapter_snapshots() {
    let rewind = Op {
        id: RuntimeString::from("rw"),
        op: OpKind::Rewind {
            num_turns: 2,
            scope: None,
        },
    };
    insta::assert_snapshot!("rewind_op_encoded", roundtrip(&rewind));

    let zero =
        parse_from("{\"id\":\"x\",\"op\":{\"type\":\"rewind\",\"numTurns\":0}}").unwrap_err();
    let fractional =
        parse_from("{\"id\":\"x\",\"op\":{\"type\":\"rewind\",\"numTurns\":1.5}}").unwrap_err();
    let non_number =
        parse_from("{\"id\":\"x\",\"op\":{\"type\":\"rewind\",\"numTurns\":\"3\"}}").unwrap_err();
    insta::assert_snapshot!(
        "rewind_num_turns_rejections",
        format!("{zero}\n{fractional}\n{non_number}")
    );
}
