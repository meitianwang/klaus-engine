//! Transport regressions for frame segmentation and typed failure details.

use super::{bridge, credential};
use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine};
use serde_json::{json, Value};

fn frame(value: &Value, terminal: bool) -> Vec<u8> {
    let mut bytes = bridge::encode_frame(value.clone()).expect("frame").to_vec();
    bytes[0] = if terminal { 2 } else { 0 };
    bytes
}

#[test]
fn decoder_preserves_coalesced_frames_across_every_header_and_body_split() {
    let expected = (0..8)
        .map(|index| (index == 7, json!({"i":index})))
        .collect::<Vec<_>>();
    let bytes = expected
        .iter()
        .flat_map(|(terminal, value)| frame(value, *terminal))
        .collect::<Vec<_>>();
    assert!(bytes.len() > 16 * 2);
    for split in 0..=bytes.len() {
        let mut decoder = bridge::Decoder::with_max_frame(16).expect("decoder");
        let mut actual = decoder.push(&bytes[..split]).expect("prefix");
        actual.extend(decoder.push(&bytes[split..]).expect("suffix"));
        assert_eq!(actual, expected, "split {split}");
        assert!(decoder.is_empty());
    }
    let mut decoder = bridge::Decoder::with_max_frame(16).expect("decoder");
    let actual = bytes
        .iter()
        .flat_map(|byte| decoder.push(&[*byte]).expect("single byte"))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    assert!(decoder.is_empty());
}

#[test]
fn decoder_retains_only_the_partial_tail_after_many_complete_frames() {
    let first = frame(&json!({"i":1}), false);
    let last = frame(&json!({"i":2}), true);
    for split in 1..last.len() {
        let mut chunk = first.repeat(12);
        chunk.extend_from_slice(&last[..split]);
        let mut decoder = bridge::Decoder::with_max_frame(16).expect("decoder");
        assert_eq!(decoder.push(&chunk).expect("complete frames").len(), 12);
        assert!(!decoder.is_empty());
        assert_eq!(
            decoder.push(&last[split..]).expect("tail"),
            vec![(true, json!({"i":2}))]
        );
        assert!(decoder.is_empty());
    }
}

#[test]
fn decoder_rejects_invalid_or_oversized_headers_before_receiving_the_body() {
    for (bytes, expected) in [
        ([0, 0, 0, 0, 17], "cursor_frame_too_large"),
        ([1, 0, 0, 0, 0], "cursor_frame_encoding_unsupported"),
        ([4, 0, 0, 0, 0], "cursor_frame_encoding_unsupported"),
    ] {
        for split in 0..5 {
            let mut decoder = bridge::Decoder::with_max_frame(16).expect("decoder");
            assert!(decoder
                .push(&bytes[..split])
                .expect("partial header")
                .is_empty());
            assert_eq!(decoder.push(&bytes[split..]).unwrap_err(), expected);
        }
    }
    let mut decoder = bridge::Decoder::default();
    assert_eq!(
        decoder.push(&[0, 0, 0, 0, 1, b'{']).unwrap_err(),
        "cursor_frame_invalid"
    );
}

#[test]
fn decoder_frame_budget_is_cumulative_and_independent_of_network_chunking() {
    let one = frame(&json!({}), false);
    let mut decoder = bridge::Decoder::with_max_frame(16).expect("decoder");
    for _ in 0..32_769 {
        assert_eq!(decoder.push(&one).expect("allowed frame").len(), 1);
    }
    assert_eq!(decoder.push(&one).unwrap_err(), "cursor_response_too_large");
    let mut decoder = bridge::Decoder::with_max_frame(16).expect("decoder");
    assert_eq!(
        decoder.push(&one.repeat(32_770)).unwrap_err(),
        "cursor_response_too_large"
    );
}

#[test]
fn decoder_byte_budget_applies_across_successful_pushes() {
    let bytes = frame(&json!("x".repeat(8 * 1024 * 1024 - 7)), false);
    assert_eq!(bytes.len(), 8 * 1024 * 1024);
    let mut decoder = bridge::Decoder::default();
    for _ in 0..4 {
        assert_eq!(decoder.push(&bytes).expect("allowed bytes").len(), 1);
        assert!(decoder.is_empty());
    }
    assert_eq!(decoder.push(&[0]).unwrap_err(), "cursor_response_too_large");
}

#[test]
fn decoder_frame_limit_and_stream_limit_allow_header_and_terminal_overhead() {
    let value = json!("x".repeat(14));
    let mut bytes = frame(&value, false);
    assert_eq!(bytes.len(), 16 + 5);
    bytes.extend(frame(&json!({}), true));
    for split in 0..=bytes.len() {
        let mut decoder = bridge::Decoder::with_limits(16, bytes.len()).expect("limits");
        let mut actual = decoder.push(&bytes[..split]).expect("prefix");
        actual.extend(decoder.push(&bytes[split..]).expect("suffix"));
        assert_eq!(actual, vec![(false, value.clone()), (true, json!({}))]);
        assert!(decoder.is_empty());
        assert_eq!(decoder.push(&[0]).unwrap_err(), "cursor_response_too_large");
    }
    let mut decoder = bridge::Decoder::with_limits(15, bytes.len()).expect("limits");
    assert_eq!(decoder.push(&bytes).unwrap_err(), "cursor_frame_too_large");
    assert!(bridge::Decoder::with_limits(16, 0).is_err());
    assert!(bridge::Decoder::with_max_response(super::step_history::MAX_HISTORY * 2 + 1).is_err());
}

#[test]
fn outbound_budget_charges_initial_request_and_every_encoded_blob_reply() {
    let initial = json!({"runRequest":{}});
    let reply = json!({"kvClientMessage":{"id":1,"getBlobResult":{
        "blobData":STANDARD_NO_PAD.encode([0_u8; 96])
    }}});
    let initial_bytes = frame(&initial, false);
    let reply_bytes = frame(&reply, false);
    assert!(reply_bytes.len() > 96);
    let mut encoder = bridge::Encoder::with_max_bytes(initial_bytes.len() + reply_bytes.len() * 2)
        .expect("budget");
    assert_eq!(
        encoder.frame(initial).expect("initial").as_ref(),
        initial_bytes.as_slice()
    );
    for _ in 0..2 {
        assert_eq!(
            encoder.frame(reply.clone()).expect("reply").as_ref(),
            reply_bytes.as_slice()
        );
    }
    assert_eq!(
        encoder.frame(reply).unwrap_err(),
        "cursor_request_too_large"
    );
}

#[test]
fn outbound_budget_includes_frame_headers_and_rejects_unbounded_limits() {
    let mut encoder = bridge::Encoder::with_max_bytes(6).expect("budget");
    assert_eq!(
        encoder.frame(json!({})).unwrap_err(),
        "cursor_request_too_large"
    );
    assert!(bridge::Encoder::with_max_bytes(0).is_err());
    assert!(bridge::Encoder::with_max_bytes(super::step_history::MAX_HISTORY * 2 + 1).is_err());
}

fn binary_error(code: u8) -> Value {
    json!({"code":"resource_exhausted","message":"private bearer and prompt", "details":[{
        "type":"aiserver.v1.ErrorDetails", "value":STANDARD_NO_PAD.encode([8, code])
    }]})
}

#[test]
fn typed_binary_details_distinguish_limits_cancellation_and_account_conditions() {
    for (code, expected) in [
        (43, "cursor_context_length_exceeded"),
        (20, "cursor_token_limit_exceeded"),
        (21, "cursor_request_cancelled"),
        (35, "cursor_request_cancelled"),
        (58, "cursor_model_blocked"),
        (60, "cursor_max_mode_required"),
        (23, "cursor_subscription_required"),
        (44, "cursor_usage_pricing_required"),
        (9, "cursor_usage_limit"),
        (8, "cursor_rate_limit"),
        (63, "cursor_overloaded"),
        (12, credential::NEEDS_REIMPORT),
    ] {
        let error = binary_error(code);
        assert_eq!(credential::upstream_error(&error), expected);
        assert_eq!(
            credential::upstream_error(&json!({"error":error})),
            expected
        );
        assert_eq!(
            credential::http_error(429, Some(&binary_error(code))),
            expected
        );
    }
    assert_eq!(
        credential::http_error(401, Some(&binary_error(43))),
        credential::NEEDS_REIMPORT
    );
}

#[test]
fn typed_json_any_and_binary_any_preserve_the_same_failure_category() {
    for code in [json!(43), json!("ERROR_CONVERSATION_TOO_LONG")] {
        let error = json!({"code":8,"details":[{
            "@type":"type.googleapis.com/aiserver.v1.ErrorDetails","error":code
        }]});
        assert_eq!(
            credential::upstream_error(&error),
            "cursor_context_length_exceeded"
        );
    }
    let error = json!({"code":8,"details":[{
        "typeUrl":"type.googleapis.com/aiserver.v1.ErrorDetails", "value":STANDARD_NO_PAD.encode([8,43])
    }]});
    assert_eq!(
        credential::upstream_error(&error),
        "cursor_context_length_exceeded"
    );
    let mut error = binary_error(8);
    error["details"][0]["debug"] = json!({"error":"ERROR_CONVERSATION_TOO_LONG"});
    assert_eq!(credential::upstream_error(&error), "cursor_rate_limit");
}

#[test]
fn malformed_unknown_or_oversized_details_cannot_invent_a_typed_failure() {
    let malformed = [
        vec![],
        vec![8],
        vec![8, 43, 18, 9, 0],
        vec![8, 43, 8, 20],
        vec![10, 1, 43],
        vec![8, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2],
        vec![0, 43],
        vec![8, 43, 19],
        vec![8, 43, 17, 0],
        vec![8, 43, 29, 0],
        vec![0; 16 * 1024 + 1],
    ];
    for bytes in malformed {
        let mut error = binary_error(43);
        error["details"][0]["value"] = json!(STANDARD_NO_PAD.encode(bytes));
        error["details"][0]["debug"] = json!({"error":"ERROR_CONVERSATION_TOO_LONG"});
        assert_eq!(credential::upstream_error(&error), "cursor_rate_limit");
    }
    for detail in [
        json!({"type":"other.ErrorDetails","value":STANDARD_NO_PAD.encode([8,43])}),
        json!({"type":"aiserver.v1.ErrorDetails","value":"%%%","debug":{"error":43}}),
        json!({"type":"aiserver.v1.ErrorDetails","debug":{"error":43}}),
        json!({"@type":"other/ErrorDetails","error":43}),
        json!({"@type":"type.googleapis.com/aiserver.v1.ErrorDetails","error":true}),
        json!({"error":43}),
    ] {
        assert_eq!(
            credential::upstream_error(&json!({"code":"resource_exhausted","details":[detail]})),
            "cursor_rate_limit"
        );
    }
    let mut error = binary_error(43);
    error["details"] = json!(vec![error["details"][0].clone(); 17]);
    assert_eq!(credential::upstream_error(&error), "cursor_rate_limit");
    let mut error = binary_error(43);
    error["details"]
        .as_array_mut()
        .unwrap()
        .push(binary_error(20)["details"][0].clone());
    assert_eq!(credential::upstream_error(&error), "cursor_rate_limit");
}

#[test]
fn binary_detail_extensions_are_skipped_without_reading_private_content() {
    let bytes = [8, 43, 34, 6, b's', b'e', b'c', b'r', b'e', b't', 24, 1];
    let error = json!({"code":8,"details":[{
        "type":"aiserver.v1.ErrorDetails","value":STANDARD_NO_PAD.encode(bytes)
    }]});
    assert_eq!(
        credential::upstream_error(&error),
        "cursor_context_length_exceeded"
    );
    assert_eq!(
        credential::upstream_error(&json!({"code":1})),
        "cursor_request_cancelled"
    );
    assert_eq!(
        credential::upstream_error(&json!({"code":"canceled"})),
        "cursor_request_cancelled"
    );
}
