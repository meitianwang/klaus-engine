//! Public media capability wire contracts, including per-model session initialization.

use klaus_agent_core::runtime_values::{runtime_codec, RuntimeCodecErrorKind, RuntimeString};
use klaus_agent_core::types::provider::ImageLimits;
use klaus_agent_protocol::types::{ModelCapabilitiesInit, ProtocolSessionInit};

#[test]
fn legacy_session_initialization_omits_new_media_fields() {
    let legacy = RuntimeString::from(
        r#"{"capabilities":{"vision":true},"capabilitiesByModel":[{"model":"summary","capabilities":{}}]}"#,
    );
    let init: ProtocolSessionInit = runtime_codec::decode_text(&legacy).unwrap();
    assert_eq!(init.capabilities.as_ref().unwrap().image_limits, None);
    assert_eq!(init.capabilities.as_ref().unwrap().max_media_blocks, None);
    let summary = &init.capabilities_by_model.as_ref().unwrap()[0].capabilities;
    assert_eq!(summary.image_limits, None);
    assert_eq!(summary.max_media_blocks, None);
    assert_eq!(runtime_codec::encode_text(&init).unwrap(), legacy);
}

#[test]
fn session_initialization_round_trips_independent_main_and_summary_media_limits() {
    let wire = RuntimeString::from(
        r#"{"capabilities":{"imageLimits":{"maxWidth":2000,"maxHeight":2000,"maxBase64Size":5242880},"maxMediaBlocks":600},"capabilitiesByModel":[{"model":"summary","capabilities":{"imageLimits":{"maxWidth":1200,"targetRawSize":512000},"maxMediaBlocks":100}}]}"#,
    );
    let init: ProtocolSessionInit = runtime_codec::decode_text(&wire).unwrap();
    let main: ImageLimits = init.capabilities.as_ref().unwrap().image_limits.unwrap();
    assert_eq!(main.max_width.unwrap().get(), 2_000);
    assert_eq!(main.max_base64_size.unwrap().get(), 5_242_880);
    assert_eq!(main.target_raw_size, None);
    let summary = &init.capabilities_by_model.as_ref().unwrap()[0].capabilities;
    assert_eq!(
        summary.image_limits.unwrap().max_width.unwrap().get(),
        1_200
    );
    assert_eq!(summary.image_limits.unwrap().max_height, None);
    assert_eq!(
        summary.image_limits.unwrap().target_raw_size.unwrap().get(),
        512_000
    );
    assert_eq!(summary.max_media_blocks.unwrap().get(), 100);
    assert_eq!(runtime_codec::encode_text(&init).unwrap(), wire);
}

#[test]
fn public_media_codec_rejects_non_positive_fractional_and_out_of_range_values() {
    for field in ["maxWidth", "maxHeight", "maxBase64Size", "targetRawSize"] {
        for value in ["0", "-1", "1.5", "4294967296", "\"2000\""] {
            let wire = format!(r#"{{"imageLimits":{{"{field}":{value}}}}}"#);
            let error =
                runtime_codec::decode_text::<ModelCapabilitiesInit>(&wire.into()).unwrap_err();
            assert_eq!(
                error.pointer(),
                RuntimeString::from(format!("/imageLimits/{field}"))
            );
        }
    }
    for value in ["0", "-1", "1.5", "4294967296", "\"100\""] {
        let wire = format!(r#"{{"maxMediaBlocks":{value}}}"#);
        let error = runtime_codec::decode_text::<ModelCapabilitiesInit>(&wire.into()).unwrap_err();
        assert_eq!(error.pointer(), RuntimeString::from("/maxMediaBlocks"));
    }
}

#[test]
fn public_media_codec_keeps_strict_unknown_field_rejection_at_every_capability_boundary() {
    for (wire, pointer) in [
        (
            r#"{"capabilities":{"imageLimits":{"maxWidht":2000}}}"#,
            "/capabilities/imageLimits/maxWidht",
        ),
        (
            r#"{"capabilities":{"maxMediaBlock":100}}"#,
            "/capabilities/maxMediaBlock",
        ),
        (
            r#"{"capabilitiesByModel":[{"model":"summary","capabilities":{"imageLimits":{"unknown":1}}}]}"#,
            "/capabilitiesByModel/0/capabilities/imageLimits/unknown",
        ),
    ] {
        let error = runtime_codec::decode_text::<ProtocolSessionInit>(&wire.into()).unwrap_err();
        assert_eq!(error.kind(), &RuntimeCodecErrorKind::UnknownField);
        assert_eq!(error.pointer(), RuntimeString::from(pointer));
    }
}
