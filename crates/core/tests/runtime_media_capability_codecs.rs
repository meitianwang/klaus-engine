//! Public runtime codecs preserve media declarations without eagerly resolving defaults.

use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeCodecErrorKind, RuntimeDecode, RuntimeString,
};
use klaus_agent_core::types::provider::{
    ImageLimits, ModelCapabilities, ModelRequest, ProviderCapabilities,
};

const LEGACY_PROVIDER: &str = r#"{"streaming":true,"tools":true,"thinking":false,"structuredOutput":false,"webSearch":false,"promptCacheControl":false,"contextWindow":200000,"maxOutputTokens":8192,"nativePdf":true,"vision":true}"#;
const LEGACY_REQUEST: &str =
    r#"{"model":"fixture","system":[],"messages":[],"tools":[],"maxOutputTokens":8192}"#;

#[test]
fn native_media_capabilities_and_content_round_trip_without_pdf_semantics() {
    use klaus_agent_core::schemas::content::ContentBlock;
    let legacy: ModelCapabilities = runtime_codec::decode_text(&"{}".into()).unwrap();
    assert!(legacy.media_input_mime_types.is_empty());
    assert_eq!(runtime_codec::encode_text(&legacy).unwrap(), "{}");
    let wire = r#"{"mediaInputMimeTypes":["audio/wav","audio/webm;codecs=opus","video/mp4"]}"#;
    let model: ModelCapabilities = runtime_codec::decode_text(&wire.into()).unwrap();
    assert_eq!(model.media_input_mime_types.len(), 3);
    assert!(!model.native_pdf);
    assert_eq!(runtime_codec::encode_text(&model).unwrap(), wire);
    let wire =
        r#"{"type":"media","mimeType":"video/mp4","base64":"AAAAHGZ0eXA=","filename":"clip.mp4"}"#;
    let block: ContentBlock = runtime_codec::decode_text(&wire.into()).unwrap();
    assert!(matches!(block, ContentBlock::Media(_)));
    assert_eq!(runtime_codec::encode_text(&block).unwrap(), wire);
}

#[test]
fn public_provider_and_request_codecs_read_legacy_wire_without_emitting_null_fields() {
    let provider: ProviderCapabilities =
        runtime_codec::decode_text(&LEGACY_PROVIDER.into()).unwrap();
    assert_eq!(provider.image_limits, None);
    assert_eq!(provider.max_media_blocks, None);
    assert_eq!(
        runtime_codec::encode_text(&provider).unwrap(),
        LEGACY_PROVIDER
    );
    assert_eq!(serde_json::to_string(&provider).unwrap(), LEGACY_PROVIDER);
    let request: ModelRequest = runtime_codec::decode_text(&LEGACY_REQUEST.into()).unwrap();
    assert_eq!(request.image_limits, None);
    assert_eq!(request.max_media_blocks, None);
    assert_eq!(
        runtime_codec::encode_text(&request).unwrap(),
        LEGACY_REQUEST
    );
}

#[test]
fn public_codecs_round_trip_all_four_image_fields_and_positive_media_count() {
    let image_wire =
        r#"{"maxWidth":2000,"maxHeight":1200,"maxBase64Size":10485760,"targetRawSize":512000}"#;
    let limits: ImageLimits = runtime_codec::decode_text(&image_wire.into()).unwrap();
    assert_eq!(runtime_codec::encode_text(&limits).unwrap(), image_wire);
    assert_eq!(serde_json::to_string(&limits).unwrap(), image_wire);
    let mut provider: ProviderCapabilities =
        runtime_codec::decode_text(&LEGACY_PROVIDER.into()).unwrap();
    provider.image_limits = Some(limits);
    provider.max_media_blocks = std::num::NonZeroU32::new(600);
    let encoded = runtime_codec::encode_text(&provider).unwrap();
    assert_eq!(
        runtime_codec::decode_text::<ProviderCapabilities>(&encoded).unwrap(),
        provider
    );
    assert_eq!(
        serde_json::to_string(&provider).unwrap(),
        encoded.try_to_utf8().unwrap()
    );
    let mut request: ModelRequest = runtime_codec::decode_text(&LEGACY_REQUEST.into()).unwrap();
    request.bind_media_capabilities(&provider);
    let encoded = runtime_codec::encode_text(&request).unwrap();
    assert_eq!(
        runtime_codec::decode_text::<ModelRequest>(&encoded).unwrap(),
        request
    );
    let model_wire = format!(r#"{{"imageLimits":{image_wire},"maxMediaBlocks":600}}"#);
    let model: ModelCapabilities = runtime_codec::decode_text(&model_wire.as_str().into()).unwrap();
    assert_eq!(model.image_limits, provider.image_limits);
    assert_eq!(model.max_media_blocks, provider.max_media_blocks);
    assert_eq!(
        runtime_codec::encode_text(&model).unwrap(),
        model_wire.as_str()
    );
}

fn assert_invalid<T: RuntimeDecode + std::fmt::Debug>(wire: &str, pointer: &str) {
    let error = runtime_codec::decode_text::<T>(&RuntimeString::from(wire)).unwrap_err();
    assert_eq!(error.pointer(), RuntimeString::from(pointer));
}

#[test]
fn public_provider_and_request_codecs_reject_invalid_media_metadata() {
    let provider_wire = LEGACY_PROVIDER.strip_suffix('}').unwrap();
    let request_wire = LEGACY_REQUEST.strip_suffix('}').unwrap();
    for invalid in ["0", "-1", "1.5", "4294967296"] {
        assert_invalid::<ProviderCapabilities>(
            &format!(r#"{provider_wire},"maxMediaBlocks":{invalid}}}"#),
            "/maxMediaBlocks",
        );
        assert_invalid::<ModelRequest>(
            &format!(r#"{request_wire},"imageLimits":{{"maxBase64Size":{invalid}}}}}"#),
            "/imageLimits/maxBase64Size",
        );
    }
    for (wire, kind) in [
        (
            format!(r#"{provider_wire},"imageLimits":{{"unknown":true}}}}"#),
            RuntimeCodecErrorKind::UnknownField,
        ),
        (
            format!(r#"{provider_wire},"future":true,"imageLimits":{{"maxWidth":0}}}}"#),
            RuntimeCodecErrorKind::IntegerOutOfRange,
        ),
    ] {
        let error =
            runtime_codec::decode_text::<ProviderCapabilities>(&wire.as_str().into()).unwrap_err();
        assert_eq!(error.kind(), &kind);
    }
}

#[test]
fn internal_runtime_objects_keep_unknown_field_compatibility_and_decode_declared_media() {
    let provider_wire = format!(
        r#"{},"future":true,"imageLimits":{{"maxWidth":1200}},"maxMediaBlocks":42}}"#,
        LEGACY_PROVIDER.strip_suffix('}').unwrap(),
    );
    let provider: ProviderCapabilities =
        runtime_codec::decode_text(&provider_wire.as_str().into()).unwrap();
    assert_eq!(
        provider.image_limits.unwrap().max_width.unwrap().get(),
        1_200
    );
    assert_eq!(provider.max_media_blocks.unwrap().get(), 42);
    assert_eq!(
        serde_json::from_str::<ProviderCapabilities>(&provider_wire).unwrap(),
        provider
    );
    assert!(!runtime_codec::encode_text(&provider)
        .unwrap()
        .contains("future"));

    let request_wire = format!(
        r#"{},"future":true,"imageLimits":{{"maxWidth":1200}},"maxMediaBlocks":42}}"#,
        LEGACY_REQUEST.strip_suffix('}').unwrap(),
    );
    let request: ModelRequest = runtime_codec::decode_text(&request_wire.as_str().into()).unwrap();
    assert_eq!(request.image_limits, provider.image_limits);
    assert_eq!(request.max_media_blocks, provider.max_media_blocks);
    assert!(!runtime_codec::encode_text(&request)
        .unwrap()
        .contains("future"));

    let model: ModelCapabilities = runtime_codec::decode_text(&RuntimeString::from(
        r#"{"future":true,"imageLimits":{"maxWidth":1200},"maxMediaBlocks":42}"#,
    ))
    .unwrap();
    assert_eq!(model.image_limits, provider.image_limits);
    assert_eq!(model.max_media_blocks, provider.max_media_blocks);
    assert!(!runtime_codec::encode_text(&model)
        .unwrap()
        .contains("future"));

    assert_invalid::<ModelRequest>(
        &format!(
            r#"{},"future":true,"maxMediaBlocks":0}}"#,
            LEGACY_REQUEST.strip_suffix('}').unwrap()
        ),
        "/maxMediaBlocks",
    );
}
