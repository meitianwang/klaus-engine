use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeCodecErrorKind, RuntimeJson, RuntimeObject, RuntimeString,
};
use klaus_agent_core::types::errors::{
    EngineErrorCode, InputValidationErrorCode, ProviderErrorKind, TerminalReason, ToolErrorKind,
};

macro_rules! assert_unit_enum_contract {
    ($type:ty; $( $variant:path => $tag:literal ),+ $(,)?) => {{
        let cases: &[($type, &str)] = &[$(($variant, $tag)),+];
        for (value, tag) in cases {
            let expected_text = RuntimeString::from(format!("\"{tag}\""));

            let runtime_text = runtime_codec::encode_text(value).unwrap();
            assert_eq!(runtime_text, expected_text);
            assert_eq!(
                runtime_codec::decode_text::<$type>(&runtime_text).unwrap(),
                *value,
            );
        }

        let unknown = runtime_codec::decode_text::<$type>(&RuntimeString::from(
            "\"__runtime_unknown_variant__\"",
        ))
        .unwrap_err();
        assert_eq!(unknown.kind(), &RuntimeCodecErrorKind::UnknownEnumVariant);
        assert_eq!(unknown.pointer(), RuntimeString::default());

        for wrong_shape in [RuntimeJson::Null, RuntimeJson::Bool(true)] {
            let error = runtime_codec::decode_value::<$type>(&wrong_shape).unwrap_err();
            assert_eq!(error.kind(), &RuntimeCodecErrorKind::InvalidEnumShape);
            assert_eq!(error.pointer(), RuntimeString::default());
        }

        let first_tag = cases[0].1;
        let mut payload = RuntimeObject::new();
        payload.insert(first_tag, RuntimeJson::Null);
        let error = runtime_codec::decode_value::<$type>(&RuntimeJson::Object(payload))
            .unwrap_err();
        assert_eq!(error.kind(), &RuntimeCodecErrorKind::InvalidEnumShape);
        assert_eq!(error.pointer(), RuntimeString::from(format!("/{first_tag}")));

        let surrogate = RuntimeString::from_utf16([b'"' as u16, 0xd800, b'"' as u16]);
        let error = runtime_codec::decode_text::<$type>(&surrogate).unwrap_err();
        assert_eq!(error.kind(), &RuntimeCodecErrorKind::UnknownEnumVariant);
        assert_eq!(error.pointer(), RuntimeString::default());
    }};
}

#[test]
fn error_unit_runtime_codecs_cover_the_complete_family() {
    assert_unit_enum_contract!(ProviderErrorKind;
        ProviderErrorKind::Auth => "auth",
        ProviderErrorKind::RateLimit => "rate_limit",
        ProviderErrorKind::Overloaded => "overloaded",
        ProviderErrorKind::PromptTooLong => "prompt_too_long",
        ProviderErrorKind::RequestTooLarge => "request_too_large",
        ProviderErrorKind::Quota => "quota",
        ProviderErrorKind::MaxOutput => "max_output",
        ProviderErrorKind::Withheld => "withheld",
        ProviderErrorKind::Network => "network",
        ProviderErrorKind::InvalidRequest => "invalid_request",
        ProviderErrorKind::Unknown => "unknown",
    );
    assert_unit_enum_contract!(ToolErrorKind;
        ToolErrorKind::Model => "model",
        ToolErrorKind::Fatal => "fatal",
    );
    assert_unit_enum_contract!(InputValidationErrorCode;
        InputValidationErrorCode::InvalidDocumentMime => "invalid_document_mime",
        InputValidationErrorCode::InvalidDocumentBase64 => "invalid_document_base64",
        InputValidationErrorCode::InvalidDocumentSignature => "invalid_document_signature",
        InputValidationErrorCode::InvalidDocumentFilename => "invalid_document_filename",
        InputValidationErrorCode::DocumentTooLarge => "document_too_large",
        InputValidationErrorCode::TooManyDocuments => "too_many_documents",
        InputValidationErrorCode::RequestTooLarge => "request_too_large",
        InputValidationErrorCode::UnsupportedInput => "unsupported_input",
    );
    assert_unit_enum_contract!(EngineErrorCode;
        EngineErrorCode::AuthFailed => "auth_failed",
        EngineErrorCode::PromptTooLong => "prompt_too_long",
        EngineErrorCode::BudgetExhausted => "budget_exhausted",
        EngineErrorCode::ProviderError => "provider_error",
        EngineErrorCode::Aborted => "aborted",
        EngineErrorCode::InvalidInput => "invalid_input",
        EngineErrorCode::Internal => "internal",
        EngineErrorCode::CompactionThrash => "compaction_thrash",
        EngineErrorCode::LoopDetected => "loop_detected",
    );
    assert_unit_enum_contract!(TerminalReason;
        TerminalReason::Completed => "completed",
        TerminalReason::Aborted => "aborted",
        TerminalReason::PromptTooLong => "prompt_too_long",
        TerminalReason::AuthFailed => "auth_failed",
        TerminalReason::TurnLimitExhausted => "turn_limit_exhausted",
        TerminalReason::SpendLimitExhausted => "spend_limit_exhausted",
        TerminalReason::Error => "error",
    );
    // Legacy rollouts recorded the pre-split shared tag; resume must still work.
    assert_eq!(
        runtime_codec::decode_text::<TerminalReason>(&RuntimeString::from("\"budget_exhausted\""))
            .unwrap(),
        TerminalReason::TurnLimitExhausted
    );
}
