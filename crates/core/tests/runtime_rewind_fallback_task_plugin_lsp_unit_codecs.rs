use klaus_agent_core::engine::rewind::RewindScope;
use klaus_agent_core::extensions::plugins::loader::PluginLoadIssueKind;
use klaus_agent_core::lsp::types::LspSeverity;
use klaus_agent_core::provider::model_fallback::ModelFallbackKind;
use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeCodecErrorKind, RuntimeJson, RuntimeObject, RuntimeString,
};
use klaus_agent_core::tasks::task_store::TaskStatus;

macro_rules! assert_unit_enum_contract {
    ($type:ty; $( $variant:path => $tag:literal ),+ $(,)?) => {{
        let cases: &[($type, &str)] = &[$(($variant, $tag)),+];
        for (value, tag) in cases {
            let expected = RuntimeString::from(format!("\"{tag}\""));
            let runtime_text = runtime_codec::encode_text(value).unwrap();
            assert_eq!(runtime_text, expected);
            assert_eq!(
                runtime_codec::decode_text::<$type>(&runtime_text).unwrap(),
                value.clone(),
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
fn rewind_fallback_task_plugin_lsp_unit_runtime_codecs_cover_the_complete_families() {
    assert_unit_enum_contract!(RewindScope;
        RewindScope::Conversation => "conversation",
        RewindScope::Files => "files",
        RewindScope::Both => "both",
    );
    assert_unit_enum_contract!(ModelFallbackKind;
        ModelFallbackKind::ModelNotFound => "model_not_found",
        ModelFallbackKind::PermissionDenied => "permission_denied",
        ModelFallbackKind::Overloaded => "overloaded",
    );
    assert_unit_enum_contract!(TaskStatus;
        TaskStatus::Pending => "pending",
        TaskStatus::InProgress => "in_progress",
        TaskStatus::Completed => "completed",
    );
    assert_unit_enum_contract!(PluginLoadIssueKind;
        PluginLoadIssueKind::InvalidJson => "invalid_json",
        PluginLoadIssueKind::InvalidManifest => "invalid_manifest",
        PluginLoadIssueKind::UnsupportedRuntimeCapability => "unsupported_runtime_capability",
        PluginLoadIssueKind::DuplicateMcpServerSuppressed => "duplicate_mcp_server_suppressed",
    );
    assert_unit_enum_contract!(LspSeverity;
        LspSeverity::Error => "error",
        LspSeverity::Warning => "warning",
        LspSeverity::Information => "information",
        LspSeverity::Hint => "hint",
    );
}
