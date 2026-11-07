use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeCodecErrorKind, RuntimeJson, RuntimeObject, RuntimeString,
};
use klaus_agent_core::types::events::{
    CompactionMode, FileChangeKind, FileChangeStatus, ItemStatus, NotificationLevel, ThreadItem,
    ToolLifecyclePhase, ToolLifecycleSourceKind,
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
fn event_unit_runtime_codecs_cover_the_complete_family() {
    assert_unit_enum_contract!(ItemStatus;
        ItemStatus::InProgress => "in_progress",
        ItemStatus::Completed => "completed",
        ItemStatus::Failed => "failed",
    );
    assert_unit_enum_contract!(FileChangeStatus;
        FileChangeStatus::Completed => "completed",
        FileChangeStatus::Failed => "failed",
    );
    assert_unit_enum_contract!(FileChangeKind;
        FileChangeKind::Create => "create",
        FileChangeKind::Modify => "modify",
        FileChangeKind::Delete => "delete",
    );
    assert_unit_enum_contract!(ToolLifecyclePhase;
        ToolLifecyclePhase::Requested => "requested",
        ToolLifecyclePhase::Approved => "approved",
        ToolLifecyclePhase::Started => "started",
        ToolLifecyclePhase::Progress => "progress",
        ToolLifecyclePhase::Completed => "completed",
        ToolLifecyclePhase::Failed => "failed",
        ToolLifecyclePhase::Cancelled => "cancelled",
    );
    assert_unit_enum_contract!(ToolLifecycleSourceKind;
        ToolLifecycleSourceKind::Builtin => "builtin",
        ToolLifecycleSourceKind::Mcp => "mcp",
        ToolLifecycleSourceKind::Workflow => "workflow",
        ToolLifecycleSourceKind::Subagent => "subagent",
        ToolLifecycleSourceKind::CodeMode => "code_mode",
        ToolLifecycleSourceKind::Shell => "shell",
        ToolLifecycleSourceKind::Browser => "browser",
        ToolLifecycleSourceKind::Host => "host",
    );
    assert_unit_enum_contract!(CompactionMode;
        CompactionMode::Micro => "micro",
        CompactionMode::Full => "full",
    );
    assert_unit_enum_contract!(NotificationLevel;
        NotificationLevel::Info => "info",
        NotificationLevel::Warn => "warn",
    );
}

#[test]
fn thread_item_codec_rejects_removed_todo_list_and_preserves_error() {
    let error = runtime_codec::decode_text::<ThreadItem>(&RuntimeString::from(
        r#"{"type":"todo_list","id":"obsolete","items":[]}"#,
    ))
    .unwrap_err();
    assert_eq!(error.kind(), &RuntimeCodecErrorKind::UnknownEnumVariant);

    let item = ThreadItem::Error {
        id: "visible-error".into(),
        message: "Failed to finish the task".into(),
    };
    let encoded = runtime_codec::encode_text(&item).unwrap();
    assert_eq!(
        encoded,
        RuntimeString::from(
            r#"{"type":"error","id":"visible-error","message":"Failed to finish the task"}"#,
        ),
    );
    assert_eq!(
        runtime_codec::decode_text::<ThreadItem>(&encoded).unwrap(),
        item
    );
}
