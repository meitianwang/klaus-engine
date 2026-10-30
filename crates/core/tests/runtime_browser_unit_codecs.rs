use klaus_agent_core::browser::{
    BrowserArtifactKind, BrowserClipboardAction, BrowserCloseTarget, BrowserComputerAction,
    BrowserConsoleLevel, BrowserDialogAction, BrowserMouseButton, BrowserReadPageFilter,
    BrowserRuntimeErrorCode, BrowserScreenshotFormat, BrowserScrollDirection, BrowserTabMarkStatus,
    BrowserTabsAction, BrowserWaitUntil,
};
use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeCodecErrorKind, RuntimeJson, RuntimeObject, RuntimeString,
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

        let unknown = runtime_codec::decode_text::<$type>(&RuntimeString::from("\"unknown\""))
            .unwrap_err();
        assert_eq!(unknown.pointer(), RuntimeString::default());
        assert_eq!(unknown.kind(), &RuntimeCodecErrorKind::UnknownEnumVariant);

        for wrong_shape in [RuntimeJson::Null, RuntimeJson::Bool(true)] {
            let error = runtime_codec::decode_value::<$type>(&wrong_shape).unwrap_err();
            assert_eq!(error.pointer(), RuntimeString::default());
            assert_eq!(error.kind(), &RuntimeCodecErrorKind::InvalidEnumShape);
        }

        let first_tag = cases[0].1;
        let mut payload = RuntimeObject::new();
        payload.insert(first_tag, RuntimeJson::Null);
        let payload = runtime_codec::decode_value::<$type>(&RuntimeJson::Object(payload))
            .unwrap_err();
        assert_eq!(payload.pointer(), RuntimeString::from(format!("/{first_tag}")));
        assert_eq!(payload.kind(), &RuntimeCodecErrorKind::InvalidEnumShape);

        let mut unknown_payload = RuntimeObject::new();
        unknown_payload.insert("unknown", RuntimeJson::Null);
        let unknown_payload =
            runtime_codec::decode_value::<$type>(&RuntimeJson::Object(unknown_payload))
                .unwrap_err();
        assert_eq!(unknown_payload.pointer(), RuntimeString::from("/unknown"));
        assert_eq!(
            unknown_payload.kind(),
            &RuntimeCodecErrorKind::UnknownEnumVariant,
        );

        let surrogate = RuntimeString::from_utf16([b'"' as u16, 0xD800, b'"' as u16]);
        let surrogate = runtime_codec::decode_text::<$type>(&surrogate).unwrap_err();
        assert_eq!(surrogate.pointer(), RuntimeString::default());
        assert_eq!(surrogate.kind(), &RuntimeCodecErrorKind::UnknownEnumVariant);
    }};
}

#[test]
fn browser_unit_runtime_codecs_match_native_contract_and_fail_closed() {
    assert_unit_enum_contract!(BrowserWaitUntil;
        BrowserWaitUntil::Commit => "commit",
        BrowserWaitUntil::DomContentLoaded => "dom_content_loaded",
        BrowserWaitUntil::Load => "load",
        BrowserWaitUntil::NetworkIdle => "network_idle",
    );
    assert_unit_enum_contract!(BrowserScreenshotFormat;
        BrowserScreenshotFormat::Png => "png",
        BrowserScreenshotFormat::Jpeg => "jpeg",
    );
    assert_unit_enum_contract!(BrowserMouseButton;
        BrowserMouseButton::Left => "left",
        BrowserMouseButton::Middle => "middle",
        BrowserMouseButton::Right => "right",
    );
    assert_unit_enum_contract!(BrowserCloseTarget;
        BrowserCloseTarget::Tab => "tab",
        BrowserCloseTarget::Session => "session",
        BrowserCloseTarget::All => "all",
    );
    assert_unit_enum_contract!(BrowserReadPageFilter;
        BrowserReadPageFilter::Interactive => "interactive",
        BrowserReadPageFilter::All => "all",
    );
    assert_unit_enum_contract!(BrowserComputerAction;
        BrowserComputerAction::LeftClick => "left_click",
        BrowserComputerAction::RightClick => "right_click",
        BrowserComputerAction::DoubleClick => "double_click",
        BrowserComputerAction::TripleClick => "triple_click",
        BrowserComputerAction::Type => "type",
        BrowserComputerAction::Screenshot => "screenshot",
        BrowserComputerAction::Wait => "wait",
        BrowserComputerAction::Scroll => "scroll",
        BrowserComputerAction::Key => "key",
        BrowserComputerAction::LeftClickDrag => "left_click_drag",
        BrowserComputerAction::Zoom => "zoom",
        BrowserComputerAction::ScrollTo => "scroll_to",
        BrowserComputerAction::Hover => "hover",
    );
    assert_unit_enum_contract!(BrowserScrollDirection;
        BrowserScrollDirection::Up => "up",
        BrowserScrollDirection::Down => "down",
        BrowserScrollDirection::Left => "left",
        BrowserScrollDirection::Right => "right",
    );
    assert_unit_enum_contract!(BrowserTabsAction;
        BrowserTabsAction::List => "list",
        BrowserTabsAction::User => "user",
        BrowserTabsAction::New => "new",
        BrowserTabsAction::Get => "get",
        BrowserTabsAction::Selected => "selected",
        BrowserTabsAction::Close => "close",
        BrowserTabsAction::Finalize => "finalize",
        BrowserTabsAction::NameSession => "name_session",
    );
    assert_unit_enum_contract!(BrowserClipboardAction;
        BrowserClipboardAction::Read => "read",
        BrowserClipboardAction::ReadText => "read_text",
        BrowserClipboardAction::Write => "write",
        BrowserClipboardAction::WriteText => "write_text",
    );
    assert_unit_enum_contract!(BrowserDialogAction;
        BrowserDialogAction::Get => "get",
        BrowserDialogAction::Accept => "accept",
        BrowserDialogAction::Dismiss => "dismiss",
    );
    assert_unit_enum_contract!(BrowserTabMarkStatus;
        BrowserTabMarkStatus::Deliverable => "deliverable",
        BrowserTabMarkStatus::Handoff => "handoff",
    );
    assert_unit_enum_contract!(BrowserArtifactKind;
        BrowserArtifactKind::Screenshot => "screenshot",
        BrowserArtifactKind::Gif => "gif",
        BrowserArtifactKind::DomSnapshot => "domsnapshot",
        BrowserArtifactKind::Har => "har",
        BrowserArtifactKind::Download => "download",
        BrowserArtifactKind::Trace => "trace",
        BrowserArtifactKind::Video => "video",
        BrowserArtifactKind::ConsoleLog => "consolelog",
        BrowserArtifactKind::NetworkLog => "networklog",
        BrowserArtifactKind::Other => "other",
    );
    assert_unit_enum_contract!(BrowserConsoleLevel;
        BrowserConsoleLevel::Debug => "debug",
        BrowserConsoleLevel::Info => "info",
        BrowserConsoleLevel::Warning => "warning",
        BrowserConsoleLevel::Error => "error",
        BrowserConsoleLevel::Log => "log",
    );
    assert_unit_enum_contract!(BrowserRuntimeErrorCode;
        BrowserRuntimeErrorCode::Unavailable => "unavailable",
        BrowserRuntimeErrorCode::PermissionDenied => "permission_denied",
        BrowserRuntimeErrorCode::Timeout => "timeout",
        BrowserRuntimeErrorCode::Cancelled => "cancelled",
        BrowserRuntimeErrorCode::NotFound => "not_found",
        BrowserRuntimeErrorCode::InvalidRequest => "invalid_request",
        BrowserRuntimeErrorCode::NavigationFailed => "navigation_failed",
        BrowserRuntimeErrorCode::EvaluationFailed => "evaluation_failed",
        BrowserRuntimeErrorCode::HostError => "host_error",
    );
}
