use klaus_agent_core::browser::{BrowserPermissionSet, BrowserViewport};
use klaus_agent_core::command_runtime::interactive::TerminalOptions;
use klaus_agent_core::command_runtime::process::TerminalSize;
use klaus_agent_core::context::memory_files::MemoryGates;
use klaus_agent_core::fs::pdf::PdfPageRange;
use klaus_agent_core::lsp::types::LspPosition;
use klaus_agent_core::mcp::types::McpToolAnnotations;
use klaus_agent_core::memory::sync::MemorySyncApplyResult;
use klaus_agent_core::observability::cost_tracker::TotalUsage;
use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode, RuntimeJson, RuntimeNumber,
    RuntimeObject, RuntimeString,
};
use klaus_agent_core::schemas::message::Usage;
use klaus_agent_core::schemas::rollout::RollbackItem;
use klaus_agent_core::worktree::worktree::ChangeSummary;

fn assert_decode_error<T: RuntimeDecode + std::fmt::Debug>(
    value: &RuntimeJson,
    kind: RuntimeCodecErrorKind,
    pointer: &str,
) {
    let error = runtime_codec::decode_value::<T>(value).unwrap_err();
    assert_eq!(error.kind(), &kind);
    assert_eq!(error.pointer(), RuntimeString::from(pointer));
}

fn assert_wire_round_trip<T>(value: &T, expected: &str)
where
    T: RuntimeEncode + RuntimeDecode + PartialEq + std::fmt::Debug,
{
    assert_eq!(
        runtime_codec::encode_text(value).unwrap(),
        RuntimeString::from(expected),
    );
    assert_eq!(
        runtime_codec::decode_text::<T>(&RuntimeString::from(expected)).unwrap(),
        *value,
    );
}

#[test]
fn terminal_options_runtime_codec_matches_required_u16_contract() {
    let value = TerminalOptions {
        cols: 120,
        rows: 40,
    };
    assert_wire_round_trip(&value, r#"{"cols":120,"rows":40}"#);

    let encoded = runtime_codec::encode_value(&value).unwrap();
    let RuntimeJson::Object(ref encoded) = encoded else {
        panic!("TerminalOptions must encode as an object");
    };
    for field in ["cols", "rows"] {
        let mut missing = (*encoded).clone();
        missing.remove(field);
        assert_decode_error::<TerminalOptions>(
            &RuntimeJson::Object(missing),
            RuntimeCodecErrorKind::MissingField,
            &format!("/{field}"),
        );
    }

    let mut overflow = (*encoded).clone();
    overflow.insert("cols", RuntimeJson::from(65_536_u64));
    assert_decode_error::<TerminalOptions>(
        &RuntimeJson::Object(overflow),
        RuntimeCodecErrorKind::IntegerOutOfRange,
        "/cols",
    );

    let mut unknown = (*encoded).clone();
    unknown.insert(RuntimeString::from_utf16([0xd800]), RuntimeJson::Null);
    assert_eq!(
        runtime_codec::decode_value::<TerminalOptions>(&RuntimeJson::Object(unknown)).unwrap(),
        value,
    );
}

#[test]
fn lsp_position_runtime_codec_matches_required_u32_contract() {
    let value = LspPosition {
        line: 12,
        character: 34,
    };
    assert_wire_round_trip(&value, r#"{"line":12,"character":34}"#);

    let mut missing = RuntimeObject::new();
    missing.insert("line", RuntimeJson::from(12));
    assert_decode_error::<LspPosition>(
        &RuntimeJson::Object(missing),
        RuntimeCodecErrorKind::MissingField,
        "/character",
    );

    let mut fractional = RuntimeObject::new();
    fractional.insert("line", RuntimeJson::Number(RuntimeNumber::from_f64(1.5)));
    fractional.insert("character", RuntimeJson::from(34));
    assert_decode_error::<LspPosition>(
        &RuntimeJson::Object(fractional),
        RuntimeCodecErrorKind::NonIntegralInteger,
        "/line",
    );
}

#[test]
fn total_usage_runtime_codec_matches_four_required_u64_fields() {
    let value = TotalUsage {
        input_tokens: 1,
        output_tokens: 2,
        cache_read_input_tokens: 3,
        cache_creation_input_tokens: 4,
    };
    assert_wire_round_trip(
        &value,
        r#"{"inputTokens":1,"outputTokens":2,"cacheReadInputTokens":3,"cacheCreationInputTokens":4}"#,
    );

    let encoded = runtime_codec::encode_value(&value).unwrap();
    let RuntimeJson::Object(ref encoded) = encoded else {
        panic!("TotalUsage must encode as an object");
    };
    for field in [
        "inputTokens",
        "outputTokens",
        "cacheReadInputTokens",
        "cacheCreationInputTokens",
    ] {
        let mut missing = (*encoded).clone();
        missing.remove(field);
        assert_decode_error::<TotalUsage>(
            &RuntimeJson::Object(missing),
            RuntimeCodecErrorKind::MissingField,
            &format!("/{field}"),
        );
    }

    let mut negative = (*encoded).clone();
    negative.insert("inputTokens", RuntimeJson::from(-1));
    assert_decode_error::<TotalUsage>(
        &RuntimeJson::Object(negative),
        RuntimeCodecErrorKind::IntegerOutOfRange,
        "/inputTokens",
    );
}

#[test]
fn pdf_page_range_runtime_codec_matches_explicit_null_option_contract() {
    let open = PdfPageRange {
        first_page: 3,
        last_page: None,
    };
    assert_wire_round_trip(&open, r#"{"firstPage":3,"lastPage":null}"#);

    let closed = PdfPageRange {
        first_page: 3,
        last_page: Some(9),
    };
    assert_wire_round_trip(&closed, r#"{"firstPage":3,"lastPage":9}"#);

    let mut missing_optional = RuntimeObject::new();
    missing_optional.insert("firstPage", RuntimeJson::from(3));
    missing_optional.insert(RuntimeString::from_utf16([0xd800]), RuntimeJson::Null);
    assert_eq!(
        runtime_codec::decode_value::<PdfPageRange>(&RuntimeJson::Object(missing_optional))
            .unwrap(),
        open,
    );

    let mut missing_required = RuntimeObject::new();
    missing_required.insert("lastPage", RuntimeJson::Null);
    assert_decode_error::<PdfPageRange>(
        &RuntimeJson::Object(missing_required),
        RuntimeCodecErrorKind::MissingField,
        "/firstPage",
    );

    let mut wrong_optional = RuntimeObject::new();
    wrong_optional.insert("firstPage", RuntimeJson::from(3));
    wrong_optional.insert("lastPage", RuntimeJson::from("nine"));
    assert_decode_error::<PdfPageRange>(
        &RuntimeJson::Object(wrong_optional),
        RuntimeCodecErrorKind::ExpectedNumber,
        "/lastPage",
    );
}

#[test]
fn browser_permission_set_runtime_codec_matches_ten_required_bool_fields() {
    let value = BrowserPermissionSet {
        navigate: true,
        read_page: false,
        capture_screenshot: true,
        interact: false,
        javascript_eval: true,
        read_console: false,
        read_network: true,
        manage_tabs: false,
        session_state: true,
        file_transfer: false,
    };
    let expected = r#"{"navigate":true,"readPage":false,"captureScreenshot":true,"interact":false,"javascriptEval":true,"readConsole":false,"readNetwork":true,"manageTabs":false,"sessionState":true,"fileTransfer":false}"#;
    assert_wire_round_trip(&value, expected);

    let encoded_value = runtime_codec::encode_value(&value).unwrap();
    let RuntimeJson::Object(ref encoded) = encoded_value else {
        panic!("BrowserPermissionSet must encode as an object");
    };
    for field in [
        "navigate",
        "readPage",
        "captureScreenshot",
        "interact",
        "javascriptEval",
        "readConsole",
        "readNetwork",
        "manageTabs",
        "sessionState",
        "fileTransfer",
    ] {
        let mut missing = (*encoded).clone();
        missing.remove(field);
        assert_decode_error::<BrowserPermissionSet>(
            &RuntimeJson::Object(missing),
            RuntimeCodecErrorKind::MissingField,
            &format!("/{field}"),
        );
    }
}

#[test]
fn browser_viewport_runtime_codec_omits_none_and_accepts_missing_or_null() {
    let minimal = BrowserViewport {
        width: 1280,
        height: 720,
        device_scale_factor: None,
        is_mobile: None,
    };
    assert_wire_round_trip(&minimal, r#"{"width":1280,"height":720}"#);

    let complete = BrowserViewport {
        width: 390,
        height: 844,
        device_scale_factor: Some(3),
        is_mobile: Some(true),
    };
    assert_wire_round_trip(
        &complete,
        r#"{"width":390,"height":844,"deviceScaleFactor":3,"isMobile":true}"#,
    );

    let mut nullable = RuntimeObject::new();
    nullable.insert("width", RuntimeJson::from(1280));
    nullable.insert("height", RuntimeJson::from(720));
    nullable.insert("deviceScaleFactor", RuntimeJson::Null);
    nullable.insert("isMobile", RuntimeJson::Null);
    nullable.insert(RuntimeString::from_utf16([0xd800]), RuntimeJson::Null);
    assert_eq!(
        runtime_codec::decode_value::<BrowserViewport>(&RuntimeJson::Object(nullable)).unwrap(),
        minimal,
    );
}

#[test]
fn memory_gates_runtime_codec_omits_none_and_preserves_declaration_order() {
    let empty = MemoryGates::default();
    assert_wire_round_trip(&empty, "{}");

    let value = MemoryGates {
        enabled: Some(true),
        user: None,
        project: Some(false),
        local: Some(true),
        allow_external_includes: Some(false),
    };
    assert_wire_round_trip(
        &value,
        r#"{"enabled":true,"project":false,"local":true,"allowExternalIncludes":false}"#,
    );

    let mut nullable = RuntimeObject::new();
    nullable.insert("enabled", RuntimeJson::Null);
    nullable.insert("allowExternalIncludes", RuntimeJson::Null);
    assert_eq!(
        runtime_codec::decode_value::<MemoryGates>(&RuntimeJson::Object(nullable)).unwrap(),
        empty,
    );
}

#[test]
fn mcp_tool_annotations_runtime_codec_omits_none_and_accepts_missing_or_null() {
    let empty = McpToolAnnotations::default();
    assert_wire_round_trip(&empty, "{}");

    let value = McpToolAnnotations {
        read_only_hint: Some(true),
        idempotent_hint: Some(false),
        destructive_hint: None,
        open_world_hint: Some(true),
    };
    assert_wire_round_trip(
        &value,
        r#"{"readOnlyHint":true,"idempotentHint":false,"openWorldHint":true}"#,
    );

    let mut nullable = RuntimeObject::new();
    nullable.insert("readOnlyHint", RuntimeJson::Null);
    nullable.insert("destructiveHint", RuntimeJson::Null);
    assert_eq!(
        runtime_codec::decode_value::<McpToolAnnotations>(&RuntimeJson::Object(nullable)).unwrap(),
        empty,
    );
}

#[test]
fn usage_runtime_codec_matches_required_default_zero_and_omitted_total_contract() {
    let minimal = Usage {
        input_tokens: 11,
        output_tokens: 7,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: None,
    };
    assert_wire_round_trip(
        &minimal,
        r#"{"inputTokens":11,"outputTokens":7,"cacheReadInputTokens":0,"cacheCreationInputTokens":0}"#,
    );

    let complete = Usage {
        input_tokens: 11,
        output_tokens: 7,
        cache_read_input_tokens: 5,
        cache_creation_input_tokens: 3,
        total_tokens: Some(26),
    };
    assert_wire_round_trip(
        &complete,
        r#"{"inputTokens":11,"outputTokens":7,"cacheReadInputTokens":5,"cacheCreationInputTokens":3,"totalTokens":26}"#,
    );

    let mut defaults = RuntimeObject::new();
    defaults.insert("inputTokens", RuntimeJson::from(11));
    defaults.insert("outputTokens", RuntimeJson::from(7));
    defaults.insert("totalTokens", RuntimeJson::Null);
    defaults.insert(RuntimeString::from_utf16([0xd800]), RuntimeJson::Null);
    assert_eq!(
        runtime_codec::decode_value::<Usage>(&RuntimeJson::Object(defaults)).unwrap(),
        minimal,
    );

    let mut null_default = RuntimeObject::new();
    null_default.insert("inputTokens", RuntimeJson::from(11));
    null_default.insert("outputTokens", RuntimeJson::from(7));
    null_default.insert("cacheReadInputTokens", RuntimeJson::Null);
    assert_decode_error::<Usage>(
        &RuntimeJson::Object(null_default),
        RuntimeCodecErrorKind::ExpectedNumber,
        "/cacheReadInputTokens",
    );
}

#[test]
fn terminal_size_runtime_codec_matches_declaration_order_and_required_u16_fields() {
    let value = TerminalSize { rows: 24, cols: 80 };
    assert_wire_round_trip(&value, r#"{"rows":24,"cols":80}"#);

    let mut overflow = RuntimeObject::new();
    overflow.insert("rows", RuntimeJson::from(65_536_u64));
    overflow.insert("cols", RuntimeJson::from(80));
    assert_decode_error::<TerminalSize>(
        &RuntimeJson::Object(overflow),
        RuntimeCodecErrorKind::IntegerOutOfRange,
        "/rows",
    );
}

#[test]
fn memory_sync_apply_result_runtime_codec_matches_required_usize_contract() {
    let value = MemorySyncApplyResult {
        applied_operations: 17,
    };
    assert_wire_round_trip(&value, r#"{"appliedOperations":17}"#);

    assert_decode_error::<MemorySyncApplyResult>(
        &RuntimeJson::Object(RuntimeObject::new()),
        RuntimeCodecErrorKind::MissingField,
        "/appliedOperations",
    );
}

#[test]
fn rollback_item_runtime_codec_matches_required_u32_contract() {
    let value = RollbackItem { num_turns: 3 };
    assert_wire_round_trip(&value, r#"{"numTurns":3}"#);

    let mut negative = RuntimeObject::new();
    negative.insert("numTurns", RuntimeJson::from(-1));
    assert_decode_error::<RollbackItem>(
        &RuntimeJson::Object(negative),
        RuntimeCodecErrorKind::IntegerOutOfRange,
        "/numTurns",
    );
}

#[test]
fn change_summary_runtime_codec_matches_two_required_usize_fields() {
    let value = ChangeSummary {
        changed_files: 5,
        commits: 2,
    };
    assert_wire_round_trip(&value, r#"{"changedFiles":5,"commits":2}"#);

    let mut unknown = RuntimeObject::new();
    unknown.insert("changedFiles", RuntimeJson::from(5));
    unknown.insert("commits", RuntimeJson::from(2));
    unknown.insert(RuntimeString::from_utf16([0xd800]), RuntimeJson::Null);
    assert_eq!(
        runtime_codec::decode_value::<ChangeSummary>(&RuntimeJson::Object(unknown)).unwrap(),
        value,
    );
}

#[test]
fn core_plain_struct_runtime_codecs_cover_all_thirteen_roots() {
    terminal_options_runtime_codec_matches_required_u16_contract();
    lsp_position_runtime_codec_matches_required_u32_contract();
    total_usage_runtime_codec_matches_four_required_u64_fields();
    pdf_page_range_runtime_codec_matches_explicit_null_option_contract();
    browser_permission_set_runtime_codec_matches_ten_required_bool_fields();
    browser_viewport_runtime_codec_omits_none_and_accepts_missing_or_null();
    memory_gates_runtime_codec_omits_none_and_preserves_declaration_order();
    mcp_tool_annotations_runtime_codec_omits_none_and_accepts_missing_or_null();
    usage_runtime_codec_matches_required_default_zero_and_omitted_total_contract();
    terminal_size_runtime_codec_matches_declaration_order_and_required_u16_fields();
    memory_sync_apply_result_runtime_codec_matches_required_usize_contract();
    rollback_item_runtime_codec_matches_required_u32_contract();
    change_summary_runtime_codec_matches_two_required_usize_fields();
}
