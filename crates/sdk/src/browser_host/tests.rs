use klaus_agent_core::runtime_values::{
    runtime_json, RuntimeJson, RuntimeObject, RuntimePath, RuntimeString,
};

use super::*;

fn s(value: &str) -> RuntimeString {
    RuntimeString::from(value)
}

fn path(value: &str) -> RuntimePath {
    RuntimePath::from(value)
}

fn utf8(value: &RuntimeString) -> String {
    value.try_to_utf8().expect("fixture is scalar Unicode")
}

fn invoke(parsed: BrowserUtilityParse) -> BrowserUtilityInvocation {
    match parsed {
        BrowserUtilityParse::Invoke(value) => value,
        BrowserUtilityParse::Immediate(output) => {
            panic!("expected invocation, got immediate output: {output:?}")
        }
    }
}

fn immediate(parsed: BrowserUtilityParse) -> BrowserCliOutput {
    match parsed {
        BrowserUtilityParse::Immediate(value) => value,
        BrowserUtilityParse::Invoke(invocation) => {
            panic!("expected immediate output, got invocation: {invocation:?}")
        }
    }
}

fn host_configuration() -> BrowserHostConfiguration {
    BrowserHostConfiguration {
        artifact_dir: None,
        headless: true,
        backend: s("launch"),
        cdp_endpoint: None,
        user_data_dir: None,
        channel: None,
        executable_path: None,
    }
}

fn navigate_request(id: &str, navigate_permission: bool) -> Vec<u8> {
    format!(
        concat!(
            "{{\"id\":\"{}\",\"request\":{{",
            "\"operation\":{{\"type\":\"navigate\",\"url\":\"https://example.test/\"}},",
            "\"target\":{{}},",
            "\"permissions\":{{",
            "\"navigate\":{},\"readPage\":true,\"captureScreenshot\":false,",
            "\"interact\":false,\"javascriptEval\":false,\"readConsole\":false,",
            "\"readNetwork\":false,\"manageTabs\":false,\"sessionState\":true,",
            "\"fileTransfer\":false}},",
            "\"timeoutMs\":30000,\"metadata\":{{}}}},",
            "\"options\":{{\"headless\":true}}}}"
        ),
        id, navigate_permission
    )
    .into_bytes()
}

fn browser_object(entries: impl IntoIterator<Item = (&'static str, RuntimeJson)>) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    for (key, value) in entries {
        object.insert(key, value);
    }
    RuntimeJson::Object(object)
}

fn browser_permissions_for(operation: &str) -> BrowserHostPermissionSet {
    let in_set = |values: &[&str]| values.contains(&operation);
    BrowserHostPermissionSet {
        navigate: in_set(&["navigate"]),
        read_page: in_set(&[
            "navigate",
            "snapshot",
            "screenshot",
            "click",
            "type",
            "eval",
            "read_page",
            "find",
            "form_input",
            "computer",
            "locator",
            "page_text",
            "resize",
            "gif_creator",
            "upload_image",
            "file_upload",
            "shortcuts_list",
            "history",
            "tabs_content",
            "content_export",
            "clipboard",
            "dialog",
            "download",
            "playwright_wait_for_event",
            "playwright_wait_for_load_state",
            "playwright_wait_for_url",
            "playwright_dom_snapshot",
            "playwright_element_info",
            "playwright_element_screenshot",
        ]),
        capture_screenshot: in_set(&[
            "screenshot",
            "computer",
            "gif_creator",
            "playwright_element_screenshot",
        ]),
        interact: in_set(&[
            "click",
            "type",
            "form_input",
            "computer",
            "locator",
            "batch",
            "file_upload",
            "upload_image",
            "shortcuts_execute",
            "clipboard",
            "dialog",
            "download",
            "playwright_file_chooser_set_files",
        ]),
        javascript_eval: operation == "eval",
        read_console: operation == "console",
        read_network: operation == "network",
        manage_tabs: in_set(&[
            "tabs",
            "close",
            "batch",
            "switch_browser",
            "select_browser",
            "resolve_tab",
            "claim_tab",
            "mark_tab",
            // Deliberate vector change: loading urls in TEMPORARY tabs opens tabs and
            // closes them again, which is tab management by any reading — the same
            // capability `tabs {action: "new"}` declares. It was absent, so a policy
            // that withheld tab management still admitted this operation.
            "tabs_content",
        ]),
        session_state: in_set(&[
            "navigate",
            "click",
            "type",
            "eval",
            "close",
            "tabs",
            "form_input",
            "computer",
            "locator",
            "resize",
            "file_upload",
            "upload_image",
            "shortcuts_list",
            "shortcuts_execute",
            "switch_browser",
            "select_browser",
            "resolve_tab",
            "claim_tab",
            "list_connected_browsers",
            "history",
            "tabs_content",
            "clipboard",
            "dialog",
            "mark_tab",
            "download",
            "playwright_wait_for_event",
            "playwright_file_chooser_set_files",
            "playwright_wait_for_load_state",
            "playwright_wait_for_url",
            "playwright_dom_snapshot",
            "playwright_element_info",
            "playwright_element_screenshot",
        ]),
        file_transfer: in_set(&[
            "batch",
            "file_upload",
            "upload_image",
            "content_export",
            "download",
            "playwright_wait_for_event",
            "playwright_file_chooser_set_files",
        ]),
    }
}

fn browser_request_bytes_with_permissions(
    id: &'static str,
    operation: RuntimeJson,
    permissions: BrowserHostPermissionSet,
) -> Vec<u8> {
    let permissions = browser_object([
        ("navigate", RuntimeJson::Bool(permissions.navigate)),
        ("readPage", RuntimeJson::Bool(permissions.read_page)),
        (
            "captureScreenshot",
            RuntimeJson::Bool(permissions.capture_screenshot),
        ),
        ("interact", RuntimeJson::Bool(permissions.interact)),
        (
            "javascriptEval",
            RuntimeJson::Bool(permissions.javascript_eval),
        ),
        ("readConsole", RuntimeJson::Bool(permissions.read_console)),
        ("readNetwork", RuntimeJson::Bool(permissions.read_network)),
        ("manageTabs", RuntimeJson::Bool(permissions.manage_tabs)),
        ("sessionState", RuntimeJson::Bool(permissions.session_state)),
        ("fileTransfer", RuntimeJson::Bool(permissions.file_transfer)),
    ]);
    let request = browser_object([
        ("operation", operation),
        ("target", browser_object([])),
        ("permissions", permissions),
        ("timeoutMs", RuntimeJson::from(30_000)),
        ("metadata", browser_object([])),
    ]);
    runtime_json::stringify(&browser_object([
        ("id", RuntimeJson::from(id)),
        ("request", request),
        (
            "options",
            browser_object([("headless", RuntimeJson::Bool(true))]),
        ),
    ]))
    .to_node_utf8()
}

fn browser_request_result(operation: RuntimeJson) -> BrowserHostSendResult {
    browser_request_result_with(
        operation,
        browser_object([]),
        RuntimeJson::from(30_000),
        browser_object([]),
    )
}

fn browser_request_result_with(
    operation: RuntimeJson,
    target: RuntimeJson,
    timeout_ms: RuntimeJson,
    metadata: RuntimeJson,
) -> BrowserHostSendResult {
    let RuntimeJson::Object(operation_object) = &operation else {
        panic!("operation fixture must be an object");
    };
    let RuntimeJson::String(operation_type) = operation_object
        .get("type")
        .expect("operation fixture must carry type")
    else {
        panic!("operation fixture type must be a string");
    };
    let operation_type = utf8(operation_type);
    let permissions = browser_permissions_for(&operation_type);
    let permissions = browser_object([
        ("navigate", RuntimeJson::Bool(permissions.navigate)),
        ("readPage", RuntimeJson::Bool(permissions.read_page)),
        (
            "captureScreenshot",
            RuntimeJson::Bool(permissions.capture_screenshot),
        ),
        ("interact", RuntimeJson::Bool(permissions.interact)),
        (
            "javascriptEval",
            RuntimeJson::Bool(permissions.javascript_eval),
        ),
        ("readConsole", RuntimeJson::Bool(permissions.read_console)),
        ("readNetwork", RuntimeJson::Bool(permissions.read_network)),
        ("manageTabs", RuntimeJson::Bool(permissions.manage_tabs)),
        ("sessionState", RuntimeJson::Bool(permissions.session_state)),
        ("fileTransfer", RuntimeJson::Bool(permissions.file_transfer)),
    ]);
    let request = browser_object([
        ("operation", operation),
        ("target", target),
        ("permissions", permissions),
        ("timeoutMs", timeout_ms),
        ("metadata", metadata),
    ]);
    let envelope = browser_object([
        ("id", RuntimeJson::from("browser-schema-test")),
        ("request", request),
        (
            "options",
            browser_object([("headless", RuntimeJson::Bool(true))]),
        ),
    ]);
    let encoded = runtime_json::stringify(&envelope).to_node_utf8();
    let mut endpoint = BrowserHostEndpoint::new(BrowserHostPolicy::default());
    endpoint.open(host_configuration()).unwrap();
    endpoint.send(&encoded, 0)
}

struct BrowserOperationFixture {
    name: &'static str,
    operation: RuntimeJson,
    required: &'static [&'static str],
}

macro_rules! browser_operation_fixture {
    ($name:literal, required [$($required:literal),* $(,)?], {$($key:literal => $value:expr),* $(,)?}) => {
        BrowserOperationFixture {
            name: $name,
            operation: browser_object([
                ("type", RuntimeJson::from($name)),
                $(($key, $value)),*
            ]),
            required: &[$($required),*],
        }
    };
}

fn string_array(values: &[&str]) -> RuntimeJson {
    RuntimeJson::Array(
        values
            .iter()
            .map(|value| RuntimeJson::from(*value))
            .collect(),
    )
}

fn number_array(values: &[f64]) -> RuntimeJson {
    RuntimeJson::Array(values.iter().copied().map(RuntimeJson::from).collect())
}

fn viewport() -> RuntimeJson {
    browser_object([
        ("width", RuntimeJson::from(1280)),
        ("height", RuntimeJson::from(720)),
        ("deviceScaleFactor", RuntimeJson::from(2)),
        ("isMobile", RuntimeJson::Bool(false)),
    ])
}

#[allow(clippy::too_many_lines)]
fn browser_operation_fixtures() -> Vec<BrowserOperationFixture> {
    vec![
        browser_operation_fixture!("navigate", required ["url"], {
            "url" => RuntimeJson::from("https://example.test/"),
            "waitUntil" => RuntimeJson::from("load"),
            "viewport" => viewport()
        }),
        browser_operation_fixture!("snapshot", required [], {
            "selector" => RuntimeJson::from("main"),
            "includeText" => RuntimeJson::Bool(true),
            "includeLinks" => RuntimeJson::Bool(false)
        }),
        browser_operation_fixture!("screenshot", required [], {
            "selector" => RuntimeJson::from("main"),
            "fullPage" => RuntimeJson::Bool(true),
            "format" => RuntimeJson::from("jpeg"),
            "quality" => RuntimeJson::from(80)
        }),
        browser_operation_fixture!("click", required [], {
            "selector" => RuntimeJson::from("button"),
            "x" => RuntimeJson::from(10.5),
            "y" => RuntimeJson::from(20.25),
            "button" => RuntimeJson::from("left"),
            "clickCount" => RuntimeJson::from(2),
            "modifiers" => string_array(&["Shift", "Meta"])
        }),
        browser_operation_fixture!("type", required ["text"], {
            "selector" => RuntimeJson::from("input"),
            "text" => RuntimeJson::from("hello"),
            "clear" => RuntimeJson::Bool(true),
            "pressEnter" => RuntimeJson::Bool(false),
            "delayMs" => RuntimeJson::from(5)
        }),
        browser_operation_fixture!("eval", required ["script"], {
            "script" => RuntimeJson::from("args.value"),
            "args" => browser_object([("value", RuntimeJson::from(1))]),
            "awaitPromise" => RuntimeJson::Bool(true)
        }),
        browser_operation_fixture!("console", required [], {
            "levels" => string_array(&["debug", "warning"]),
            "sinceMs" => RuntimeJson::from(100),
            "limit" => RuntimeJson::from(20),
            "clear" => RuntimeJson::Bool(false)
        }),
        browser_operation_fixture!("network", required [], {
            "sinceMs" => RuntimeJson::from(100),
            "limit" => RuntimeJson::from(20),
            "includeBodies" => RuntimeJson::Bool(true),
            "urlFilter" => RuntimeJson::from("example.test")
        }),
        browser_operation_fixture!("gif_creator", required [], {
            "filename" => RuntimeJson::from("capture.gif"),
            "durationSeconds" => RuntimeJson::from(1.5),
            "fps" => RuntimeJson::from(5),
            "fullPage" => RuntimeJson::Bool(false)
        }),
        browser_operation_fixture!("upload_image", required ["path"], {
            "path" => RuntimeJson::from("/tmp/image.png"),
            "selector" => RuntimeJson::from("input[type=file]"),
            "prompt" => RuntimeJson::from("attach")
        }),
        browser_operation_fixture!("file_upload", required ["paths"], {
            "paths" => string_array(&["/tmp/a.txt", "/tmp/b.txt"]),
            "selector" => RuntimeJson::from("input[type=file]"),
            "refId" => RuntimeJson::from("ref-1")
        }),
        browser_operation_fixture!("shortcuts_list", required [], {
            "query" => RuntimeJson::from("save")
        }),
        browser_operation_fixture!("shortcuts_execute", required ["name"], {
            "name" => RuntimeJson::from("save"),
            "waitForCompletion" => RuntimeJson::Bool(true)
        }),
        browser_operation_fixture!("switch_browser", required [], {
            "prompt" => RuntimeJson::from("choose"),
            "waitMs" => RuntimeJson::from(500)
        }),
        browser_operation_fixture!("list_connected_browsers", required [], {
            "localOnly" => RuntimeJson::Bool(true)
        }),
        browser_operation_fixture!("select_browser", required ["deviceId"], {
            "deviceId" => RuntimeJson::from("device-1")
        }),
        browser_operation_fixture!("history", required [], {
            "queries" => string_array(&["docs"]),
            "limit" => RuntimeJson::from(10),
            "from" => RuntimeJson::from("2026-01-01T00:00:00.000Z"),
            "to" => RuntimeJson::from("2026-02-01T00:00:00.000Z")
        }),
        browser_operation_fixture!("tabs_content", required ["urls"], {
            "urls" => string_array(&["https://example.test/"]),
            "contentType" => RuntimeJson::from("markdown"),
            "maxChars" => RuntimeJson::from(50_000),
            "waitUntil" => RuntimeJson::from("dom_content_loaded")
        }),
        browser_operation_fixture!("content_export", required [], {
            "format" => RuntimeJson::from("html"),
            "selector" => RuntimeJson::from("main"),
            "maxChars" => RuntimeJson::from(100_000)
        }),
        browser_operation_fixture!("download", required [], {
            "selector" => RuntimeJson::from("a.download"),
            "url" => RuntimeJson::from("https://example.test/file"),
            "x" => RuntimeJson::from(10.0),
            "y" => RuntimeJson::from(20.0),
            "filename" => RuntimeJson::from("file.bin")
        }),
        browser_operation_fixture!("clipboard", required ["action"], {
            "action" => RuntimeJson::from("write"),
            "text" => RuntimeJson::from("hello"),
            "items" => RuntimeJson::Array(vec![browser_object([(
                "text",
                RuntimeJson::from("hello"),
            )])])
        }),
        browser_operation_fixture!("dialog", required ["action"], {
            "action" => RuntimeJson::from("accept"),
            "promptText" => RuntimeJson::from("yes")
        }),
        browser_operation_fixture!("mark_tab", required ["status"], {
            "status" => RuntimeJson::from("deliverable")
        }),
        browser_operation_fixture!("playwright_wait_for_event", required ["event"], {
            "event" => RuntimeJson::from("download")
        }),
        browser_operation_fixture!("playwright_file_chooser_set_files", required ["chooserId", "files"], {
            "chooserId" => RuntimeJson::from("chooser-1"),
            "files" => string_array(&["/tmp/a.txt"])
        }),
        browser_operation_fixture!("playwright_wait_for_load_state", required [], {
            "state" => RuntimeJson::from("network_idle")
        }),
        browser_operation_fixture!("playwright_wait_for_url", required ["url"], {
            "url" => RuntimeJson::from("https://example.test/done"),
            "waitUntil" => RuntimeJson::from("commit")
        }),
        browser_operation_fixture!("playwright_dom_snapshot", required [], {
            "visibleOnly" => RuntimeJson::Bool(true),
            "maxChars" => RuntimeJson::from(100_000)
        }),
        browser_operation_fixture!("playwright_element_info", required [], {
            "x" => RuntimeJson::from(10.0),
            "y" => RuntimeJson::from(20.0),
            "selector" => RuntimeJson::from("button")
        }),
        browser_operation_fixture!("playwright_element_screenshot", required [], {
            "x" => RuntimeJson::from(10.0),
            "y" => RuntimeJson::from(20.0),
            "selector" => RuntimeJson::from("button"),
            "format" => RuntimeJson::from("png")
        }),
        browser_operation_fixture!("read_page", required [], {
            "filter" => RuntimeJson::from("interactive"),
            "depth" => RuntimeJson::from(15),
            "refId" => RuntimeJson::from("ref-1"),
            "maxChars" => RuntimeJson::from(50_000)
        }),
        browser_operation_fixture!("find", required ["query"], {
            "query" => RuntimeJson::from("submit"),
            "limit" => RuntimeJson::from(20)
        }),
        browser_operation_fixture!("form_input", required ["ref", "value"], {
            "ref" => RuntimeJson::from("ref-1"),
            "value" => browser_object([("nested", RuntimeJson::Bool(true))])
        }),
        browser_operation_fixture!("computer", required ["action"], {
            "action" => RuntimeJson::from("left_click"),
            "coordinate" => number_array(&[10.0, 20.0]),
            "startCoordinate" => number_array(&[1.0, 2.0]),
            "text" => RuntimeJson::from("A"),
            "delayMs" => RuntimeJson::from(10),
            "duration" => RuntimeJson::from(0.5),
            "scrollDirection" => RuntimeJson::from("down"),
            "amount" => RuntimeJson::from(500.0),
            "refId" => RuntimeJson::from("ref-1"),
            "ref" => RuntimeJson::from("ref-alias"),
            "save" => RuntimeJson::Bool(true)
        }),
        browser_operation_fixture!("tabs", required ["action"], {
            "action" => RuntimeJson::from("new"),
            "url" => RuntimeJson::from("https://example.test/"),
            "name" => RuntimeJson::from("session")
        }),
        browser_operation_fixture!("locator", required ["selector", "action", "options"], {
            "selector" => RuntimeJson::from("button"),
            "frameSelector" => RuntimeJson::from("iframe"),
            "nthIndex" => RuntimeJson::from(-1),
            "filter" => browser_object([
                ("hasText", RuntimeJson::from("save")),
                ("hasNotText", RuntimeJson::from("cancel")),
                ("has", RuntimeJson::from("span")),
                ("hasNot", RuntimeJson::from("disabled")),
            ]),
            "andSelector" => RuntimeJson::from(".enabled"),
            "orSelector" => RuntimeJson::from(".primary"),
            "action" => RuntimeJson::from("click"),
            "value" => RuntimeJson::Array(vec![RuntimeJson::from("choice")]),
            "options" => browser_object([
                ("timeoutMs", RuntimeJson::from(1_000)),
                ("force", RuntimeJson::Bool(true)),
            ])
        }),
        browser_operation_fixture!("page_text", required [], {
            "maxChars" => RuntimeJson::from(50_000)
        }),
        browser_operation_fixture!("batch", required ["actions"], {
            "actions" => RuntimeJson::Array(vec![browser_object([
                ("type", RuntimeJson::from("navigate")),
                ("url", RuntimeJson::from("https://example.test/")),
            ])])
        }),
        browser_operation_fixture!("resize", required ["viewport"], {
            "viewport" => viewport()
        }),
        browser_operation_fixture!("close", required [], {
            "target" => RuntimeJson::from("all")
        }),
    ]
}

fn operation_with_only_required(fixture: &BrowserOperationFixture) -> RuntimeJson {
    let RuntimeJson::Object(operation) = &fixture.operation else {
        panic!("fixture operation must be an object");
    };
    let mut compact = RuntimeObject::new();
    for (key, value) in operation.entries() {
        if key == "type" || fixture.required.iter().any(|required| key == *required) {
            compact.insert(key, value.clone());
        }
    }
    RuntimeJson::Object(compact)
}

fn mutate_operation_field(
    operation: &RuntimeJson,
    field: &str,
    replacement: RuntimeJson,
) -> RuntimeJson {
    let RuntimeJson::Object(operation) = operation else {
        panic!("fixture operation must be an object");
    };
    let mut mutated = RuntimeObject::new();
    let mut found = false;
    for (key, value) in operation.entries() {
        if key == field {
            mutated.insert(key, replacement.clone());
            found = true;
        } else {
            mutated.insert(key, value.clone());
        }
    }
    assert!(found, "fixture field {field} must exist");
    RuntimeJson::Object(mutated)
}

fn remove_operation_field(operation: &RuntimeJson, field: &str) -> RuntimeJson {
    let RuntimeJson::Object(operation) = operation else {
        panic!("fixture operation must be an object");
    };
    let mut mutated = RuntimeObject::new();
    let mut found = false;
    for (key, value) in operation.entries() {
        if key == field {
            found = true;
        } else {
            mutated.insert(key, value.clone());
        }
    }
    assert!(found, "fixture field {field} must exist");
    RuntimeJson::Object(mutated)
}

fn append_operation_field(
    operation: &RuntimeJson,
    field: &'static str,
    value: RuntimeJson,
) -> RuntimeJson {
    let RuntimeJson::Object(operation) = operation else {
        panic!("fixture operation must be an object");
    };
    let mut mutated = operation.clone();
    assert!(mutated.insert(field, value).is_none());
    RuntimeJson::Object(mutated)
}

fn swap_first_two_operation_fields(operation: &RuntimeJson) -> RuntimeJson {
    let RuntimeJson::Object(operation) = operation else {
        panic!("fixture operation must be an object");
    };
    let mut entries = operation
        .entries()
        .map(|(key, value)| (key, value.clone()))
        .collect::<Vec<_>>();
    assert!(entries.len() >= 2);
    entries.swap(0, 1);
    let mut mutated = RuntimeObject::new();
    for (key, value) in entries {
        mutated.insert(key, value);
    }
    RuntimeJson::Object(mutated)
}

#[test]
fn all_six_subcommands_preserve_their_distinct_argument_quirks() {
    assert_eq!(
        identify_subcommand(&s("open-chrome-window")),
        Some(BrowserUtilitySubcommand::OpenChromeWindow)
    );
    assert_eq!(identify_subcommand(&s("Open-Chrome-Window")), None);

    assert_eq!(
        invoke(parse_browser_utility(
            BrowserUtilitySubcommand::CheckExtensionInstalled,
            &[s("--help"), s("unknown"), s("--json")],
        )),
        BrowserUtilityInvocation::CheckExtensionInstalled { json: true }
    );
    assert_eq!(
        invoke(parse_browser_utility(
            BrowserUtilitySubcommand::CheckNativeHostManifest,
            &[s("--help"), s("--json"), s("--json")],
        )),
        BrowserUtilityInvocation::CheckNativeHostManifest { json: true }
    );
    assert_eq!(
        invoke(parse_browser_utility(
            BrowserUtilitySubcommand::InstallNativeHostManifest,
            &[s("ignored"), s("--dry-run"), s("--json")],
        )),
        BrowserUtilityInvocation::InstallNativeHostManifest {
            dry_run: true,
            json: true,
        }
    );

    let chrome_help = immediate(parse_browser_utility(
        BrowserUtilitySubcommand::ChromeIsRunning,
        &[s("--bad"), s("-h")],
    ));
    assert_eq!(chrome_help.exit_code(), 0);
    assert_eq!(
        utf8(chrome_help.stderr()),
        "Usage: scripts/chrome-is-running.mjs [--check] [--json]\n"
    );
    let chrome_bad = immediate(parse_browser_utility(
        BrowserUtilitySubcommand::ChromeIsRunning,
        &[s("--check"), s("nope")],
    ));
    assert_eq!(chrome_bad.exit_code(), 2);
    assert_eq!(
        invoke(parse_browser_utility(
            BrowserUtilitySubcommand::ChromeIsRunning,
            &[s("--check"), s("--check"), s("--json")],
        )),
        BrowserUtilityInvocation::ChromeIsRunning {
            check: true,
            json: true,
        }
    );

    let installed_help = immediate(parse_browser_utility(
        BrowserUtilitySubcommand::InstalledBrowsers,
        &[s("bad"), s("--help")],
    ));
    assert_eq!(installed_help.exit_code(), 0);
    assert_eq!(
        utf8(installed_help.stderr()),
        "Usage: scripts/installed-browsers.mjs [--json]\n"
    );
    assert_eq!(
        immediate(parse_browser_utility(
            BrowserUtilitySubcommand::InstalledBrowsers,
            &[s("--dry-run")],
        ))
        .exit_code(),
        2
    );

    let open_help = immediate(parse_browser_utility(
        BrowserUtilitySubcommand::OpenChromeWindow,
        &[s("first"), s("second"), s("--help"), s("--bad")],
    ));
    assert_eq!(open_help.exit_code(), 0, "--help wins before invalid args");
    assert_eq!(
        utf8(open_help.stderr()),
        concat!(
            "Usage: scripts/open-chrome-window.mjs [--dry-run] [--json] [url]\n",
            "Optional profile-root override: KLAUS_CHROME_USER_DATA_DIR=/path/to/chrome-root\n",
            "Optional preferences-file override: KLAUS_CHROME_PREFERENCES_PATH=/path/to/Profile/Preferences\n",
        )
    );
    assert_eq!(
        invoke(parse_browser_utility(
            BrowserUtilitySubcommand::OpenChromeWindow,
            &[s("-h")],
        )),
        BrowserUtilityInvocation::OpenChromeWindow {
            dry_run: false,
            json: false,
            url: s("-h"),
        },
        "the source treats -h as the URL"
    );
    assert_eq!(
        immediate(parse_browser_utility(
            BrowserUtilitySubcommand::OpenChromeWindow,
            &[s("--unknown")],
        ))
        .exit_code(),
        2
    );
    assert_eq!(
        immediate(parse_browser_utility(
            BrowserUtilitySubcommand::OpenChromeWindow,
            &[s("a"), s("b")],
        ))
        .exit_code(),
        2
    );
    assert_eq!(
        invoke(parse_browser_utility(
            BrowserUtilitySubcommand::OpenChromeWindow,
            &[s("--json"), s("--json")],
        )),
        BrowserUtilityInvocation::OpenChromeWindow {
            dry_run: false,
            json: true,
            url: s("about:blank"),
        }
    );
}

#[test]
fn shadow_module_and_launch_executable_authority_are_crate_private() {
    let sdk_root = include_str!("../lib.rs");
    assert!(
        sdk_root.contains("mod browser_host;"),
        "the Task 29 shadow must remain crate-private"
    );
    assert!(
        !sdk_root.contains("pub mod browser_host;"),
        "downstream crates must not obtain browser executable/path authority"
    );

    let plan_source = include_str!("plan.rs");
    assert!(
        plan_source.contains("VerifiedChromeLaunchTarget"),
        "every Chrome executable/application path must carry a crate-owned verified brand"
    );
}

#[test]
fn open_chrome_explicit_empty_positional_defaults_to_about_blank() {
    assert_eq!(
        invoke(parse_browser_utility(
            BrowserUtilitySubcommand::OpenChromeWindow,
            &[RuntimeString::default()],
        )),
        BrowserUtilityInvocation::OpenChromeWindow {
            dry_run: false,
            json: false,
            url: s("about:blank"),
        }
    );
}

#[test]
fn extension_status_preserves_json_text_and_four_exit_classes() {
    let profile = ExtensionProfileStatus {
        profile_directory: s("Default"),
        profile_path: path("/chrome/Default"),
        preferences_path: path("/chrome/Default/Preferences"),
        installed: true,
        enabled: false,
        registered: true,
        state: RuntimeJson::from(0),
        disable_reasons: vec![s("1")],
        versions: vec![s("2.0"), s("10.0")],
        unpacked_path: None,
    };
    let status = ExtensionInstallStatus {
        user_data_directory: path("/chrome"),
        installed: true,
        enabled: false,
        profiles: vec![profile],
    };
    let json = render_check_extension_status(&status, true);
    assert_eq!(json.exit_code(), 1);
    assert_eq!(
        utf8(json.stdout()),
        concat!(
            "{\n",
            "  \"ok\": true,\n",
            "  \"extensionId\": \"gjamccelebgpogpbiceajehcajghphnb\",\n",
            "  \"userDataDirectory\": \"/chrome\",\n",
            "  \"installed\": true,\n",
            "  \"enabled\": false,\n",
            "  \"profiles\": [\n",
            "    {\n",
            "      \"profileDirectory\": \"Default\",\n",
            "      \"profilePath\": \"/chrome/Default\",\n",
            "      \"preferencesPath\": \"/chrome/Default/Preferences\",\n",
            "      \"installed\": true,\n",
            "      \"enabled\": false,\n",
            "      \"registered\": true,\n",
            "      \"state\": 0,\n",
            "      \"disableReasons\": [\n",
            "        \"1\"\n",
            "      ],\n",
            "      \"versions\": [\n",
            "        \"2.0\",\n",
            "        \"10.0\"\n",
            "      ],\n",
            "      \"unpackedPath\": null\n",
            "    }\n",
            "  ]\n",
            "}\n",
        )
    );
    assert_eq!(
        utf8(render_check_extension_status(&status, false).stdout()),
        concat!(
            "installed-disabled gjamccelebgpogpbiceajehcajghphnb\n",
            "Default: installed=true enabled=false path=/chrome/Default\n",
        )
    );

    let absent = ExtensionInstallStatus {
        user_data_directory: path("/chrome"),
        installed: false,
        enabled: false,
        profiles: Vec::new(),
    };
    assert_eq!(render_check_extension_status(&absent, false).exit_code(), 2);
    let enabled = ExtensionInstallStatus {
        enabled: true,
        installed: true,
        ..absent
    };
    assert_eq!(
        render_check_extension_status(&enabled, false).exit_code(),
        0
    );

    let error = render_check_extension_error(s("boom"), false);
    assert_eq!(error.exit_code(), 3);
    assert_eq!(
        utf8(error.stderr()),
        concat!(
            "boom\n",
            "Set KLAUS_CHROME_USER_DATA_DIR or KLAUS_CHROME_PREFERENCES_PATH to inspect a non-default Chrome profile.\n"
        )
    );
}

#[test]
fn manifest_check_preserves_conditional_member_order_and_error_streams() {
    let missing = NativeHostManifestStatus {
        correct: false,
        manifest_path: path("/hosts/com.klaus.browser.json"),
        exists: false,
        manifest: None,
        problem: Some(s("missing")),
    };
    let missing_json = render_check_native_host_status(&missing, true);
    assert_eq!(missing_json.exit_code(), 1);
    assert_eq!(
        utf8(missing_json.stdout()),
        concat!(
            "{\n",
            "  \"ok\": true,\n",
            "  \"correct\": false,\n",
            "  \"hostName\": \"com.klaus.browser\",\n",
            "  \"extensionId\": \"gjamccelebgpogpbiceajehcajghphnb\",\n",
            "  \"expectedOrigin\": \"chrome-extension://gjamccelebgpogpbiceajehcajghphnb/\",\n",
            "  \"manifestPath\": \"/hosts/com.klaus.browser.json\",\n",
            "  \"exists\": false,\n",
            "  \"problem\": \"missing\"\n",
            "}\n",
        )
    );

    let mut parsed = RuntimeObject::new();
    parsed.insert("path", RuntimeJson::from("/bin/host"));
    parsed.insert("name", RuntimeJson::from(NATIVE_HOST_NAME));
    let existing = NativeHostManifestStatus {
        correct: true,
        exists: true,
        manifest: Some(RuntimeJson::Object(parsed)),
        problem: None,
        ..missing
    };
    let existing_json = utf8(render_check_native_host_status(&existing, true).stdout());
    assert!(
        existing_json.find("\"exists\"").unwrap() < existing_json.find("\"manifest\"").unwrap()
    );
    assert!(
        existing_json.find("\"manifest\"").unwrap() < existing_json.find("\"problem\"").unwrap()
    );
    assert_eq!(
        render_check_native_host_status(&existing, true).exit_code(),
        0
    );

    let error = render_check_native_host_error(s("bad json"), true);
    assert_eq!(error.exit_code(), 2);
    assert_eq!(
        utf8(error.stdout()),
        "{\n  \"ok\": false,\n  \"error\": \"bad json\"\n}\n"
    );
}

#[test]
fn install_plan_requires_the_absolute_rust_native_host_and_fixes_action_order() {
    assert_eq!(
        VerifiedNativeHostBinary::from_attested_distribution_path(
            BrowserPlatform::Linux,
            path("runtime/native-host.mjs"),
        ),
        Err(VerifiedNativeHostPathError::NotAbsolute)
    );
    assert_eq!(
        VerifiedNativeHostBinary::from_attested_distribution_path(
            BrowserPlatform::Linux,
            path("/bundle/runtime/native-host.mjs"),
        ),
        Err(VerifiedNativeHostPathError::WrongBinaryName)
    );
    let host = VerifiedNativeHostBinary::from_attested_distribution_path(
        BrowserPlatform::Linux,
        path("/signed/bin/klaus-browser-native-host"),
    )
    .unwrap();
    let plan = install_native_host_manifest_plan(
        BrowserPlatform::Linux,
        path("/home/me/.config/google-chrome/NativeMessagingHosts/com.klaus.browser.json"),
        &host,
        false,
    )
    .unwrap();
    assert_eq!(
        plan.action(),
        &InstallNativeHostAction::UnixWriteManifestThenChmodHost0755
    );
    let json = plan.render_success(false, true);
    assert_eq!(json.exit_code(), 0);
    let bytes = utf8(json.stdout());
    assert!(bytes.contains("\"hostPath\": \"/signed/bin/klaus-browser-native-host\""));
    assert!(bytes.contains("\"path\": \"/signed/bin/klaus-browser-native-host\""));
    assert!(!bytes.contains("native-host.mjs"));
    assert!(bytes.ends_with("  \"registryKey\": null\n}\n"));

    let win_host = VerifiedNativeHostBinary::from_attested_distribution_path(
        BrowserPlatform::Windows,
        path("C:\\Klaus\\klaus-browser-native-host.exe"),
    )
    .unwrap();
    let manifest_path = path("C:\\Hosts\\com.klaus.browser.json");
    let win_plan = install_native_host_manifest_plan(
        BrowserPlatform::Windows,
        manifest_path.clone(),
        &win_host,
        false,
    )
    .unwrap();
    assert_eq!(
        win_plan.action(),
        &InstallNativeHostAction::WindowsWriteManifestThenRegister { manifest_path }
    );
    assert!(utf8(win_plan.render_success(false, false).stdout())
        .ends_with("HKCU\\Software\\Google\\Chrome\\NativeMessagingHosts\\com.klaus.browser\n"));

    let dry = install_native_host_manifest_plan(
        BrowserPlatform::Windows,
        path("C:\\Hosts\\com.klaus.browser.json"),
        &win_host,
        true,
    )
    .unwrap();
    assert_eq!(dry.action(), &InstallNativeHostAction::DryRun);
    assert!(utf8(dry.render_success(true, false).stdout())
        .starts_with("would install com.klaus.browser\n"));
    assert_eq!(
        render_install_native_host_error(s("denied"), true).exit_code(),
        1
    );
}

#[test]
fn native_host_brand_compares_the_complete_platform_basename() {
    assert_eq!(
        VerifiedNativeHostBinary::from_attested_distribution_path(
            BrowserPlatform::Linux,
            path("/signed/bin/evil-klaus-browser-native-host"),
        ),
        Err(VerifiedNativeHostPathError::WrongBinaryName)
    );
    assert_eq!(
        VerifiedNativeHostBinary::from_attested_distribution_path(
            BrowserPlatform::Windows,
            path("C:\\Klaus\\evil-klaus-browser-native-host.exe"),
        ),
        Err(VerifiedNativeHostPathError::WrongBinaryName)
    );
    assert!(VerifiedNativeHostBinary::from_attested_distribution_path(
        BrowserPlatform::Windows,
        path("C:\\Klaus\\KLAUS-BROWSER-NATIVE-HOST.EXE"),
    )
    .is_ok());
}

#[test]
fn running_and_installed_results_preserve_target_shapes_and_exit_laws() {
    let running = ChromeRunningStatus {
        processes: vec![ChromeProcessStatus {
            pid: 42,
            process_name: s("chrome.exe"),
            command: s("chrome.exe"),
        }],
        platform: BrowserPlatform::Windows,
    };
    assert_eq!(
        utf8(render_chrome_running_status(&running, true, false).stdout()),
        "running 1\n"
    );
    assert_eq!(
        render_chrome_running_status(&running, true, true).exit_code(),
        0
    );
    let stopped = ChromeRunningStatus {
        processes: Vec::new(),
        platform: BrowserPlatform::Linux,
    };
    assert_eq!(
        render_chrome_running_status(&stopped, false, false).exit_code(),
        0
    );
    assert_eq!(
        render_chrome_running_status(&stopped, true, false).exit_code(),
        1
    );
    assert_eq!(
        utf8(render_chrome_running_status(&stopped, true, true).stdout()),
        "{\n  \"running\": false,\n  \"processes\": [],\n  \"platform\": \"linux\"\n}\n"
    );

    let mac = InstalledBrowsersResult {
        browsers: vec![InstalledBrowser::MacOs {
            name: s("Google Chrome"),
            bundle_id: s("com.google.Chrome"),
            path: path("/Applications/Google Chrome.app"),
            version: Some(s("130.0")),
        }],
        platform: BrowserPlatform::MacOs,
    };
    assert_eq!(
        utf8(render_installed_browsers_result(&mac, false).stdout()),
        "Google Chrome 130.0 /Applications/Google Chrome.app\n"
    );
    let mac_json = utf8(render_installed_browsers_result(&mac, true).stdout());
    assert!(mac_json.contains(concat!(
        "\"name\": \"Google Chrome\",\n",
        "      \"bundle_id\": \"com.google.Chrome\",\n",
        "      \"path\": \"/Applications/Google Chrome.app\",\n",
        "      \"version\": \"130.0\""
    )));
    let none = InstalledBrowsersResult {
        browsers: Vec::new(),
        platform: BrowserPlatform::Linux,
    };
    assert_eq!(
        utf8(render_installed_browsers_result(&none, false).stdout()),
        "none\n"
    );
}

#[test]
fn closed_process_plans_freeze_probe_order_without_generic_commands() {
    let running = BrowserUtilityInvocation::ChromeIsRunning {
        check: false,
        json: false,
    };
    assert_eq!(
        browser_process_plan(&running, BrowserPlatform::MacOs),
        BrowserProcessPlan::ChromeIsRunning(ChromeRunningProcessPlan::MacOsSingletonThenPs)
    );
    let installed = BrowserUtilityInvocation::InstalledBrowsers { json: false };
    let BrowserProcessPlan::InstalledBrowsers(windows) =
        browser_process_plan(&installed, BrowserPlatform::Windows)
    else {
        panic!("expected installed-browser plan");
    };
    assert_eq!(
        windows.probes(),
        &[
            BrowserQuietProbe::WindowsAppPathCurrentUser,
            BrowserQuietProbe::WindowsAppPathLocalMachine,
            BrowserQuietProbe::WindowsAppPathWow6432,
            BrowserQuietProbe::WindowsWhereChrome,
            BrowserQuietProbe::WindowsVersionCurrentUser,
            BrowserQuietProbe::WindowsVersionLocalMachine,
        ]
    );

    for source in [
        include_str!("cli.rs"),
        include_str!("plan.rs"),
        include_str!("result.rs"),
    ] {
        assert!(!source.contains("std::process"));
        assert!(!source.contains("tokio::process"));
        assert!(!source.contains("Command::"));
        assert!(!source.contains("LogicalSpawnRequest"));
    }
}

#[test]
fn open_chrome_dry_run_is_immediate_and_launch_flushes_success_before_error() {
    let profile = OpenChromeProfilePlan {
        user_data_directory: path("/home/me/chrome"),
        profile_directory: s("Profile 2"),
        preferences_path: path("/home/me/chrome/Profile 2/Preferences"),
        include_user_data_argument: true,
    };
    let linux_target = VerifiedChromeLaunchTarget::from_verified_discovery_path(
        BrowserPlatform::Linux,
        path("/usr/bin/google-chrome"),
    )
    .unwrap();
    assert_eq!(
        OpenChromeProcessPlan::windows(linux_target.clone(), profile.clone(), s("about:blank"),),
        Err(OpenChromePlanError::PlatformMismatch)
    );
    let linux = OpenChromeProcessPlan::linux(
        linux_target,
        profile.clone(),
        s("https://example.test/\u{1f600}"),
    )
    .unwrap();
    let OpenChromeExecutionPlan::DryRun(dry) =
        open_chrome_execution_plan(linux.clone(), true, true)
    else {
        panic!("dry-run must not own a launch task");
    };
    assert_eq!(dry.exit_code(), 0);
    let dry_json = utf8(dry.stdout());
    assert!(dry_json.contains("\"launched\": false"));
    assert!(dry_json.contains(concat!(
        "\"args\": [\n",
        "    \"--user-data-dir=/home/me/chrome\",\n",
        "    \"--profile-directory=Profile 2\",\n",
        "    \"https://example.test/😀\"\n",
        "  ]"
    )));

    let OpenChromeExecutionPlan::Launch(launch) =
        open_chrome_execution_plan(linux.clone(), false, false)
    else {
        panic!("non-dry-run must own one launch-resolution task");
    };
    let effects = launch.failed_effects(2);
    assert_eq!(
        effects,
        vec![
            OpenChromeEffect::AttemptDetachedLaunch(linux),
            OpenChromeEffect::WriteStdout(s("opened /usr/bin/google-chrome\n")),
            OpenChromeEffect::FlushStdout,
            OpenChromeEffect::WriteStderr(s("Failed to launch Chrome: os-error errno:2\n")),
            OpenChromeEffect::Exit(1),
        ]
    );

    let windows_target = VerifiedChromeLaunchTarget::from_verified_discovery_path(
        BrowserPlatform::Windows,
        path("C:\\Chrome\\chrome.exe"),
    )
    .unwrap();
    let windows =
        OpenChromeProcessPlan::windows(windows_target, profile, s("about:blank")).unwrap();
    let OpenChromeExecutionPlan::Launch(windows_launch) =
        open_chrome_execution_plan(windows, false, true)
    else {
        panic!("non-dry-run must own a launch task");
    };
    assert!(matches!(
        windows_launch.failed_effects(5).as_slice(),
        [
            OpenChromeEffect::AttemptDetachedLaunch(_),
            OpenChromeEffect::WriteStdout(_),
            OpenChromeEffect::FlushStdout,
            OpenChromeEffect::WriteStderr(message),
            OpenChromeEffect::Exit(1),
        ] if message == &s("Failed to launch Chrome: os-error win32:5\n")
    ));

    let sync_error = render_open_chrome_error(s("chrome not found"), true);
    assert_eq!(sync_error.exit_code(), 1);
    assert_eq!(
        utf8(sync_error.stdout()),
        "{\n  \"ok\": false,\n  \"error\": \"chrome not found\"\n}\n"
    );
}

#[test]
fn browser_host_endpoint_registration_is_generation_bound_and_non_reentrant() {
    let mut endpoint = BrowserHostEndpoint::new(BrowserHostPolicy::default());
    assert_eq!(endpoint.phase(), BrowserHostEndpointPhase::Idle);
    let first = endpoint.open(host_configuration()).unwrap();
    assert_eq!(first, 1);
    assert_eq!(endpoint.phase(), BrowserHostEndpointPhase::Registered);
    assert_eq!(endpoint.generation(), Some(1));
    assert!(
        endpoint.pop_callback().is_none(),
        "open never calls back inline"
    );

    let duplicate = endpoint.open(host_configuration()).unwrap_err();
    assert_eq!(duplicate.code, "host_error");
    assert_eq!(
        duplicate.message,
        "Browser host transport endpoint is already open"
    );

    endpoint.close();
    assert_eq!(endpoint.phase(), BrowserHostEndpointPhase::Closed);
    assert_eq!(
        endpoint.pop_callback(),
        Some(BrowserHostCallback {
            sequence: 1,
            endpoint_generation: first,
            kind: BrowserHostCallbackKind::Close(None),
        })
    );
    let second = endpoint.open(host_configuration()).unwrap();
    assert_eq!(second, 2);
    assert!(
        !endpoint.message(first, b"{}"),
        "late old-generation callback"
    );
    assert!(endpoint.message(second, b"{}"));
    assert_eq!(
        endpoint.pop_callback(),
        Some(BrowserHostCallback {
            sequence: 2,
            endpoint_generation: second,
            kind: BrowserHostCallbackKind::Message(b"{}".to_vec()),
        })
    );
}

#[test]
fn browser_host_send_copies_canonical_bytes_and_preserves_global_callback_fifo() {
    let mut endpoint = BrowserHostEndpoint::new(BrowserHostPolicy::default());
    let generation = endpoint.open(host_configuration()).unwrap();
    let mut encoded = navigate_request("browser-client-1", true);
    let original = encoded.clone();
    let BrowserHostSendResult::Admitted(admission) = endpoint.send(&encoded, 7) else {
        panic!("canonical request should be admitted");
    };
    encoded.fill(b'x');
    assert_eq!(admission.endpoint_generation, generation);
    assert_eq!(admission.id, s("browser-client-1"));
    assert_eq!(admission.operation_type, s("navigate"));
    assert_eq!(admission.timeout_ms, 30_000);
    assert_eq!(admission.deadline_ms, 30_007);
    assert_eq!(
        admission.encoded, original,
        "send retains an owned byte copy"
    );
    assert!(endpoint.complete_send(generation, None));
    assert!(endpoint.message(generation, b"{\"id\":\"browser-client-1\",\"event\":null}"));
    assert!(endpoint.generation_closed(
        generation,
        BrowserHostError {
            code: "host_error",
            message: "Browser sidecar exited.",
        },
    ));
    let callbacks = std::iter::from_fn(|| endpoint.pop_callback()).collect::<Vec<_>>();
    assert_eq!(callbacks.len(), 3);
    assert_eq!(
        callbacks
            .iter()
            .map(|callback| callback.sequence)
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert!(matches!(
        callbacks[0].kind,
        BrowserHostCallbackKind::SendDone(None)
    ));
    assert!(matches!(
        callbacks[1].kind,
        BrowserHostCallbackKind::Message(_)
    ));
    assert!(matches!(
        callbacks[2].kind,
        BrowserHostCallbackKind::Close(Some(_))
    ));
}

#[test]
fn browser_host_rejects_noncanonical_permissions_duplicates_and_policy_bypass() {
    let mut endpoint = BrowserHostEndpoint::new(BrowserHostPolicy::default());
    let generation = endpoint.open(host_configuration()).unwrap();
    assert_eq!(
        endpoint.send(b" {\"id\":\"bad\"}", 0),
        BrowserHostSendResult::DeferredError
    );
    assert_eq!(
        endpoint.pop_callback(),
        Some(BrowserHostCallback {
            sequence: 1,
            endpoint_generation: generation,
            kind: BrowserHostCallbackKind::SendDone(Some(BrowserHostError {
                code: "host_error",
                message: "Invalid canonical browser RuntimeJson bytes.",
            })),
        })
    );

    assert_eq!(
        endpoint.send(&navigate_request("browser-client-1", false), 0),
        BrowserHostSendResult::ValidationResponse
    );
    assert!(matches!(
        endpoint.pop_callback().unwrap().kind,
        BrowserHostCallbackKind::SendDone(None)
    ));
    let BrowserHostCallbackKind::Message(response) = endpoint.pop_callback().unwrap().kind else {
        panic!("permission mismatch must return one encoded generation response");
    };
    assert_eq!(
        String::from_utf8(response).unwrap(),
        "{\"id\":\"browser-client-1\",\"ok\":false,\"error\":{\"code\":\"host_error\",\"message\":\"Browser host request validation failed.\"}}"
    );

    let valid = navigate_request("browser-client-2", true);
    assert!(matches!(
        endpoint.send(&valid, 0),
        BrowserHostSendResult::Admitted(_)
    ));
    assert_eq!(
        endpoint.send(&valid, 0),
        BrowserHostSendResult::ValidationResponse,
        "request ids are never reused within an endpoint generation"
    );

    let denied = BrowserHostPermissionSet {
        navigate: false,
        read_page: true,
        capture_screenshot: true,
        interact: true,
        javascript_eval: true,
        read_console: true,
        read_network: true,
        manage_tabs: true,
        session_state: true,
        file_transfer: true,
    };
    let mut policy_endpoint = BrowserHostEndpoint::new(BrowserHostPolicy {
        permissions: denied,
    });
    policy_endpoint.open(host_configuration()).unwrap();
    assert_eq!(
        policy_endpoint.send(&navigate_request("browser-client-3", true), 0),
        BrowserHostSendResult::ValidationResponse
    );
    let _done = policy_endpoint.pop_callback().unwrap();
    let BrowserHostCallbackKind::Message(response) = policy_endpoint.pop_callback().unwrap().kind
    else {
        panic!("policy denial must be a generation response");
    };
    assert!(String::from_utf8(response)
        .unwrap()
        .contains("Browser request is not permitted by the Rust host policy."));
}

#[test]
fn browser_batch_requires_the_full_permission_set_before_nested_dispatch() {
    let batch = browser_object([
        ("type", RuntimeJson::from("batch")),
        (
            "actions",
            RuntimeJson::Array(vec![browser_object([
                ("type", RuntimeJson::from("eval")),
                (
                    "script",
                    RuntimeJson::from("globalThis.__batch_escape = true"),
                ),
            ])]),
        ),
    ]);
    let encode = |permissions: BrowserHostPermissionSet| {
        let permissions = browser_object([
            ("navigate", RuntimeJson::Bool(permissions.navigate)),
            ("readPage", RuntimeJson::Bool(permissions.read_page)),
            (
                "captureScreenshot",
                RuntimeJson::Bool(permissions.capture_screenshot),
            ),
            ("interact", RuntimeJson::Bool(permissions.interact)),
            (
                "javascriptEval",
                RuntimeJson::Bool(permissions.javascript_eval),
            ),
            ("readConsole", RuntimeJson::Bool(permissions.read_console)),
            ("readNetwork", RuntimeJson::Bool(permissions.read_network)),
            ("manageTabs", RuntimeJson::Bool(permissions.manage_tabs)),
            ("sessionState", RuntimeJson::Bool(permissions.session_state)),
            ("fileTransfer", RuntimeJson::Bool(permissions.file_transfer)),
        ]);
        let request = browser_object([
            ("operation", batch.clone()),
            ("target", browser_object([])),
            ("permissions", permissions),
            ("timeoutMs", RuntimeJson::from(30_000)),
            ("metadata", browser_object([])),
        ]);
        runtime_json::stringify(&browser_object([
            ("id", RuntimeJson::from("browser-batch-permission-test")),
            ("request", request),
            (
                "options",
                browser_object([("headless", RuntimeJson::Bool(true))]),
            ),
        ]))
        .to_node_utf8()
    };

    let legacy_underclaim = BrowserHostPermissionSet {
        navigate: false,
        read_page: false,
        capture_screenshot: false,
        interact: true,
        javascript_eval: false,
        read_console: false,
        read_network: false,
        manage_tabs: true,
        session_state: false,
        file_transfer: true,
    };
    let mut overclaim_endpoint = BrowserHostEndpoint::new(BrowserHostPolicy::default());
    overclaim_endpoint.open(host_configuration()).unwrap();
    assert_eq!(
        overclaim_endpoint.send(&encode(BrowserHostPermissionSet::ALL), 0),
        BrowserHostSendResult::ValidationResponse,
        "the host still requires the exact client-visible batch claim"
    );

    let mut limited_endpoint = BrowserHostEndpoint::new(BrowserHostPolicy {
        permissions: legacy_underclaim,
    });
    limited_endpoint.open(host_configuration()).unwrap();
    assert_eq!(
        limited_endpoint.send(&encode(legacy_underclaim), 0),
        BrowserHostSendResult::ValidationResponse,
        "nested eval authority must be revalidated independently of the fixed batch claim"
    );

    let mut all_endpoint = BrowserHostEndpoint::new(BrowserHostPolicy::default());
    all_endpoint.open(host_configuration()).unwrap();
    assert!(matches!(
        all_endpoint.send(&encode(legacy_underclaim), 0),
        BrowserHostSendResult::Admitted(_)
    ));
}

#[test]
fn browser_action_sensitive_policy_cannot_be_bypassed_by_static_client_claims() {
    let locator_claim = browser_permissions_for("locator");
    assert!(!locator_claim.capture_screenshot);
    let locator_screenshot = browser_object([
        ("type", RuntimeJson::from("locator")),
        ("selector", RuntimeJson::from("main")),
        ("action", RuntimeJson::from("screenshot")),
        ("options", browser_object([])),
    ]);
    let mut locator_endpoint = BrowserHostEndpoint::new(BrowserHostPolicy {
        permissions: locator_claim,
    });
    locator_endpoint.open(host_configuration()).unwrap();
    assert_eq!(
        locator_endpoint.send(
            &browser_request_bytes_with_permissions(
                "browser-locator-policy-test",
                locator_screenshot,
                locator_claim,
            ),
            0,
        ),
        BrowserHostSendResult::ValidationResponse,
        "locator.screenshot must require captureScreenshot at the Rust policy boundary"
    );

    let tabs_claim = browser_permissions_for("tabs");
    assert!(!tabs_claim.navigate && !tabs_claim.read_page);
    let tabs_new = browser_object([
        ("type", RuntimeJson::from("tabs")),
        ("action", RuntimeJson::from("new")),
        ("url", RuntimeJson::from("https://example.test/")),
    ]);
    let mut tabs_endpoint = BrowserHostEndpoint::new(BrowserHostPolicy {
        permissions: tabs_claim,
    });
    tabs_endpoint.open(host_configuration()).unwrap();
    assert_eq!(
        tabs_endpoint.send(
            &browser_request_bytes_with_permissions(
                "browser-tabs-policy-test",
                tabs_new,
                tabs_claim,
            ),
            0,
        ),
        BrowserHostSendResult::ValidationResponse,
        "tabs.new must require navigate and readPage at the Rust policy boundary"
    );

    let tabs_content_claim = browser_permissions_for("tabs_content");
    assert!(!tabs_content_claim.navigate);
    let tabs_content = browser_object([
        ("type", RuntimeJson::from("tabs_content")),
        (
            "urls",
            RuntimeJson::Array(vec![RuntimeJson::from("https://example.test/")]),
        ),
    ]);
    let mut content_endpoint = BrowserHostEndpoint::new(BrowserHostPolicy {
        permissions: tabs_content_claim,
    });
    content_endpoint.open(host_configuration()).unwrap();
    assert_eq!(
        content_endpoint.send(
            &browser_request_bytes_with_permissions(
                "browser-tabs-content-policy-test",
                tabs_content,
                tabs_content_claim,
            ),
            0,
        ),
        BrowserHostSendResult::ValidationResponse,
        "tabs_content must require navigate before its internal page.goto calls"
    );

    // `save` on a coordinate action writes the capture to a FILE, and the base set for
    // `computer` does not include file transfer — so a policy that granted captures but
    // not file transfer used to admit it. The claim is the base set here, which is what
    // makes this a POLICY denial rather than a validation one.
    let computer_claim = browser_permissions_for("computer");
    assert!(!computer_claim.file_transfer);
    let saving_capture = browser_object([
        ("type", RuntimeJson::from("computer")),
        ("action", RuntimeJson::from("screenshot")),
        ("save", RuntimeJson::Bool(true)),
    ]);
    let mut capture_endpoint = BrowserHostEndpoint::new(BrowserHostPolicy {
        permissions: computer_claim,
    });
    capture_endpoint.open(host_configuration()).unwrap();
    assert_eq!(
        capture_endpoint.send(
            &browser_request_bytes_with_permissions(
                "browser-computer-save-policy-test",
                saving_capture,
                computer_claim,
            ),
            0,
        ),
        BrowserHostSendResult::ValidationResponse,
        "a saved capture must require file transfer, not just a screenshot"
    );
}

#[test]
fn browser_host_rejects_operation_authority_extras_wrong_types_and_missing_required_fields() {
    for forbidden in ["process", "path", "env"] {
        let operation = browser_object([
            ("type", RuntimeJson::from("navigate")),
            ("url", RuntimeJson::from("https://example.test/")),
            (forbidden, browser_object([])),
        ]);
        assert!(
            matches!(
                browser_request_result(operation),
                BrowserHostSendResult::ValidationResponse
            ),
            "operation field {forbidden} must not cross the Rust host boundary"
        );
    }

    assert!(
        matches!(
            browser_request_result(browser_object([
                ("type", RuntimeJson::from("navigate")),
                ("url", RuntimeJson::Bool(true)),
            ])),
            BrowserHostSendResult::ValidationResponse
        ),
        "navigate.url must be a string"
    );
    assert!(
        matches!(
            browser_request_result(browser_object([("type", RuntimeJson::from("navigate"),)])),
            BrowserHostSendResult::ValidationResponse
        ),
        "navigate.url is required"
    );
}

#[test]
fn browser_host_accepts_all_forty_closed_operation_schemas_and_compact_omissions() {
    let fixtures = browser_operation_fixtures();
    assert_eq!(fixtures.len(), 40);
    let mut names = fixtures
        .iter()
        .map(|fixture| fixture.name)
        .collect::<Vec<_>>();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), 40, "every schema fixture must be unique");

    for fixture in &fixtures {
        assert!(
            matches!(
                browser_request_result(fixture.operation.clone()),
                BrowserHostSendResult::Admitted(_)
            ),
            "full {} operation must be admitted",
            fixture.name
        );
        assert!(
            matches!(
                browser_request_result(operation_with_only_required(fixture)),
                BrowserHostSendResult::Admitted(_)
            ),
            "compact {} operation must admit every optional omission",
            fixture.name
        );
    }
}

#[test]
fn browser_host_operation_field_mutation_matrix_is_closed_and_order_compatible() {
    for fixture in browser_operation_fixtures() {
        assert!(
            matches!(
                browser_request_result(append_operation_field(
                    &fixture.operation,
                    "process",
                    browser_object([("env", browser_object([]))]),
                )),
                BrowserHostSendResult::ValidationResponse
            ),
            "{} must reject an unknown authority field",
            fixture.name
        );
        assert!(
            matches!(
                browser_request_result(swap_first_two_operation_fields(&fixture.operation)),
                BrowserHostSendResult::Admitted(_)
            ),
            "{} must preserve the fixed client's ordinary-object field order",
            fixture.name
        );
        assert!(
            matches!(
                browser_request_result(mutate_operation_field(
                    &fixture.operation,
                    "type",
                    RuntimeJson::from("unknown_operation"),
                )),
                BrowserHostSendResult::ValidationResponse
            ),
            "{} must reject a mutated discriminator",
            fixture.name
        );

        let RuntimeJson::Object(operation) = &fixture.operation else {
            unreachable!();
        };
        for (field, _) in operation.entries() {
            if field == "type" || matches!(utf8(&field).as_str(), "args" | "items" | "value") {
                continue;
            }
            assert!(
                matches!(
                    browser_request_result(mutate_operation_field(
                        &fixture.operation,
                        &utf8(&field),
                        RuntimeJson::Null,
                    )),
                    BrowserHostSendResult::ValidationResponse
                ),
                "{}.{} must reject the wrong field type",
                fixture.name,
                utf8(&field)
            );
        }
        for required in fixture.required {
            assert!(
                matches!(
                    browser_request_result(remove_operation_field(&fixture.operation, required,)),
                    BrowserHostSendResult::ValidationResponse
                ),
                "{}.{} must remain required",
                fixture.name,
                required
            );
        }
    }
}

#[test]
fn browser_host_rejects_every_closed_operation_enum_mutation() {
    let fixtures = browser_operation_fixtures();
    for (operation, field) in [
        ("navigate", "waitUntil"),
        ("screenshot", "format"),
        ("click", "button"),
        ("tabs_content", "contentType"),
        ("tabs_content", "waitUntil"),
        ("content_export", "format"),
        ("clipboard", "action"),
        ("dialog", "action"),
        ("mark_tab", "status"),
        ("playwright_wait_for_event", "event"),
        ("playwright_wait_for_load_state", "state"),
        ("playwright_wait_for_url", "waitUntil"),
        ("playwright_element_screenshot", "format"),
        ("read_page", "filter"),
        ("computer", "action"),
        ("computer", "scrollDirection"),
        ("tabs", "action"),
        ("locator", "action"),
        ("close", "target"),
    ] {
        let fixture = fixtures
            .iter()
            .find(|fixture| fixture.name == operation)
            .unwrap();
        assert!(
            matches!(
                browser_request_result(mutate_operation_field(
                    &fixture.operation,
                    field,
                    RuntimeJson::from("not_in_the_enum"),
                )),
                BrowserHostSendResult::ValidationResponse
            ),
            "{operation}.{field} must reject unknown enum values"
        );
    }

    let console = fixtures
        .iter()
        .find(|fixture| fixture.name == "console")
        .unwrap();
    assert!(matches!(
        browser_request_result(mutate_operation_field(
            &console.operation,
            "levels",
            string_array(&["warn"]),
        )),
        BrowserHostSendResult::ValidationResponse
    ));
    assert!(matches!(
        browser_request_result(mutate_operation_field(
            &console.operation,
            "levels",
            string_array(&["fatal"]),
        )),
        BrowserHostSendResult::ValidationResponse
    ));

    let content = fixtures
        .iter()
        .find(|fixture| fixture.name == "content_export")
        .unwrap();
    assert!(matches!(
        browser_request_result(mutate_operation_field(
            &content.operation,
            "format",
            RuntimeJson::from("md"),
        )),
        BrowserHostSendResult::Admitted(_)
    ));
}

#[test]
fn browser_host_validates_nested_viewport_filter_arrays_and_batch_operations() {
    let navigate = |viewport| {
        browser_object([
            ("type", RuntimeJson::from("navigate")),
            ("url", RuntimeJson::from("https://example.test/")),
            ("viewport", viewport),
        ])
    };
    for invalid in [
        browser_object([
            ("width", RuntimeJson::from(1280)),
            ("height", RuntimeJson::from(720)),
            ("process", browser_object([])),
        ]),
        browser_object([("height", RuntimeJson::from(720))]),
        browser_object([
            ("width", RuntimeJson::Bool(true)),
            ("height", RuntimeJson::from(720)),
        ]),
        browser_object([
            ("width", RuntimeJson::from(1280)),
            ("height", RuntimeJson::from(720.5)),
        ]),
        browser_object([
            ("width", RuntimeJson::from(-1)),
            ("height", RuntimeJson::from(720)),
        ]),
        browser_object([
            ("width", RuntimeJson::from(1280)),
            ("height", RuntimeJson::from(720)),
            ("isMobile", RuntimeJson::from("false")),
        ]),
    ] {
        assert!(matches!(
            browser_request_result(navigate(invalid)),
            BrowserHostSendResult::ValidationResponse
        ));
    }
    assert!(matches!(
        browser_request_result(navigate(browser_object([
            ("width", RuntimeJson::from(1280)),
            ("height", RuntimeJson::from(720)),
            ("deviceScaleFactor", RuntimeJson::from(1.5)),
        ]))),
        BrowserHostSendResult::Admitted(_)
    ));

    assert!(matches!(
        browser_request_result(navigate(browser_object([
            ("height", RuntimeJson::from(720)),
            ("width", RuntimeJson::from(1280)),
        ]))),
        BrowserHostSendResult::Admitted(_)
    ));

    assert!(matches!(
        browser_request_result(browser_object([
            ("type", RuntimeJson::from("batch")),
            (
                "actions",
                RuntimeJson::Array(vec![browser_object([
                    ("url", RuntimeJson::from("https://example.test/")),
                    ("type", RuntimeJson::from("navigate")),
                ])]),
            ),
        ])),
        BrowserHostSendResult::Admitted(_)
    ));

    let locator = |filter| {
        browser_object([
            ("type", RuntimeJson::from("locator")),
            ("selector", RuntimeJson::from("button")),
            ("filter", filter),
            ("action", RuntimeJson::from("click")),
            ("options", browser_object([])),
        ])
    };
    for invalid in [
        browser_object([("env", browser_object([]))]),
        browser_object([("hasText", RuntimeJson::Bool(true))]),
    ] {
        assert!(matches!(
            browser_request_result(locator(invalid)),
            BrowserHostSendResult::ValidationResponse
        ));
    }
    assert!(matches!(
        browser_request_result(locator(browser_object([
            ("hasNotText", RuntimeJson::from("cancel")),
            ("hasText", RuntimeJson::from("save")),
        ]))),
        BrowserHostSendResult::Admitted(_)
    ));

    let fixtures = browser_operation_fixtures();
    for (operation, field, invalid) in [
        (
            "click",
            "modifiers",
            RuntimeJson::Array(vec![RuntimeJson::Bool(true)]),
        ),
        (
            "file_upload",
            "paths",
            RuntimeJson::Array(vec![RuntimeJson::from(1)]),
        ),
        ("computer", "coordinate", number_array(&[1.0])),
        (
            "computer",
            "startCoordinate",
            RuntimeJson::Array(vec![RuntimeJson::from(1.0), RuntimeJson::from("2")]),
        ),
    ] {
        let fixture = fixtures
            .iter()
            .find(|fixture| fixture.name == operation)
            .unwrap();
        assert!(
            matches!(
                browser_request_result(mutate_operation_field(&fixture.operation, field, invalid,)),
                BrowserHostSendResult::ValidationResponse
            ),
            "{operation}.{field} must reject an invalid array element"
        );
    }

    let batch = |action| {
        browser_object([
            ("type", RuntimeJson::from("batch")),
            ("actions", RuntimeJson::Array(vec![action])),
        ])
    };
    for invalid in [
        RuntimeJson::Null,
        browser_object([
            ("type", RuntimeJson::from("navigate")),
            ("url", RuntimeJson::from("https://example.test/")),
            ("env", browser_object([])),
        ]),
        browser_object([("type", RuntimeJson::from("navigate"))]),
        browser_object([
            ("type", RuntimeJson::from("navigate")),
            ("url", RuntimeJson::Bool(true)),
        ]),
        batch(browser_object([
            ("type", RuntimeJson::from("upload_image")),
            ("path", RuntimeJson::from("/tmp/image.png")),
            ("process", browser_object([])),
        ])),
    ] {
        assert!(matches!(
            browser_request_result(batch(invalid)),
            BrowserHostSendResult::ValidationResponse
        ));
    }
}

#[test]
fn browser_host_validates_target_timeout_metadata_and_json_fields_without_roundtrip() {
    let operation = browser_object([
        ("type", RuntimeJson::from("navigate")),
        ("url", RuntimeJson::from("https://example.test/")),
    ]);
    let target = browser_object([
        ("sessionId", RuntimeJson::from("session-1")),
        ("tabId", RuntimeJson::from("tab-1")),
    ]);
    let metadata = browser_object([
        (
            "nested",
            RuntimeJson::Array(vec![
                RuntimeJson::Null,
                RuntimeJson::Bool(true),
                RuntimeJson::from(1.25),
                RuntimeJson::from("text"),
                browser_object([("env", RuntimeJson::from("inert metadata"))]),
            ]),
        ),
        ("browserId", RuntimeJson::from("browser-1")),
    ]);
    assert!(matches!(
        browser_request_result_with(
            operation.clone(),
            target,
            RuntimeJson::from(9_007_199_254_740_991_f64),
            metadata,
        ),
        BrowserHostSendResult::Admitted(_)
    ));

    for target in [
        browser_object([("path", RuntimeJson::from("/tmp"))]),
        browser_object([("sessionId", RuntimeJson::Bool(true))]),
    ] {
        assert!(matches!(
            browser_request_result_with(
                operation.clone(),
                target,
                RuntimeJson::from(30_000),
                browser_object([]),
            ),
            BrowserHostSendResult::ValidationResponse
        ));
    }
    assert!(matches!(
        browser_request_result_with(
            operation.clone(),
            browser_object([
                ("tabId", RuntimeJson::from("tab-1")),
                ("sessionId", RuntimeJson::from("session-1")),
            ]),
            RuntimeJson::from(30_000),
            browser_object([]),
        ),
        BrowserHostSendResult::Admitted(_)
    ));
    for timeout in [
        RuntimeJson::from("30000"),
        RuntimeJson::from(-1),
        RuntimeJson::from(1.5),
        RuntimeJson::from(9_007_199_254_740_992_f64),
    ] {
        assert!(matches!(
            browser_request_result_with(
                operation.clone(),
                browser_object([]),
                timeout,
                browser_object([]),
            ),
            BrowserHostSendResult::ValidationResponse
        ));
    }
    for metadata in [RuntimeJson::Null, RuntimeJson::Array(Vec::new())] {
        assert!(matches!(
            browser_request_result_with(
                operation.clone(),
                browser_object([]),
                RuntimeJson::from(30_000),
                metadata,
            ),
            BrowserHostSendResult::ValidationResponse
        ));
    }

    for value in [
        RuntimeJson::Null,
        RuntimeJson::Bool(true),
        RuntimeJson::from(1.25),
        RuntimeJson::from("text"),
        RuntimeJson::Array(vec![RuntimeJson::from(1)]),
        browser_object([("nested", RuntimeJson::Null)]),
    ] {
        assert!(matches!(
            browser_request_result(browser_object([
                ("type", RuntimeJson::from("form_input")),
                ("ref", RuntimeJson::from("ref-1")),
                ("value", value),
            ])),
            BrowserHostSendResult::Admitted(_)
        ));
    }
}

#[test]
fn browser_host_rejects_duplicate_operation_fields_before_shape_validation() {
    let duplicated = String::from_utf8(navigate_request("browser-client-duplicate", true))
        .unwrap()
        .replace(
            "\"type\":\"navigate\",",
            "\"type\":\"navigate\",\"type\":\"navigate\",",
        );
    let mut endpoint = BrowserHostEndpoint::new(BrowserHostPolicy::default());
    endpoint.open(host_configuration()).unwrap();
    assert_eq!(
        endpoint.send(duplicated.as_bytes(), 0),
        BrowserHostSendResult::DeferredError
    );
    assert!(matches!(
        endpoint.pop_callback().unwrap().kind,
        BrowserHostCallbackKind::SendDone(Some(BrowserHostError {
            message: "Invalid canonical browser RuntimeJson bytes.",
            ..
        }))
    ));
}

#[test]
fn unix_package_readiness_is_exact_and_never_overreads_the_first_mux_byte() {
    let generation = 0x0102_0304_0506_0708;
    let challenge = [0xa5; 32];
    let permit = BrowserBootstrapRecord::encode_unix(
        UnixPackageRecordType::PackageLoadPermit,
        generation,
        challenge,
    );
    assert_eq!(&permit[..4], b"KBU1");
    assert_eq!(&permit[4..8], &[1, 3, 2, 0]);
    assert_eq!(&permit[8..16], &generation.to_be_bytes());
    assert_eq!(&permit[16..], &challenge);
    assert_eq!(
        BrowserBootstrapRecord::decode_unix(
            &permit,
            UnixPackageRecordType::PackageLoadPermit,
            generation,
            &challenge,
        )
        .unwrap()
        .record_type,
        1
    );

    let ready = BrowserBootstrapRecord::encode_unix(
        UnixPackageRecordType::PackageReady,
        generation,
        challenge,
    );
    assert_eq!(&ready[4..8], &[2, 2, 3, 0]);
    let mut reader = ExactBootstrapRecordReader::default();
    for byte in &ready[..47] {
        assert_eq!(reader.feed(std::slice::from_ref(byte)), 1);
        assert!(!reader.is_complete());
    }
    let final_and_mux = [ready[47], 0xde, 0xad, 0xbe, 0xef];
    assert_eq!(
        reader.feed(&final_and_mux),
        1,
        "the mux prefix is untouched"
    );
    assert!(reader.is_complete());
    assert_eq!(reader.record(), Some(&ready));
    assert_eq!(reader.feed(&final_and_mux[1..]), 0);
    BrowserBootstrapRecord::decode_unix(
        reader.record().unwrap(),
        UnixPackageRecordType::PackageReady,
        generation,
        &challenge,
    )
    .unwrap();
}

#[test]
fn bootstrap_records_fail_closed_on_every_authenticated_field() {
    let generation = 7;
    let token = [0x33; 32];
    let canonical =
        BrowserBootstrapRecord::encode_unix(UnixPackageRecordType::PackageReady, generation, token);
    assert_eq!(
        BrowserBootstrapRecord::decode_unix(
            &canonical[..47],
            UnixPackageRecordType::PackageReady,
            generation,
            &token,
        ),
        Err(BrowserBootstrapWireError::InvalidLength)
    );
    for (index, error) in [
        (0, BrowserBootstrapWireError::InvalidMagic),
        (4, BrowserBootstrapWireError::InvalidType),
        (5, BrowserBootstrapWireError::InvalidRole),
        (6, BrowserBootstrapWireError::InvalidRole),
        (7, BrowserBootstrapWireError::InvalidReserved),
        (8, BrowserBootstrapWireError::WrongGeneration),
        (16, BrowserBootstrapWireError::WrongToken),
    ] {
        let mut mutated = canonical;
        mutated[index] ^= 0xff;
        assert_eq!(
            BrowserBootstrapRecord::decode_unix(
                &mutated,
                UnixPackageRecordType::PackageReady,
                generation,
                &token,
            ),
            Err(error),
            "byte {index} must fail with its stable class"
        );
    }

    let windows = BrowserBootstrapRecord::encode_windows_authenticated(
        WindowsBootstrapPhase::PackageReady,
        WindowsBrokerLanePair::Mux,
        generation,
        token,
    );
    let decoded = BrowserBootstrapRecord::decode_windows_authenticated(
        &windows,
        WindowsBootstrapPhase::PackageReady,
        WindowsBrokerLanePair::Mux,
        generation,
        &token,
    )
    .unwrap();
    assert_eq!(decoded.record_type, 8);
    assert_eq!(decoded.generation, generation);
    assert_eq!(decoded.token, token);
}

#[test]
fn windows_bootstrap_phase_pair_generation_and_token_are_all_authenticated() {
    let source = include_str!("broker_wire.rs");
    assert!(source.contains("pub fn decode_windows_authenticated("));
    assert!(
        !source.contains("pub fn decode_windows("),
        "protocol consumers must not receive an unauthenticated KBB1 decoder"
    );

    let generation = 0x0102_0304_0506_0708;
    let token = [0xa7; 32];
    let phases = [
        WindowsBootstrapPhase::ServerHello,
        WindowsBootstrapPhase::ClientHello,
        WindowsBootstrapPhase::AdoptionChallenge,
        WindowsBootstrapPhase::AdoptionEcho,
        WindowsBootstrapPhase::Commit,
        WindowsBootstrapPhase::CommitEcho,
        WindowsBootstrapPhase::AdoptionAck,
        WindowsBootstrapPhase::PackageReady,
    ];
    for phase in phases {
        assert!((1..=8).contains(&(phase as u8)));
        for pair in [WindowsBrokerLanePair::Sync, WindowsBrokerLanePair::Mux] {
            let encoded = BrowserBootstrapRecord::encode_windows_authenticated(
                phase, pair, generation, token,
            );
            let host_to_client = matches!(
                phase,
                WindowsBootstrapPhase::ServerHello
                    | WindowsBootstrapPhase::AdoptionChallenge
                    | WindowsBootstrapPhase::Commit
                    | WindowsBootstrapPhase::AdoptionAck
            );
            let (host_role, client_role) = match pair {
                WindowsBrokerLanePair::Sync => (1_u8, 0_u8),
                WindowsBrokerLanePair::Mux => (3_u8, 2_u8),
            };
            let (expected_own, expected_peer) = if host_to_client {
                (host_role, client_role)
            } else {
                (client_role, host_role)
            };
            assert_eq!(
                &encoded[4..8],
                &[phase as u8, expected_own, expected_peer, 0],
                "KBB1 phase and pair have one fixed physical direction"
            );
            let decoded = BrowserBootstrapRecord::decode_windows_authenticated(
                &encoded, phase, pair, generation, &token,
            )
            .unwrap();
            assert_eq!(decoded.record_type, phase as u8);
            assert_eq!(decoded.generation, generation);
            assert_eq!(decoded.token, token);
        }
    }

    let package_ready = BrowserBootstrapRecord::encode_windows_authenticated(
        WindowsBootstrapPhase::PackageReady,
        WindowsBrokerLanePair::Sync,
        generation,
        token,
    );
    let mut unknown_type = package_ready;
    unknown_type[4] = 9;
    assert_eq!(
        BrowserBootstrapRecord::decode_windows_authenticated(
            &unknown_type,
            WindowsBootstrapPhase::PackageReady,
            WindowsBrokerLanePair::Sync,
            generation,
            &token,
        ),
        Err(BrowserBootstrapWireError::InvalidType)
    );
    assert_eq!(
        BrowserBootstrapRecord::decode_windows_authenticated(
            &package_ready,
            WindowsBootstrapPhase::ServerHello,
            WindowsBrokerLanePair::Sync,
            generation,
            &token,
        ),
        Err(BrowserBootstrapWireError::InvalidType)
    );
    assert_eq!(
        BrowserBootstrapRecord::decode_windows_authenticated(
            &package_ready,
            WindowsBootstrapPhase::PackageReady,
            WindowsBrokerLanePair::Mux,
            generation,
            &token,
        ),
        Err(BrowserBootstrapWireError::InvalidRole),
        "type-8 PackageReady cannot cross from the sync pair to the mux pair"
    );
    assert_eq!(
        BrowserBootstrapRecord::decode_windows_authenticated(
            &package_ready,
            WindowsBootstrapPhase::PackageReady,
            WindowsBrokerLanePair::Sync,
            generation + 1,
            &token,
        ),
        Err(BrowserBootstrapWireError::WrongGeneration)
    );
    let mut wrong_token = token;
    wrong_token[31] ^= 1;
    assert_eq!(
        BrowserBootstrapRecord::decode_windows_authenticated(
            &package_ready,
            WindowsBootstrapPhase::PackageReady,
            WindowsBrokerLanePair::Sync,
            generation,
            &wrong_token,
        ),
        Err(BrowserBootstrapWireError::WrongToken)
    );
}

fn broker_object(entries: impl IntoIterator<Item = (&'static str, RuntimeJson)>) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    for (key, value) in entries {
        object.insert(key, value);
    }
    RuntimeJson::Object(object)
}

fn mux_frame(
    direction: BrowserMuxDirection,
    kind: BrowserMuxKind,
    stream_id: u16,
    sequence: u64,
    correlation: u64,
    credit: u32,
    payload: BrowserMuxPayload,
) -> BrowserMuxFrame {
    BrowserMuxFrame::new(
        direction,
        0x0102_0304_0506_0708,
        0x1112_1314_1516_1718,
        stream_id,
        kind,
        sequence,
        correlation,
        credit,
        payload,
    )
    .unwrap()
}

fn valid_mux_payload(kind: BrowserMuxKind) -> BrowserMuxPayload {
    match kind {
        BrowserMuxKind::StdoutData
        | BrowserMuxKind::StderrData
        | BrowserMuxKind::ExtraData
        | BrowserMuxKind::Write => BrowserMuxPayload::Bytes(vec![0xff]),
        BrowserMuxKind::StreamError => BrowserMuxPayload::RuntimeJson(broker_object([
            ("code", RuntimeJson::from("EPIPE")),
            ("message", RuntimeJson::from("closed")),
        ])),
        BrowserMuxKind::Exit => BrowserMuxPayload::RuntimeJson(broker_object([
            ("code", RuntimeJson::Null),
            ("signal", RuntimeJson::Null),
        ])),
        BrowserMuxKind::CleanupComplete
        | BrowserMuxKind::GracefulCloseResult
        | BrowserMuxKind::ForceCloseResult => BrowserMuxPayload::RuntimeJson(broker_object([
            ("ok", RuntimeJson::Bool(true)),
            ("error", RuntimeJson::Null),
        ])),
        BrowserMuxKind::WriteAck
        | BrowserMuxKind::ReadCredit
        | BrowserMuxKind::HalfClose
        | BrowserMuxKind::StreamClose
        | BrowserMuxKind::CloseHooksReady
        | BrowserMuxKind::Close
        | BrowserMuxKind::BeginGracefulClose
        | BrowserMuxKind::BeginForceClose => BrowserMuxPayload::Empty,
    }
}

fn representative_mux_fields(kind: BrowserMuxKind) -> (BrowserMuxDirection, u16, u64, u32) {
    match kind {
        BrowserMuxKind::StdoutData => (BrowserMuxDirection::HostToJs, 1, 0, 0),
        BrowserMuxKind::StderrData => (BrowserMuxDirection::HostToJs, 2, 0, 0),
        BrowserMuxKind::ExtraData => (BrowserMuxDirection::HostToJs, 3, 0, 0),
        BrowserMuxKind::Write => (BrowserMuxDirection::JsToHost, 3, 0, 0),
        BrowserMuxKind::WriteAck => (BrowserMuxDirection::HostToJs, 3, 1, 0),
        BrowserMuxKind::ReadCredit => (BrowserMuxDirection::JsToHost, 1, 0, 1),
        BrowserMuxKind::HalfClose => (BrowserMuxDirection::JsToHost, 3, 0, 0),
        BrowserMuxKind::StreamError => (BrowserMuxDirection::HostToJs, 1, 0, 0),
        BrowserMuxKind::StreamClose => (BrowserMuxDirection::JsToHost, 1, 0, 0),
        BrowserMuxKind::CloseHooksReady => (BrowserMuxDirection::JsToHost, 0, 0, 0),
        BrowserMuxKind::Exit => (BrowserMuxDirection::HostToJs, 0, 0, 0),
        BrowserMuxKind::Close => (BrowserMuxDirection::HostToJs, 0, 0, 0),
        BrowserMuxKind::CleanupComplete => (BrowserMuxDirection::HostToJs, 0, 1, 0),
        BrowserMuxKind::BeginGracefulClose => (BrowserMuxDirection::HostToJs, 0, 0, 0),
        BrowserMuxKind::GracefulCloseResult => (BrowserMuxDirection::JsToHost, 0, 1, 0),
        BrowserMuxKind::BeginForceClose => (BrowserMuxDirection::HostToJs, 0, 1, 0),
        BrowserMuxKind::ForceCloseResult => (BrowserMuxDirection::JsToHost, 0, 1, 0),
    }
}

#[test]
fn sync_wire_has_fixed_sealed_kinds_exact_layout_and_canonical_runtime_json() {
    assert_eq!(BrowserSyncKind::PlaywrightLaunch as u16, 1);
    assert_eq!(BrowserSyncKind::PlaywrightTreeKill as u16, 2);
    assert_eq!(BrowserSyncKind::PlaywrightMacOsSwVers as u16, 3);
    assert_eq!(BrowserSyncKind::PlaywrightLinuxLdd as u16, 4);
    assert_eq!(BrowserSyncKind::PlaywrightLinuxLdconfig as u16, 5);
    assert_eq!(BrowserSyncKind::PlaywrightWindowsWinldd as u16, 6);
    assert_eq!(BrowserSyncKind::BrowserFfmpegEncode as u16, 7);

    let generation = 0x0102_0304_0506_0708;
    let request_id = 0x1112_1314_1516_1718;
    let payload = broker_object([
        (
            "value",
            RuntimeJson::String(RuntimeString::from_utf16(vec![0xd800])),
        ),
        ("ok", RuntimeJson::Bool(true)),
    ]);
    let frame = BrowserSyncFrame::new(
        generation,
        request_id,
        BrowserSyncKind::PlaywrightLaunch,
        payload.clone(),
    )
    .unwrap();
    let encoded = frame.encode().unwrap();
    assert_eq!(
        u32::from_be_bytes(encoded[..4].try_into().unwrap()) as usize,
        encoded.len() - 4
    );
    assert_eq!(&encoded[4..12], &generation.to_be_bytes());
    assert_eq!(&encoded[12..20], &request_id.to_be_bytes());
    assert_eq!(&encoded[20..22], &1_u16.to_be_bytes());
    assert_eq!(&encoded[22..24], &0_u16.to_be_bytes());
    assert_eq!(&encoded[24..], br#"{"value":"\ud800","ok":true}"#);
    assert_eq!(BrowserSyncFrame::decode_exact(&encoded).unwrap(), frame);

    let mut unknown_kind = encoded.clone();
    unknown_kind[20..22].copy_from_slice(&8_u16.to_be_bytes());
    assert_eq!(
        BrowserSyncFrame::decode_exact(&unknown_kind),
        Err(BrowserBrokerWireError::UnknownSyncKind)
    );
    let mut flags = encoded.clone();
    flags[23] = 1;
    assert_eq!(
        BrowserSyncFrame::decode_exact(&flags),
        Err(BrowserBrokerWireError::InvalidFlags)
    );
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert_eq!(
        BrowserSyncFrame::decode_exact(&trailing),
        Err(BrowserBrokerWireError::TrailingBytes)
    );

    let mut noncanonical = Vec::new();
    let body = br#"{ "ok":true}"#;
    noncanonical.extend_from_slice(&(20_u32 + u32::try_from(body.len()).unwrap()).to_be_bytes());
    noncanonical.extend_from_slice(&generation.to_be_bytes());
    noncanonical.extend_from_slice(&request_id.to_be_bytes());
    noncanonical.extend_from_slice(&(BrowserSyncKind::PlaywrightLaunch as u16).to_be_bytes());
    noncanonical.extend_from_slice(&0_u16.to_be_bytes());
    noncanonical.extend_from_slice(body);
    assert_eq!(
        BrowserSyncFrame::decode_exact(&noncanonical),
        Err(BrowserBrokerWireError::InvalidCanonicalRuntimeJson)
    );
}

#[test]
fn sync_payloads_close_launch_shape_a_and_semantic_ffmpeg_kind_seven() {
    let launch = broker_object([
        ("command", RuntimeJson::from("/verified/chromium")),
        (
            "args",
            RuntimeJson::Array(vec![RuntimeJson::from("--remote-debugging-pipe")]),
        ),
        (
            "options",
            broker_object([
                ("detached", RuntimeJson::Bool(true)),
                ("cwd", RuntimeJson::Null),
                (
                    "env",
                    RuntimeJson::Array(vec![RuntimeJson::Array(vec![
                        RuntimeJson::from("A"),
                        RuntimeJson::from("B"),
                    ])]),
                ),
                ("shell", RuntimeJson::Null),
                (
                    "stdio",
                    RuntimeJson::Array(
                        ["ignore", "pipe", "pipe", "pipe", "pipe"]
                            .into_iter()
                            .map(RuntimeJson::from)
                            .collect(),
                    ),
                ),
            ]),
        ),
        ("sourceOptions", broker_object([])),
        (
            "tempDirectories",
            RuntimeJson::Array(vec![
                RuntimeJson::from("/tmp/playwright-profile"),
                RuntimeJson::from("/tmp/playwright-artifacts"),
            ]),
        ),
        ("mode", RuntimeJson::from("ephemeral")),
        (
            "plannerTrace",
            broker_object([
                ("executable", RuntimeJson::from("/verified/chromium")),
                (
                    "browserArguments",
                    RuntimeJson::Array(vec![RuntimeJson::from("--remote-debugging-pipe")]),
                ),
                ("userDataDir", RuntimeJson::from("/tmp/playwright-profile")),
                (
                    "artifactsDir",
                    RuntimeJson::from("/tmp/playwright-artifacts"),
                ),
                (
                    "tempDirectories",
                    RuntimeJson::Array(vec![
                        RuntimeJson::from("/tmp/playwright-profile"),
                        RuntimeJson::from("/tmp/playwright-artifacts"),
                    ]),
                ),
            ]),
        ),
    ]);
    let decoded = decode_sync_request_payload(BrowserSyncKind::PlaywrightLaunch, &launch).unwrap();
    assert!(matches!(
        decoded,
        BrowserSyncRequestPayload::PlaywrightLaunch(BrowserPlaywrightLaunchRequest {
            mode: BrowserPlaywrightLaunchMode::Ephemeral,
            ..
        })
    ));

    let ffmpeg = broker_object([
        ("listPath", RuntimeJson::from("/tmp/input.txt")),
        ("filePath", RuntimeJson::from("/tmp/output.gif")),
        ("fps", RuntimeJson::from(30)),
    ]);
    assert_eq!(
        decode_sync_request_payload(BrowserSyncKind::BrowserFfmpegEncode, &ffmpeg).unwrap(),
        BrowserSyncRequestPayload::BrowserFfmpegEncode(BrowserFfmpegEncodeRequest {
            list_path: s("/tmp/input.txt"),
            file_path: s("/tmp/output.gif"),
            fps: 30,
        })
    );

    let wrong_order = broker_object([
        ("filePath", RuntimeJson::from("/tmp/output.gif")),
        ("listPath", RuntimeJson::from("/tmp/input.txt")),
        ("fps", RuntimeJson::from(30)),
    ]);
    assert_eq!(
        decode_sync_request_payload(BrowserSyncKind::BrowserFfmpegEncode, &wrong_order),
        Err(BrowserBrokerWireError::InvalidPayload)
    );
    let out_of_range = broker_object([
        ("listPath", RuntimeJson::from("/tmp/input.txt")),
        ("filePath", RuntimeJson::from("/tmp/output.gif")),
        ("fps", RuntimeJson::from(31)),
    ]);
    assert_eq!(
        decode_sync_request_payload(BrowserSyncKind::BrowserFfmpegEncode, &out_of_range),
        Err(BrowserBrokerWireError::InvalidPayload)
    );
}

#[test]
fn sync_reader_never_overreads_and_exchange_rejects_wrong_or_concurrent_ids() {
    let first = BrowserSyncFrame::new(
        9,
        41,
        BrowserSyncKind::PlaywrightTreeKill,
        RuntimeJson::Null,
    )
    .unwrap();
    let second = BrowserSyncFrame::new(
        9,
        42,
        BrowserSyncKind::PlaywrightMacOsSwVers,
        RuntimeJson::Null,
    )
    .unwrap();
    let first_bytes = first.encode().unwrap();
    let second_bytes = second.encode().unwrap();
    let joined = [first_bytes.as_slice(), second_bytes.as_slice()].concat();
    let mut reader = ExactSyncFrameReader::default();
    assert_eq!(reader.feed(&joined).unwrap(), first_bytes.len());
    assert!(reader.is_complete());
    assert_eq!(reader.frame(), Some(first_bytes.as_slice()));
    assert_eq!(reader.feed(&second_bytes).unwrap(), 0);

    let mut exchange = BrowserSyncExchange::new(9);
    exchange.admit_request(&first).unwrap();
    assert_eq!(
        exchange.admit_request(&second),
        Err(BrowserBrokerWireError::SyncRequestAlreadyInFlight)
    );
    assert_eq!(
        exchange.accept_response(&second),
        Err(BrowserBrokerWireError::WrongRequestId)
    );
    let wrong_generation = BrowserSyncFrame::new(
        10,
        41,
        BrowserSyncKind::PlaywrightTreeKill,
        RuntimeJson::Null,
    )
    .unwrap();
    assert_eq!(
        exchange.accept_response(&wrong_generation),
        Err(BrowserBrokerWireError::WrongGeneration)
    );
    exchange.accept_response(&first).unwrap();
    exchange.admit_request(&second).unwrap();

    let mut oversized = ExactSyncFrameReader::default();
    assert_eq!(
        oversized.feed(&(16_u32 * 1024 * 1024 + 1).to_be_bytes()),
        Err(BrowserBrokerWireError::FrameTooLarge)
    );
}

#[test]
fn mux_wire_numeric_kinds_and_all_seventeen_static_schemas_are_closed() {
    let numeric = [
        (BrowserMuxKind::StdoutData, 1),
        (BrowserMuxKind::StderrData, 2),
        (BrowserMuxKind::ExtraData, 3),
        (BrowserMuxKind::Write, 4),
        (BrowserMuxKind::WriteAck, 5),
        (BrowserMuxKind::ReadCredit, 6),
        (BrowserMuxKind::HalfClose, 7),
        (BrowserMuxKind::StreamError, 8),
        (BrowserMuxKind::StreamClose, 9),
        (BrowserMuxKind::CloseHooksReady, 10),
        (BrowserMuxKind::Exit, 11),
        (BrowserMuxKind::Close, 12),
        (BrowserMuxKind::CleanupComplete, 13),
        (BrowserMuxKind::BeginGracefulClose, 14),
        (BrowserMuxKind::GracefulCloseResult, 15),
        (BrowserMuxKind::BeginForceClose, 16),
        (BrowserMuxKind::ForceCloseResult, 17),
    ];
    for (kind, value) in numeric {
        assert_eq!(kind as u16, value);
    }

    let stream_error = || {
        BrowserMuxPayload::RuntimeJson(broker_object([
            ("code", RuntimeJson::from("EPIPE")),
            ("message", RuntimeJson::from("closed")),
        ]))
    };
    let exit = || {
        BrowserMuxPayload::RuntimeJson(broker_object([
            ("code", RuntimeJson::from(-9)),
            ("signal", RuntimeJson::from("SIGKILL")),
        ]))
    };
    let result = || {
        BrowserMuxPayload::RuntimeJson(broker_object([
            ("ok", RuntimeJson::Bool(false)),
            ("error", RuntimeJson::from("failed")),
        ]))
    };
    let cases = vec![
        mux_frame(
            BrowserMuxDirection::HostToJs,
            BrowserMuxKind::StdoutData,
            1,
            1,
            0,
            0,
            BrowserMuxPayload::Bytes(vec![0xff]),
        ),
        mux_frame(
            BrowserMuxDirection::HostToJs,
            BrowserMuxKind::StderrData,
            2,
            1,
            0,
            0,
            BrowserMuxPayload::Bytes(vec![0xfe]),
        ),
        mux_frame(
            BrowserMuxDirection::HostToJs,
            BrowserMuxKind::ExtraData,
            3,
            1,
            0,
            0,
            BrowserMuxPayload::Bytes(vec![1]),
        ),
        mux_frame(
            BrowserMuxDirection::JsToHost,
            BrowserMuxKind::Write,
            4,
            1,
            0,
            0,
            BrowserMuxPayload::Bytes(vec![2]),
        ),
        mux_frame(
            BrowserMuxDirection::HostToJs,
            BrowserMuxKind::WriteAck,
            3,
            1,
            1,
            0,
            BrowserMuxPayload::Empty,
        ),
        mux_frame(
            BrowserMuxDirection::JsToHost,
            BrowserMuxKind::ReadCredit,
            1,
            1,
            0,
            1,
            BrowserMuxPayload::Empty,
        ),
        mux_frame(
            BrowserMuxDirection::JsToHost,
            BrowserMuxKind::HalfClose,
            4,
            1,
            0,
            0,
            BrowserMuxPayload::Empty,
        ),
        mux_frame(
            BrowserMuxDirection::HostToJs,
            BrowserMuxKind::StreamError,
            1,
            1,
            0,
            0,
            stream_error(),
        ),
        mux_frame(
            BrowserMuxDirection::JsToHost,
            BrowserMuxKind::StreamClose,
            2,
            1,
            0,
            0,
            BrowserMuxPayload::Empty,
        ),
        mux_frame(
            BrowserMuxDirection::JsToHost,
            BrowserMuxKind::CloseHooksReady,
            0,
            1,
            0,
            0,
            BrowserMuxPayload::Empty,
        ),
        mux_frame(
            BrowserMuxDirection::HostToJs,
            BrowserMuxKind::Exit,
            0,
            1,
            0,
            0,
            exit(),
        ),
        mux_frame(
            BrowserMuxDirection::HostToJs,
            BrowserMuxKind::Close,
            0,
            1,
            0,
            0,
            BrowserMuxPayload::Empty,
        ),
        mux_frame(
            BrowserMuxDirection::HostToJs,
            BrowserMuxKind::CleanupComplete,
            0,
            1,
            1,
            0,
            result(),
        ),
        mux_frame(
            BrowserMuxDirection::HostToJs,
            BrowserMuxKind::BeginGracefulClose,
            0,
            1,
            0,
            0,
            BrowserMuxPayload::Empty,
        ),
        mux_frame(
            BrowserMuxDirection::JsToHost,
            BrowserMuxKind::GracefulCloseResult,
            0,
            1,
            1,
            0,
            result(),
        ),
        mux_frame(
            BrowserMuxDirection::HostToJs,
            BrowserMuxKind::BeginForceClose,
            0,
            1,
            1,
            0,
            BrowserMuxPayload::Empty,
        ),
        mux_frame(
            BrowserMuxDirection::JsToHost,
            BrowserMuxKind::ForceCloseResult,
            0,
            1,
            1,
            0,
            result(),
        ),
    ];
    assert_eq!(cases.len(), 17);
    for frame in cases {
        let encoded = frame.encode().unwrap();
        assert_eq!(
            BrowserMuxFrame::decode_exact(&encoded, frame.direction).unwrap(),
            frame
        );
    }

    let layout = mux_frame(
        BrowserMuxDirection::HostToJs,
        BrowserMuxKind::StdoutData,
        1,
        0x2122_2324_2526_2728,
        0,
        0,
        BrowserMuxPayload::Bytes(vec![0xaa, 0xbb]),
    );
    let encoded = layout.encode().unwrap();
    assert_eq!(u32::from_be_bytes(encoded[..4].try_into().unwrap()), 42);
    assert_eq!(&encoded[4..12], &layout.generation.to_be_bytes());
    assert_eq!(&encoded[12..20], &layout.object_id.to_be_bytes());
    assert_eq!(&encoded[20..22], &1_u16.to_be_bytes());
    assert_eq!(&encoded[22..24], &1_u16.to_be_bytes());
    assert_eq!(&encoded[24..32], &layout.sequence.to_be_bytes());
    assert_eq!(&encoded[32..40], &0_u64.to_be_bytes());
    assert_eq!(&encoded[40..44], &0_u32.to_be_bytes());
    assert_eq!(&encoded[44..], &[0xaa, 0xbb]);

    let mut unknown = encoded.clone();
    unknown[22..24].copy_from_slice(&18_u16.to_be_bytes());
    assert_eq!(
        BrowserMuxFrame::decode_exact(&unknown, BrowserMuxDirection::HostToJs),
        Err(BrowserBrokerWireError::UnknownMuxKind)
    );
}

#[test]
fn mux_table_exhaustively_freezes_direction_stream_class_and_payload_grammar() {
    let kinds = [
        BrowserMuxKind::StdoutData,
        BrowserMuxKind::StderrData,
        BrowserMuxKind::ExtraData,
        BrowserMuxKind::Write,
        BrowserMuxKind::WriteAck,
        BrowserMuxKind::ReadCredit,
        BrowserMuxKind::HalfClose,
        BrowserMuxKind::StreamError,
        BrowserMuxKind::StreamClose,
        BrowserMuxKind::CloseHooksReady,
        BrowserMuxKind::Exit,
        BrowserMuxKind::Close,
        BrowserMuxKind::CleanupComplete,
        BrowserMuxKind::BeginGracefulClose,
        BrowserMuxKind::GracefulCloseResult,
        BrowserMuxKind::BeginForceClose,
        BrowserMuxKind::ForceCloseResult,
    ];
    for kind in kinds {
        let expected_class = match kind {
            BrowserMuxKind::StdoutData | BrowserMuxKind::StderrData | BrowserMuxKind::ExtraData => {
                BrowserMuxSequenceClass::Data
            }
            BrowserMuxKind::Write => BrowserMuxSequenceClass::Write,
            BrowserMuxKind::ReadCredit => BrowserMuxSequenceClass::Credit,
            _ => BrowserMuxSequenceClass::Control,
        };
        let (representative_direction, representative_stream, correlation, credit) =
            representative_mux_fields(kind);
        let representative = BrowserMuxFrame::new(
            representative_direction,
            1,
            1,
            representative_stream,
            kind,
            1,
            correlation,
            credit,
            valid_mux_payload(kind),
        )
        .unwrap();
        assert_eq!(representative.sequence_class(), expected_class);

        for direction in [BrowserMuxDirection::HostToJs, BrowserMuxDirection::JsToHost] {
            for stream in 0..=5 {
                let direction_ok = match kind {
                    BrowserMuxKind::StreamError | BrowserMuxKind::StreamClose => true,
                    _ => direction == representative_direction,
                };
                let stream_ok = match kind {
                    BrowserMuxKind::StdoutData => stream == 1,
                    BrowserMuxKind::StderrData => stream == 2,
                    BrowserMuxKind::ExtraData
                    | BrowserMuxKind::Write
                    | BrowserMuxKind::WriteAck
                    | BrowserMuxKind::HalfClose => matches!(stream, 3 | 4),
                    BrowserMuxKind::ReadCredit
                    | BrowserMuxKind::StreamError
                    | BrowserMuxKind::StreamClose => (1..=4).contains(&stream),
                    _ => stream == 0,
                };
                let direction_correlation = if kind == BrowserMuxKind::StreamClose
                    && direction == BrowserMuxDirection::JsToHost
                {
                    0
                } else {
                    correlation
                };
                let actual = BrowserMuxFrame::new(
                    direction,
                    1,
                    1,
                    stream,
                    kind,
                    1,
                    direction_correlation,
                    credit,
                    valid_mux_payload(kind),
                );
                assert_eq!(
                    actual.is_ok(),
                    direction_ok && stream_ok,
                    "kind={kind:?} direction={direction:?} stream={stream}"
                );
            }
        }

        let wrong_payload = match valid_mux_payload(kind) {
            BrowserMuxPayload::Empty => BrowserMuxPayload::Bytes(Vec::new()),
            BrowserMuxPayload::Bytes(_) | BrowserMuxPayload::RuntimeJson(_) => {
                BrowserMuxPayload::Empty
            }
        };
        assert_eq!(
            BrowserMuxFrame::new(
                representative_direction,
                1,
                1,
                representative_stream,
                kind,
                1,
                correlation,
                credit,
                wrong_payload,
            ),
            Err(BrowserBrokerWireError::InvalidPayload),
            "kind={kind:?} must have exactly one payload grammar"
        );
    }
}

#[test]
fn mux_static_validation_rejects_direction_stream_payload_credit_and_trailing_bytes() {
    let invalid_direction = BrowserMuxFrame::new(
        BrowserMuxDirection::JsToHost,
        1,
        1,
        1,
        BrowserMuxKind::StdoutData,
        1,
        0,
        0,
        BrowserMuxPayload::Bytes(vec![1]),
    );
    assert_eq!(
        invalid_direction,
        Err(BrowserBrokerWireError::InvalidDirection)
    );
    assert_eq!(
        BrowserMuxFrame::new(
            BrowserMuxDirection::HostToJs,
            1,
            1,
            2,
            BrowserMuxKind::StdoutData,
            1,
            0,
            0,
            BrowserMuxPayload::Bytes(vec![1]),
        ),
        Err(BrowserBrokerWireError::InvalidStream)
    );
    assert_eq!(
        BrowserMuxFrame::new(
            BrowserMuxDirection::JsToHost,
            1,
            1,
            1,
            BrowserMuxKind::ReadCredit,
            1,
            0,
            0,
            BrowserMuxPayload::Empty,
        ),
        Err(BrowserBrokerWireError::InvalidCredit)
    );
    assert_eq!(
        BrowserMuxFrame::new(
            BrowserMuxDirection::HostToJs,
            1,
            1,
            1,
            BrowserMuxKind::StreamError,
            1,
            0,
            0,
            BrowserMuxPayload::RuntimeJson(broker_object([
                ("message", RuntimeJson::from("wrong order")),
                ("code", RuntimeJson::from("E")),
            ])),
        ),
        Err(BrowserBrokerWireError::InvalidPayload)
    );
    assert_eq!(
        BrowserMuxFrame::new(
            BrowserMuxDirection::HostToJs,
            1,
            0,
            0,
            BrowserMuxKind::Close,
            1,
            0,
            0,
            BrowserMuxPayload::Empty,
        ),
        Err(BrowserBrokerWireError::InvalidObjectId)
    );

    let frame = mux_frame(
        BrowserMuxDirection::HostToJs,
        BrowserMuxKind::StreamError,
        1,
        1,
        0,
        0,
        BrowserMuxPayload::RuntimeJson(broker_object([
            ("code", RuntimeJson::from("E")),
            ("message", RuntimeJson::from("m")),
        ])),
    );
    let mut trailing = frame.encode().unwrap();
    trailing.push(0);
    assert_eq!(
        BrowserMuxFrame::decode_exact(&trailing, BrowserMuxDirection::HostToJs),
        Err(BrowserBrokerWireError::TrailingBytes)
    );

    let canonical = frame.encode().unwrap();
    let mut noncanonical = canonical[..44].to_vec();
    noncanonical.extend_from_slice(br#"{"code":"E", "message":"m"}"#);
    let noncanonical_remaining = u32::try_from(noncanonical.len() - 4).unwrap();
    noncanonical[..4].copy_from_slice(&noncanonical_remaining.to_be_bytes());
    assert_eq!(
        BrowserMuxFrame::decode_exact(&noncanonical, BrowserMuxDirection::HostToJs),
        Err(BrowserBrokerWireError::InvalidCanonicalRuntimeJson)
    );

    let mut wrong_order = canonical[..44].to_vec();
    wrong_order.extend_from_slice(br#"{"message":"m","code":"E"}"#);
    let wrong_order_remaining = u32::try_from(wrong_order.len() - 4).unwrap();
    wrong_order[..4].copy_from_slice(&wrong_order_remaining.to_be_bytes());
    assert_eq!(
        BrowserMuxFrame::decode_exact(&wrong_order, BrowserMuxDirection::HostToJs),
        Err(BrowserBrokerWireError::InvalidPayload)
    );

    for invalid_exit in [
        broker_object([
            ("code", RuntimeJson::from(1.5)),
            ("signal", RuntimeJson::Null),
        ]),
        broker_object([
            ("code", RuntimeJson::Null),
            ("signal", RuntimeJson::from(9)),
        ]),
    ] {
        assert_eq!(
            BrowserMuxFrame::new(
                BrowserMuxDirection::HostToJs,
                1,
                1,
                0,
                BrowserMuxKind::Exit,
                1,
                0,
                0,
                BrowserMuxPayload::RuntimeJson(invalid_exit),
            ),
            Err(BrowserBrokerWireError::InvalidPayload)
        );
    }
    let mut reader = ExactMuxFrameReader::default();
    assert_eq!(
        reader.feed(&(256_u32 * 1024 * 1024 + 1).to_be_bytes()),
        Err(BrowserBrokerWireError::FrameTooLarge)
    );
}

#[test]
fn mux_state_enforces_sequence_class_credit_correlation_and_post_close() {
    let generation = 0x0102_0304_0506_0708;
    let object_id = 0x1112_1314_1516_1718;
    let mut state = BrowserMuxState::new(generation);
    state.register_committed_object(object_id).unwrap();

    let accept = |state: &mut BrowserMuxState,
                  direction,
                  kind,
                  stream,
                  sequence,
                  correlation,
                  credit,
                  payload| {
        let frame = BrowserMuxFrame::new(
            direction,
            generation,
            object_id,
            stream,
            kind,
            sequence,
            correlation,
            credit,
            payload,
        )
        .unwrap();
        state.accept(&frame)
    };

    accept(
        &mut state,
        BrowserMuxDirection::JsToHost,
        BrowserMuxKind::ReadCredit,
        1,
        1,
        0,
        1,
        BrowserMuxPayload::Empty,
    )
    .unwrap();
    accept(
        &mut state,
        BrowserMuxDirection::HostToJs,
        BrowserMuxKind::StdoutData,
        1,
        1,
        0,
        0,
        BrowserMuxPayload::Bytes(vec![1]),
    )
    .unwrap();
    assert_eq!(
        accept(
            &mut state,
            BrowserMuxDirection::HostToJs,
            BrowserMuxKind::StdoutData,
            1,
            2,
            0,
            0,
            BrowserMuxPayload::Bytes(vec![2])
        ),
        Err(BrowserBrokerWireError::CreditNotAvailable)
    );
    accept(
        &mut state,
        BrowserMuxDirection::JsToHost,
        BrowserMuxKind::ReadCredit,
        1,
        2,
        1,
        1,
        BrowserMuxPayload::Empty,
    )
    .unwrap();
    accept(
        &mut state,
        BrowserMuxDirection::HostToJs,
        BrowserMuxKind::StdoutData,
        1,
        2,
        0,
        0,
        BrowserMuxPayload::Bytes(vec![2]),
    )
    .unwrap();
    assert_eq!(
        accept(
            &mut state,
            BrowserMuxDirection::JsToHost,
            BrowserMuxKind::ReadCredit,
            1,
            4,
            2,
            1,
            BrowserMuxPayload::Empty
        ),
        Err(BrowserBrokerWireError::InvalidSequence)
    );

    accept(
        &mut state,
        BrowserMuxDirection::JsToHost,
        BrowserMuxKind::Write,
        3,
        1,
        0,
        0,
        BrowserMuxPayload::Bytes(vec![3]),
    )
    .unwrap();
    assert_eq!(
        accept(
            &mut state,
            BrowserMuxDirection::HostToJs,
            BrowserMuxKind::WriteAck,
            3,
            1,
            2,
            0,
            BrowserMuxPayload::Empty
        ),
        Err(BrowserBrokerWireError::InvalidCorrelation)
    );
    accept(
        &mut state,
        BrowserMuxDirection::HostToJs,
        BrowserMuxKind::WriteAck,
        3,
        1,
        1,
        0,
        BrowserMuxPayload::Empty,
    )
    .unwrap();
    accept(
        &mut state,
        BrowserMuxDirection::JsToHost,
        BrowserMuxKind::HalfClose,
        3,
        1,
        1,
        0,
        BrowserMuxPayload::Empty,
    )
    .unwrap();
    assert_eq!(
        accept(
            &mut state,
            BrowserMuxDirection::JsToHost,
            BrowserMuxKind::Write,
            3,
            2,
            0,
            0,
            BrowserMuxPayload::Bytes(vec![4])
        ),
        Err(BrowserBrokerWireError::StreamHalfClosed)
    );

    accept(
        &mut state,
        BrowserMuxDirection::JsToHost,
        BrowserMuxKind::CloseHooksReady,
        0,
        1,
        0,
        0,
        BrowserMuxPayload::Empty,
    )
    .unwrap();
    accept(
        &mut state,
        BrowserMuxDirection::HostToJs,
        BrowserMuxKind::Exit,
        0,
        1,
        0,
        0,
        BrowserMuxPayload::RuntimeJson(broker_object([
            ("code", RuntimeJson::Null),
            ("signal", RuntimeJson::Null),
        ])),
    )
    .unwrap();
    accept(
        &mut state,
        BrowserMuxDirection::HostToJs,
        BrowserMuxKind::Close,
        0,
        2,
        1,
        0,
        BrowserMuxPayload::Empty,
    )
    .unwrap();
    accept(
        &mut state,
        BrowserMuxDirection::HostToJs,
        BrowserMuxKind::CleanupComplete,
        0,
        3,
        2,
        0,
        BrowserMuxPayload::RuntimeJson(broker_object([
            ("ok", RuntimeJson::Bool(true)),
            ("error", RuntimeJson::Null),
        ])),
    )
    .unwrap();
    assert_eq!(
        accept(
            &mut state,
            BrowserMuxDirection::HostToJs,
            BrowserMuxKind::StreamClose,
            1,
            1,
            0,
            0,
            BrowserMuxPayload::Empty
        ),
        Err(BrowserBrokerWireError::ObjectClosed)
    );
}

#[test]
fn mux_state_binds_graceful_force_and_stream_close_receipts() {
    let generation = 0x0102_0304_0506_0708;
    let object_id = 0x1112_1314_1516_1718;
    let result = || {
        BrowserMuxPayload::RuntimeJson(broker_object([
            ("ok", RuntimeJson::Bool(true)),
            ("error", RuntimeJson::Null),
        ]))
    };
    let mut state = BrowserMuxState::new(generation);
    state.register_committed_object(object_id).unwrap();
    let make = |direction, kind, stream, sequence, correlation, payload| {
        BrowserMuxFrame::new(
            direction,
            generation,
            object_id,
            stream,
            kind,
            sequence,
            correlation,
            0,
            payload,
        )
        .unwrap()
    };
    state
        .accept(&make(
            BrowserMuxDirection::JsToHost,
            BrowserMuxKind::CloseHooksReady,
            0,
            1,
            0,
            BrowserMuxPayload::Empty,
        ))
        .unwrap();
    let graceful = make(
        BrowserMuxDirection::HostToJs,
        BrowserMuxKind::BeginGracefulClose,
        0,
        1,
        0,
        BrowserMuxPayload::Empty,
    );
    state.accept(&graceful).unwrap();
    let bad_result = make(
        BrowserMuxDirection::JsToHost,
        BrowserMuxKind::GracefulCloseResult,
        0,
        2,
        2,
        result(),
    );
    assert_eq!(
        state.accept(&bad_result),
        Err(BrowserBrokerWireError::InvalidCorrelation)
    );
    state
        .accept(&make(
            BrowserMuxDirection::JsToHost,
            BrowserMuxKind::GracefulCloseResult,
            0,
            2,
            1,
            result(),
        ))
        .unwrap();
    state
        .accept(&make(
            BrowserMuxDirection::HostToJs,
            BrowserMuxKind::BeginForceClose,
            0,
            2,
            1,
            BrowserMuxPayload::Empty,
        ))
        .unwrap();
    state
        .accept(&make(
            BrowserMuxDirection::JsToHost,
            BrowserMuxKind::ForceCloseResult,
            0,
            3,
            2,
            result(),
        ))
        .unwrap();

    let request = make(
        BrowserMuxDirection::JsToHost,
        BrowserMuxKind::StreamClose,
        4,
        1,
        0,
        BrowserMuxPayload::Empty,
    );
    state.accept(&request).unwrap();
    let bad_ack = make(
        BrowserMuxDirection::HostToJs,
        BrowserMuxKind::StreamClose,
        4,
        1,
        2,
        BrowserMuxPayload::Empty,
    );
    assert_eq!(
        state.accept(&bad_ack),
        Err(BrowserBrokerWireError::InvalidCorrelation)
    );
    state
        .accept(&make(
            BrowserMuxDirection::HostToJs,
            BrowserMuxKind::StreamClose,
            4,
            1,
            1,
            BrowserMuxPayload::Empty,
        ))
        .unwrap();
    assert_eq!(
        state.accept(&make(
            BrowserMuxDirection::JsToHost,
            BrowserMuxKind::StreamError,
            4,
            2,
            0,
            BrowserMuxPayload::RuntimeJson(broker_object([
                ("code", RuntimeJson::from("E")),
                ("message", RuntimeJson::from("late"))
            ]))
        )),
        Err(BrowserBrokerWireError::StreamClosed)
    );
}

#[test]
fn mux_zero_close_is_bound_to_explicit_pidless_spawn_failure_admission() {
    let generation = 17;
    let committed_id = 71;
    let pidless_id = 72;
    let mut state = BrowserMuxState::new(generation);
    state.register_committed_object(committed_id).unwrap();
    assert_eq!(
        state.register_pidless_spawn_failure(committed_id),
        Err(BrowserBrokerWireError::DuplicateObjectId),
        "an admitted object cannot be relabelled as a pid-less failure"
    );
    assert!(
        !include_str!("broker_wire.rs").contains("pub fn register_object("),
        "there is no outcome-free object admission API"
    );

    let frame = |object_id, direction, kind, sequence, correlation, payload| {
        BrowserMuxFrame::new(
            direction,
            generation,
            object_id,
            0,
            kind,
            sequence,
            correlation,
            0,
            payload,
        )
        .unwrap()
    };
    state
        .accept(&frame(
            committed_id,
            BrowserMuxDirection::JsToHost,
            BrowserMuxKind::CloseHooksReady,
            1,
            0,
            BrowserMuxPayload::Empty,
        ))
        .unwrap();
    state
        .accept(&frame(
            committed_id,
            BrowserMuxDirection::HostToJs,
            BrowserMuxKind::BeginGracefulClose,
            1,
            0,
            BrowserMuxPayload::Empty,
        ))
        .unwrap();
    assert_eq!(
        state.accept(&frame(
            committed_id,
            BrowserMuxDirection::HostToJs,
            BrowserMuxKind::Close,
            2,
            0,
            BrowserMuxPayload::Empty,
        )),
        Err(BrowserBrokerWireError::InvalidSpawnOutcome),
        "a committed child cannot forge the pid-less Close(0) path"
    );
    state
        .accept(&frame(
            committed_id,
            BrowserMuxDirection::HostToJs,
            BrowserMuxKind::Exit,
            2,
            0,
            BrowserMuxPayload::RuntimeJson(broker_object([
                ("code", RuntimeJson::from(0)),
                ("signal", RuntimeJson::Null),
            ])),
        ))
        .unwrap();
    state
        .accept(&frame(
            committed_id,
            BrowserMuxDirection::HostToJs,
            BrowserMuxKind::Close,
            3,
            2,
            BrowserMuxPayload::Empty,
        ))
        .unwrap();

    state.register_pidless_spawn_failure(pidless_id).unwrap();
    assert_eq!(
        state.accept(&frame(
            pidless_id,
            BrowserMuxDirection::JsToHost,
            BrowserMuxKind::CloseHooksReady,
            1,
            0,
            BrowserMuxPayload::Empty,
        )),
        Err(BrowserBrokerWireError::InvalidSpawnOutcome)
    );
    state
        .accept(&frame(
            pidless_id,
            BrowserMuxDirection::HostToJs,
            BrowserMuxKind::Close,
            1,
            0,
            BrowserMuxPayload::Empty,
        ))
        .unwrap();
    state
        .accept(&frame(
            pidless_id,
            BrowserMuxDirection::HostToJs,
            BrowserMuxKind::CleanupComplete,
            2,
            1,
            BrowserMuxPayload::RuntimeJson(broker_object([
                ("ok", RuntimeJson::Bool(true)),
                ("error", RuntimeJson::Null),
            ])),
        ))
        .unwrap();
}

#[test]
fn mux_reader_stops_exactly_before_the_next_frame() {
    let first = mux_frame(
        BrowserMuxDirection::HostToJs,
        BrowserMuxKind::StdoutData,
        1,
        1,
        0,
        0,
        BrowserMuxPayload::Bytes(vec![1, 2, 3]),
    );
    let second = mux_frame(
        BrowserMuxDirection::HostToJs,
        BrowserMuxKind::StderrData,
        2,
        1,
        0,
        0,
        BrowserMuxPayload::Bytes(vec![4]),
    );
    let first_bytes = first.encode().unwrap();
    let second_bytes = second.encode().unwrap();
    let joined = [first_bytes.as_slice(), second_bytes.as_slice()].concat();
    let mut reader = ExactMuxFrameReader::default();
    assert_eq!(reader.feed(&joined).unwrap(), first_bytes.len());
    assert_eq!(reader.frame(), Some(first_bytes.as_slice()));
    assert_eq!(reader.feed(&second_bytes).unwrap(), 0);
    assert_eq!(
        BrowserMuxFrame::decode_exact(reader.frame().unwrap(), BrowserMuxDirection::HostToJs)
            .unwrap(),
        first
    );
}

#[test]
fn browser_host_tab_mentions_validate_exact_targets_and_permission_claims() {
    let mention = RuntimeJson::from("plugin://browser@klaus-bundled?mention=tab-v1&source=extension&browserId=chrome%3Aprofile-1&tabId=42&title=Orders&url=https%3A%2F%2Fshop.test%2Forders");
    let operation = browser_object([
        ("type", RuntimeJson::from("resolve_tab")),
        ("mention", mention),
    ]);
    assert!(matches!(
        browser_request_result(operation.clone()),
        BrowserHostSendResult::Admitted(_)
    ));
    assert!(matches!(
        browser_request_result(browser_object(
            [("type", RuntimeJson::from("resolve_tab")),]
        )),
        BrowserHostSendResult::ValidationResponse
    ));
    assert!(matches!(
        browser_request_result_with(
            browser_object([("type", RuntimeJson::from("claim_tab"))]),
            browser_object([
                ("browserId", RuntimeJson::from("chrome:profile-1")),
                ("tabId", RuntimeJson::from("42")),
            ]),
            RuntimeJson::from(30_000),
            browser_object([]),
        ),
        BrowserHostSendResult::Admitted(_)
    ));
    assert!(matches!(
        browser_request_result_with(
            operation.clone(),
            browser_object([("browserId", RuntimeJson::Bool(true))]),
            RuntimeJson::from(30_000),
            browser_object([]),
        ),
        BrowserHostSendResult::ValidationResponse
    ));
    let mut permissions = browser_permissions_for("resolve_tab");
    permissions.manage_tabs = false;
    let mut endpoint = BrowserHostEndpoint::new(BrowserHostPolicy::default());
    endpoint.open(host_configuration()).unwrap();
    assert_eq!(
        endpoint.send(
            &browser_request_bytes_with_permissions(
                "tab-mention-no-ownership",
                operation,
                permissions
            ),
            0
        ),
        BrowserHostSendResult::ValidationResponse
    );
}
