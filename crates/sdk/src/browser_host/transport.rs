use std::collections::{HashSet, VecDeque};

use klaus_agent_core::runtime_values::{
    runtime_json, CanonicalRuntimeJsonUtf8, RuntimeJson, RuntimeObject, RuntimePath, RuntimeString,
};

const INVALID_CANONICAL_BYTES: &str = "Invalid canonical browser RuntimeJson bytes.";
const SIDECAR_EXITED: &str = "Browser sidecar exited.";
const ENDPOINT_ALREADY_OPEN: &str = "Browser host transport endpoint is already open";
const REQUEST_VALIDATION_FAILED: &str = "Browser host request validation failed.";
const REQUEST_POLICY_DENIED: &str = "Browser request is not permitted by the Rust host policy.";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BrowserHostConfiguration {
    pub artifact_dir: Option<RuntimePath>,
    pub headless: bool,
    pub backend: RuntimeString,
    pub cdp_endpoint: Option<RuntimeString>,
    pub user_data_dir: Option<RuntimePath>,
    pub channel: Option<RuntimeString>,
    pub executable_path: Option<RuntimePath>,
}

impl BrowserHostConfiguration {
    fn is_valid(&self) -> bool {
        !self.backend.is_empty()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct BrowserHostPermissionSet {
    pub navigate: bool,
    pub read_page: bool,
    pub capture_screenshot: bool,
    pub interact: bool,
    pub javascript_eval: bool,
    pub read_console: bool,
    pub read_network: bool,
    pub manage_tabs: bool,
    pub session_state: bool,
    pub file_transfer: bool,
}

impl BrowserHostPermissionSet {
    pub(super) const ALL: Self = Self {
        navigate: true,
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

    fn permits(self, required: Self) -> bool {
        (!required.navigate || self.navigate)
            && (!required.read_page || self.read_page)
            && (!required.capture_screenshot || self.capture_screenshot)
            && (!required.interact || self.interact)
            && (!required.javascript_eval || self.javascript_eval)
            && (!required.read_console || self.read_console)
            && (!required.read_network || self.read_network)
            && (!required.manage_tabs || self.manage_tabs)
            && (!required.session_state || self.session_state)
            && (!required.file_transfer || self.file_transfer)
    }

    fn union(self, other: Self) -> Self {
        Self {
            navigate: self.navigate || other.navigate,
            read_page: self.read_page || other.read_page,
            capture_screenshot: self.capture_screenshot || other.capture_screenshot,
            interact: self.interact || other.interact,
            javascript_eval: self.javascript_eval || other.javascript_eval,
            read_console: self.read_console || other.read_console,
            read_network: self.read_network || other.read_network,
            manage_tabs: self.manage_tabs || other.manage_tabs,
            session_state: self.session_state || other.session_state,
            file_transfer: self.file_transfer || other.file_transfer,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct BrowserHostPolicy {
    pub permissions: BrowserHostPermissionSet,
}

impl Default for BrowserHostPolicy {
    fn default() -> Self {
        Self {
            permissions: BrowserHostPermissionSet::ALL,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BrowserHostEndpointPhase {
    Idle,
    Registering,
    Registered,
    Closing,
    Closed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BrowserHostError {
    pub code: &'static str,
    pub message: &'static str,
}

impl BrowserHostError {
    fn host(message: &'static str) -> Self {
        Self {
            code: "host_error",
            message,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum BrowserHostCallbackKind {
    Message(Vec<u8>),
    SendDone(Option<BrowserHostError>),
    Close(Option<BrowserHostError>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BrowserHostCallback {
    pub sequence: u64,
    pub endpoint_generation: u64,
    pub kind: BrowserHostCallbackKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BrowserHostAdmission {
    pub endpoint_generation: u64,
    pub id: RuntimeString,
    pub operation_type: RuntimeString,
    pub encoded: Vec<u8>,
    pub timeout_ms: u64,
    pub deadline_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum BrowserHostSendResult {
    Admitted(BrowserHostAdmission),
    DeferredError,
    ValidationResponse,
}

#[derive(Debug)]
pub(super) struct BrowserHostEndpoint {
    phase: BrowserHostEndpointPhase,
    next_generation: u64,
    generation: Option<u64>,
    configuration: Option<BrowserHostConfiguration>,
    policy: BrowserHostPolicy,
    request_ids: HashSet<RuntimeString>,
    next_callback_sequence: u64,
    callbacks: VecDeque<BrowserHostCallback>,
}

impl BrowserHostEndpoint {
    pub fn new(policy: BrowserHostPolicy) -> Self {
        Self {
            phase: BrowserHostEndpointPhase::Idle,
            next_generation: 0,
            generation: None,
            configuration: None,
            policy,
            request_ids: HashSet::new(),
            next_callback_sequence: 0,
            callbacks: VecDeque::new(),
        }
    }

    pub fn phase(&self) -> BrowserHostEndpointPhase {
        self.phase
    }

    pub fn generation(&self) -> Option<u64> {
        self.generation
    }

    pub fn open(
        &mut self,
        configuration: BrowserHostConfiguration,
    ) -> Result<u64, BrowserHostError> {
        if matches!(
            self.phase,
            BrowserHostEndpointPhase::Registering | BrowserHostEndpointPhase::Registered
        ) {
            return Err(BrowserHostError::host(ENDPOINT_ALREADY_OPEN));
        }
        if !configuration.is_valid() {
            return Err(BrowserHostError::host(REQUEST_VALIDATION_FAILED));
        }
        self.phase = BrowserHostEndpointPhase::Registering;
        let generation = self
            .next_generation
            .checked_add(1)
            .ok_or_else(|| BrowserHostError::host(REQUEST_VALIDATION_FAILED))?;
        self.next_generation = generation;
        self.generation = Some(generation);
        self.configuration = Some(configuration);
        self.request_ids.clear();
        self.phase = BrowserHostEndpointPhase::Registered;
        Ok(generation)
    }

    pub fn send(&mut self, encoded: &[u8], now_ms: u64) -> BrowserHostSendResult {
        let Some(generation) = self
            .generation
            .filter(|_| self.phase == BrowserHostEndpointPhase::Registered)
        else {
            self.push_callback(
                0,
                BrowserHostCallbackKind::SendDone(Some(BrowserHostError::host(SIDECAR_EXITED))),
            );
            return BrowserHostSendResult::DeferredError;
        };
        let value = match CanonicalRuntimeJsonUtf8::decode(encoded) {
            Ok(value) => value,
            Err(_) => {
                self.push_callback(
                    generation,
                    BrowserHostCallbackKind::SendDone(Some(BrowserHostError::host(
                        INVALID_CANONICAL_BYTES,
                    ))),
                );
                return BrowserHostSendResult::DeferredError;
            }
        };
        let configuration = self
            .configuration
            .as_ref()
            .expect("registered endpoint has immutable configuration");
        let validated = match validate_request(&value, configuration) {
            Ok(validated) => validated,
            Err(error) => {
                return self.validation_response(generation, error.id, error.message);
            }
        };
        if !self
            .policy
            .permissions
            .permits(validated.required_permissions)
        {
            return self.validation_response(generation, Some(validated.id), REQUEST_POLICY_DENIED);
        }
        if !self.request_ids.insert(validated.id.clone()) {
            return self.validation_response(
                generation,
                Some(validated.id),
                REQUEST_VALIDATION_FAILED,
            );
        }
        let Some(deadline_ms) = now_ms.checked_add(validated.timeout_ms) else {
            self.request_ids.remove(&validated.id);
            return self.validation_response(
                generation,
                Some(validated.id),
                REQUEST_VALIDATION_FAILED,
            );
        };
        BrowserHostSendResult::Admitted(BrowserHostAdmission {
            endpoint_generation: generation,
            id: validated.id,
            operation_type: validated.operation_type,
            encoded: encoded.to_vec(),
            timeout_ms: validated.timeout_ms,
            deadline_ms,
        })
    }

    pub fn complete_send(&mut self, generation: u64, error: Option<BrowserHostError>) -> bool {
        if self.generation != Some(generation) || self.phase != BrowserHostEndpointPhase::Registered
        {
            return false;
        }
        self.push_callback(generation, BrowserHostCallbackKind::SendDone(error));
        true
    }

    pub fn message(&mut self, generation: u64, encoded: &[u8]) -> bool {
        if self.generation != Some(generation) || self.phase != BrowserHostEndpointPhase::Registered
        {
            return false;
        }
        self.push_callback(
            generation,
            BrowserHostCallbackKind::Message(encoded.to_vec()),
        );
        true
    }

    pub fn generation_closed(&mut self, generation: u64, error: BrowserHostError) -> bool {
        if self.generation != Some(generation) || self.phase != BrowserHostEndpointPhase::Registered
        {
            return false;
        }
        self.phase = BrowserHostEndpointPhase::Closed;
        self.configuration = None;
        self.request_ids.clear();
        self.push_callback(generation, BrowserHostCallbackKind::Close(Some(error)));
        true
    }

    pub fn close(&mut self) {
        if matches!(
            self.phase,
            BrowserHostEndpointPhase::Idle | BrowserHostEndpointPhase::Closed
        ) {
            return;
        }
        let generation = self.generation.unwrap_or(0);
        self.phase = BrowserHostEndpointPhase::Closing;
        self.configuration = None;
        self.request_ids.clear();
        self.phase = BrowserHostEndpointPhase::Closed;
        self.push_callback(generation, BrowserHostCallbackKind::Close(None));
    }

    pub fn pop_callback(&mut self) -> Option<BrowserHostCallback> {
        self.callbacks.pop_front()
    }

    fn validation_response(
        &mut self,
        generation: u64,
        id: Option<RuntimeString>,
        message: &'static str,
    ) -> BrowserHostSendResult {
        let Some(id) = id else {
            self.push_callback(
                generation,
                BrowserHostCallbackKind::SendDone(Some(BrowserHostError::host(message))),
            );
            return BrowserHostSendResult::DeferredError;
        };
        self.push_callback(generation, BrowserHostCallbackKind::SendDone(None));
        self.push_callback(
            generation,
            BrowserHostCallbackKind::Message(error_response(&id, message)),
        );
        BrowserHostSendResult::ValidationResponse
    }

    fn push_callback(&mut self, generation: u64, kind: BrowserHostCallbackKind) {
        self.next_callback_sequence = self
            .next_callback_sequence
            .checked_add(1)
            .expect("browser callback sequence exhausted");
        self.callbacks.push_back(BrowserHostCallback {
            sequence: self.next_callback_sequence,
            endpoint_generation: generation,
            kind,
        });
    }
}

struct ValidatedRequest {
    id: RuntimeString,
    operation_type: RuntimeString,
    required_permissions: BrowserHostPermissionSet,
    timeout_ms: u64,
}

struct ValidationFailure {
    id: Option<RuntimeString>,
    message: &'static str,
}

fn validate_request(
    value: &RuntimeJson,
    configuration: &BrowserHostConfiguration,
) -> Result<ValidatedRequest, ValidationFailure> {
    let envelope = as_object(value, None)?;
    exact_keys(envelope, &["id", "request", "options"], None)?;
    let id = string_field(envelope, "id", None)?.clone();
    let request = object_field(envelope, "request", Some(id.clone()))?;
    exact_keys(
        request,
        &[
            "operation",
            "target",
            "permissions",
            "timeoutMs",
            "metadata",
        ],
        Some(id.clone()),
    )?;
    let operation = object_field(request, "operation", Some(id.clone()))?;
    let operation_type = validate_operation(operation, &id)?.clone();
    validate_target(object_field(request, "target", Some(id.clone()))?, &id)?;
    let claimed =
        validate_permissions(object_field(request, "permissions", Some(id.clone()))?, &id)?;
    let expected = permissions_for(&operation_type)
        .ok_or_else(|| failure(Some(id.clone()), REQUEST_VALIDATION_FAILED))?;
    if claimed != expected {
        return Err(failure(Some(id), REQUEST_VALIDATION_FAILED));
    }
    let timeout_ms = timeout_field(request, "timeoutMs", &id)?;
    if !matches!(request.get("metadata"), Some(RuntimeJson::Object(_))) {
        return Err(failure(Some(id), REQUEST_VALIDATION_FAILED));
    }
    validate_options(
        object_field(envelope, "options", Some(id.clone()))?,
        configuration,
        &id,
    )?;
    let required_permissions = policy_permissions_for_operation(operation)
        .ok_or_else(|| failure(Some(id.clone()), REQUEST_VALIDATION_FAILED))?;
    Ok(ValidatedRequest {
        id,
        operation_type,
        required_permissions,
        timeout_ms,
    })
}

#[derive(Clone, Copy)]
enum BrowserFieldKind {
    Literal(&'static str),
    String,
    Bool,
    Number,
    NonNegativeSafeInteger,
    SafeInteger,
    StringArray,
    NumberArray,
    Object,
    Viewport,
    Json,
    Enum(&'static [&'static str]),
    EnumArray(&'static [&'static str]),
    LocatorFilter,
    OperationArray,
}

#[derive(Clone, Copy)]
struct BrowserField {
    name: &'static str,
    kind: BrowserFieldKind,
    required: bool,
}

const fn required_field(name: &'static str, kind: BrowserFieldKind) -> BrowserField {
    BrowserField {
        name,
        kind,
        required: true,
    }
}

const fn optional_field(name: &'static str, kind: BrowserFieldKind) -> BrowserField {
    BrowserField {
        name,
        kind,
        required: false,
    }
}

const WAIT_UNTIL: &[&str] = &["commit", "dom_content_loaded", "load", "network_idle"];
const SCREENSHOT_FORMAT: &[&str] = &["png", "jpeg"];
const MOUSE_BUTTON: &[&str] = &["left", "middle", "right"];
const CONSOLE_LEVEL: &[&str] = &["debug", "info", "warning", "error", "log"];
const CONTENT_FORMAT: &[&str] = &["text", "markdown", "md", "html"];
const CLIPBOARD_ACTION: &[&str] = &["read", "read_text", "write", "write_text"];
const DIALOG_ACTION: &[&str] = &["get", "accept", "dismiss"];
const TAB_MARK_STATUS: &[&str] = &["deliverable", "handoff"];
const PLAYWRIGHT_EVENT: &[&str] = &["download", "filechooser"];
const READ_PAGE_FILTER: &[&str] = &["interactive", "all"];
const COMPUTER_ACTION: &[&str] = &[
    "left_click",
    "right_click",
    "double_click",
    "triple_click",
    "type",
    "screenshot",
    "wait",
    "scroll",
    "key",
    "left_click_drag",
    "zoom",
    "scroll_to",
    "hover",
];
const SCROLL_DIRECTION: &[&str] = &["up", "down", "left", "right"];
const TABS_ACTION: &[&str] = &[
    "list",
    "user",
    "new",
    "get",
    "selected",
    "close",
    "finalize",
    "name_session",
];
const LOCATOR_ACTION: &[&str] = &[
    "click",
    "double_click",
    "type",
    "fill",
    "inner_text",
    "get_attribute",
    "is_enabled",
    "press",
    "count",
    "text",
    "text_content",
    "all_text_contents",
    "is_visible",
    "wait_for",
    "screenshot",
    "check",
    "uncheck",
    "set_checked",
    "select_option",
];
const CLOSE_TARGET: &[&str] = &["tab", "session", "all"];

const COMPUTER_FIELDS: &[BrowserField] = &[
    required_field("type", BrowserFieldKind::Literal("computer")),
    required_field("action", BrowserFieldKind::Enum(COMPUTER_ACTION)),
    optional_field("coordinate", BrowserFieldKind::NumberArray),
    optional_field("startCoordinate", BrowserFieldKind::NumberArray),
    optional_field("text", BrowserFieldKind::String),
    optional_field("delayMs", BrowserFieldKind::NonNegativeSafeInteger),
    optional_field("duration", BrowserFieldKind::Number),
    optional_field("scrollDirection", BrowserFieldKind::Enum(SCROLL_DIRECTION)),
    optional_field("amount", BrowserFieldKind::Number),
    optional_field("refId", BrowserFieldKind::String),
    optional_field("ref", BrowserFieldKind::String),
    optional_field("save", BrowserFieldKind::Bool),
];

const VIEWPORT_FIELDS: &[BrowserField] = &[
    required_field("width", BrowserFieldKind::NonNegativeSafeInteger),
    required_field("height", BrowserFieldKind::NonNegativeSafeInteger),
    optional_field("deviceScaleFactor", BrowserFieldKind::Number),
    optional_field("isMobile", BrowserFieldKind::Bool),
];

const LOCATOR_FILTER_FIELDS: &[BrowserField] = &[
    optional_field("hasText", BrowserFieldKind::String),
    optional_field("hasNotText", BrowserFieldKind::String),
    optional_field("has", BrowserFieldKind::String),
    optional_field("hasNot", BrowserFieldKind::String),
];

fn validate_operation<'a>(
    operation: &'a RuntimeObject,
    id: &RuntimeString,
) -> Result<&'a RuntimeString, ValidationFailure> {
    let operation_type = string_field(operation, "type", Some(id.clone()))?;
    let mut pending = vec![operation];
    while let Some(current) = pending.pop() {
        let current_type = string_field(current, "type", Some(id.clone()))?;
        let Some(name) = known_operation_name(current_type) else {
            return Err(failure(Some(id.clone()), REQUEST_VALIDATION_FAILED));
        };
        if name == "computer" {
            validate_closed_fields(current, COMPUTER_FIELDS, id, &mut pending)?;
        } else {
            validate_operation_fields(current, name, id, &mut pending)?;
        }
    }
    Ok(operation_type)
}

#[allow(clippy::too_many_lines)]
fn validate_operation_fields<'a>(
    object: &'a RuntimeObject,
    operation: &str,
    id: &RuntimeString,
    pending_operations: &mut Vec<&'a RuntimeObject>,
) -> Result<(), ValidationFailure> {
    let fields: &[BrowserField] = match operation {
        "navigate" => &[
            required_field("type", BrowserFieldKind::Literal("navigate")),
            required_field("url", BrowserFieldKind::String),
            optional_field("waitUntil", BrowserFieldKind::Enum(WAIT_UNTIL)),
            optional_field("viewport", BrowserFieldKind::Viewport),
        ],
        "snapshot" => &[
            required_field("type", BrowserFieldKind::Literal("snapshot")),
            optional_field("selector", BrowserFieldKind::String),
            optional_field("includeText", BrowserFieldKind::Bool),
            optional_field("includeLinks", BrowserFieldKind::Bool),
        ],
        "screenshot" => &[
            required_field("type", BrowserFieldKind::Literal("screenshot")),
            optional_field("selector", BrowserFieldKind::String),
            optional_field("fullPage", BrowserFieldKind::Bool),
            optional_field("format", BrowserFieldKind::Enum(SCREENSHOT_FORMAT)),
            optional_field("quality", BrowserFieldKind::NonNegativeSafeInteger),
        ],
        "click" => &[
            required_field("type", BrowserFieldKind::Literal("click")),
            optional_field("selector", BrowserFieldKind::String),
            optional_field("x", BrowserFieldKind::Number),
            optional_field("y", BrowserFieldKind::Number),
            optional_field("button", BrowserFieldKind::Enum(MOUSE_BUTTON)),
            optional_field("clickCount", BrowserFieldKind::NonNegativeSafeInteger),
            optional_field("modifiers", BrowserFieldKind::StringArray),
        ],
        "type" => &[
            required_field("type", BrowserFieldKind::Literal("type")),
            optional_field("selector", BrowserFieldKind::String),
            required_field("text", BrowserFieldKind::String),
            optional_field("clear", BrowserFieldKind::Bool),
            optional_field("pressEnter", BrowserFieldKind::Bool),
            optional_field("delayMs", BrowserFieldKind::NonNegativeSafeInteger),
        ],
        "eval" => &[
            required_field("type", BrowserFieldKind::Literal("eval")),
            required_field("script", BrowserFieldKind::String),
            optional_field("args", BrowserFieldKind::Json),
            optional_field("awaitPromise", BrowserFieldKind::Bool),
        ],
        "console" => &[
            required_field("type", BrowserFieldKind::Literal("console")),
            optional_field("levels", BrowserFieldKind::EnumArray(CONSOLE_LEVEL)),
            optional_field("sinceMs", BrowserFieldKind::NonNegativeSafeInteger),
            optional_field("limit", BrowserFieldKind::NonNegativeSafeInteger),
            optional_field("clear", BrowserFieldKind::Bool),
        ],
        "network" => &[
            required_field("type", BrowserFieldKind::Literal("network")),
            optional_field("sinceMs", BrowserFieldKind::NonNegativeSafeInteger),
            optional_field("limit", BrowserFieldKind::NonNegativeSafeInteger),
            optional_field("includeBodies", BrowserFieldKind::Bool),
            optional_field("urlFilter", BrowserFieldKind::String),
        ],
        "gif_creator" => &[
            required_field("type", BrowserFieldKind::Literal("gif_creator")),
            optional_field("filename", BrowserFieldKind::String),
            optional_field("durationSeconds", BrowserFieldKind::Number),
            optional_field("fps", BrowserFieldKind::NonNegativeSafeInteger),
            optional_field("fullPage", BrowserFieldKind::Bool),
        ],
        "upload_image" => &[
            required_field("type", BrowserFieldKind::Literal("upload_image")),
            required_field("path", BrowserFieldKind::String),
            optional_field("selector", BrowserFieldKind::String),
            optional_field("prompt", BrowserFieldKind::String),
        ],
        "file_upload" => &[
            required_field("type", BrowserFieldKind::Literal("file_upload")),
            required_field("paths", BrowserFieldKind::StringArray),
            optional_field("selector", BrowserFieldKind::String),
            optional_field("refId", BrowserFieldKind::String),
        ],
        "shortcuts_list" => &[
            required_field("type", BrowserFieldKind::Literal("shortcuts_list")),
            optional_field("query", BrowserFieldKind::String),
        ],
        "shortcuts_execute" => &[
            required_field("type", BrowserFieldKind::Literal("shortcuts_execute")),
            required_field("name", BrowserFieldKind::String),
            optional_field("waitForCompletion", BrowserFieldKind::Bool),
        ],
        "switch_browser" => &[
            required_field("type", BrowserFieldKind::Literal("switch_browser")),
            optional_field("prompt", BrowserFieldKind::String),
            optional_field("waitMs", BrowserFieldKind::NonNegativeSafeInteger),
        ],
        "list_connected_browsers" => &[
            required_field("type", BrowserFieldKind::Literal("list_connected_browsers")),
            optional_field("localOnly", BrowserFieldKind::Bool),
        ],
        "select_browser" => &[
            required_field("type", BrowserFieldKind::Literal("select_browser")),
            required_field("deviceId", BrowserFieldKind::String),
        ],
        "resolve_tab" => &[
            required_field("type", BrowserFieldKind::Literal("resolve_tab")),
            required_field("mention", BrowserFieldKind::String),
        ],
        "claim_tab" => &[required_field(
            "type",
            BrowserFieldKind::Literal("claim_tab"),
        )],
        "history" => &[
            required_field("type", BrowserFieldKind::Literal("history")),
            optional_field("queries", BrowserFieldKind::StringArray),
            optional_field("limit", BrowserFieldKind::NonNegativeSafeInteger),
            optional_field("from", BrowserFieldKind::String),
            optional_field("to", BrowserFieldKind::String),
        ],
        "tabs_content" => &[
            required_field("type", BrowserFieldKind::Literal("tabs_content")),
            required_field("urls", BrowserFieldKind::StringArray),
            optional_field("contentType", BrowserFieldKind::Enum(CONTENT_FORMAT)),
            optional_field("maxChars", BrowserFieldKind::NonNegativeSafeInteger),
            optional_field("waitUntil", BrowserFieldKind::Enum(WAIT_UNTIL)),
        ],
        "content_export" => &[
            required_field("type", BrowserFieldKind::Literal("content_export")),
            optional_field("format", BrowserFieldKind::Enum(CONTENT_FORMAT)),
            optional_field("selector", BrowserFieldKind::String),
            optional_field("maxChars", BrowserFieldKind::NonNegativeSafeInteger),
        ],
        "download" => &[
            required_field("type", BrowserFieldKind::Literal("download")),
            optional_field("selector", BrowserFieldKind::String),
            optional_field("url", BrowserFieldKind::String),
            optional_field("x", BrowserFieldKind::Number),
            optional_field("y", BrowserFieldKind::Number),
            optional_field("filename", BrowserFieldKind::String),
        ],
        "clipboard" => &[
            required_field("type", BrowserFieldKind::Literal("clipboard")),
            required_field("action", BrowserFieldKind::Enum(CLIPBOARD_ACTION)),
            optional_field("text", BrowserFieldKind::String),
            optional_field("items", BrowserFieldKind::Json),
        ],
        "dialog" => &[
            required_field("type", BrowserFieldKind::Literal("dialog")),
            required_field("action", BrowserFieldKind::Enum(DIALOG_ACTION)),
            optional_field("promptText", BrowserFieldKind::String),
        ],
        "mark_tab" => &[
            required_field("type", BrowserFieldKind::Literal("mark_tab")),
            required_field("status", BrowserFieldKind::Enum(TAB_MARK_STATUS)),
        ],
        "playwright_wait_for_event" => &[
            required_field(
                "type",
                BrowserFieldKind::Literal("playwright_wait_for_event"),
            ),
            required_field("event", BrowserFieldKind::Enum(PLAYWRIGHT_EVENT)),
        ],
        "playwright_file_chooser_set_files" => &[
            required_field(
                "type",
                BrowserFieldKind::Literal("playwright_file_chooser_set_files"),
            ),
            required_field("chooserId", BrowserFieldKind::String),
            required_field("files", BrowserFieldKind::StringArray),
        ],
        "playwright_wait_for_load_state" => &[
            required_field(
                "type",
                BrowserFieldKind::Literal("playwright_wait_for_load_state"),
            ),
            optional_field("state", BrowserFieldKind::Enum(WAIT_UNTIL)),
        ],
        "playwright_wait_for_url" => &[
            required_field("type", BrowserFieldKind::Literal("playwright_wait_for_url")),
            required_field("url", BrowserFieldKind::String),
            optional_field("waitUntil", BrowserFieldKind::Enum(WAIT_UNTIL)),
        ],
        "playwright_dom_snapshot" => &[
            required_field("type", BrowserFieldKind::Literal("playwright_dom_snapshot")),
            optional_field("visibleOnly", BrowserFieldKind::Bool),
            optional_field("maxChars", BrowserFieldKind::NonNegativeSafeInteger),
        ],
        "playwright_element_info" => &[
            required_field("type", BrowserFieldKind::Literal("playwright_element_info")),
            optional_field("x", BrowserFieldKind::Number),
            optional_field("y", BrowserFieldKind::Number),
            optional_field("selector", BrowserFieldKind::String),
        ],
        "playwright_element_screenshot" => &[
            required_field(
                "type",
                BrowserFieldKind::Literal("playwright_element_screenshot"),
            ),
            optional_field("x", BrowserFieldKind::Number),
            optional_field("y", BrowserFieldKind::Number),
            optional_field("selector", BrowserFieldKind::String),
            optional_field("format", BrowserFieldKind::Enum(SCREENSHOT_FORMAT)),
        ],
        "read_page" => &[
            required_field("type", BrowserFieldKind::Literal("read_page")),
            optional_field("filter", BrowserFieldKind::Enum(READ_PAGE_FILTER)),
            optional_field("depth", BrowserFieldKind::NonNegativeSafeInteger),
            optional_field("refId", BrowserFieldKind::String),
            optional_field("maxChars", BrowserFieldKind::NonNegativeSafeInteger),
        ],
        "find" => &[
            required_field("type", BrowserFieldKind::Literal("find")),
            required_field("query", BrowserFieldKind::String),
            optional_field("limit", BrowserFieldKind::NonNegativeSafeInteger),
        ],
        "form_input" => &[
            required_field("type", BrowserFieldKind::Literal("form_input")),
            required_field("ref", BrowserFieldKind::String),
            required_field("value", BrowserFieldKind::Json),
        ],
        "tabs" => &[
            required_field("type", BrowserFieldKind::Literal("tabs")),
            required_field("action", BrowserFieldKind::Enum(TABS_ACTION)),
            optional_field("url", BrowserFieldKind::String),
            optional_field("name", BrowserFieldKind::String),
        ],
        "locator" => &[
            required_field("type", BrowserFieldKind::Literal("locator")),
            required_field("selector", BrowserFieldKind::String),
            optional_field("frameSelector", BrowserFieldKind::String),
            optional_field("nthIndex", BrowserFieldKind::SafeInteger),
            optional_field("filter", BrowserFieldKind::LocatorFilter),
            optional_field("andSelector", BrowserFieldKind::String),
            optional_field("orSelector", BrowserFieldKind::String),
            required_field("action", BrowserFieldKind::Enum(LOCATOR_ACTION)),
            optional_field("value", BrowserFieldKind::Json),
            required_field("options", BrowserFieldKind::Object),
        ],
        "page_text" => &[
            required_field("type", BrowserFieldKind::Literal("page_text")),
            optional_field("maxChars", BrowserFieldKind::NonNegativeSafeInteger),
        ],
        "batch" => &[
            required_field("type", BrowserFieldKind::Literal("batch")),
            required_field("actions", BrowserFieldKind::OperationArray),
        ],
        "resize" => &[
            required_field("type", BrowserFieldKind::Literal("resize")),
            required_field("viewport", BrowserFieldKind::Viewport),
        ],
        "close" => &[
            required_field("type", BrowserFieldKind::Literal("close")),
            optional_field("target", BrowserFieldKind::Enum(CLOSE_TARGET)),
        ],
        _ => return Err(failure(Some(id.clone()), REQUEST_VALIDATION_FAILED)),
    };
    validate_closed_fields(object, fields, id, pending_operations)
}

fn validate_closed_fields<'a>(
    object: &'a RuntimeObject,
    fields: &[BrowserField],
    id: &RuntimeString,
    pending_operations: &mut Vec<&'a RuntimeObject>,
) -> Result<(), ValidationFailure> {
    for (key, value) in object.entries() {
        let Some(field) = fields.iter().find(|field| key == field.name) else {
            return Err(failure(Some(id.clone()), REQUEST_VALIDATION_FAILED));
        };
        validate_field(value, field.kind, id, pending_operations)?;
    }
    if fields
        .iter()
        .any(|field| field.required && object.get(field.name).is_none())
    {
        return Err(failure(Some(id.clone()), REQUEST_VALIDATION_FAILED));
    }
    Ok(())
}

fn validate_field<'a>(
    value: &'a RuntimeJson,
    kind: BrowserFieldKind,
    id: &RuntimeString,
    pending_operations: &mut Vec<&'a RuntimeObject>,
) -> Result<(), ValidationFailure> {
    let valid = match kind {
        BrowserFieldKind::Literal(expected) => {
            matches!(value, RuntimeJson::String(actual) if actual == &RuntimeString::from(expected))
        }
        BrowserFieldKind::String => matches!(value, RuntimeJson::String(_)),
        BrowserFieldKind::Bool => matches!(value, RuntimeJson::Bool(_)),
        BrowserFieldKind::Number => is_finite_number(value),
        BrowserFieldKind::NonNegativeSafeInteger => is_safe_integer(value, false),
        BrowserFieldKind::SafeInteger => is_safe_integer(value, true),
        BrowserFieldKind::StringArray => matches!(
            value,
            RuntimeJson::Array(values)
                if values.iter().all(|value| matches!(value, RuntimeJson::String(_)))
        ),
        BrowserFieldKind::NumberArray => matches!(
            value,
            RuntimeJson::Array(values)
                if values.len() == 2 && values.iter().all(is_finite_number)
        ),
        BrowserFieldKind::Object => matches!(value, RuntimeJson::Object(_)),
        BrowserFieldKind::Viewport => {
            let RuntimeJson::Object(viewport) = value else {
                return Err(failure(Some(id.clone()), REQUEST_VALIDATION_FAILED));
            };
            validate_closed_fields(viewport, VIEWPORT_FIELDS, id, pending_operations)?;
            true
        }
        BrowserFieldKind::Json => true,
        BrowserFieldKind::Enum(values) => enum_value(value, values),
        BrowserFieldKind::EnumArray(allowed) => matches!(
            value,
            RuntimeJson::Array(values) if values.iter().all(|value| enum_value(value, allowed))
        ),
        BrowserFieldKind::LocatorFilter => {
            let RuntimeJson::Object(filter) = value else {
                return Err(failure(Some(id.clone()), REQUEST_VALIDATION_FAILED));
            };
            validate_closed_fields(filter, LOCATOR_FILTER_FIELDS, id, pending_operations)?;
            true
        }
        BrowserFieldKind::OperationArray => {
            let RuntimeJson::Array(values) = value else {
                return Err(failure(Some(id.clone()), REQUEST_VALIDATION_FAILED));
            };
            for value in values.iter().rev() {
                let RuntimeJson::Object(operation) = value else {
                    return Err(failure(Some(id.clone()), REQUEST_VALIDATION_FAILED));
                };
                pending_operations.push(operation);
            }
            true
        }
    };
    if valid {
        Ok(())
    } else {
        Err(failure(Some(id.clone()), REQUEST_VALIDATION_FAILED))
    }
}

fn is_finite_number(value: &RuntimeJson) -> bool {
    matches!(value, RuntimeJson::Number(value) if value.as_f64().is_finite())
}

fn is_safe_integer(value: &RuntimeJson, signed: bool) -> bool {
    let RuntimeJson::Number(value) = value else {
        return false;
    };
    let value = value.as_f64();
    value.is_finite()
        && value.fract() == 0.0
        && value <= 9_007_199_254_740_991.0
        && value
            >= if signed {
                -9_007_199_254_740_991.0
            } else {
                0.0
            }
}

fn enum_value(value: &RuntimeJson, allowed: &[&str]) -> bool {
    let RuntimeJson::String(value) = value else {
        return false;
    };
    allowed
        .iter()
        .any(|candidate| value == &RuntimeString::from(*candidate))
}

fn validate_options(
    options: &RuntimeObject,
    configuration: &BrowserHostConfiguration,
    id: &RuntimeString,
) -> Result<(), ValidationFailure> {
    let expected_keys: &[&str] = if configuration.artifact_dir.is_some() {
        &["artifactDir", "headless"]
    } else {
        &["headless"]
    };
    exact_keys(options, expected_keys, Some(id.clone()))?;
    if options.get("headless") != Some(&RuntimeJson::Bool(configuration.headless)) {
        return Err(failure(Some(id.clone()), REQUEST_VALIDATION_FAILED));
    }
    match (&configuration.artifact_dir, options.get("artifactDir")) {
        (None, None) => {}
        (Some(expected), Some(RuntimeJson::String(actual)))
            if actual == expected.as_runtime_string() => {}
        _ => return Err(failure(Some(id.clone()), REQUEST_VALIDATION_FAILED)),
    }
    Ok(())
}

fn validate_target(target: &RuntimeObject, id: &RuntimeString) -> Result<(), ValidationFailure> {
    for (key, value) in target.entries() {
        if key != "sessionId" && key != "tabId" && key != "browserId" {
            return Err(failure(Some(id.clone()), REQUEST_VALIDATION_FAILED));
        }
        if !matches!(value, RuntimeJson::String(_)) {
            return Err(failure(Some(id.clone()), REQUEST_VALIDATION_FAILED));
        }
    }
    Ok(())
}

fn validate_permissions(
    value: &RuntimeObject,
    id: &RuntimeString,
) -> Result<BrowserHostPermissionSet, ValidationFailure> {
    const KEYS: &[&str] = &[
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
    ];
    exact_keys(value, KEYS, Some(id.clone()))?;
    Ok(BrowserHostPermissionSet {
        navigate: bool_field(value, KEYS[0], id)?,
        read_page: bool_field(value, KEYS[1], id)?,
        capture_screenshot: bool_field(value, KEYS[2], id)?,
        interact: bool_field(value, KEYS[3], id)?,
        javascript_eval: bool_field(value, KEYS[4], id)?,
        read_console: bool_field(value, KEYS[5], id)?,
        read_network: bool_field(value, KEYS[6], id)?,
        manage_tabs: bool_field(value, KEYS[7], id)?,
        session_state: bool_field(value, KEYS[8], id)?,
        file_transfer: bool_field(value, KEYS[9], id)?,
    })
}

fn bool_field(
    object: &RuntimeObject,
    key: &str,
    id: &RuntimeString,
) -> Result<bool, ValidationFailure> {
    match object.get(key) {
        Some(RuntimeJson::Bool(value)) => Ok(*value),
        _ => Err(failure(Some(id.clone()), REQUEST_VALIDATION_FAILED)),
    }
}

fn timeout_field(
    object: &RuntimeObject,
    key: &str,
    id: &RuntimeString,
) -> Result<u64, ValidationFailure> {
    let Some(RuntimeJson::Number(value)) = object.get(key) else {
        return Err(failure(Some(id.clone()), REQUEST_VALIDATION_FAILED));
    };
    let value = value.as_f64();
    if !value.is_finite() || value < 0.0 || value.fract() != 0.0 || value > 9_007_199_254_740_991.0
    {
        return Err(failure(Some(id.clone()), REQUEST_VALIDATION_FAILED));
    }
    Ok(value as u64)
}

fn as_object(
    value: &RuntimeJson,
    id: Option<RuntimeString>,
) -> Result<&RuntimeObject, ValidationFailure> {
    match value {
        RuntimeJson::Object(value) => Ok(value),
        _ => Err(failure(id, REQUEST_VALIDATION_FAILED)),
    }
}

fn object_field<'a>(
    object: &'a RuntimeObject,
    key: &str,
    id: Option<RuntimeString>,
) -> Result<&'a RuntimeObject, ValidationFailure> {
    object
        .get(key)
        .ok_or_else(|| failure(id.clone(), REQUEST_VALIDATION_FAILED))
        .and_then(|value| as_object(value, id))
}

fn string_field<'a>(
    object: &'a RuntimeObject,
    key: &str,
    id: Option<RuntimeString>,
) -> Result<&'a RuntimeString, ValidationFailure> {
    match object.get(key) {
        Some(RuntimeJson::String(value)) => Ok(value),
        _ => Err(failure(id, REQUEST_VALIDATION_FAILED)),
    }
}

fn exact_keys(
    object: &RuntimeObject,
    expected: &[&str],
    id: Option<RuntimeString>,
) -> Result<(), ValidationFailure> {
    if object.len() != expected.len()
        || !object
            .entries()
            .zip(expected)
            .all(|((actual, _), expected)| actual == *expected)
    {
        return Err(failure(id, REQUEST_VALIDATION_FAILED));
    }
    Ok(())
}

fn failure(id: Option<RuntimeString>, message: &'static str) -> ValidationFailure {
    ValidationFailure { id, message }
}

const OPERATIONS: &[&str] = &[
    "navigate",
    "snapshot",
    "screenshot",
    "click",
    "type",
    "eval",
    "console",
    "network",
    "gif_creator",
    "upload_image",
    "file_upload",
    "shortcuts_list",
    "shortcuts_execute",
    "switch_browser",
    "list_connected_browsers",
    "select_browser",
    "resolve_tab",
    "claim_tab",
    "history",
    "tabs_content",
    "content_export",
    "download",
    "clipboard",
    "dialog",
    "mark_tab",
    "playwright_wait_for_event",
    "playwright_file_chooser_set_files",
    "playwright_wait_for_load_state",
    "playwright_wait_for_url",
    "playwright_dom_snapshot",
    "playwright_element_info",
    "playwright_element_screenshot",
    "read_page",
    "find",
    "form_input",
    "computer",
    "tabs",
    "locator",
    "page_text",
    "batch",
    "resize",
    "close",
];

fn known_operation_name(operation: &RuntimeString) -> Option<&'static str> {
    OPERATIONS
        .iter()
        .copied()
        .find(|candidate| operation == &RuntimeString::from(*candidate))
}

fn is_known_operation(operation: &RuntimeString) -> bool {
    known_operation_name(operation).is_some()
}

fn permissions_for(operation: &RuntimeString) -> Option<BrowserHostPermissionSet> {
    if !is_known_operation(operation) {
        return None;
    }
    let in_set = |values: &[&str]| {
        values
            .iter()
            .any(|candidate| operation == &RuntimeString::from(*candidate))
    };
    Some(BrowserHostPermissionSet {
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
        javascript_eval: in_set(&["eval"]),
        read_console: in_set(&["console"]),
        read_network: in_set(&["network"]),
        manage_tabs: in_set(&[
            "tabs",
            "close",
            "batch",
            "switch_browser",
            "select_browser",
            "resolve_tab",
            "claim_tab",
            "mark_tab",
            // Loading urls in TEMPORARY TABS is tab management, whatever else it is.
            // It opens tabs and closes them again, which is the same capability
            // `tabs {action: "new"}` declares — and a policy that granted navigation
            // and page reads but not tab management was letting it through.
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
    })
}

fn policy_permissions_for_operation(operation: &RuntimeObject) -> Option<BrowserHostPermissionSet> {
    let mut required = BrowserHostPermissionSet {
        navigate: false,
        read_page: false,
        capture_screenshot: false,
        interact: false,
        javascript_eval: false,
        read_console: false,
        read_network: false,
        manage_tabs: false,
        session_state: false,
        file_transfer: false,
    };
    let mut pending = vec![operation];
    while let Some(current) = pending.pop() {
        let RuntimeJson::String(operation_type) = current.get("type")? else {
            return None;
        };
        let mut operation_required = permissions_for(operation_type)?;
        if operation_type == &RuntimeString::from("locator")
            && matches!(current.get("action"), Some(RuntimeJson::String(action)) if action == &RuntimeString::from("screenshot"))
        {
            operation_required.capture_screenshot = true;
        }
        if operation_type == &RuntimeString::from("tabs_content") {
            operation_required.navigate = true;
        }
        // `save` on a coordinate action writes the capture to a FILE, which is a file
        // transfer and was declared as a screenshot. A policy that granted captures but
        // not file transfer was letting bytes onto disk.
        if operation_type == &RuntimeString::from("computer")
            && matches!(current.get("save"), Some(RuntimeJson::Bool(true)))
        {
            operation_required.file_transfer = true;
        }
        if operation_type == &RuntimeString::from("tabs") {
            let Some(RuntimeJson::String(action)) = current.get("action") else {
                return None;
            };
            if ["list", "user", "get", "selected"]
                .iter()
                .any(|candidate| action == &RuntimeString::from(*candidate))
            {
                operation_required.read_page = true;
            } else if action == &RuntimeString::from("new") {
                operation_required.navigate = true;
                operation_required.read_page = true;
            }
        }
        required = required.union(operation_required);
        if operation_type == &RuntimeString::from("batch") {
            let RuntimeJson::Array(actions) = current.get("actions")? else {
                return None;
            };
            for action in actions.iter().rev() {
                let RuntimeJson::Object(action) = action else {
                    return None;
                };
                pending.push(action);
            }
        }
    }
    Some(required)
}

fn error_response(id: &RuntimeString, message: &'static str) -> Vec<u8> {
    let mut error = RuntimeObject::new();
    error.insert("code", RuntimeJson::from("host_error"));
    error.insert("message", RuntimeJson::from(message));
    let mut response = RuntimeObject::new();
    response.insert("id", RuntimeJson::String(id.clone()));
    response.insert("ok", RuntimeJson::Bool(false));
    response.insert("error", RuntimeJson::Object(error));
    runtime_json::stringify(&RuntimeJson::Object(response)).to_node_utf8()
}
