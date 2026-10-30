//! Host-injected browser runtime contract.
//!
//! The core engine owns the model-facing protocol, permission flags, event
//! shapes, artifacts, logs, timeout and cancellation wiring. It deliberately
//! does not embed Chromium, Playwright, or a Chrome extension; hosts/plugins
//! provide an unsafe-attested [`BrowserExecutor`] that Core seals into an
//! opaque [`BrowserRuntime`].

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::runtime_process::RuntimeProcessNamespaceId;
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObject, RuntimeObjectDecoder, RuntimeObjectEncoder,
    RuntimeString,
};
use crate::schemas::content::OutputBlock;
use crate::types::abort::AbortSignal;
use crate::types::tool::{BoxFuture, InvokeToolFn, InvokeToolResult};

/// Callback used by a browser runtime to stream structured progress back to the
/// engine while a browser operation is still running.
pub type BrowserEventSink = Arc<dyn Fn(BrowserRuntimeEvent) + Send + Sync>;

fn decode_provider_unit_enum<T: Copy>(
    value: &RuntimeJson,
    variants: &[RuntimeEnumVariant<'_>],
    values: &[T],
) -> Result<T, RuntimeCodecError> {
    runtime_codec::decode_external(value, variants, |variant| match variant {
        RuntimeExternalVariant::Unit { variant } => values
            .get(variant)
            .copied()
            .ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownEnumVariant)),
        RuntimeExternalVariant::Payload { .. } => Err(RuntimeCodecError::new(
            RuntimeCodecErrorKind::InvalidEnumShape,
        )),
    })
}

fn encode_optional_field<T: RuntimeEncode>(
    object: &mut RuntimeObjectEncoder,
    name: &str,
    value: &Option<T>,
) -> Result<(), RuntimeCodecError> {
    if let Some(value) = value {
        object.field(name, value)?;
    }
    Ok(())
}

fn encode_runtime_btree_map(values: &BTreeMap<RuntimeString, RuntimeJson>) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    for (key, value) in values {
        object.insert(key.clone(), value.clone());
    }
    RuntimeJson::Object(object)
}

fn decode_runtime_btree_map(
    value: &RuntimeJson,
) -> Result<BTreeMap<RuntimeString, RuntimeJson>, RuntimeCodecError> {
    let RuntimeJson::Object(object) = value else {
        return Err(RuntimeCodecError::new(
            RuntimeCodecErrorKind::ExpectedObject,
        ));
    };
    Ok(object
        .entries()
        .map(|(key, value)| (key, value.clone()))
        .collect())
}

/// Closed process binding carried by every registered browser runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrowserProcessBinding {
    /// The executor is remote or in-process and never creates, controls, or
    /// delegates to a local OS process.
    SpawnFree,
    /// Every local process effect is bound to this exact runtime-process
    /// namespace.
    ContextBound(RuntimeProcessNamespaceId),
}

mod browser_runtime_sealed {
    pub trait Sealed {}
}

/// An opaque, registered browser runtime.
///
/// This trait is sealed: only Core's private registered adapter implements it,
/// and callers can only obtain an `Arc<dyn BrowserRuntime>` through the unsafe
/// registration functions below. Both safe and unsafe downstream impls are
/// rejected at compile time.
///
/// ```compile_fail
/// use async_trait::async_trait;
/// use klaus_agent_core::browser::{
///     BrowserEventSink, BrowserRuntime, BrowserRuntimeError,
///     BrowserRuntimeRequest, BrowserRuntimeResponse,
/// };
/// use klaus_agent_core::types::abort::AbortSignal;
///
/// struct UntrustedLocalRuntime;
///
/// #[async_trait]
/// impl BrowserRuntime for UntrustedLocalRuntime {
///     async fn execute(
///         &self,
///         _request: BrowserRuntimeRequest,
///         _signal: AbortSignal,
///         _emit: BrowserEventSink,
///     ) -> Result<BrowserRuntimeResponse, BrowserRuntimeError> {
///         unreachable!()
///     }
///
///     fn process_binding(&self) -> klaus_agent_core::browser::BrowserProcessBinding {
///         klaus_agent_core::browser::BrowserProcessBinding::SpawnFree
///     }
/// }
/// ```
///
/// ```compile_fail
/// use async_trait::async_trait;
/// use klaus_agent_core::browser::{
///     BrowserEventSink, BrowserRuntime, BrowserRuntimeError,
///     BrowserRuntimeRequest, BrowserRuntimeResponse,
/// };
/// use klaus_agent_core::types::abort::AbortSignal;
///
/// struct UntrustedLocalRuntime;
///
/// #[async_trait]
/// unsafe impl BrowserRuntime for UntrustedLocalRuntime {
///     async fn execute(
///         &self,
///         _request: BrowserRuntimeRequest,
///         _signal: AbortSignal,
///         _emit: BrowserEventSink,
///     ) -> Result<BrowserRuntimeResponse, BrowserRuntimeError> {
///         unreachable!()
///     }
///
///     fn process_binding(&self) -> klaus_agent_core::browser::BrowserProcessBinding {
///         klaus_agent_core::browser::BrowserProcessBinding::SpawnFree
///     }
/// }
/// ```
#[async_trait]
pub trait BrowserRuntime: browser_runtime_sealed::Sealed + Send + Sync {
    async fn execute(
        &self,
        request: BrowserRuntimeRequest,
        signal: AbortSignal,
        emit: BrowserEventSink,
    ) -> Result<BrowserRuntimeResponse, BrowserRuntimeError>;

    fn process_binding(&self) -> BrowserProcessBinding;
}

/// Executable half of a browser runtime registration.
///
/// # Safety
///
/// Implementors must not conceal process creation, process control, or process
/// delegation from the code that registers them. The registration call must
/// accurately classify every such effect as spawn-free or context-bound.
#[async_trait]
pub unsafe trait BrowserExecutor: Send + Sync {
    async fn execute(
        &self,
        request: BrowserRuntimeRequest,
        signal: AbortSignal,
        emit: BrowserEventSink,
    ) -> Result<BrowserRuntimeResponse, BrowserRuntimeError>;
}

struct RegisteredBrowserRuntime {
    executor: Arc<dyn BrowserExecutor>,
    binding: BrowserProcessBinding,
}

impl browser_runtime_sealed::Sealed for RegisteredBrowserRuntime {}

#[async_trait]
impl BrowserRuntime for RegisteredBrowserRuntime {
    async fn execute(
        &self,
        request: BrowserRuntimeRequest,
        signal: AbortSignal,
        emit: BrowserEventSink,
    ) -> Result<BrowserRuntimeResponse, BrowserRuntimeError> {
        self.executor.execute(request, signal, emit).await
    }

    fn process_binding(&self) -> BrowserProcessBinding {
        self.binding
    }
}

/// Register a remote or in-process executor as genuinely spawn-free.
///
/// # Safety
///
/// `executor` must never create, control, or delegate to a local OS process,
/// directly or indirectly.
pub unsafe fn attest_spawn_free(executor: Arc<dyn BrowserExecutor>) -> Arc<dyn BrowserRuntime> {
    Arc::new(RegisteredBrowserRuntime {
        executor,
        binding: BrowserProcessBinding::SpawnFree,
    })
}

/// Register an executor whose complete local-process authority is bound to one
/// exact runtime-process namespace.
///
/// # Safety
///
/// Every local process created, controlled, or delegated to by `executor` must
/// be owned by `namespace_id`; it must not use a second namespace or a raw
/// process primitive outside that namespace.
pub unsafe fn context_bound(
    namespace_id: RuntimeProcessNamespaceId,
    executor: Arc<dyn BrowserExecutor>,
) -> Arc<dyn BrowserRuntime> {
    Arc::new(RegisteredBrowserRuntime {
        executor,
        binding: BrowserProcessBinding::ContextBound(namespace_id),
    })
}

/// Browser session/tab address used by every request. Empty target means "use
/// or create the runtime's default session/tab".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BrowserTarget {
    pub session_id: Option<RuntimeString>,
    pub tab_id: Option<RuntimeString>,
    /// Which browser runs this one operation, naming a device from
    /// `list_connected_browsers` (the built-in browser, or the user's connected
    /// Chrome). The backend follows from the id.
    ///
    /// Per-request rather than per-session on purpose: `select_browser` sets a
    /// sticky default, but two interleaved operations that belong to different
    /// browsers would otherwise race that single setting and land in the wrong
    /// one. `None` means "whatever the session selected", which keeps every
    /// existing call unchanged.
    pub browser_id: Option<RuntimeString>,
}

impl RuntimeEncode for BrowserTarget {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {
            session_id,
            tab_id,
            browser_id,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = session_id {
            object.field("sessionId", value)?;
        }
        if let Some(value) = tab_id {
            object.field("tabId", value)?;
        }
        if let Some(value) = browser_id {
            object.field("browserId", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for BrowserTarget {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                session_id: object.optional("sessionId", &[])?,
                tab_id: object.optional("tabId", &[])?,
                browser_id: object.optional("browserId", &[])?,
            })
        })
    }
}

/// Common browser viewport contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrowserViewport {
    pub width: u32,
    pub height: u32,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub device_scale_factor: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub is_mobile: Option<bool>,
}

impl RuntimeEncode for BrowserViewport {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {
            width,
            height,
            device_scale_factor,
            is_mobile,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("width", width)?;
        object.field("height", height)?;
        if let Some(value) = device_scale_factor {
            object.field("deviceScaleFactor", value)?;
        }
        if let Some(value) = is_mobile {
            object.field("isMobile", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for BrowserViewport {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                width: object.required("width", &[])?,
                height: object.required("height", &[])?,
                device_scale_factor: object.optional("deviceScaleFactor", &[])?,
                is_mobile: object.optional("isMobile", &[])?,
            })
        })
    }
}

/// Fine-grained capability flags attached to every request. A runtime sees the
/// exact powers the tool wrapper expects it to exercise and can fail closed when
/// the host's own policy is stricter.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrowserPermissionSet {
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

impl RuntimeEncode for BrowserPermissionSet {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {
            navigate,
            read_page,
            capture_screenshot,
            interact,
            javascript_eval,
            read_console,
            read_network,
            manage_tabs,
            session_state,
            file_transfer,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("navigate", navigate)?;
        object.field("readPage", read_page)?;
        object.field("captureScreenshot", capture_screenshot)?;
        object.field("interact", interact)?;
        object.field("javascriptEval", javascript_eval)?;
        object.field("readConsole", read_console)?;
        object.field("readNetwork", read_network)?;
        object.field("manageTabs", manage_tabs)?;
        object.field("sessionState", session_state)?;
        object.field("fileTransfer", file_transfer)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for BrowserPermissionSet {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                navigate: object.required("navigate", &[])?,
                read_page: object.required("readPage", &[])?,
                capture_screenshot: object.required("captureScreenshot", &[])?,
                interact: object.required("interact", &[])?,
                javascript_eval: object.required("javascriptEval", &[])?,
                read_console: object.required("readConsole", &[])?,
                read_network: object.required("readNetwork", &[])?,
                manage_tabs: object.required("manageTabs", &[])?,
                session_state: object.required("sessionState", &[])?,
                file_transfer: object.required("fileTransfer", &[])?,
            })
        })
    }
}

impl BrowserPermissionSet {
    pub fn for_operation(op: &BrowserOperation) -> Self {
        let mut p = BrowserPermissionSet::default();
        match op {
            BrowserOperation::Navigate { .. } => {
                p.navigate = true;
                p.read_page = true;
                p.session_state = true;
            }
            BrowserOperation::Snapshot { .. } => {
                p.read_page = true;
            }
            BrowserOperation::Screenshot { save, .. } => {
                p.capture_screenshot = true;
                p.read_page = true;
                p.file_transfer = *save;
            }
            BrowserOperation::Click { .. } | BrowserOperation::Type { .. } => {
                p.interact = true;
                p.read_page = true;
                p.session_state = true;
            }
            BrowserOperation::Eval { .. } => {
                p.javascript_eval = true;
                p.read_page = true;
                p.session_state = true;
            }
            BrowserOperation::Console { .. } => {
                p.read_console = true;
            }
            BrowserOperation::Network { .. } => {
                p.read_network = true;
            }
            BrowserOperation::GifCreator { .. } => {
                p.capture_screenshot = true;
                p.read_page = true;
            }
            BrowserOperation::UploadImage { .. } | BrowserOperation::FileUpload { .. } => {
                p.file_transfer = true;
                p.interact = true;
                p.read_page = true;
                p.session_state = true;
            }
            BrowserOperation::ShortcutsList { .. } => {
                p.read_page = true;
                p.session_state = true;
            }
            BrowserOperation::ShortcutsExecute { .. } => {
                p.interact = true;
                p.session_state = true;
            }
            BrowserOperation::SwitchBrowser { .. }
            | BrowserOperation::SelectBrowser { .. }
            | BrowserOperation::ResolveTab { .. }
            | BrowserOperation::ClaimTab => {
                p.manage_tabs = true;
                p.session_state = true;
            }
            BrowserOperation::ListConnectedBrowsers { .. } => {
                p.session_state = true;
            }
            BrowserOperation::History { .. } => {
                p.read_page = true;
                p.session_state = true;
            }
            BrowserOperation::TabsContent { .. } => {
                p.navigate = true;
                p.read_page = true;
                p.session_state = true;
                // Loading urls in TEMPORARY TABS is tab management, whatever else it
                // is: it opens tabs and closes them again, the same capability
                // `Tabs::New` declares. Leaving it out let a policy that granted
                // navigation and page reads run an operation that manages tabs.
                p.manage_tabs = true;
            }
            BrowserOperation::ContentExport { .. } => {
                p.read_page = true;
                p.file_transfer = true;
            }
            BrowserOperation::Download { .. } => {
                p.read_page = true;
                p.interact = true;
                p.file_transfer = true;
                p.session_state = true;
            }
            BrowserOperation::Clipboard { .. } => {
                p.read_page = true;
                p.session_state = true;
                // Reading interacts too, and saying otherwise was wrong twice over: the
                // read drives the browser's own paste command through a scratch element
                // in the page — it focuses, pastes and restores focus — and the host
                // transport's own classifier has always required `interact` for every
                // clipboard action. Two classifiers disagreeing about one operation is
                // its own defect: whichever is consulted decides what the user approved.
                p.interact = true;
            }
            BrowserOperation::Dialog { action, .. } => {
                p.read_page = true;
                if !matches!(action, BrowserDialogAction::Get) {
                    p.interact = true;
                    p.session_state = true;
                }
            }
            BrowserOperation::MarkTab { .. } => {
                p.manage_tabs = true;
                p.session_state = true;
            }
            BrowserOperation::ReadPage { .. }
            | BrowserOperation::Find { .. }
            | BrowserOperation::PageText { .. } => {
                p.read_page = true;
            }
            BrowserOperation::FormInput { .. } => {
                p.interact = true;
                p.read_page = true;
                p.session_state = true;
            }
            BrowserOperation::Computer { action, save, .. } => {
                p.read_page = true;
                // `save` writes the capture to a FILE. It was declared as a screenshot,
                // so a policy that granted captures but not file transfer let bytes onto
                // disk.
                if *save {
                    p.file_transfer = true;
                }
                match action {
                    BrowserComputerAction::Screenshot
                    | BrowserComputerAction::Zoom
                    | BrowserComputerAction::Wait => {
                        p.capture_screenshot = matches!(
                            action,
                            BrowserComputerAction::Screenshot | BrowserComputerAction::Zoom
                        );
                    }
                    _ => {
                        p.interact = true;
                        p.capture_screenshot = true;
                        p.session_state = true;
                    }
                }
            }
            BrowserOperation::Tabs { action, .. } => {
                p.session_state = true;
                match action {
                    BrowserTabsAction::List
                    | BrowserTabsAction::User
                    | BrowserTabsAction::Get
                    | BrowserTabsAction::Selected => {
                        p.read_page = true;
                    }
                    BrowserTabsAction::New => {
                        p.navigate = true;
                        p.manage_tabs = true;
                        p.read_page = true;
                    }
                    BrowserTabsAction::Close
                    | BrowserTabsAction::Finalize
                    | BrowserTabsAction::NameSession => {
                        p.manage_tabs = true;
                    }
                }
            }
            BrowserOperation::Locator { action, .. } => {
                p.read_page = true;
                if action != "count"
                    && action != "text"
                    && action != "text_content"
                    && action != "all_text_contents"
                    && action != "is_visible"
                    && action != "screenshot"
                    && action != "wait_for"
                {
                    p.interact = true;
                    p.session_state = true;
                }
                if action == "screenshot" {
                    p.capture_screenshot = true;
                }
            }
            BrowserOperation::Batch { .. } => {
                p.navigate = true;
                p.read_page = true;
                p.capture_screenshot = true;
                p.interact = true;
                p.javascript_eval = true;
                p.read_console = true;
                p.read_network = true;
                p.manage_tabs = true;
                p.session_state = true;
                p.file_transfer = true;
            }
            BrowserOperation::Resize { .. } => {
                p.read_page = true;
                p.session_state = true;
            }
            BrowserOperation::Close { .. } => {
                p.manage_tabs = true;
                p.session_state = true;
            }
            BrowserOperation::Permission { .. } => {
                // Browser state that OUTLIVES the call, on the user's own profile: a
                // granted camera or geolocation permission stays granted after the
                // session ends. `session_state` is the capability a policy withholds
                // to keep an agent from changing what the browser remembers.
                p.session_state = true;
            }
        }
        p
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum BrowserWaitUntil {
    Commit,
    DomContentLoaded,
    #[default]
    Load,
    NetworkIdle,
}

impl RuntimeEncode for BrowserWaitUntil {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Commit => "commit",
            Self::DomContentLoaded => "dom_content_loaded",
            Self::Load => "load",
            Self::NetworkIdle => "network_idle",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for BrowserWaitUntil {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("commit", &[]),
                RuntimeEnumVariant::new("dom_content_loaded", &[]),
                RuntimeEnumVariant::new("load", &[]),
                RuntimeEnumVariant::new("network_idle", &[]),
            ],
            &[
                Self::Commit,
                Self::DomContentLoaded,
                Self::Load,
                Self::NetworkIdle,
            ],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum BrowserScreenshotFormat {
    #[default]
    Png,
    Jpeg,
}

impl RuntimeEncode for BrowserScreenshotFormat {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Png => "png",
            Self::Jpeg => "jpeg",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for BrowserScreenshotFormat {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("png", &[]),
                RuntimeEnumVariant::new("jpeg", &[]),
            ],
            &[Self::Png, Self::Jpeg],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum BrowserMouseButton {
    #[default]
    Left,
    Middle,
    Right,
}

impl RuntimeEncode for BrowserMouseButton {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Left => "left",
            Self::Middle => "middle",
            Self::Right => "right",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for BrowserMouseButton {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("left", &[]),
                RuntimeEnumVariant::new("middle", &[]),
                RuntimeEnumVariant::new("right", &[]),
            ],
            &[Self::Left, Self::Middle, Self::Right],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum BrowserCloseTarget {
    #[default]
    Tab,
    Session,
    All,
}

impl RuntimeEncode for BrowserCloseTarget {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Tab => "tab",
            Self::Session => "session",
            Self::All => "all",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for BrowserCloseTarget {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("tab", &[]),
                RuntimeEnumVariant::new("session", &[]),
                RuntimeEnumVariant::new("all", &[]),
            ],
            &[Self::Tab, Self::Session, Self::All],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserReadPageFilter {
    Interactive,
    All,
}

impl RuntimeEncode for BrowserReadPageFilter {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Interactive => "interactive",
            Self::All => "all",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for BrowserReadPageFilter {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("interactive", &[]),
                RuntimeEnumVariant::new("all", &[]),
            ],
            &[Self::Interactive, Self::All],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserComputerAction {
    LeftClick,
    RightClick,
    DoubleClick,
    TripleClick,
    Type,
    Screenshot,
    Wait,
    Scroll,
    Key,
    LeftClickDrag,
    Zoom,
    ScrollTo,
    Hover,
}

impl RuntimeEncode for BrowserComputerAction {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::LeftClick => "left_click",
            Self::RightClick => "right_click",
            Self::DoubleClick => "double_click",
            Self::TripleClick => "triple_click",
            Self::Type => "type",
            Self::Screenshot => "screenshot",
            Self::Wait => "wait",
            Self::Scroll => "scroll",
            Self::Key => "key",
            Self::LeftClickDrag => "left_click_drag",
            Self::Zoom => "zoom",
            Self::ScrollTo => "scroll_to",
            Self::Hover => "hover",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for BrowserComputerAction {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("left_click", &[]),
                RuntimeEnumVariant::new("right_click", &[]),
                RuntimeEnumVariant::new("double_click", &[]),
                RuntimeEnumVariant::new("triple_click", &[]),
                RuntimeEnumVariant::new("type", &[]),
                RuntimeEnumVariant::new("screenshot", &[]),
                RuntimeEnumVariant::new("wait", &[]),
                RuntimeEnumVariant::new("scroll", &[]),
                RuntimeEnumVariant::new("key", &[]),
                RuntimeEnumVariant::new("left_click_drag", &[]),
                RuntimeEnumVariant::new("zoom", &[]),
                RuntimeEnumVariant::new("scroll_to", &[]),
                RuntimeEnumVariant::new("hover", &[]),
            ],
            &[
                Self::LeftClick,
                Self::RightClick,
                Self::DoubleClick,
                Self::TripleClick,
                Self::Type,
                Self::Screenshot,
                Self::Wait,
                Self::Scroll,
                Self::Key,
                Self::LeftClickDrag,
                Self::Zoom,
                Self::ScrollTo,
                Self::Hover,
            ],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserScrollDirection {
    Up,
    Down,
    Left,
    Right,
}

impl RuntimeEncode for BrowserScrollDirection {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Up => "up",
            Self::Down => "down",
            Self::Left => "left",
            Self::Right => "right",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for BrowserScrollDirection {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("up", &[]),
                RuntimeEnumVariant::new("down", &[]),
                RuntimeEnumVariant::new("left", &[]),
                RuntimeEnumVariant::new("right", &[]),
            ],
            &[Self::Up, Self::Down, Self::Left, Self::Right],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserTabsAction {
    List,
    User,
    New,
    Get,
    Selected,
    Close,
    Finalize,
    NameSession,
}

impl RuntimeEncode for BrowserTabsAction {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::List => "list",
            Self::User => "user",
            Self::New => "new",
            Self::Get => "get",
            Self::Selected => "selected",
            Self::Close => "close",
            Self::Finalize => "finalize",
            Self::NameSession => "name_session",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for BrowserTabsAction {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("list", &[]),
                RuntimeEnumVariant::new("user", &[]),
                RuntimeEnumVariant::new("new", &[]),
                RuntimeEnumVariant::new("get", &[]),
                RuntimeEnumVariant::new("selected", &[]),
                RuntimeEnumVariant::new("close", &[]),
                RuntimeEnumVariant::new("finalize", &[]),
                RuntimeEnumVariant::new("name_session", &[]),
            ],
            &[
                Self::List,
                Self::User,
                Self::New,
                Self::Get,
                Self::Selected,
                Self::Close,
                Self::Finalize,
                Self::NameSession,
            ],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserClipboardAction {
    Read,
    ReadText,
    Write,
    WriteText,
}

impl RuntimeEncode for BrowserClipboardAction {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Read => "read",
            Self::ReadText => "read_text",
            Self::Write => "write",
            Self::WriteText => "write_text",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for BrowserClipboardAction {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("read", &[]),
                RuntimeEnumVariant::new("read_text", &[]),
                RuntimeEnumVariant::new("write", &[]),
                RuntimeEnumVariant::new("write_text", &[]),
            ],
            &[Self::Read, Self::ReadText, Self::Write, Self::WriteText],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserDialogAction {
    Get,
    Accept,
    Dismiss,
}

impl RuntimeEncode for BrowserDialogAction {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Get => "get",
            Self::Accept => "accept",
            Self::Dismiss => "dismiss",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for BrowserDialogAction {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("get", &[]),
                RuntimeEnumVariant::new("accept", &[]),
                RuntimeEnumVariant::new("dismiss", &[]),
            ],
            &[Self::Get, Self::Accept, Self::Dismiss],
        )
    }
}

/// What a `Permission` operation does to a site's browser permissions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserPermissionAction {
    Grant,
    Set,
    Reset,
}

impl RuntimeEncode for BrowserPermissionAction {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Grant => "grant",
            Self::Set => "set",
            Self::Reset => "reset",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for BrowserPermissionAction {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("grant", &[]),
                RuntimeEnumVariant::new("set", &[]),
                RuntimeEnumVariant::new("reset", &[]),
            ],
            &[Self::Grant, Self::Set, Self::Reset],
        )
    }
}

/// What one permission is set TO.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserPermissionSetting {
    Granted,
    Denied,
    Prompt,
}

impl RuntimeEncode for BrowserPermissionSetting {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Granted => "granted",
            Self::Denied => "denied",
            Self::Prompt => "prompt",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for BrowserPermissionSetting {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("granted", &[]),
                RuntimeEnumVariant::new("denied", &[]),
                RuntimeEnumVariant::new("prompt", &[]),
            ],
            &[Self::Granted, Self::Denied, Self::Prompt],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserTabMarkStatus {
    Deliverable,
    Handoff,
}

impl RuntimeEncode for BrowserTabMarkStatus {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Deliverable => "deliverable",
            Self::Handoff => "handoff",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for BrowserTabMarkStatus {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("deliverable", &[]),
                RuntimeEnumVariant::new("handoff", &[]),
            ],
            &[Self::Deliverable, Self::Handoff],
        )
    }
}

/// Semantic element constraints shared by target-aware browser operations.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BrowserElementCriteria {
    pub role: Option<RuntimeString>,
    pub name: Option<RuntimeString>,
    pub label: Option<RuntimeString>,
    pub placeholder: Option<RuntimeString>,
    pub test_id: Option<RuntimeString>,
    pub query: Option<RuntimeString>,
    pub exact: Option<bool>,
    pub nth: Option<i64>,
}

impl BrowserElementCriteria {
    fn encode_fields(&self, object: &mut RuntimeObjectEncoder) -> Result<(), RuntimeCodecError> {
        encode_optional_field(object, "role", &self.role)?;
        encode_optional_field(object, "name", &self.name)?;
        encode_optional_field(object, "label", &self.label)?;
        encode_optional_field(object, "placeholder", &self.placeholder)?;
        encode_optional_field(object, "testId", &self.test_id)?;
        encode_optional_field(object, "query", &self.query)?;
        encode_optional_field(object, "exact", &self.exact)?;
        encode_optional_field(object, "nth", &self.nth)?;
        Ok(())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            role: object.optional("role", &[])?,
            name: object.optional("name", &[])?,
            label: object.optional("label", &[])?,
            placeholder: object.optional("placeholder", &[])?,
            test_id: object.optional("testId", &[])?,
            query: object.optional("query", &[])?,
            exact: object.optional("exact", &[])?,
            nth: object.optional("nth", &[])?,
        })
    }
}

/// The operation-specific portion of a browser runtime request.
#[derive(Debug, Clone, PartialEq)]
pub enum BrowserOperation {
    Navigate {
        url: RuntimeString,
        wait_until: Option<BrowserWaitUntil>,
        viewport: Option<BrowserViewport>,
    },
    Snapshot {
        selector: Option<RuntimeString>,
        include_text: bool,
        include_links: bool,
    },
    Screenshot {
        criteria: BrowserElementCriteria,
        ref_id: Option<RuntimeString>,
        selector: Option<RuntimeString>,
        full_page: bool,
        format: Option<BrowserScreenshotFormat>,
        quality: Option<u8>,
        save: bool,
    },
    Click {
        criteria: BrowserElementCriteria,
        ref_id: Option<RuntimeString>,
        selector: Option<RuntimeString>,
        x: Option<f64>,
        y: Option<f64>,
        button: Option<BrowserMouseButton>,
        click_count: Option<u32>,
        modifiers: Option<Vec<RuntimeString>>,
        /// Act on an element the pre-click checks would hide — `aria-disabled`, a
        /// zero-size hit box, something drawn over it. The host implements this and
        /// says which path it took; without the field, an agent facing a control its
        /// own page has made unreachable had no move left.
        force: bool,
    },
    Type {
        criteria: BrowserElementCriteria,
        ref_id: Option<RuntimeString>,
        selector: Option<RuntimeString>,
        text: RuntimeString,
        clear: bool,
        press_enter: bool,
        delay_ms: Option<u64>,
        /// A named key to press on the element after any text: Escape, Tab,
        /// ArrowDown, cmd+Enter. Distinct from `press_enter`, and from typing: it
        /// goes to the element named here rather than to whatever holds focus.
        key: Option<RuntimeString>,
    },
    Eval {
        script: RuntimeString,
        args: Option<RuntimeJson>,
        await_promise: bool,
    },
    Console {
        levels: Option<Vec<BrowserConsoleLevel>>,
        since_ms: Option<u64>,
        limit: Option<u32>,
        clear: bool,
    },
    Network {
        since_ms: Option<u64>,
        limit: Option<u32>,
        include_bodies: bool,
        url_filter: Option<RuntimeString>,
    },
    GifCreator {
        filename: Option<RuntimeString>,
        duration_seconds: Option<f64>,
        fps: Option<u32>,
        full_page: bool,
    },
    UploadImage {
        criteria: BrowserElementCriteria,
        ref_id: Option<RuntimeString>,
        path: RuntimeString,
        selector: Option<RuntimeString>,
        prompt: Option<RuntimeString>,
    },
    FileUpload {
        criteria: BrowserElementCriteria,
        paths: Vec<RuntimeString>,
        selector: Option<RuntimeString>,
        ref_id: Option<RuntimeString>,
    },
    ShortcutsList {
        query: Option<RuntimeString>,
    },
    ShortcutsExecute {
        name: RuntimeString,
        wait_for_completion: bool,
    },
    SwitchBrowser {
        prompt: Option<RuntimeString>,
        wait_ms: Option<u64>,
    },
    ListConnectedBrowsers {
        local_only: bool,
    },
    SelectBrowser {
        device_id: RuntimeString,
    },
    /// Resolve a user-selected tab reference through the owning browser host.
    ResolveTab {
        mention: RuntimeString,
    },
    /// Claim an existing external user tab without replacing it.
    ClaimTab,
    History {
        queries: Vec<RuntimeString>,
        limit: Option<u32>,
        from: Option<RuntimeString>,
        to: Option<RuntimeString>,
    },
    TabsContent {
        urls: Vec<RuntimeString>,
        content_type: Option<RuntimeString>,
        max_chars: Option<u32>,
        wait_until: Option<BrowserWaitUntil>,
    },
    ContentExport {
        format: Option<RuntimeString>,
        selector: Option<RuntimeString>,
        max_chars: Option<u32>,
    },
    Download {
        selector: Option<RuntimeString>,
        url: Option<RuntimeString>,
        x: Option<f64>,
        y: Option<f64>,
        filename: Option<RuntimeString>,
        /// Wait for a download something ELSE started rather than starting one. With
        /// a `url` and without this the operation fetches that url, so the
        /// click-then-wait shape needs a way to say "do not start anything".
        wait_only: bool,
    },
    Clipboard {
        action: BrowserClipboardAction,
        text: Option<RuntimeString>,
        items: Option<RuntimeJson>,
    },
    Dialog {
        action: BrowserDialogAction,
        prompt_text: Option<RuntimeString>,
    },
    MarkTab {
        status: BrowserTabMarkStatus,
    },
    ReadPage {
        filter: Option<BrowserReadPageFilter>,
        depth: Option<u32>,
        ref_id: Option<RuntimeString>,
        max_chars: Option<u32>,
    },
    Find {
        query: RuntimeString,
        limit: Option<u32>,
    },
    FormInput {
        reference: RuntimeString,
        value: RuntimeJson,
    },
    Computer {
        action: BrowserComputerAction,
        coordinate: Option<Vec<f64>>,
        start_coordinate: Option<Vec<f64>>,
        text: Option<RuntimeString>,
        duration: Option<f64>,
        scroll_direction: Option<BrowserScrollDirection>,
        amount: Option<f64>,
        ref_id: Option<RuntimeString>,
        save: bool,
        /// A key COMBINATION, spelled as its parts: ["cmd", "a"]. Without it the host
        /// fell back to Enter, so select-all, copy and paste were not expressible.
        keys: Option<Vec<RuntimeString>>,
        /// The points a drag passes THROUGH, for a control that follows the pointer:
        /// a slider, a canvas, a reorder handle. Without it a drag is a jump from the
        /// start to the end, which such controls do not react to.
        path: Option<RuntimeJson>,
        /// Signed pixels to scroll, for what `amount` and `scroll_direction` cannot
        /// express — a horizontal scroller, or a precise offset rather than a
        /// direction and a count.
        scroll_x: Option<f64>,
        scroll_y: Option<f64>,
    },
    Tabs {
        action: BrowserTabsAction,
        url: Option<RuntimeString>,
        name: Option<RuntimeString>,
    },
    Locator {
        selector: RuntimeString,
        action: RuntimeString,
        value: Option<RuntimeJson>,
        options: RuntimeJson,
    },
    PageText {
        max_chars: Option<u32>,
    },
    Batch {
        actions: Vec<RuntimeJson>,
    },
    Resize {
        /// Absent when `reset` is set: putting the viewport back needs no size.
        viewport: Option<BrowserViewport>,
        /// Clear the emulation override instead of setting one. An override with no
        /// way to clear it outlives the reason it was set — the user's own tab stays
        /// at whatever size the agent narrowed it to for the rest of the session.
        reset: bool,
    },
    Close {
        target: Option<BrowserCloseTarget>,
    },
    /// A site's browser permissions: geolocation, notifications, camera, microphone.
    ///
    /// Last in the list on purpose — appending keeps every earlier variant's wire tag
    /// where it was. The host has implemented this on both of its browsers and there
    /// was no tool for it here, so a permission prompt was a wall: browsing never
    /// prompts, `dialog` cannot answer a permission chip, and the flow behind it — a
    /// map, a notification opt-in, anything asking for the camera — could not be
    /// completed at all.
    Permission {
        action: BrowserPermissionAction,
        /// The http(s) origin the permissions belong to. Required for grant and set.
        origin: Option<RuntimeString>,
        /// For grant: the permission names to allow.
        permissions: Option<Vec<RuntimeString>>,
        /// For set: the single permission name.
        permission: Option<RuntimeString>,
        /// For set: what to set it to.
        setting: Option<BrowserPermissionSetting>,
    },
}

impl RuntimeEncode for BrowserOperation {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let (tag, fields) = match self {
            Self::Navigate {
                url,
                wait_until,
                viewport,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("url", url)?;
                encode_optional_field(&mut fields, "waitUntil", wait_until)?;
                encode_optional_field(&mut fields, "viewport", viewport)?;
                ("navigate", fields)
            }
            Self::Snapshot {
                selector,
                include_text,
                include_links,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                encode_optional_field(&mut fields, "selector", selector)?;
                fields.field("includeText", include_text)?;
                fields.field("includeLinks", include_links)?;
                ("snapshot", fields)
            }
            Self::Screenshot {
                criteria,
                ref_id,
                selector,
                full_page,
                format,
                quality,
                save,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                criteria.encode_fields(&mut fields)?;
                encode_optional_field(&mut fields, "refId", ref_id)?;
                encode_optional_field(&mut fields, "selector", selector)?;
                fields.field("fullPage", full_page)?;
                encode_optional_field(&mut fields, "format", format)?;
                encode_optional_field(&mut fields, "quality", quality)?;
                fields.field("save", save)?;
                ("screenshot", fields)
            }
            Self::Click {
                criteria,
                ref_id,
                selector,
                x,
                y,
                button,
                click_count,
                modifiers,
                force,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                criteria.encode_fields(&mut fields)?;
                encode_optional_field(&mut fields, "refId", ref_id)?;
                encode_optional_field(&mut fields, "selector", selector)?;
                encode_optional_field(&mut fields, "x", x)?;
                encode_optional_field(&mut fields, "y", y)?;
                encode_optional_field(&mut fields, "button", button)?;
                encode_optional_field(&mut fields, "clickCount", click_count)?;
                encode_optional_field(&mut fields, "modifiers", modifiers)?;
                fields.field("force", force)?;
                ("click", fields)
            }
            Self::Type {
                criteria,
                ref_id,
                selector,
                text,
                clear,
                press_enter,
                delay_ms,
                key,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                criteria.encode_fields(&mut fields)?;
                encode_optional_field(&mut fields, "refId", ref_id)?;
                encode_optional_field(&mut fields, "selector", selector)?;
                fields.field("text", text)?;
                fields.field("clear", clear)?;
                fields.field("pressEnter", press_enter)?;
                encode_optional_field(&mut fields, "key", key)?;
                encode_optional_field(&mut fields, "delayMs", delay_ms)?;
                ("type", fields)
            }
            Self::Eval {
                script,
                args,
                await_promise,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("script", script)?;
                encode_optional_field(&mut fields, "args", args)?;
                fields.field("awaitPromise", await_promise)?;
                ("eval", fields)
            }
            Self::Console {
                levels,
                since_ms,
                limit,
                clear,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                encode_optional_field(&mut fields, "levels", levels)?;
                encode_optional_field(&mut fields, "sinceMs", since_ms)?;
                encode_optional_field(&mut fields, "limit", limit)?;
                fields.field("clear", clear)?;
                ("console", fields)
            }
            Self::Network {
                since_ms,
                limit,
                include_bodies,
                url_filter,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                encode_optional_field(&mut fields, "sinceMs", since_ms)?;
                encode_optional_field(&mut fields, "limit", limit)?;
                fields.field("includeBodies", include_bodies)?;
                encode_optional_field(&mut fields, "urlFilter", url_filter)?;
                ("network", fields)
            }
            Self::GifCreator {
                filename,
                duration_seconds,
                fps,
                full_page,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                encode_optional_field(&mut fields, "filename", filename)?;
                encode_optional_field(&mut fields, "durationSeconds", duration_seconds)?;
                encode_optional_field(&mut fields, "fps", fps)?;
                fields.field("fullPage", full_page)?;
                ("gif_creator", fields)
            }
            Self::UploadImage {
                criteria,
                ref_id,
                path,
                selector,
                prompt,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                criteria.encode_fields(&mut fields)?;
                encode_optional_field(&mut fields, "refId", ref_id)?;
                fields.field("path", path)?;
                encode_optional_field(&mut fields, "selector", selector)?;
                encode_optional_field(&mut fields, "prompt", prompt)?;
                ("upload_image", fields)
            }
            Self::FileUpload {
                criteria,
                paths,
                selector,
                ref_id,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                criteria.encode_fields(&mut fields)?;
                fields.field("paths", paths)?;
                encode_optional_field(&mut fields, "selector", selector)?;
                encode_optional_field(&mut fields, "refId", ref_id)?;
                ("file_upload", fields)
            }
            Self::ShortcutsList { query } => {
                let mut fields = RuntimeObjectEncoder::new();
                encode_optional_field(&mut fields, "query", query)?;
                ("shortcuts_list", fields)
            }
            Self::ShortcutsExecute {
                name,
                wait_for_completion,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("name", name)?;
                fields.field("waitForCompletion", wait_for_completion)?;
                ("shortcuts_execute", fields)
            }
            Self::SwitchBrowser { prompt, wait_ms } => {
                let mut fields = RuntimeObjectEncoder::new();
                encode_optional_field(&mut fields, "prompt", prompt)?;
                encode_optional_field(&mut fields, "waitMs", wait_ms)?;
                ("switch_browser", fields)
            }
            Self::ListConnectedBrowsers { local_only } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("localOnly", local_only)?;
                ("list_connected_browsers", fields)
            }
            Self::SelectBrowser { device_id } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("deviceId", device_id)?;
                ("select_browser", fields)
            }
            Self::ResolveTab { mention } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("mention", mention)?;
                ("resolve_tab", fields)
            }
            Self::ClaimTab => ("claim_tab", RuntimeObjectEncoder::new()),
            Self::History {
                queries,
                limit,
                from,
                to,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                if !queries.is_empty() {
                    fields.field("queries", queries)?;
                }
                encode_optional_field(&mut fields, "limit", limit)?;
                encode_optional_field(&mut fields, "from", from)?;
                encode_optional_field(&mut fields, "to", to)?;
                ("history", fields)
            }
            Self::TabsContent {
                urls,
                content_type,
                max_chars,
                wait_until,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("urls", urls)?;
                encode_optional_field(&mut fields, "contentType", content_type)?;
                encode_optional_field(&mut fields, "maxChars", max_chars)?;
                encode_optional_field(&mut fields, "waitUntil", wait_until)?;
                ("tabs_content", fields)
            }
            Self::ContentExport {
                format,
                selector,
                max_chars,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                encode_optional_field(&mut fields, "format", format)?;
                encode_optional_field(&mut fields, "selector", selector)?;
                encode_optional_field(&mut fields, "maxChars", max_chars)?;
                ("content_export", fields)
            }
            Self::Download {
                selector,
                url,
                x,
                y,
                filename,
                wait_only,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                encode_optional_field(&mut fields, "selector", selector)?;
                encode_optional_field(&mut fields, "url", url)?;
                encode_optional_field(&mut fields, "x", x)?;
                encode_optional_field(&mut fields, "y", y)?;
                encode_optional_field(&mut fields, "filename", filename)?;
                fields.field("waitOnly", wait_only)?;
                ("download", fields)
            }
            Self::Clipboard {
                action,
                text,
                items,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("action", action)?;
                encode_optional_field(&mut fields, "text", text)?;
                encode_optional_field(&mut fields, "items", items)?;
                ("clipboard", fields)
            }
            Self::Dialog {
                action,
                prompt_text,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("action", action)?;
                encode_optional_field(&mut fields, "promptText", prompt_text)?;
                ("dialog", fields)
            }
            Self::MarkTab { status } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("status", status)?;
                ("mark_tab", fields)
            }
            Self::ReadPage {
                filter,
                depth,
                ref_id,
                max_chars,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                encode_optional_field(&mut fields, "filter", filter)?;
                encode_optional_field(&mut fields, "depth", depth)?;
                encode_optional_field(&mut fields, "refId", ref_id)?;
                encode_optional_field(&mut fields, "maxChars", max_chars)?;
                ("read_page", fields)
            }
            Self::Find { query, limit } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("query", query)?;
                encode_optional_field(&mut fields, "limit", limit)?;
                ("find", fields)
            }
            Self::FormInput { reference, value } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("ref", reference)?;
                fields.field("value", value)?;
                ("form_input", fields)
            }
            Self::Computer {
                action,
                coordinate,
                start_coordinate,
                text,
                duration,
                scroll_direction,
                amount,
                ref_id,
                save,
                keys,
                path,
                scroll_x,
                scroll_y,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("action", action)?;
                encode_optional_field(&mut fields, "coordinate", coordinate)?;
                encode_optional_field(&mut fields, "startCoordinate", start_coordinate)?;
                encode_optional_field(&mut fields, "text", text)?;
                encode_optional_field(&mut fields, "duration", duration)?;
                encode_optional_field(&mut fields, "scrollDirection", scroll_direction)?;
                encode_optional_field(&mut fields, "amount", amount)?;
                encode_optional_field(&mut fields, "keys", keys)?;
                encode_optional_field(&mut fields, "path", path)?;
                encode_optional_field(&mut fields, "scrollX", scroll_x)?;
                encode_optional_field(&mut fields, "scrollY", scroll_y)?;
                encode_optional_field(&mut fields, "refId", ref_id)?;
                fields.field("save", save)?;
                ("computer", fields)
            }
            Self::Tabs { action, url, name } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("action", action)?;
                encode_optional_field(&mut fields, "url", url)?;
                encode_optional_field(&mut fields, "name", name)?;
                ("tabs", fields)
            }
            Self::Locator {
                selector,
                action,
                value,
                options,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("selector", selector)?;
                fields.field("action", action)?;
                encode_optional_field(&mut fields, "value", value)?;
                fields.field("options", options)?;
                ("locator", fields)
            }
            Self::PageText { max_chars } => {
                let mut fields = RuntimeObjectEncoder::new();
                encode_optional_field(&mut fields, "maxChars", max_chars)?;
                ("page_text", fields)
            }
            Self::Batch { actions } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("actions", actions)?;
                ("batch", fields)
            }
            Self::Resize { viewport, reset } => {
                let mut fields = RuntimeObjectEncoder::new();
                encode_optional_field(&mut fields, "viewport", viewport)?;
                fields.field("reset", reset)?;
                ("resize", fields)
            }
            Self::Close { target } => {
                let mut fields = RuntimeObjectEncoder::new();
                encode_optional_field(&mut fields, "target", target)?;
                ("close", fields)
            }
            Self::Permission {
                action,
                origin,
                permissions,
                permission,
                setting,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("action", action)?;
                encode_optional_field(&mut fields, "origin", origin)?;
                encode_optional_field(&mut fields, "permissions", permissions)?;
                encode_optional_field(&mut fields, "permission", permission)?;
                encode_optional_field(&mut fields, "setting", setting)?;
                ("permission", fields)
            }
        };
        runtime_codec::encode_internal(out, "type", &[], tag, &fields.finish())
    }
}

impl RuntimeDecode for BrowserOperation {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "type",
            &[],
            &[
                RuntimeEnumVariant::new("navigate", &[]),
                RuntimeEnumVariant::new("snapshot", &[]),
                RuntimeEnumVariant::new("screenshot", &[]),
                RuntimeEnumVariant::new("click", &[]),
                RuntimeEnumVariant::new("type", &[]),
                RuntimeEnumVariant::new("eval", &[]),
                RuntimeEnumVariant::new("console", &[]),
                RuntimeEnumVariant::new("network", &[]),
                RuntimeEnumVariant::new("gif_creator", &[]),
                RuntimeEnumVariant::new("upload_image", &[]),
                RuntimeEnumVariant::new("file_upload", &[]),
                RuntimeEnumVariant::new("shortcuts_list", &[]),
                RuntimeEnumVariant::new("shortcuts_execute", &[]),
                RuntimeEnumVariant::new("switch_browser", &[]),
                RuntimeEnumVariant::new("list_connected_browsers", &[]),
                RuntimeEnumVariant::new("select_browser", &[]),
                RuntimeEnumVariant::new("history", &[]),
                RuntimeEnumVariant::new("tabs_content", &[]),
                RuntimeEnumVariant::new("content_export", &[]),
                RuntimeEnumVariant::new("download", &[]),
                RuntimeEnumVariant::new("clipboard", &[]),
                RuntimeEnumVariant::new("dialog", &[]),
                RuntimeEnumVariant::new("mark_tab", &[]),
                RuntimeEnumVariant::new("read_page", &[]),
                RuntimeEnumVariant::new("find", &[]),
                RuntimeEnumVariant::new("form_input", &[]),
                RuntimeEnumVariant::new("computer", &[]),
                RuntimeEnumVariant::new("tabs", &[]),
                RuntimeEnumVariant::new("locator", &[]),
                RuntimeEnumVariant::new("page_text", &[]),
                RuntimeEnumVariant::new("batch", &[]),
                RuntimeEnumVariant::new("resize", &[]),
                RuntimeEnumVariant::new("close", &[]),
                RuntimeEnumVariant::new("permission", &[]),
                RuntimeEnumVariant::new("resolve_tab", &[]),
                RuntimeEnumVariant::new("claim_tab", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::Navigate {
                    url: object.required("url", &[])?,
                    wait_until: object.optional("waitUntil", &[])?,
                    viewport: object.optional("viewport", &[])?,
                }),
                1 => Ok(Self::Snapshot {
                    selector: object.optional("selector", &[])?,
                    include_text: object.defaulted("includeText", &[], || false)?,
                    include_links: object.defaulted("includeLinks", &[], || false)?,
                }),
                2 => Ok(Self::Screenshot {
                    criteria: BrowserElementCriteria::decode_fields(object)?,
                    ref_id: object.optional("refId", &["ref"])?,
                    selector: object.optional("selector", &[])?,
                    full_page: object.defaulted("fullPage", &[], || false)?,
                    format: object.optional("format", &[])?,
                    quality: object.optional("quality", &[])?,
                    save: object.defaulted("save", &[], || true)?,
                }),
                3 => Ok(Self::Click {
                    criteria: BrowserElementCriteria::decode_fields(object)?,
                    ref_id: object.optional("refId", &["ref"])?,
                    selector: object.optional("selector", &[])?,
                    x: object.optional("x", &[])?,
                    y: object.optional("y", &[])?,
                    button: object.optional("button", &[])?,
                    click_count: object.optional("clickCount", &[])?,
                    modifiers: object.optional("modifiers", &[])?,
                    force: object.defaulted("force", &[], || false)?,
                }),
                4 => Ok(Self::Type {
                    criteria: BrowserElementCriteria::decode_fields(object)?,
                    ref_id: object.optional("refId", &["ref"])?,
                    selector: object.optional("selector", &[])?,
                    text: object.required("text", &[])?,
                    clear: object.defaulted("clear", &[], || false)?,
                    press_enter: object.defaulted("pressEnter", &[], || false)?,
                    delay_ms: object.optional("delayMs", &[])?,
                    key: object.optional("key", &[])?,
                }),
                5 => Ok(Self::Eval {
                    script: object.required("script", &[])?,
                    args: object.optional("args", &[])?,
                    await_promise: object.defaulted("awaitPromise", &[], || false)?,
                }),
                6 => Ok(Self::Console {
                    levels: object.optional("levels", &[])?,
                    since_ms: object.optional("sinceMs", &[])?,
                    limit: object.optional("limit", &[])?,
                    clear: object.defaulted("clear", &[], || false)?,
                }),
                7 => Ok(Self::Network {
                    since_ms: object.optional("sinceMs", &[])?,
                    limit: object.optional("limit", &[])?,
                    include_bodies: object.defaulted("includeBodies", &[], || false)?,
                    url_filter: object.optional("urlFilter", &[])?,
                }),
                8 => Ok(Self::GifCreator {
                    filename: object.optional("filename", &[])?,
                    duration_seconds: object.optional("durationSeconds", &[])?,
                    fps: object.optional("fps", &[])?,
                    full_page: object.defaulted("fullPage", &[], || false)?,
                }),
                9 => Ok(Self::UploadImage {
                    criteria: BrowserElementCriteria::decode_fields(object)?,
                    ref_id: object.optional("refId", &["ref"])?,
                    path: object.required("path", &[])?,
                    selector: object.optional("selector", &[])?,
                    prompt: object.optional("prompt", &[])?,
                }),
                10 => Ok(Self::FileUpload {
                    criteria: BrowserElementCriteria::decode_fields(object)?,
                    paths: object.required("paths", &[])?,
                    selector: object.optional("selector", &[])?,
                    ref_id: object.optional("refId", &[])?,
                }),
                11 => Ok(Self::ShortcutsList {
                    query: object.optional("query", &[])?,
                }),
                12 => Ok(Self::ShortcutsExecute {
                    name: object.required("name", &[])?,
                    wait_for_completion: object.defaulted("waitForCompletion", &[], || false)?,
                }),
                13 => Ok(Self::SwitchBrowser {
                    prompt: object.optional("prompt", &[])?,
                    wait_ms: object.optional("waitMs", &[])?,
                }),
                14 => Ok(Self::ListConnectedBrowsers {
                    local_only: object.defaulted("localOnly", &[], || false)?,
                }),
                15 => Ok(Self::SelectBrowser {
                    device_id: object.required("deviceId", &[])?,
                }),
                16 => Ok(Self::History {
                    queries: object.defaulted("queries", &[], Vec::new)?,
                    limit: object.optional("limit", &[])?,
                    from: object.optional("from", &[])?,
                    to: object.optional("to", &[])?,
                }),
                17 => Ok(Self::TabsContent {
                    urls: object.required("urls", &[])?,
                    content_type: object.optional("contentType", &[])?,
                    max_chars: object.optional("maxChars", &[])?,
                    wait_until: object.optional("waitUntil", &[])?,
                }),
                18 => Ok(Self::ContentExport {
                    format: object.optional("format", &[])?,
                    selector: object.optional("selector", &[])?,
                    max_chars: object.optional("maxChars", &[])?,
                }),
                19 => Ok(Self::Download {
                    selector: object.optional("selector", &[])?,
                    url: object.optional("url", &[])?,
                    x: object.optional("x", &[])?,
                    y: object.optional("y", &[])?,
                    filename: object.optional("filename", &[])?,
                    wait_only: object.defaulted("waitOnly", &[], || false)?,
                }),
                20 => Ok(Self::Clipboard {
                    action: object.required("action", &[])?,
                    text: object.optional("text", &[])?,
                    items: object.optional("items", &[])?,
                }),
                21 => Ok(Self::Dialog {
                    action: object.required("action", &[])?,
                    prompt_text: object.optional("promptText", &[])?,
                }),
                22 => Ok(Self::MarkTab {
                    status: object.required("status", &[])?,
                }),
                23 => Ok(Self::ReadPage {
                    filter: object.optional("filter", &[])?,
                    depth: object.optional("depth", &[])?,
                    ref_id: object.optional("refId", &[])?,
                    max_chars: object.optional("maxChars", &[])?,
                }),
                24 => Ok(Self::Find {
                    query: object.required("query", &[])?,
                    limit: object.optional("limit", &[])?,
                }),
                25 => Ok(Self::FormInput {
                    reference: object.required("ref", &[])?,
                    value: object.required("value", &[])?,
                }),
                26 => Ok(Self::Computer {
                    action: object.required("action", &[])?,
                    coordinate: object.optional("coordinate", &[])?,
                    start_coordinate: object.optional("startCoordinate", &[])?,
                    text: object.optional("text", &[])?,
                    duration: object.optional("duration", &[])?,
                    scroll_direction: object.optional("scrollDirection", &[])?,
                    amount: object.optional("amount", &[])?,
                    ref_id: object.optional("refId", &[])?,
                    save: object.defaulted("save", &[], || false)?,
                    keys: object.optional("keys", &[])?,
                    path: object.optional("path", &[])?,
                    scroll_x: object.optional("scrollX", &[])?,
                    scroll_y: object.optional("scrollY", &[])?,
                }),
                27 => Ok(Self::Tabs {
                    action: object.required("action", &[])?,
                    url: object.optional("url", &[])?,
                    name: object.optional("name", &[])?,
                }),
                28 => Ok(Self::Locator {
                    selector: object.required("selector", &[])?,
                    action: object.required("action", &[])?,
                    value: object.optional("value", &[])?,
                    options: object.defaulted("options", &[], || RuntimeJson::Null)?,
                }),
                29 => Ok(Self::PageText {
                    max_chars: object.optional("maxChars", &[])?,
                }),
                30 => Ok(Self::Batch {
                    actions: object.required("actions", &[])?,
                }),
                31 => Ok(Self::Resize {
                    viewport: object.optional("viewport", &[])?,
                    reset: object.defaulted("reset", &[], || false)?,
                }),
                32 => Ok(Self::Close {
                    target: object.optional("target", &[])?,
                }),
                33 => Ok(Self::Permission {
                    action: object.required("action", &[])?,
                    origin: object.optional("origin", &[])?,
                    permissions: object.optional("permissions", &[])?,
                    permission: object.optional("permission", &[])?,
                    setting: object.optional("setting", &[])?,
                }),
                34 => Ok(Self::ResolveTab {
                    mention: object.required("mention", &[])?,
                }),
                35 => Ok(Self::ClaimTab),
                _ => Err(RuntimeCodecError::new(
                    RuntimeCodecErrorKind::UnknownEnumVariant,
                )),
            },
        )
    }
}

/// A complete browser request as delivered to a host/plugin runtime.
#[derive(Debug, Clone, PartialEq)]
pub struct BrowserRuntimeRequest {
    pub operation: BrowserOperation,
    pub target: BrowserTarget,
    pub permissions: BrowserPermissionSet,
    pub timeout_ms: Option<u64>,
    pub metadata: BTreeMap<RuntimeString, RuntimeJson>,
}

impl BrowserRuntimeRequest {
    pub fn new(
        operation: BrowserOperation,
        target: BrowserTarget,
        timeout_ms: Option<u64>,
    ) -> Self {
        let permissions = BrowserPermissionSet::for_operation(&operation);
        BrowserRuntimeRequest {
            operation,
            target,
            permissions,
            timeout_ms,
            metadata: BTreeMap::new(),
        }
    }
}

impl RuntimeEncode for BrowserRuntimeRequest {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {
            operation,
            target,
            permissions,
            timeout_ms,
            metadata,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("operation", operation)?;
        object.field("target", target)?;
        object.field("permissions", permissions)?;
        encode_optional_field(&mut object, "timeoutMs", timeout_ms)?;
        if !metadata.is_empty() {
            object.field_value("metadata", encode_runtime_btree_map(metadata));
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for BrowserRuntimeRequest {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                operation: object.required("operation", &[])?,
                target: object.required("target", &[])?,
                permissions: object.required("permissions", &[])?,
                timeout_ms: object.optional("timeoutMs", &[])?,
                metadata: match object.optional::<RuntimeJson>("metadata", &[])? {
                    Some(value) => decode_runtime_btree_map(&value)?,
                    None => BTreeMap::new(),
                },
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BrowserElementSnapshot {
    pub selector: Option<RuntimeString>,
    pub role: Option<RuntimeString>,
    pub name: Option<RuntimeString>,
    pub text: Option<RuntimeString>,
    pub x: Option<f64>,
    pub y: Option<f64>,
    pub width: Option<f64>,
    pub height: Option<f64>,
    pub visible: Option<bool>,
    pub disabled: Option<bool>,
}

impl RuntimeEncode for BrowserElementSnapshot {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {
            selector,
            role,
            name,
            text,
            x,
            y,
            width,
            height,
            visible,
            disabled,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        encode_optional_field(&mut object, "selector", selector)?;
        encode_optional_field(&mut object, "role", role)?;
        encode_optional_field(&mut object, "name", name)?;
        encode_optional_field(&mut object, "text", text)?;
        encode_optional_field(&mut object, "x", x)?;
        encode_optional_field(&mut object, "y", y)?;
        encode_optional_field(&mut object, "width", width)?;
        encode_optional_field(&mut object, "height", height)?;
        encode_optional_field(&mut object, "visible", visible)?;
        encode_optional_field(&mut object, "disabled", disabled)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for BrowserElementSnapshot {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                selector: object.optional("selector", &[])?,
                role: object.optional("role", &[])?,
                name: object.optional("name", &[])?,
                text: object.optional("text", &[])?,
                x: object.optional("x", &[])?,
                y: object.optional("y", &[])?,
                width: object.optional("width", &[])?,
                height: object.optional("height", &[])?,
                visible: object.optional("visible", &[])?,
                disabled: object.optional("disabled", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct BrowserSnapshot {
    pub html: Option<RuntimeString>,
    pub text: Option<RuntimeString>,
    pub elements: Vec<BrowserElementSnapshot>,
}

impl RuntimeEncode for BrowserSnapshot {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {
            html,
            text,
            elements,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        encode_optional_field(&mut object, "html", html)?;
        encode_optional_field(&mut object, "text", text)?;
        if !elements.is_empty() {
            object.field("elements", elements)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for BrowserSnapshot {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                html: object.optional("html", &[])?,
                text: object.optional("text", &[])?,
                elements: object.defaulted("elements", &[], Vec::new)?,
            })
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BrowserArtifactKind {
    Screenshot,
    Gif,
    DomSnapshot,
    Har,
    Download,
    Trace,
    Video,
    ConsoleLog,
    NetworkLog,
    Other,
}

impl RuntimeEncode for BrowserArtifactKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Screenshot => "screenshot",
            Self::Gif => "gif",
            Self::DomSnapshot => "domsnapshot",
            Self::Har => "har",
            Self::Download => "download",
            Self::Trace => "trace",
            Self::Video => "video",
            Self::ConsoleLog => "consolelog",
            Self::NetworkLog => "networklog",
            Self::Other => "other",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for BrowserArtifactKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("screenshot", &[]),
                RuntimeEnumVariant::new("gif", &[]),
                RuntimeEnumVariant::new("domsnapshot", &[]),
                RuntimeEnumVariant::new("har", &[]),
                RuntimeEnumVariant::new("download", &[]),
                RuntimeEnumVariant::new("trace", &[]),
                RuntimeEnumVariant::new("video", &[]),
                RuntimeEnumVariant::new("consolelog", &[]),
                RuntimeEnumVariant::new("networklog", &[]),
                RuntimeEnumVariant::new("other", &[]),
            ],
            &[
                Self::Screenshot,
                Self::Gif,
                Self::DomSnapshot,
                Self::Har,
                Self::Download,
                Self::Trace,
                Self::Video,
                Self::ConsoleLog,
                Self::NetworkLog,
                Self::Other,
            ],
        )
    }
}

/// Artifact descriptors are references to host-managed files/objects. The core
/// records paths/URIs and metadata, but does not move large browser bytes.
#[derive(Debug, Clone, PartialEq)]
pub struct BrowserArtifact {
    pub id: RuntimeString,
    pub kind: BrowserArtifactKind,
    pub uri: Option<RuntimeString>,
    pub path: Option<RuntimeString>,
    pub mime_type: Option<RuntimeString>,
    pub size_bytes: Option<u64>,
    pub metadata: BTreeMap<RuntimeString, RuntimeJson>,
}

impl RuntimeEncode for BrowserArtifact {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {
            id,
            kind,
            uri,
            path,
            mime_type,
            size_bytes,
            metadata,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("id", id)?;
        object.field("kind", kind)?;
        encode_optional_field(&mut object, "uri", uri)?;
        encode_optional_field(&mut object, "path", path)?;
        encode_optional_field(&mut object, "mimeType", mime_type)?;
        encode_optional_field(&mut object, "sizeBytes", size_bytes)?;
        if !metadata.is_empty() {
            object.field_value("metadata", encode_runtime_btree_map(metadata));
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for BrowserArtifact {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                id: object.required("id", &[])?,
                kind: object.required("kind", &[])?,
                uri: object.optional("uri", &[])?,
                path: object.optional("path", &[])?,
                mime_type: object.optional("mimeType", &[])?,
                size_bytes: object.optional("sizeBytes", &[])?,
                metadata: match object.optional::<RuntimeJson>("metadata", &[])? {
                    Some(value) => decode_runtime_btree_map(&value)?,
                    None => BTreeMap::new(),
                },
            })
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BrowserConsoleLevel {
    Debug,
    Info,
    Warning,
    Error,
    Log,
}

impl RuntimeEncode for BrowserConsoleLevel {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
            Self::Log => "log",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for BrowserConsoleLevel {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("debug", &[]),
                RuntimeEnumVariant::new("info", &[]),
                RuntimeEnumVariant::new("warning", &[]),
                RuntimeEnumVariant::new("error", &[]),
                RuntimeEnumVariant::new("log", &[]),
            ],
            &[
                Self::Debug,
                Self::Info,
                Self::Warning,
                Self::Error,
                Self::Log,
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BrowserConsoleEntry {
    pub level: BrowserConsoleLevel,
    pub text: RuntimeString,
    pub timestamp_ms: Option<u64>,
    pub url: Option<RuntimeString>,
    pub line: Option<u32>,
    pub column: Option<u32>,
    pub source: Option<RuntimeString>,
    pub args: Option<Vec<RuntimeJson>>,
}

impl RuntimeEncode for BrowserConsoleEntry {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {
            level,
            text,
            timestamp_ms,
            url,
            line,
            column,
            source,
            args,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("level", level)?;
        object.field("text", text)?;
        encode_optional_field(&mut object, "timestampMs", timestamp_ms)?;
        encode_optional_field(&mut object, "url", url)?;
        encode_optional_field(&mut object, "line", line)?;
        encode_optional_field(&mut object, "column", column)?;
        encode_optional_field(&mut object, "source", source)?;
        encode_optional_field(&mut object, "args", args)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for BrowserConsoleEntry {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                level: object.required("level", &[])?,
                text: object.required("text", &[])?,
                timestamp_ms: object.optional("timestampMs", &[])?,
                url: object.optional("url", &[])?,
                line: object.optional("line", &[])?,
                column: object.optional("column", &[])?,
                source: object.optional("source", &[])?,
                args: object.optional("args", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BrowserNetworkEntry {
    pub request_id: RuntimeString,
    pub url: RuntimeString,
    pub method: RuntimeString,
    pub status: Option<u16>,
    pub resource_type: Option<RuntimeString>,
    pub request_headers: Option<RuntimeJson>,
    pub response_headers: Option<RuntimeJson>,
    pub error: Option<RuntimeString>,
    pub started_at_ms: Option<u64>,
    pub ended_at_ms: Option<u64>,
    pub mime_type: Option<RuntimeString>,
    pub failed: Option<bool>,
    pub body: Option<RuntimeString>,
    pub base64_encoded: Option<bool>,
    pub body_truncated: Option<bool>,
    pub body_error: Option<RuntimeString>,
}

impl RuntimeEncode for BrowserNetworkEntry {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {
            request_id,
            url,
            method,
            status,
            resource_type,
            request_headers,
            response_headers,
            error,
            started_at_ms,
            ended_at_ms,
            mime_type,
            failed,
            body,
            base64_encoded,
            body_truncated,
            body_error,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("requestId", request_id)?;
        object.field("url", url)?;
        object.field("method", method)?;
        encode_optional_field(&mut object, "status", status)?;
        encode_optional_field(&mut object, "resourceType", resource_type)?;
        encode_optional_field(&mut object, "requestHeaders", request_headers)?;
        encode_optional_field(&mut object, "responseHeaders", response_headers)?;
        encode_optional_field(&mut object, "error", error)?;
        encode_optional_field(&mut object, "startedAtMs", started_at_ms)?;
        encode_optional_field(&mut object, "endedAtMs", ended_at_ms)?;
        encode_optional_field(&mut object, "mimeType", mime_type)?;
        encode_optional_field(&mut object, "failed", failed)?;
        encode_optional_field(&mut object, "body", body)?;
        encode_optional_field(&mut object, "base64Encoded", base64_encoded)?;
        encode_optional_field(&mut object, "bodyTruncated", body_truncated)?;
        encode_optional_field(&mut object, "bodyError", body_error)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for BrowserNetworkEntry {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                request_id: object.required("requestId", &[])?,
                url: object.required("url", &[])?,
                method: object.required("method", &[])?,
                status: object.optional("status", &[])?,
                resource_type: object.optional("resourceType", &[])?,
                request_headers: object.optional("requestHeaders", &[])?,
                response_headers: object.optional("responseHeaders", &[])?,
                error: object.optional("error", &[])?,
                started_at_ms: object.optional("startedAtMs", &[])?,
                ended_at_ms: object.optional("endedAtMs", &[])?,
                mime_type: object.optional("mimeType", &[])?,
                failed: object.optional("failed", &[])?,
                body: object.optional("body", &[])?,
                base64_encoded: object.optional("base64Encoded", &[])?,
                body_truncated: object.optional("bodyTruncated", &[])?,
                body_error: object.optional("bodyError", &[])?,
            })
        })
    }
}

/// Structured browser-domain events emitted while a request runs. These are
/// render-agnostic and safe for lifecycle logs.
#[derive(Debug, Clone, PartialEq)]
pub enum BrowserRuntimeEvent {
    Status {
        message: RuntimeString,
    },
    SessionStarted {
        session_id: RuntimeString,
    },
    TabOpened {
        session_id: RuntimeString,
        tab_id: RuntimeString,
    },
    NavigationStarted {
        url: RuntimeString,
        session_id: Option<RuntimeString>,
        tab_id: Option<RuntimeString>,
    },
    NavigationCompleted {
        url: RuntimeString,
        status: Option<u16>,
        title: Option<RuntimeString>,
    },
    DomSnapshot {
        snapshot: BrowserSnapshot,
    },
    ScreenshotCaptured {
        artifact: BrowserArtifact,
    },
    Console {
        entry: BrowserConsoleEntry,
    },
    Network {
        entry: Box<BrowserNetworkEntry>,
    },
    Artifact {
        artifact: BrowserArtifact,
    },
    Download {
        artifact: BrowserArtifact,
    },
    Error {
        code: BrowserRuntimeErrorCode,
        message: RuntimeString,
    },
}

impl RuntimeEncode for BrowserRuntimeEvent {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let (tag, fields) = match self {
            Self::Status { message } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("message", message)?;
                ("status", fields)
            }
            Self::SessionStarted { session_id } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("sessionId", session_id)?;
                ("session_started", fields)
            }
            Self::TabOpened { session_id, tab_id } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("sessionId", session_id)?;
                fields.field("tabId", tab_id)?;
                ("tab_opened", fields)
            }
            Self::NavigationStarted {
                url,
                session_id,
                tab_id,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("url", url)?;
                encode_optional_field(&mut fields, "sessionId", session_id)?;
                encode_optional_field(&mut fields, "tabId", tab_id)?;
                ("navigation_started", fields)
            }
            Self::NavigationCompleted { url, status, title } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("url", url)?;
                encode_optional_field(&mut fields, "status", status)?;
                encode_optional_field(&mut fields, "title", title)?;
                ("navigation_completed", fields)
            }
            Self::DomSnapshot { snapshot } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("snapshot", snapshot)?;
                ("dom_snapshot", fields)
            }
            Self::ScreenshotCaptured { artifact } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("artifact", artifact)?;
                ("screenshot_captured", fields)
            }
            Self::Console { entry } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("entry", entry)?;
                ("console", fields)
            }
            Self::Network { entry } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("entry", entry)?;
                ("network", fields)
            }
            Self::Artifact { artifact } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("artifact", artifact)?;
                ("artifact", fields)
            }
            Self::Download { artifact } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("artifact", artifact)?;
                ("download", fields)
            }
            Self::Error { code, message } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("code", code)?;
                fields.field("message", message)?;
                ("error", fields)
            }
        };
        runtime_codec::encode_internal(out, "type", &[], tag, &fields.finish())
    }
}

impl RuntimeDecode for BrowserRuntimeEvent {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "type",
            &[],
            &[
                RuntimeEnumVariant::new("status", &[]),
                RuntimeEnumVariant::new("session_started", &[]),
                RuntimeEnumVariant::new("tab_opened", &[]),
                RuntimeEnumVariant::new("navigation_started", &[]),
                RuntimeEnumVariant::new("navigation_completed", &[]),
                RuntimeEnumVariant::new("dom_snapshot", &[]),
                RuntimeEnumVariant::new("screenshot_captured", &[]),
                RuntimeEnumVariant::new("console", &[]),
                RuntimeEnumVariant::new("network", &[]),
                RuntimeEnumVariant::new("artifact", &[]),
                RuntimeEnumVariant::new("download", &[]),
                RuntimeEnumVariant::new("error", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::Status {
                    message: object.required("message", &[])?,
                }),
                1 => Ok(Self::SessionStarted {
                    session_id: object.required("sessionId", &[])?,
                }),
                2 => Ok(Self::TabOpened {
                    session_id: object.required("sessionId", &[])?,
                    tab_id: object.required("tabId", &[])?,
                }),
                3 => Ok(Self::NavigationStarted {
                    url: object.required("url", &[])?,
                    session_id: object.optional("sessionId", &[])?,
                    tab_id: object.optional("tabId", &[])?,
                }),
                4 => Ok(Self::NavigationCompleted {
                    url: object.required("url", &[])?,
                    status: object.optional("status", &[])?,
                    title: object.optional("title", &[])?,
                }),
                5 => Ok(Self::DomSnapshot {
                    snapshot: object.required("snapshot", &[])?,
                }),
                6 => Ok(Self::ScreenshotCaptured {
                    artifact: object.required("artifact", &[])?,
                }),
                7 => Ok(Self::Console {
                    entry: object.required("entry", &[])?,
                }),
                8 => Ok(Self::Network {
                    entry: object.required("entry", &[])?,
                }),
                9 => Ok(Self::Artifact {
                    artifact: object.required("artifact", &[])?,
                }),
                10 => Ok(Self::Download {
                    artifact: object.required("artifact", &[])?,
                }),
                11 => Ok(Self::Error {
                    code: object.required("code", &[])?,
                    message: object.required("message", &[])?,
                }),
                _ => Err(RuntimeCodecError::new(
                    RuntimeCodecErrorKind::UnknownEnumVariant,
                )),
            },
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct BrowserRuntimeResponse {
    pub session_id: Option<RuntimeString>,
    pub tab_id: Option<RuntimeString>,
    pub url: Option<RuntimeString>,
    pub title: Option<RuntimeString>,
    pub text: Option<RuntimeString>,
    pub snapshot: Option<BrowserSnapshot>,
    pub value: Option<RuntimeJson>,
    pub artifacts: Vec<BrowserArtifact>,
    pub console: Vec<BrowserConsoleEntry>,
    pub network: Vec<BrowserNetworkEntry>,
    pub events: Vec<BrowserRuntimeEvent>,
    pub message: Option<RuntimeString>,
    pub count: Option<u64>,
    pub total: Option<u64>,
    pub truncated: Option<bool>,
    pub evicted: Option<u64>,
    pub error: Option<BrowserRuntimeError>,
    pub partial: bool,
}

impl RuntimeEncode for BrowserRuntimeResponse {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {
            session_id,
            tab_id,
            url,
            title,
            text,
            snapshot,
            value,
            artifacts,
            console,
            network,
            events,
            message,
            count,
            total,
            truncated,
            evicted,
            error,
            partial,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        encode_optional_field(&mut object, "sessionId", session_id)?;
        encode_optional_field(&mut object, "tabId", tab_id)?;
        encode_optional_field(&mut object, "url", url)?;
        encode_optional_field(&mut object, "title", title)?;
        encode_optional_field(&mut object, "text", text)?;
        encode_optional_field(&mut object, "snapshot", snapshot)?;
        encode_optional_field(&mut object, "value", value)?;
        if !artifacts.is_empty() {
            object.field("artifacts", artifacts)?;
        }
        if !console.is_empty() {
            object.field("console", console)?;
        }
        if !network.is_empty() {
            object.field("network", network)?;
        }
        if !events.is_empty() {
            object.field("events", events)?;
        }
        encode_optional_field(&mut object, "message", message)?;
        encode_optional_field(&mut object, "count", count)?;
        encode_optional_field(&mut object, "total", total)?;
        encode_optional_field(&mut object, "truncated", truncated)?;
        encode_optional_field(&mut object, "evicted", evicted)?;
        encode_optional_field(&mut object, "error", error)?;
        if *partial {
            object.field("partial", partial)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for BrowserRuntimeResponse {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                session_id: object.optional("sessionId", &[])?,
                tab_id: object.optional("tabId", &[])?,
                url: object.optional("url", &[])?,
                title: object.optional("title", &[])?,
                text: object.optional("text", &[])?,
                snapshot: object.optional("snapshot", &[])?,
                value: object.optional("value", &[])?,
                artifacts: object.defaulted("artifacts", &[], Vec::new)?,
                console: object.defaulted("console", &[], Vec::new)?,
                network: object.defaulted("network", &[], Vec::new)?,
                events: object.defaulted("events", &[], Vec::new)?,
                message: object.optional("message", &[])?,
                count: object.optional("count", &[])?,
                total: object.optional("total", &[])?,
                truncated: object.optional("truncated", &[])?,
                evicted: object.optional("evicted", &[])?,
                error: object.optional("error", &[])?,
                partial: object.defaulted("partial", &[], || false)?,
            })
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserRuntimeErrorCode {
    Unavailable,
    PermissionDenied,
    Timeout,
    Cancelled,
    NotFound,
    InvalidRequest,
    NavigationFailed,
    EvaluationFailed,
    HostError,
}

impl RuntimeEncode for BrowserRuntimeErrorCode {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Unavailable => "unavailable",
            Self::PermissionDenied => "permission_denied",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::NotFound => "not_found",
            Self::InvalidRequest => "invalid_request",
            Self::NavigationFailed => "navigation_failed",
            Self::EvaluationFailed => "evaluation_failed",
            Self::HostError => "host_error",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for BrowserRuntimeErrorCode {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("unavailable", &[]),
                RuntimeEnumVariant::new("permission_denied", &[]),
                RuntimeEnumVariant::new("timeout", &[]),
                RuntimeEnumVariant::new("cancelled", &[]),
                RuntimeEnumVariant::new("not_found", &[]),
                RuntimeEnumVariant::new("invalid_request", &[]),
                RuntimeEnumVariant::new("navigation_failed", &[]),
                RuntimeEnumVariant::new("evaluation_failed", &[]),
                RuntimeEnumVariant::new("host_error", &[]),
            ],
            &[
                Self::Unavailable,
                Self::PermissionDenied,
                Self::Timeout,
                Self::Cancelled,
                Self::NotFound,
                Self::InvalidRequest,
                Self::NavigationFailed,
                Self::EvaluationFailed,
                Self::HostError,
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BrowserRuntimeError {
    pub code: BrowserRuntimeErrorCode,
    pub message: RuntimeString,
    pub details: Option<RuntimeJson>,
}

impl BrowserRuntimeError {
    pub fn new(code: BrowserRuntimeErrorCode, message: impl Into<RuntimeString>) -> Self {
        BrowserRuntimeError {
            code,
            message: message.into(),
            details: None,
        }
    }
}

impl RuntimeEncode for BrowserRuntimeError {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {
            code,
            message,
            details,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("code", code)?;
        object.field("message", message)?;
        encode_optional_field(&mut object, "details", details)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for BrowserRuntimeError {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                code: object.required("code", &[])?,
                message: object.required("message", &[])?,
                details: object.optional("details", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BrowserHostToolRequest {
    pub tool_name: RuntimeString,
    pub arguments: RuntimeJson,
    pub browser_request: BrowserRuntimeRequest,
}

impl RuntimeEncode for BrowserHostToolRequest {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {
            tool_name,
            arguments,
            browser_request,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("toolName", tool_name)?;
        object.field("arguments", arguments)?;
        object.field("browserRequest", browser_request)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for BrowserHostToolRequest {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                tool_name: object.required("toolName", &[])?,
                arguments: object.required("arguments", &[])?,
                browser_request: object.required("browserRequest", &[])?,
            })
        })
    }
}

pub type BrowserHostToolInvoker = Arc<
    dyn Fn(
            BrowserHostToolRequest,
            AbortSignal,
            BrowserEventSink,
        ) -> BoxFuture<'static, Result<BrowserRuntimeResponse, BrowserRuntimeError>>
        + Send
        + Sync,
>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowserHostToolMapping {
    pub navigate: String,
    pub snapshot: String,
    pub screenshot: String,
    pub click: String,
    pub type_text: String,
    pub eval: String,
    pub console: String,
    pub network: String,
    pub read_page: String,
    pub find: String,
    pub form_input: String,
    pub computer: String,
    pub tabs: String,
    pub locator: String,
    pub page_text: String,
    pub batch: String,
    pub resize: String,
    pub close: String,
    pub gif_creator: String,
    pub upload_image: String,
    pub file_upload: String,
    pub shortcuts_list: String,
    pub shortcuts_execute: String,
    pub switch_browser: String,
    pub list_connected_browsers: String,
    pub select_browser: String,
    pub resolve_tab: String,
    pub claim_tab: String,
    pub history: String,
    pub tabs_content: String,
    pub content_export: String,
    pub download: String,
    pub clipboard: String,
    pub dialog: String,
    pub mark_tab: String,
    pub permission: String,
}

impl Default for BrowserHostToolMapping {
    fn default() -> Self {
        Self {
            navigate: "browser_navigate".to_string(),
            snapshot: "browser_snapshot".to_string(),
            screenshot: "browser_screenshot".to_string(),
            click: "browser_click".to_string(),
            type_text: "browser_type".to_string(),
            eval: "browser_eval".to_string(),
            console: "browser_console".to_string(),
            network: "browser_network".to_string(),
            read_page: "browser_read_page".to_string(),
            find: "browser_find".to_string(),
            form_input: "browser_form_input".to_string(),
            computer: "browser_computer".to_string(),
            tabs: "browser_tabs".to_string(),
            locator: "browser_locator".to_string(),
            page_text: "browser_page_text".to_string(),
            batch: "browser_batch".to_string(),
            resize: "browser_resize".to_string(),
            close: "browser_close".to_string(),
            gif_creator: "browser_gif_creator".to_string(),
            upload_image: "browser_upload_image".to_string(),
            file_upload: "browser_file_upload".to_string(),
            shortcuts_list: "browser_shortcuts_list".to_string(),
            shortcuts_execute: "browser_shortcuts_execute".to_string(),
            switch_browser: "browser_switch_browser".to_string(),
            list_connected_browsers: "browser_list_connected_browsers".to_string(),
            select_browser: "browser_select_browser".to_string(),
            resolve_tab: "browser_resolve_tab".to_string(),
            claim_tab: "browser_claim_tab".to_string(),
            history: "browser_history".to_string(),
            tabs_content: "browser_tabs_content".to_string(),
            content_export: "browser_content_export".to_string(),
            download: "browser_download".to_string(),
            clipboard: "browser_clipboard".to_string(),
            dialog: "browser_dialog".to_string(),
            mark_tab: "browser_mark_tab".to_string(),
            permission: "browser_permission".to_string(),
        }
    }
}

impl BrowserHostToolMapping {
    pub fn with_prefix(prefix: impl AsRef<str>) -> Self {
        let prefix = prefix.as_ref();
        Self {
            navigate: format!("{prefix}navigate"),
            snapshot: format!("{prefix}snapshot"),
            screenshot: format!("{prefix}screenshot"),
            click: format!("{prefix}click"),
            type_text: format!("{prefix}type"),
            eval: format!("{prefix}eval"),
            console: format!("{prefix}console"),
            network: format!("{prefix}network"),
            read_page: format!("{prefix}read_page"),
            find: format!("{prefix}find"),
            form_input: format!("{prefix}form_input"),
            computer: format!("{prefix}computer"),
            tabs: format!("{prefix}tabs"),
            locator: format!("{prefix}locator"),
            page_text: format!("{prefix}page_text"),
            batch: format!("{prefix}batch"),
            resize: format!("{prefix}resize"),
            close: format!("{prefix}close"),
            gif_creator: format!("{prefix}gif_creator"),
            upload_image: format!("{prefix}upload_image"),
            file_upload: format!("{prefix}file_upload"),
            shortcuts_list: format!("{prefix}shortcuts_list"),
            shortcuts_execute: format!("{prefix}shortcuts_execute"),
            switch_browser: format!("{prefix}switch_browser"),
            list_connected_browsers: format!("{prefix}list_connected_browsers"),
            select_browser: format!("{prefix}select_browser"),
            resolve_tab: format!("{prefix}resolve_tab"),
            claim_tab: format!("{prefix}claim_tab"),
            history: format!("{prefix}history"),
            tabs_content: format!("{prefix}tabs_content"),
            content_export: format!("{prefix}content_export"),
            download: format!("{prefix}download"),
            clipboard: format!("{prefix}clipboard"),
            dialog: format!("{prefix}dialog"),
            mark_tab: format!("{prefix}mark_tab"),
            permission: format!("{prefix}permission"),
        }
    }

    pub fn for_mcp_server(server_name: impl AsRef<str>) -> Self {
        Self::with_prefix(format!("mcp__{}__", server_name.as_ref()))
    }

    fn tool_for(&self, operation: &BrowserOperation) -> &str {
        match operation {
            BrowserOperation::Navigate { .. } => &self.navigate,
            BrowserOperation::Snapshot { .. } => &self.snapshot,
            BrowserOperation::Screenshot { .. } => &self.screenshot,
            BrowserOperation::Click { .. } => &self.click,
            BrowserOperation::Type { .. } => &self.type_text,
            BrowserOperation::Eval { .. } => &self.eval,
            BrowserOperation::Console { .. } => &self.console,
            BrowserOperation::Network { .. } => &self.network,
            BrowserOperation::ReadPage { .. } => &self.read_page,
            BrowserOperation::Find { .. } => &self.find,
            BrowserOperation::FormInput { .. } => &self.form_input,
            BrowserOperation::Computer { .. } => &self.computer,
            BrowserOperation::Tabs { .. } => &self.tabs,
            BrowserOperation::Locator { .. } => &self.locator,
            BrowserOperation::PageText { .. } => &self.page_text,
            BrowserOperation::Batch { .. } => &self.batch,
            BrowserOperation::Resize { .. } => &self.resize,
            BrowserOperation::Close { .. } => &self.close,
            BrowserOperation::GifCreator { .. } => &self.gif_creator,
            BrowserOperation::UploadImage { .. } => &self.upload_image,
            BrowserOperation::FileUpload { .. } => &self.file_upload,
            BrowserOperation::ShortcutsList { .. } => &self.shortcuts_list,
            BrowserOperation::ShortcutsExecute { .. } => &self.shortcuts_execute,
            BrowserOperation::SwitchBrowser { .. } => &self.switch_browser,
            BrowserOperation::ListConnectedBrowsers { .. } => &self.list_connected_browsers,
            BrowserOperation::SelectBrowser { .. } => &self.select_browser,
            BrowserOperation::ResolveTab { .. } => &self.resolve_tab,
            BrowserOperation::ClaimTab => &self.claim_tab,
            BrowserOperation::History { .. } => &self.history,
            BrowserOperation::TabsContent { .. } => &self.tabs_content,
            BrowserOperation::ContentExport { .. } => &self.content_export,
            BrowserOperation::Download { .. } => &self.download,
            BrowserOperation::Clipboard { .. } => &self.clipboard,
            BrowserOperation::Dialog { .. } => &self.dialog,
            BrowserOperation::MarkTab { .. } => &self.mark_tab,
            BrowserOperation::Permission { .. } => &self.permission,
        }
    }
}

pub(crate) struct BrowserHostToolRuntime {
    mapping: BrowserHostToolMapping,
    invoke: BrowserHostToolInvoker,
}

impl BrowserHostToolRuntime {
    pub(crate) fn new(mapping: BrowserHostToolMapping, invoke: BrowserHostToolInvoker) -> Self {
        Self { mapping, invoke }
    }

    pub(crate) fn from_invoke_tool(
        mapping: BrowserHostToolMapping,
        invoke_tool: InvokeToolFn,
    ) -> Self {
        Self::new(
            mapping,
            Arc::new(move |request, signal, emit| {
                let invoke_tool = invoke_tool.clone();
                Box::pin(async move {
                    let tool_name = request.tool_name.clone();
                    emit(BrowserRuntimeEvent::Status {
                        message: RuntimeString::from("Calling browser runtime tool ")
                            .concat(&tool_name)
                            .concat(&RuntimeString::from(".")),
                    });
                    let result = tokio::select! {
                        _ = signal.cancelled() => {
                            return Err(BrowserRuntimeError::new(
                                BrowserRuntimeErrorCode::Cancelled,
                                "Browser host tool invocation cancelled.",
                            ));
                        }
                        result = invoke_tool(tool_name.clone(), request.arguments.clone()) => result,
                    };
                    let result = result.map_err(|error| {
                        BrowserRuntimeError::new(BrowserRuntimeErrorCode::HostError, error.message)
                    })?;
                    let response = parse_invoke_tool_browser_result(result)?;
                    for event in &response.events {
                        emit(event.clone());
                    }
                    Ok(response)
                })
            }),
        )
    }
}

#[async_trait]
unsafe impl BrowserExecutor for BrowserHostToolRuntime {
    async fn execute(
        &self,
        request: BrowserRuntimeRequest,
        signal: AbortSignal,
        emit: BrowserEventSink,
    ) -> Result<BrowserRuntimeResponse, BrowserRuntimeError> {
        let tool_name = RuntimeString::from(self.mapping.tool_for(&request.operation));
        let arguments = runtime_codec::encode_value(&request).map_err(|err| {
            BrowserRuntimeError::new(
                BrowserRuntimeErrorCode::InvalidRequest,
                format!("failed to encode browser request for host tool: {err}"),
            )
        })?;
        (self.invoke)(
            BrowserHostToolRequest {
                tool_name,
                arguments,
                browser_request: request,
            },
            signal,
            emit,
        )
        .await
    }
}

impl fmt::Display for BrowserRuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: ", self.code)?;
        for point in self.message.code_points() {
            if let Some(value) = char::from_u32(point) {
                write!(f, "{value}")?;
            } else {
                write!(f, "\\u{point:04x}")?;
            }
        }
        Ok(())
    }
}

fn parse_invoke_tool_browser_result(
    result: InvokeToolResult,
) -> Result<BrowserRuntimeResponse, BrowserRuntimeError> {
    let output = result
        .output
        .as_ref()
        .map(|output| output.get("structuredContent").unwrap_or(output));
    let response_value = output.map(|output| output.get("response").unwrap_or(output));
    if result.is_error {
        if let Some(value) = response_value {
            if let Ok(response) = runtime_codec::decode_value::<BrowserRuntimeResponse>(value) {
                if response.partial && response.error.is_some() {
                    return Ok(response);
                }
            }
            if let Some(error) = value
                .get("error")
                .or_else(|| output.and_then(|output| output.get("error")))
            {
                if let Ok(parsed) = runtime_codec::decode_value::<BrowserRuntimeError>(error) {
                    return Err(parsed);
                }
            }
        }
        return Err(BrowserRuntimeError::new(
            BrowserRuntimeErrorCode::HostError,
            invoke_tool_result_text(&result)
                .unwrap_or_else(|| RuntimeString::from("Browser host tool reported an error.")),
        ));
    }
    let response_value = response_value.ok_or_else(|| {
        BrowserRuntimeError::new(
            BrowserRuntimeErrorCode::HostError,
            "Browser host tool did not return structured output.",
        )
    })?;
    runtime_codec::decode_value::<BrowserRuntimeResponse>(response_value).map_err(|err| {
        BrowserRuntimeError::new(
            BrowserRuntimeErrorCode::HostError,
            format!("Browser host tool returned invalid runtime response: {err}"),
        )
    })
}

fn invoke_tool_result_text(result: &InvokeToolResult) -> Option<RuntimeString> {
    let parts = result
        .content
        .iter()
        .filter_map(|block| match block {
            OutputBlock::Text(text) => Some(text.text.clone()),
            OutputBlock::Image(_) => None,
        })
        .collect::<Vec<_>>();
    if parts.is_empty() {
        None
    } else {
        let separator = RuntimeString::from("\n");
        let mut joined = parts[0].clone();
        for part in &parts[1..] {
            joined = joined.concat(&separator).concat(part);
        }
        Some(joined)
    }
}

impl std::error::Error for BrowserRuntimeError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn parse_runtime_json(source: &str) -> RuntimeJson {
        crate::runtime_values::runtime_json::parse(&RuntimeString::from(source)).unwrap()
    }

    /// `browserId` addresses which browser runs one operation. It has to survive
    /// the round trip, and its absence has to stay absent rather than encoding a
    /// null: the host reads a missing field as "use the browser this session
    /// selected", and an explicit null would be a different statement.
    #[test]
    fn a_target_carries_the_browser_it_names_and_omits_it_when_unset() {
        let named = BrowserTarget {
            session_id: Some(RuntimeString::from("s1")),
            tab_id: Some(RuntimeString::from("t1")),
            browser_id: Some(RuntimeString::from("chrome")),
        };
        let encoded = runtime_codec::encode_value(&named).unwrap();
        assert_eq!(
            encoded.get("browserId"),
            Some(&RuntimeJson::from("chrome")),
            "the named browser rides the target"
        );
        assert_eq!(
            runtime_codec::decode_value::<BrowserTarget>(&encoded).unwrap(),
            named
        );

        let unset = BrowserTarget {
            session_id: Some(RuntimeString::from("s1")),
            tab_id: None,
            browser_id: None,
        };
        let encoded = runtime_codec::encode_value(&unset).unwrap();
        assert!(
            encoded.get("browserId").is_none(),
            "an unset browser is omitted, not null"
        );
        assert_eq!(
            runtime_codec::decode_value::<BrowserTarget>(&encoded).unwrap(),
            unset
        );
    }

    #[test]
    fn browser_runtime_codecs_round_trip_non_scalar_strings_without_serde() {
        let lone_surrogate = RuntimeString::from_utf16(vec![0xd800]);
        let mut args = RuntimeObject::new();
        args.insert(
            lone_surrogate.clone(),
            RuntimeJson::String(lone_surrogate.clone()),
        );
        let operation = BrowserOperation::Eval {
            script: lone_surrogate.clone(),
            args: Some(RuntimeJson::Object(args)),
            await_promise: true,
        };
        let mut request = BrowserRuntimeRequest::new(
            operation,
            BrowserTarget {
                session_id: Some(lone_surrogate.clone()),
                tab_id: None,
                browser_id: None,
            },
            Some(1234),
        );
        request.metadata.insert(
            lone_surrogate.clone(),
            RuntimeJson::String(lone_surrogate.clone()),
        );

        let encoded = runtime_codec::encode_value(&request).unwrap();
        let encoded_script = encoded
            .get("operation")
            .and_then(|operation| operation.get("script"))
            .and_then(RuntimeJson::as_string)
            .unwrap();
        assert_eq!(encoded_script.as_utf16(), [0xd800]);
        assert_eq!(
            runtime_codec::decode_value::<BrowserRuntimeRequest>(&encoded).unwrap(),
            request
        );

        let response = BrowserRuntimeResponse {
            value: Some(RuntimeJson::String(lone_surrogate.clone())),
            events: vec![BrowserRuntimeEvent::Status {
                message: lone_surrogate,
            }],
            ..BrowserRuntimeResponse::default()
        };
        let encoded = runtime_codec::encode_value(&response).unwrap();
        assert_eq!(
            runtime_codec::decode_value::<BrowserRuntimeResponse>(&encoded).unwrap(),
            response
        );
    }

    #[test]
    fn every_browser_operation_variant_uses_its_exact_internal_tag_and_round_trips() {
        let operations = vec![
            (
                "navigate",
                BrowserOperation::Navigate {
                    url: "https://example.com".into(),
                    wait_until: None,
                    viewport: None,
                },
            ),
            (
                "snapshot",
                BrowserOperation::Snapshot {
                    selector: None,
                    include_text: false,
                    include_links: false,
                },
            ),
            (
                "screenshot",
                BrowserOperation::Screenshot {
                    criteria: Default::default(),
                    ref_id: None,
                    selector: None,
                    full_page: false,
                    format: None,
                    quality: None,
                    save: true,
                },
            ),
            (
                "click",
                BrowserOperation::Click {
                    criteria: Default::default(),
                    ref_id: None,
                    selector: None,
                    x: None,
                    y: None,
                    button: None,
                    click_count: None,
                    modifiers: None,
                    force: false,
                },
            ),
            (
                "type",
                BrowserOperation::Type {
                    criteria: Default::default(),
                    ref_id: None,
                    selector: None,
                    text: "text".into(),
                    clear: false,
                    press_enter: false,
                    delay_ms: None,
                    key: None,
                },
            ),
            (
                "eval",
                BrowserOperation::Eval {
                    script: "1 + 1".into(),
                    args: None,
                    await_promise: false,
                },
            ),
            (
                "console",
                BrowserOperation::Console {
                    levels: None,
                    since_ms: None,
                    limit: None,
                    clear: false,
                },
            ),
            (
                "network",
                BrowserOperation::Network {
                    since_ms: None,
                    limit: None,
                    include_bodies: false,
                    url_filter: None,
                },
            ),
            (
                "gif_creator",
                BrowserOperation::GifCreator {
                    filename: None,
                    duration_seconds: None,
                    fps: None,
                    full_page: false,
                },
            ),
            (
                "upload_image",
                BrowserOperation::UploadImage {
                    criteria: Default::default(),
                    ref_id: None,
                    path: "image.png".into(),
                    selector: None,
                    prompt: None,
                },
            ),
            (
                "file_upload",
                BrowserOperation::FileUpload {
                    criteria: Default::default(),
                    paths: vec!["file.txt".into()],
                    selector: None,
                    ref_id: None,
                },
            ),
            (
                "shortcuts_list",
                BrowserOperation::ShortcutsList { query: None },
            ),
            (
                "shortcuts_execute",
                BrowserOperation::ShortcutsExecute {
                    name: "shortcut".into(),
                    wait_for_completion: false,
                },
            ),
            (
                "switch_browser",
                BrowserOperation::SwitchBrowser {
                    prompt: None,
                    wait_ms: None,
                },
            ),
            (
                "list_connected_browsers",
                BrowserOperation::ListConnectedBrowsers { local_only: false },
            ),
            (
                "select_browser",
                BrowserOperation::SelectBrowser {
                    device_id: "device".into(),
                },
            ),
            (
                "history",
                BrowserOperation::History {
                    queries: Vec::new(),
                    limit: None,
                    from: None,
                    to: None,
                },
            ),
            (
                "tabs_content",
                BrowserOperation::TabsContent {
                    urls: vec!["https://example.com".into()],
                    content_type: None,
                    max_chars: None,
                    wait_until: None,
                },
            ),
            (
                "content_export",
                BrowserOperation::ContentExport {
                    format: None,
                    selector: None,
                    max_chars: None,
                },
            ),
            (
                "download",
                BrowserOperation::Download {
                    selector: None,
                    url: None,
                    x: None,
                    y: None,
                    filename: None,
                    wait_only: false,
                },
            ),
            (
                "clipboard",
                BrowserOperation::Clipboard {
                    action: BrowserClipboardAction::Read,
                    text: None,
                    items: None,
                },
            ),
            (
                "dialog",
                BrowserOperation::Dialog {
                    action: BrowserDialogAction::Get,
                    prompt_text: None,
                },
            ),
            (
                "mark_tab",
                BrowserOperation::MarkTab {
                    status: BrowserTabMarkStatus::Deliverable,
                },
            ),
            (
                "read_page",
                BrowserOperation::ReadPage {
                    filter: None,
                    depth: None,
                    ref_id: None,
                    max_chars: None,
                },
            ),
            (
                "find",
                BrowserOperation::Find {
                    query: "query".into(),
                    limit: None,
                },
            ),
            (
                "form_input",
                BrowserOperation::FormInput {
                    reference: "ref".into(),
                    value: RuntimeJson::Null,
                },
            ),
            (
                "computer",
                BrowserOperation::Computer {
                    action: BrowserComputerAction::Screenshot,
                    coordinate: None,
                    start_coordinate: None,
                    text: None,
                    duration: None,
                    scroll_direction: None,
                    amount: None,
                    ref_id: None,
                    save: false,
                    keys: None,
                    path: None,
                    scroll_x: None,
                    scroll_y: None,
                },
            ),
            (
                "tabs",
                BrowserOperation::Tabs {
                    action: BrowserTabsAction::List,
                    url: None,
                    name: None,
                },
            ),
            (
                "locator",
                BrowserOperation::Locator {
                    selector: "body".into(),
                    action: "count".into(),
                    value: None,
                    options: RuntimeJson::Null,
                },
            ),
            ("page_text", BrowserOperation::PageText { max_chars: None }),
            (
                "batch",
                BrowserOperation::Batch {
                    actions: Vec::new(),
                },
            ),
            (
                "resize",
                BrowserOperation::Resize {
                    viewport: Some(BrowserViewport {
                        width: 800,
                        height: 600,
                        device_scale_factor: None,
                        is_mobile: None,
                    }),
                    reset: false,
                },
            ),
            ("close", BrowserOperation::Close { target: None }),
        ];

        for (tag, operation) in operations {
            let encoded = runtime_codec::encode_value(&operation).unwrap();
            assert_eq!(encoded.get("type"), Some(&RuntimeJson::from(tag)));
            assert_eq!(
                runtime_codec::decode_value::<BrowserOperation>(&encoded).unwrap(),
                operation,
                "round-trip drift for {tag}"
            );
        }
    }

    #[test]
    fn every_browser_event_variant_uses_its_exact_internal_tag_and_round_trips() {
        let artifact = || BrowserArtifact {
            id: "artifact".into(),
            kind: BrowserArtifactKind::Other,
            uri: None,
            path: None,
            mime_type: None,
            size_bytes: None,
            metadata: BTreeMap::new(),
        };
        let events = vec![
            (
                "status",
                BrowserRuntimeEvent::Status {
                    message: "status".into(),
                },
            ),
            (
                "session_started",
                BrowserRuntimeEvent::SessionStarted {
                    session_id: "session".into(),
                },
            ),
            (
                "tab_opened",
                BrowserRuntimeEvent::TabOpened {
                    session_id: "session".into(),
                    tab_id: "tab".into(),
                },
            ),
            (
                "navigation_started",
                BrowserRuntimeEvent::NavigationStarted {
                    url: "https://example.com".into(),
                    session_id: None,
                    tab_id: None,
                },
            ),
            (
                "navigation_completed",
                BrowserRuntimeEvent::NavigationCompleted {
                    url: "https://example.com".into(),
                    status: None,
                    title: None,
                },
            ),
            (
                "dom_snapshot",
                BrowserRuntimeEvent::DomSnapshot {
                    snapshot: BrowserSnapshot::default(),
                },
            ),
            (
                "screenshot_captured",
                BrowserRuntimeEvent::ScreenshotCaptured {
                    artifact: artifact(),
                },
            ),
            (
                "console",
                BrowserRuntimeEvent::Console {
                    entry: BrowserConsoleEntry {
                        level: BrowserConsoleLevel::Log,
                        text: "log".into(),
                        timestamp_ms: None,
                        url: None,
                        line: None,
                        column: None,
                        source: None,
                        args: None,
                    },
                },
            ),
            (
                "network",
                BrowserRuntimeEvent::Network {
                    entry: Box::new(BrowserNetworkEntry {
                        request_id: "request".into(),
                        url: "https://example.com".into(),
                        method: "GET".into(),
                        status: None,
                        resource_type: None,
                        request_headers: None,
                        response_headers: None,
                        error: None,
                        started_at_ms: None,
                        ended_at_ms: None,
                        mime_type: None,
                        failed: None,
                        body: None,
                        base64_encoded: None,
                        body_truncated: None,
                        body_error: None,
                    }),
                },
            ),
            (
                "artifact",
                BrowserRuntimeEvent::Artifact {
                    artifact: artifact(),
                },
            ),
            (
                "download",
                BrowserRuntimeEvent::Download {
                    artifact: artifact(),
                },
            ),
            (
                "error",
                BrowserRuntimeEvent::Error {
                    code: BrowserRuntimeErrorCode::HostError,
                    message: "error".into(),
                },
            ),
        ];

        for (tag, event) in events {
            let encoded = runtime_codec::encode_value(&event).unwrap();
            assert_eq!(encoded.get("type"), Some(&RuntimeJson::from(tag)));
            assert_eq!(
                runtime_codec::decode_value::<BrowserRuntimeEvent>(&encoded).unwrap(),
                event,
                "round-trip drift for {tag}"
            );
        }
    }

    #[tokio::test]
    async fn host_tool_runtime_maps_operation_to_configured_tool_and_typed_response() {
        let calls: Arc<Mutex<Vec<BrowserHostToolRequest>>> = Arc::new(Mutex::new(Vec::new()));
        let seen = calls.clone();
        let runtime = BrowserHostToolRuntime::new(
            BrowserHostToolMapping {
                navigate: "mcp__browser__go".to_string(),
                ..BrowserHostToolMapping::default()
            },
            Arc::new(move |request, _signal, emit| {
                let seen = seen.clone();
                Box::pin(async move {
                    emit(BrowserRuntimeEvent::Status {
                        message: "host bridge called".into(),
                    });
                    seen.lock().unwrap().push(request);
                    Ok(BrowserRuntimeResponse {
                        url: Some("https://example.com/".into()),
                        title: Some("Example".into()),
                        ..BrowserRuntimeResponse::default()
                    })
                })
            }),
        );

        let response = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Navigate {
                        url: "https://example.com/".into(),
                        wait_until: Some(BrowserWaitUntil::DomContentLoaded),
                        viewport: None,
                    },
                    BrowserTarget::default(),
                    Some(5000),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();

        assert!(response
            .title
            .as_ref()
            .is_some_and(|title| title == "Example"));
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].tool_name, "mcp__browser__go");
        assert_eq!(
            calls[0]
                .arguments
                .get("operation")
                .and_then(|operation| operation.get("type")),
            Some(&RuntimeJson::from("navigate"))
        );
        assert_eq!(
            calls[0]
                .arguments
                .get("operation")
                .and_then(|operation| operation.get("url")),
            Some(&RuntimeJson::from("https://example.com/"))
        );
        assert_eq!(calls[0].browser_request.timeout_ms, Some(5000));
    }

    #[test]
    fn registered_browser_runtime_exposes_only_its_closed_process_binding() {
        let spawn_free_executor = Arc::new(BrowserHostToolRuntime::new(
            BrowserHostToolMapping::default(),
            Arc::new(|_request, _signal, _emit| {
                Box::pin(async { Ok(BrowserRuntimeResponse::default()) })
            }),
        ));
        // SAFETY: this deterministic executor returns without any process or
        // delegated effect.
        let spawn_free = unsafe { attest_spawn_free(spawn_free_executor) };
        assert_eq!(
            spawn_free.process_binding(),
            BrowserProcessBinding::SpawnFree
        );

        let namespace = crate::runtime_process::test_runtime_process_context().namespace_id();
        let context_bound_executor = Arc::new(BrowserHostToolRuntime::new(
            BrowserHostToolMapping::default(),
            Arc::new(|_request, _signal, _emit| {
                Box::pin(async { Ok(BrowserRuntimeResponse::default()) })
            }),
        ));
        // SAFETY: this deterministic executor performs no process operation, so
        // binding it to the test namespace is a conservative valid claim.
        let bound = unsafe { context_bound(namespace, context_bound_executor) };
        assert_eq!(
            bound.process_binding(),
            BrowserProcessBinding::ContextBound(namespace)
        );
    }

    #[test]
    fn permission_flags_cover_rich_browser_operations() {
        let read_page = BrowserPermissionSet::for_operation(&BrowserOperation::ReadPage {
            filter: Some(BrowserReadPageFilter::Interactive),
            depth: Some(4),
            ref_id: None,
            max_chars: Some(10_000),
        });
        assert!(read_page.read_page);
        assert!(!read_page.interact);

        let form = BrowserPermissionSet::for_operation(&BrowserOperation::FormInput {
            reference: "ref_1".into(),
            value: RuntimeJson::from("hello"),
        });
        assert!(form.interact);
        assert!(form.read_page);
        assert!(form.session_state);

        let computer = BrowserPermissionSet::for_operation(&BrowserOperation::Computer {
            action: BrowserComputerAction::LeftClick,
            coordinate: Some(vec![10.0, 20.0]),
            start_coordinate: None,
            text: None,
            duration: None,
            scroll_direction: None,
            amount: None,
            ref_id: None,
            save: false,
            keys: None,
            path: None,
            scroll_x: None,
            scroll_y: None,
        });
        assert!(computer.interact);
        assert!(computer.capture_screenshot);

        let locator = BrowserPermissionSet::for_operation(&BrowserOperation::Locator {
            selector: "#submit".into(),
            action: "click".into(),
            value: None,
            options: RuntimeJson::Object(RuntimeObject::new()),
        });
        assert!(locator.interact);
        assert!(locator.read_page);
    }

    /// What an operation DOES has to be what it declares, or the user approved
    /// something else.
    ///
    /// The permission set is compared against the policy before the operation runs, so
    /// an under-declared capability is one the policy was never asked about. Three were
    /// under-declared, and each of them is a capability a policy can withhold on its
    /// own.
    #[test]
    fn a_permission_set_declares_what_the_operation_actually_does() {
        // Loading urls in TEMPORARY tabs opens tabs and closes them again — the same
        // capability `Tabs::New` declares — and it declared navigation and page reads
        // only.
        let read_urls = BrowserPermissionSet::for_operation(&BrowserOperation::TabsContent {
            urls: vec!["https://example.com/".into()],
            content_type: None,
            max_chars: None,
            wait_until: None,
        });
        assert!(
            read_urls.manage_tabs,
            "reading urls in temporary tabs manages tabs"
        );
        assert!(read_urls.navigate && read_urls.read_page);

        // `save` writes the capture to a FILE, and declared a screenshot.
        let coordinate_action = |save: bool| BrowserOperation::Computer {
            action: BrowserComputerAction::Screenshot,
            coordinate: None,
            start_coordinate: None,
            text: None,
            duration: None,
            scroll_direction: None,
            amount: None,
            ref_id: None,
            save,
            keys: None,
            path: None,
            scroll_x: None,
            scroll_y: None,
        };
        let saved = BrowserPermissionSet::for_operation(&coordinate_action(true));
        assert!(saved.file_transfer, "saving a capture writes a file");
        assert!(saved.capture_screenshot);
        // And without it there is no file, so nothing extra is asked for.
        assert!(!BrowserPermissionSet::for_operation(&coordinate_action(false)).file_transfer);

        // A clipboard READ drives the browser's own paste command through the page, and
        // the host transport's classifier has always required `interact` for it. Two
        // classifiers disagreeing about one operation is its own defect.
        for action in [
            BrowserClipboardAction::Read,
            BrowserClipboardAction::ReadText,
            BrowserClipboardAction::Write,
            BrowserClipboardAction::WriteText,
        ] {
            let clipboard = BrowserPermissionSet::for_operation(&BrowserOperation::Clipboard {
                action,
                text: None,
                items: None,
            });
            assert!(clipboard.interact, "every clipboard action drives the page");
            assert!(clipboard.read_page && clipboard.session_state);
        }
    }

    #[tokio::test]
    async fn host_tool_runtime_maps_host_failures_to_browser_errors() {
        let runtime = BrowserHostToolRuntime::new(
            BrowserHostToolMapping::default(),
            Arc::new(|_request, _signal, _emit| {
                Box::pin(async move {
                    Err(BrowserRuntimeError::new(
                        BrowserRuntimeErrorCode::HostError,
                        "chrome bridge disconnected",
                    ))
                })
            }),
        );

        let err = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Snapshot {
                        selector: None,
                        include_text: true,
                        include_links: false,
                    },
                    BrowserTarget::default(),
                    None,
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap_err();

        assert_eq!(err.code, BrowserRuntimeErrorCode::HostError);
        assert_eq!(err.message, "chrome bridge disconnected");
    }

    #[tokio::test]
    async fn host_tool_runtime_can_route_through_engine_invoke_tool() {
        let runtime = BrowserHostToolRuntime::from_invoke_tool(
            BrowserHostToolMapping::for_mcp_server("browser"),
            Arc::new(|name: RuntimeString, input: RuntimeJson| {
                Box::pin(async move {
                    assert_eq!(name, "mcp__browser__navigate");
                    assert_eq!(
                        input
                            .get("operation")
                            .and_then(|operation| operation.get("type")),
                        Some(&RuntimeJson::from("navigate"))
                    );
                    Ok(crate::types::tool::InvokeToolResult {
                        is_error: false,
                        output: Some(parse_runtime_json(
                            r#"{"ok":true,"response":{"url":"https://example.com/","title":"Example"}}"#,
                        )),
                        content: Vec::new(),
                    })
                })
            }),
        );

        let response = runtime
            .execute(
                BrowserRuntimeRequest::new(
                    BrowserOperation::Navigate {
                        url: "https://example.com/".into(),
                        wait_until: None,
                        viewport: None,
                    },
                    BrowserTarget::default(),
                    Some(5000),
                ),
                AbortSignal::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();

        assert!(response
            .url
            .as_ref()
            .is_some_and(|url| url == "https://example.com/"));
        assert!(response
            .title
            .as_ref()
            .is_some_and(|title| title == "Example"));
    }
}
