//! `browser_*` tools: model-facing wrappers around a host-injected
//! [`BrowserRuntime`](crate::browser::BrowserRuntime).
//!
//! The engine owns schemas, permission classification, network approval,
//! timeout/cancellation, event forwarding, and structured results. The host or a
//! plugin owns the actual browser implementation.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::Url;
use serde_json::{json, Map, Value};

use crate::browser::{
    attest_spawn_free, BrowserClipboardAction, BrowserCloseTarget, BrowserComputerAction,
    BrowserConsoleLevel, BrowserDialogAction, BrowserElementCriteria, BrowserEventSink,
    BrowserHostToolRuntime, BrowserMouseButton, BrowserOperation, BrowserPermissionAction,
    BrowserPermissionSetting, BrowserReadPageFilter, BrowserRuntime, BrowserRuntimeError,
    BrowserRuntimeErrorCode, BrowserRuntimeRequest, BrowserRuntimeResponse,
    BrowserScreenshotFormat, BrowserScrollDirection, BrowserTabMarkStatus, BrowserTabsAction,
    BrowserTarget, BrowserViewport, BrowserWaitUntil,
};
use crate::runtime_values::{
    runtime_codec, runtime_json, RuntimeJson, RuntimeObject, RuntimeString,
};
use crate::schemas::content::{ImageBlock, OutputBlock, TextBlock};
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::events::ToolDomainEvent;
use crate::types::json::{Json, JsonSchema};
use crate::types::permission::{PermissionReason, PermissionResult};
use crate::types::tool::{Tool, ToolContext, ToolProgress, ToolResult, ToolResultSerializable};

const DEFAULT_BROWSER_TIMEOUT_MS: u64 = 60_000;

/// Resolve the current session's browser at call time, including MCP-backed
/// capabilities that are unavailable when the built-in tools are constructed.
pub(super) fn resolve_runtime(
    ctx: &ToolContext,
) -> Result<Arc<dyn BrowserRuntime>, BrowserRuntimeError> {
    match (&ctx.browser, &ctx.browser_tool_mapping, &ctx.invoke_tool) {
        (Some(runtime), _, _) => Ok(runtime.clone()),
        (None, Some(mapping), Some(invoke_tool)) => {
            let executor = Arc::new(BrowserHostToolRuntime::from_invoke_tool(
                mapping.clone(),
                invoke_tool.clone(),
            ));
            // SAFETY: this adapter delegates fixed browser operations to the
            // selected host mapping and has no local process authority.
            Ok(unsafe { attest_spawn_free(executor) })
        }
        (None, Some(_), None) => Err(BrowserRuntimeError::new(
            BrowserRuntimeErrorCode::Unavailable,
            "Browser runtime requires engine tool invocation, but invoke_tool is unavailable.",
        )),
        (None, None, _) => Err(BrowserRuntimeError::new(
            BrowserRuntimeErrorCode::Unavailable,
            "Browser runtime is not enabled for this session.",
        )),
    }
}

/// Additional element names accepted by the locator tool and forwarded in options.
const LOCATOR_NAME_KEYS: &[&str] = &[
    "text",
    "role",
    "name",
    "label",
    "placeholder",
    "testId",
    "query",
    "ref",
    "refId",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BrowserToolKind {
    Navigate,
    Snapshot,
    Screenshot,
    Click,
    Type,
    Eval,
    Console,
    Network,
    ReadPage,
    Find,
    FormInput,
    Computer,
    Tabs,
    Locator,
    PageText,
    Batch,
    Resize,
    Close,
    GifCreator,
    UploadImage,
    FileUpload,
    ShortcutsList,
    ShortcutsExecute,
    SwitchBrowser,
    ListConnectedBrowsers,
    SelectBrowser,
    ResolveTab,
    ClaimTab,
    History,
    TabsContent,
    ContentExport,
    Download,
    Clipboard,
    Dialog,
    MarkTab,
    Permission,
}

impl BrowserToolKind {
    fn name(self) -> &'static str {
        match self {
            BrowserToolKind::Navigate => "browser_navigate",
            BrowserToolKind::Snapshot => "browser_snapshot",
            BrowserToolKind::Screenshot => "browser_screenshot",
            BrowserToolKind::Click => "browser_click",
            BrowserToolKind::Type => "browser_type",
            BrowserToolKind::Eval => "browser_eval",
            BrowserToolKind::Console => "browser_console",
            BrowserToolKind::Network => "browser_network",
            BrowserToolKind::ReadPage => "browser_read_page",
            BrowserToolKind::Find => "browser_find",
            BrowserToolKind::FormInput => "browser_form_input",
            BrowserToolKind::Computer => "browser_computer",
            BrowserToolKind::Tabs => "browser_tabs",
            BrowserToolKind::Locator => "browser_locator",
            BrowserToolKind::PageText => "browser_page_text",
            BrowserToolKind::Batch => "browser_batch",
            BrowserToolKind::Resize => "browser_resize",
            BrowserToolKind::Close => "browser_close",
            BrowserToolKind::GifCreator => "browser_gif_creator",
            BrowserToolKind::UploadImage => "browser_upload_image",
            BrowserToolKind::FileUpload => "browser_file_upload",
            BrowserToolKind::ShortcutsList => "browser_shortcuts_list",
            BrowserToolKind::ShortcutsExecute => "browser_shortcuts_execute",
            BrowserToolKind::SwitchBrowser => "browser_switch_browser",
            BrowserToolKind::ListConnectedBrowsers => "browser_list_connected_browsers",
            BrowserToolKind::SelectBrowser => "browser_select_browser",
            BrowserToolKind::ResolveTab => "browser_resolve_tab",
            BrowserToolKind::ClaimTab => "browser_claim_tab",
            BrowserToolKind::History => "browser_history",
            BrowserToolKind::TabsContent => "browser_tabs_content",
            BrowserToolKind::ContentExport => "browser_content_export",
            BrowserToolKind::Download => "browser_download",
            BrowserToolKind::Clipboard => "browser_clipboard",
            BrowserToolKind::Dialog => "browser_dialog",
            BrowserToolKind::MarkTab => "browser_mark_tab",
            BrowserToolKind::Permission => "browser_permission",
        }
    }

    fn description(self) -> &'static str {
        match self {
            BrowserToolKind::Navigate => {
                "Navigate a browser tab to a URL. Uses the built-in browser by default \
                 (clean, no sign-in state). If the task needs the user's logged-in \
                 session — their own accounts, orders, posts, or pages behind a login \
                 — call browser_list_connected_browsers then browser_select_browser to \
                 switch to the user's connected Chrome first. If the user names a \
                 browser, honor that."
            }
            BrowserToolKind::Snapshot => {
                "Structured DOM/accessibility snapshot of the page: the element list to \
                 pick selectors from before clicking or typing. Descends into \
                 same-origin iframes and open shadow roots, so the insides of a web \
                 component are reachable by name. `selector` scopes it to one subtree, \
                 which is how a long page is read a section at a time."
            }
            BrowserToolKind::Screenshot => {
                "Capture the page as an image artifact. `selector` captures just that \
                 ELEMENT, cropped to its own box; `fullPage` captures beyond the \
                 viewport. Saves by default and returns the absolute path with the image; \
                 set `save` to false for a transient observation. When the user requests a \
                 capture or it helps explain the result, embed the verified image using \
                 its actual saved path. Use it for visual verification, not for finding elements \
                 — read_page or a locator gives names that can be acted on."
            }
            BrowserToolKind::Click => {
                "Click an element. Name it with a `selector` — durable, waits for the \
                 element, survives a re-render — or at `x`/`y` as a last resort. Real \
                 input events, not script. `button` and `clickCount` make this the \
                 right-click and double-click of that element; `modifiers` holds keys \
                 during the click, which makes it a DIFFERENT action (the platform \
                 accelerator opens a link in a background tab, shift extends a \
                 selection)."
            }
            BrowserToolKind::Type => {
                "Type into a page field named by `selector`, which is focused first. \
                 `clear` REPLACES the existing value instead of appending, \
                 `pressEnter` submits, and `delayMs` sends the text one character at \
                 a time with real key events — which is what a field that filters as \
                 you type needs. This types into the WEB PAGE, never into browser \
                 chrome such as the address bar."
            }
            BrowserToolKind::Eval => {
                "Evaluate JavaScript in the page and return its value. For reading \
                 what no other operation reports, and for driving a widget through \
                 its own API. Prefer a locator or a click for anything a person \
                 could do: script bypasses the events a page listens for."
            }
            BrowserToolKind::Console => {
                "Read the page's console messages, newest last. `sinceMs` bounds how \
                 far back and `levels` narrows to errors. Use it to find out WHY a \
                 page did not do what a click asked, rather than guessing from the \
                 rendered result."
            }
            BrowserToolKind::Network => {
                "Read the requests the page made, with status and timing. Use it to \
                 tell \"the request failed\" apart from \"the page is still loading\", \
                 which look identical on screen."
            }
            BrowserToolKind::ReadPage => {
                "Read the page as text plus its identity, with stable element \
                 references. `maxChars` bounds how much text comes back; the answer \
                 says whether anything was left out, so a bounded read is never \
                 mistaken for the whole page."
            }
            BrowserToolKind::Find => {
                "Find rendered text on the page and return each hit with a reusable \
                 reference. Matches the text a person SEES, so a phrase split across \
                 inline markup is still found, and open shadow roots are searched \
                 too. Use it to locate content; use a locator for a control by role \
                 and name."
            }
            BrowserToolKind::FormInput => {
                "Set one form field's value by its element reference. For a whole \
                 form, call it once per field — the first failure is a failure, not a \
                 half-filled form reported as success."
            }
            BrowserToolKind::Computer => {
                "Mouse, keyboard, scrolling and waiting by COORDINATE. Prefer a \
                 locator-named click or type when the element can be named: a \
                 coordinate goes stale the moment the page moves. Use this for \
                 scrolling, dragging, and keys that belong to no element."
            }
            BrowserToolKind::Tabs => {
                "For `action:user`, select the connected external browser first; the built-in browser has no user tabs. \
                 One operation for the whole tab surface, chosen by `action`: `list` \
                 (with the handles later calls pin), `get`, `selected`, `new` (opens \
                 `url` as a SECOND page, which is how two pages are held at once), \
                 `close`, `finalize`, and `name_session` (labels this session's tabs \
                 with `name` so the user can tell Klaus's work from their own). \
                 Authoritative for \"is this page actually open\" — use it instead of \
                 a screenshot."
            }
            BrowserToolKind::Locator => {
                "A Playwright-style locator plus an `action`. Name the element by \
                 `selector`, or — more durably — by `role`, `text`, `label`, \
                 `placeholder` or `testId`, or by a `ref` an earlier call returned; a \
                 name survives the re-render that changes a class name. Reading \
                 actions are `count`, `text`, `text_content`, `all_text_contents`, \
                 `is_visible`, `screenshot`. Acting ones are `click`, \
                 `double_click`, `type`, `fill`, `press`, `check`, `uncheck`, \
                 `select_option` — `select_option` is the ONLY way to choose in a \
                 native <select>, whose dropdown is drawn by the operating system \
                 and cannot be clicked. `wait_for` waits for the element to reach a \
                 state, which is how you wait for content instead of polling."
            }
            BrowserToolKind::PageText => {
                "The page's visible text. Cheaper than a snapshot when only the \
                 content is needed. `maxChars` bounds it and the answer says whether \
                 anything was left out."
            }
            BrowserToolKind::Batch => {
                "Run several browser operations in order in one call, stopping at the \
                 first failure and returning each step's result. Use it to cut round \
                 trips on a known sequence — and to catch a navigation your own click \
                 causes, by putting the click and the wait in one batch."
            }
            BrowserToolKind::Resize => {
                "Set the page's viewport size. Viewport emulation rather than a \
                 window resize, so the layout changes without moving the window the \
                 user arranged."
            }
            BrowserToolKind::Close => {
                "Retire the tabs this session opened, leaving the user's own alone. \
                 The end-of-work move, not a way to dismiss one page."
            }
            BrowserToolKind::GifCreator => {
                "Capture a short GIF artifact from the current browser page."
            }
            BrowserToolKind::UploadImage => {
                "Upload or attach an image to the current browser page through the runtime."
            }
            BrowserToolKind::FileUpload => {
                "Attach local files through a file input or upload control. Acknowledgement \
                 means the browser accepted the selection. Read the returned observation \
                 and the visible page to confirm the attachment and site upload; the \
                 page may immediately clear or replace its file input."
            }
            BrowserToolKind::ShortcutsList => {
                "List browser extension shortcuts or workflows available for the current page."
            }
            BrowserToolKind::ShortcutsExecute => {
                "Execute a browser extension shortcut or workflow for the current page."
            }
            BrowserToolKind::SwitchBrowser => {
                "Pair with and switch this session to the user's connected Chrome \
                 (their real browser, carrying their cookies and sign-in state). Use \
                 when the task needs the user's logged-in session."
            }
            BrowserToolKind::ListConnectedBrowsers => {
                "List the browsers this session can drive so you can choose one. There \
                 are two kinds: the built-in browser (default, clean, no sign-in \
                 state) and the user's connected Chrome (their real browser with \
                 their cookies and logins), which appears only once they connect it. \
                 Choosing rule: if the user named a browser, use it; otherwise judge \
                 by the task — a page behind the user's login (their accounts, \
                 orders, posts, intranet) needs their Chrome, anything else uses the \
                 built-in browser. Call browser_select_browser to switch."
            }
            BrowserToolKind::SelectBrowser => {
                "Switch this session to a browser from browser_list_connected_browsers \
                 by its deviceId. Selection sticks for later browser operations until \
                 changed; with no selection the built-in browser is used."
            }
            BrowserToolKind::History => {
                "Search the USER's own browsing history. Their real browser only — the \
                 built-in one is a clean profile with nothing in it. Use it to find a \
                 page the user has seen rather than asking them for the url."
            }
            BrowserToolKind::TabsContent => {
                "Load urls in temporary background tabs and return their content, \
                 without disturbing the page this session is on. For reading several \
                 pages at once; the tabs are cleaned up afterwards."
            }
            BrowserToolKind::ContentExport => {
                "Export the current page's content as an artifact the user can keep. \
                 For handing over what was read, not for reading it — page_text is \
                 cheaper for that."
            }
            BrowserToolKind::Download => {
                "Download a file, an image, or the media behind an element. Name the \
                 element or pass the `url`; the file lands where the runtime puts \
                 downloads and the answer names it. Clicking a download link instead \
                 leaves nothing this session can find."
            }
            BrowserToolKind::Clipboard => {
                "Read or write the browser session's clipboard. For carrying a value \
                 out of a page that offers only a copy button, and into a field that \
                 refuses typed input."
            }
            BrowserToolKind::Dialog => {
                "Answer the page's open alert / confirm / prompt: `action` accepts or \
                 dismisses it, `promptText` fills a prompt. A page-modal dialog \
                 BLOCKS every other operation until it is answered, so this is the \
                 way out of a page that has stopped responding."
            }
            BrowserToolKind::Permission => {
                "Grant, set or reset a site's browser permissions: geolocation, \
                 notifications, camera, microphone. `action` is `grant` (with \
                 `permissions`), `set` (one `permission` plus a `setting`) or `reset` \
                 (clears them all for the origin). Use it when a page's own permission \
                 prompt is in the way: browsing never raises an approval you can answer, \
                 and `dialog` answers JavaScript dialogs rather than a permission chip, \
                 so without this the flow behind the prompt cannot be finished at all."
            }
            BrowserToolKind::ResolveTab => {
                "Resolve the full plugin:// tab mention selected by the user. The owning host verifies the exact browser, tab, title and URL and claims an external user tab before returning its controllable handle. Stale references fail explicitly; never open a replacement."
            }
            BrowserToolKind::ClaimTab => {
                "Claim an external user tab listed by browser_tabs action user, using its exact browserId and tabId. Tabs belonging to another task cannot be claimed."
            }
            BrowserToolKind::MarkTab => {
                "Mark this session's tab as a deliverable or a handoff, so cleanup \
                 leaves it open for the user instead of retiring it with the rest."
            }
        }
    }

    fn search_hint(self) -> &'static str {
        match self {
            BrowserToolKind::Navigate => "open or navigate a web page in browser runtime",
            BrowserToolKind::Snapshot => "inspect DOM text and accessible page state",
            BrowserToolKind::Screenshot => "take a browser screenshot artifact",
            BrowserToolKind::Click => "click a selector or coordinates in browser",
            BrowserToolKind::Type => "type into a browser input",
            BrowserToolKind::Eval => "evaluate JavaScript in browser page",
            BrowserToolKind::Console => "read browser console logs",
            BrowserToolKind::Network => "read browser network requests",
            BrowserToolKind::ReadPage => "read browser accessibility tree with ref ids",
            BrowserToolKind::Find => "find browser elements by natural language or text",
            BrowserToolKind::FormInput => "set browser form input by ref id",
            BrowserToolKind::Computer => "use mouse keyboard scrolling screenshot in browser",
            BrowserToolKind::Tabs => "manage browser tabs and session",
            BrowserToolKind::Locator => "use Playwright locator action in browser",
            BrowserToolKind::PageText => "extract browser page text",
            BrowserToolKind::Batch => "batch browser tool calls",
            BrowserToolKind::Resize => "resize browser viewport",
            BrowserToolKind::Close => "close browser tab or session",
            BrowserToolKind::GifCreator => "capture browser gif animation artifact",
            BrowserToolKind::UploadImage => "upload image through browser page",
            BrowserToolKind::FileUpload => "set file input browser upload files",
            BrowserToolKind::ShortcutsList => "list browser shortcuts workflows",
            BrowserToolKind::ShortcutsExecute => "execute browser shortcut workflow",
            BrowserToolKind::SwitchBrowser => "switch or pair connected browser extension",
            BrowserToolKind::ListConnectedBrowsers => "list connected browser extensions",
            BrowserToolKind::SelectBrowser => "select connected browser extension by device id",
            BrowserToolKind::ResolveTab => "resolve user browser tab mention reference",
            BrowserToolKind::ClaimTab => "claim existing external user browser tab",
            BrowserToolKind::History => "read browser history entries",
            BrowserToolKind::TabsContent => "load temporary tabs and extract content",
            BrowserToolKind::ContentExport => "export current browser tab content artifact",
            BrowserToolKind::Download => "download browser media link artifact",
            BrowserToolKind::Clipboard => "read write browser clipboard",
            BrowserToolKind::Dialog => "handle JavaScript alert confirm prompt dialog",
            BrowserToolKind::Permission => "grant or reset a site's browser permissions",
            BrowserToolKind::MarkTab => "mark browser tab deliverable handoff",
        }
    }

    fn is_read_only(self, input: &Json) -> bool {
        match self {
            BrowserToolKind::Permission
            | BrowserToolKind::Click
            | BrowserToolKind::Type
            | BrowserToolKind::Eval
            | BrowserToolKind::FormInput
            | BrowserToolKind::Batch
            | BrowserToolKind::Resize
            | BrowserToolKind::Close
            | BrowserToolKind::UploadImage
            | BrowserToolKind::FileUpload
            | BrowserToolKind::ShortcutsExecute
            | BrowserToolKind::SwitchBrowser
            | BrowserToolKind::SelectBrowser
            | BrowserToolKind::ResolveTab
            | BrowserToolKind::ClaimTab
            | BrowserToolKind::MarkTab
            | BrowserToolKind::Download => false,
            BrowserToolKind::GifCreator
            | BrowserToolKind::ShortcutsList
            | BrowserToolKind::ListConnectedBrowsers
            | BrowserToolKind::History
            | BrowserToolKind::TabsContent
            | BrowserToolKind::ContentExport => true,
            BrowserToolKind::Clipboard => read_string(input, "action")
                .as_ref()
                .is_none_or(|action| runtime_string_is_one_of(action, &["read", "read_text"])),
            BrowserToolKind::Dialog => read_string(input, "action")
                .as_ref()
                .is_none_or(|action| action == "get"),
            BrowserToolKind::Computer => {
                read_string(input, "action").as_ref().is_some_and(|action| {
                    runtime_string_is_one_of(action, &["screenshot", "wait", "zoom"])
                })
            }
            BrowserToolKind::Tabs => read_string(input, "action").as_ref().is_none_or(|action| {
                runtime_string_is_one_of(action, &["list", "user", "get", "selected"])
            }),
            BrowserToolKind::Locator => {
                read_string(input, "action").as_ref().is_some_and(|action| {
                    runtime_string_is_one_of(
                        action,
                        &[
                            "count",
                            "text",
                            "text_content",
                            "all_text_contents",
                            "is_visible",
                            "screenshot",
                            "wait_for",
                        ],
                    )
                })
            }
            BrowserToolKind::Navigate
            | BrowserToolKind::Snapshot
            | BrowserToolKind::Screenshot
            | BrowserToolKind::Console
            | BrowserToolKind::Network
            | BrowserToolKind::ReadPage
            | BrowserToolKind::Find
            | BrowserToolKind::PageText => true,
        }
    }
}

/// Construct all browser runtime tools.
pub fn create_browser_tools() -> Vec<Arc<dyn Tool>> {
    vec![
        Arc::new(BrowserTool::new(BrowserToolKind::Navigate)),
        Arc::new(BrowserTool::new(BrowserToolKind::Snapshot)),
        Arc::new(BrowserTool::new(BrowserToolKind::Screenshot)),
        Arc::new(BrowserTool::new(BrowserToolKind::Click)),
        Arc::new(BrowserTool::new(BrowserToolKind::Type)),
        Arc::new(BrowserTool::new(BrowserToolKind::Eval)),
        Arc::new(BrowserTool::new(BrowserToolKind::Console)),
        Arc::new(BrowserTool::new(BrowserToolKind::Network)),
        Arc::new(BrowserTool::new(BrowserToolKind::ReadPage)),
        Arc::new(BrowserTool::new(BrowserToolKind::Find)),
        Arc::new(BrowserTool::new(BrowserToolKind::FormInput)),
        Arc::new(BrowserTool::new(BrowserToolKind::Computer)),
        Arc::new(BrowserTool::new(BrowserToolKind::Tabs)),
        Arc::new(BrowserTool::new(BrowserToolKind::Locator)),
        Arc::new(BrowserTool::new(BrowserToolKind::PageText)),
        Arc::new(BrowserTool::new(BrowserToolKind::Batch)),
        Arc::new(BrowserTool::new(BrowserToolKind::Resize)),
        Arc::new(BrowserTool::new(BrowserToolKind::Close)),
        Arc::new(BrowserTool::new(BrowserToolKind::GifCreator)),
        Arc::new(BrowserTool::new(BrowserToolKind::UploadImage)),
        Arc::new(BrowserTool::new(BrowserToolKind::FileUpload)),
        Arc::new(BrowserTool::new(BrowserToolKind::ShortcutsList)),
        Arc::new(BrowserTool::new(BrowserToolKind::ShortcutsExecute)),
        Arc::new(BrowserTool::new(BrowserToolKind::SwitchBrowser)),
        Arc::new(BrowserTool::new(BrowserToolKind::ListConnectedBrowsers)),
        Arc::new(BrowserTool::new(BrowserToolKind::SelectBrowser)),
        Arc::new(BrowserTool::new(BrowserToolKind::ResolveTab)),
        Arc::new(BrowserTool::new(BrowserToolKind::ClaimTab)),
        Arc::new(BrowserTool::new(BrowserToolKind::History)),
        Arc::new(BrowserTool::new(BrowserToolKind::TabsContent)),
        Arc::new(BrowserTool::new(BrowserToolKind::ContentExport)),
        Arc::new(BrowserTool::new(BrowserToolKind::Download)),
        Arc::new(BrowserTool::new(BrowserToolKind::Clipboard)),
        Arc::new(BrowserTool::new(BrowserToolKind::Dialog)),
        Arc::new(BrowserTool::new(BrowserToolKind::MarkTab)),
        Arc::new(BrowserTool::new(BrowserToolKind::Permission)),
    ]
}

struct BrowserTool {
    kind: BrowserToolKind,
    input_schema: JsonSchema,
}

impl BrowserTool {
    fn new(kind: BrowserToolKind) -> Self {
        BrowserTool {
            kind,
            input_schema: input_schema(kind),
        }
    }

    fn build_request(
        &self,
        input: &RuntimeJson,
    ) -> Result<(BrowserRuntimeRequest, u64), RuntimeString> {
        let timeout_ms = read_u64(input, "timeoutMs").unwrap_or(DEFAULT_BROWSER_TIMEOUT_MS);
        let target = BrowserTarget {
            session_id: read_string(input, "sessionId"),
            tab_id: read_browser_tab_id(input)?,
            // Which browser runs this call. Set it to override the session's
            // selection for one operation; leave it out to follow the selection.
            browser_id: read_string(input, "browserId"),
        };
        let operation = match self.kind {
            BrowserToolKind::Navigate => BrowserOperation::Navigate {
                url: required_string(input, "url")?,
                wait_until: read_wait_until(input, "waitUntil")?,
                viewport: read_viewport(input, "viewport")?,
            },
            BrowserToolKind::Snapshot => BrowserOperation::Snapshot {
                selector: read_string(input, "selector"),
                include_text: read_bool(input, "includeText", true),
                include_links: read_bool(input, "includeLinks", true),
            },
            BrowserToolKind::Screenshot => BrowserOperation::Screenshot {
                criteria: read_element_criteria(input),
                ref_id: read_string(input, "refId").or_else(|| read_string(input, "ref")),
                selector: read_string(input, "selector"),
                full_page: read_bool(input, "fullPage", false),
                format: read_screenshot_format(input, "format")?,
                quality: read_u64(input, "quality").map(|v| v.min(100) as u8),
                save: read_bool(input, "save", true),
            },
            BrowserToolKind::Click => {
                let selector = read_string(input, "selector");
                let x = read_f64(input, "x");
                let y = read_f64(input, "y");
                if selector.is_none()
                    && !has_element_criteria(input)
                    && (x.is_none() || y.is_none())
                {
                    return Err("Name an element with `selector`, `ref`, or semantic criteria, or provide both `x` and `y`.".into());
                }
                BrowserOperation::Click {
                    criteria: read_element_criteria(input),
                    ref_id: read_string(input, "refId").or_else(|| read_string(input, "ref")),
                    force: read_bool(input, "force", false),
                    selector,
                    x,
                    y,
                    button: read_mouse_button(input, "button")?,
                    click_count: read_u64(input, "clickCount")
                        .map(|v| v.min(u32::MAX as u64) as u32),
                    modifiers: read_string_array(input, "modifiers"),
                }
            }
            BrowserToolKind::Type => BrowserOperation::Type {
                criteria: read_element_criteria(input),
                ref_id: read_string(input, "refId").or_else(|| read_string(input, "ref")),
                selector: read_string(input, "selector"),
                text: required_string(input, "text")?,
                clear: read_bool(input, "clear", false),
                press_enter: read_bool(input, "pressEnter", false),
                key: read_string(input, "key"),
                delay_ms: read_u64(input, "delayMs"),
            },
            BrowserToolKind::Eval => BrowserOperation::Eval {
                script: required_string(input, "script")?,
                args: input.get("args").cloned(),
                await_promise: read_bool(input, "awaitPromise", true),
            },
            BrowserToolKind::Console => BrowserOperation::Console {
                levels: read_console_levels(input, "levels")?,
                since_ms: read_u64(input, "sinceMs"),
                limit: read_u64(input, "limit").map(|v| v.min(u32::MAX as u64) as u32),
                clear: read_bool(input, "clear", false),
            },
            BrowserToolKind::Network => BrowserOperation::Network {
                since_ms: read_u64(input, "sinceMs"),
                limit: read_u64(input, "limit").map(|v| v.min(u32::MAX as u64) as u32),
                include_bodies: read_bool(input, "includeBodies", false),
                url_filter: read_string(input, "urlFilter"),
            },
            BrowserToolKind::ReadPage => BrowserOperation::ReadPage {
                filter: read_read_page_filter(input, "filter")?,
                depth: read_u64(input, "depth").map(|v| v.min(u32::MAX as u64) as u32),
                ref_id: read_string(input, "refId"),
                max_chars: read_u64(input, "maxChars").map(|v| v.min(u32::MAX as u64) as u32),
            },
            BrowserToolKind::Find => BrowserOperation::Find {
                query: required_string(input, "query")?,
                limit: read_u64(input, "limit").map(|v| v.min(u32::MAX as u64) as u32),
            },
            BrowserToolKind::FormInput => BrowserOperation::FormInput {
                reference: required_string(input, "ref")?,
                value: input
                    .get("value")
                    .cloned()
                    .ok_or_else(|| RuntimeString::from("Missing required field `value`."))?,
            },
            BrowserToolKind::Computer => BrowserOperation::Computer {
                action: read_computer_action(input, "action")?
                    .ok_or_else(|| RuntimeString::from("Missing required field `action`."))?,
                coordinate: read_number_array(input, "coordinate")?,
                start_coordinate: read_number_array(input, "startCoordinate")?,
                text: read_string(input, "text"),
                duration: read_f64(input, "duration"),
                scroll_direction: read_scroll_direction(input, "scrollDirection")?,
                amount: read_f64(input, "amount"),
                ref_id: read_string(input, "refId").or_else(|| read_string(input, "ref")),
                save: read_bool(input, "save", false),
                keys: read_string_array(input, "keys"),
                path: input.get("path").cloned(),
                scroll_x: read_f64(input, "scrollX"),
                scroll_y: read_f64(input, "scrollY"),
            },
            BrowserToolKind::Tabs => BrowserOperation::Tabs {
                action: read_tabs_action(input, "action")?.unwrap_or(BrowserTabsAction::List),
                url: read_string(input, "url"),
                name: read_string(input, "name"),
            },
            BrowserToolKind::Locator => {
                // Forward semantic names through the locator options contract.
                let mut options = match input.get("options") {
                    Some(RuntimeJson::Object(object)) => object.clone(),
                    _ => RuntimeObject::new(),
                };
                let mut named_by_criteria = false;
                for key in LOCATOR_NAME_KEYS {
                    if let Some(value) = read_string(input, key) {
                        named_by_criteria = true;
                        options.insert((*key).to_string(), RuntimeJson::String(value));
                    }
                }
                for key in ["exact", "nth"] {
                    if let Some(value) = input.get(key) {
                        options.insert(key.to_string(), value.clone());
                    }
                }
                let selector = read_string(input, "selector");
                if selector.is_none() && !named_by_criteria {
                    return Err(format!(
                        "Name the element: `selector`, or one of {}.",
                        LOCATOR_NAME_KEYS.join(", ")
                    )
                    .into());
                }
                BrowserOperation::Locator {
                    // Empty rather than absent, because the field is not optional on
                    // the wire. The host reads an empty selector as "no selector given"
                    // and falls back to the nameable-element pool the criteria filter.
                    selector: selector.unwrap_or_default(),
                    action: required_string(input, "action")?,
                    value: input.get("value").cloned(),
                    options: RuntimeJson::Object(options),
                }
            }
            BrowserToolKind::PageText => BrowserOperation::PageText {
                max_chars: read_u64(input, "maxChars").map(|v| v.min(u32::MAX as u64) as u32),
            },
            BrowserToolKind::Batch => BrowserOperation::Batch {
                actions: input
                    .get("actions")
                    .and_then(RuntimeJson::as_array)
                    .cloned()
                    .ok_or_else(|| RuntimeString::from("`actions` must be an array."))?,
            },
            BrowserToolKind::Resize => {
                // Putting the viewport BACK needs no size, and an override with no way
                // to clear it outlives the reason it was set: the user's own tab keeps
                // whatever the agent narrowed it to for the rest of the session. The
                // host has always implemented the clear; only `viewport` was offered
                // here, and it was required, so there was no way to ask for it.
                let reset = read_bool(input, "reset", false);
                let viewport = read_viewport(input, "viewport")?;
                if viewport.is_none() && !reset {
                    return Err("`viewport` is required, or `reset` to clear it.".into());
                }
                BrowserOperation::Resize { viewport, reset }
            }
            BrowserToolKind::Close => BrowserOperation::Close {
                target: read_close_target(input, "target")?,
            },
            BrowserToolKind::GifCreator => BrowserOperation::GifCreator {
                filename: read_string(input, "filename"),
                duration_seconds: read_f64(input, "durationSeconds"),
                fps: read_u64(input, "fps").map(|v| v.min(u32::MAX as u64) as u32),
                full_page: read_bool(input, "fullPage", false),
            },
            BrowserToolKind::UploadImage => BrowserOperation::UploadImage {
                criteria: read_element_criteria(input),
                ref_id: read_string(input, "refId").or_else(|| read_string(input, "ref")),
                path: required_string(input, "path")?,
                selector: read_string(input, "selector"),
                prompt: read_string(input, "prompt"),
            },
            BrowserToolKind::FileUpload => BrowserOperation::FileUpload {
                criteria: read_element_criteria(input),
                paths: required_string_array(input, "paths")?,
                selector: read_string(input, "selector"),
                ref_id: read_string(input, "refId").or_else(|| read_string(input, "ref")),
            },
            BrowserToolKind::ShortcutsList => BrowserOperation::ShortcutsList {
                query: read_string(input, "query"),
            },
            BrowserToolKind::ShortcutsExecute => BrowserOperation::ShortcutsExecute {
                name: required_string(input, "name")?,
                wait_for_completion: read_bool(input, "waitForCompletion", false),
            },
            BrowserToolKind::SwitchBrowser => BrowserOperation::SwitchBrowser {
                prompt: read_string(input, "prompt"),
                wait_ms: read_u64(input, "waitMs"),
            },
            BrowserToolKind::ListConnectedBrowsers => BrowserOperation::ListConnectedBrowsers {
                local_only: read_bool(input, "localOnly", false),
            },
            BrowserToolKind::SelectBrowser => BrowserOperation::SelectBrowser {
                device_id: required_string(input, "deviceId")?,
            },
            BrowserToolKind::ResolveTab => BrowserOperation::ResolveTab {
                mention: required_string(input, "mention")?,
            },
            BrowserToolKind::ClaimTab => {
                if target.browser_id.as_ref().is_none_or(|browser_id| {
                    browser_id
                        .try_to_utf8()
                        .map_or(true, |browser_id| browser_id.trim().is_empty())
                }) {
                    return Err("Missing required non-empty field `browserId`.".into());
                }
                if target.tab_id.is_none() {
                    return Err("Missing required field `tabId`.".into());
                }
                BrowserOperation::ClaimTab
            }
            BrowserToolKind::History => BrowserOperation::History {
                queries: read_string_array(input, "queries")
                    .or_else(|| read_string(input, "query").map(|query| vec![query]))
                    .unwrap_or_default(),
                limit: read_u64(input, "limit").map(|v| v.min(u32::MAX as u64) as u32),
                from: read_string(input, "from"),
                to: read_string(input, "to"),
            },
            BrowserToolKind::TabsContent => BrowserOperation::TabsContent {
                urls: required_string_array(input, "urls")?,
                content_type: read_string(input, "contentType"),
                max_chars: read_u64(input, "maxChars").map(|v| v.min(u32::MAX as u64) as u32),
                wait_until: read_wait_until(input, "waitUntil")?,
            },
            BrowserToolKind::ContentExport => BrowserOperation::ContentExport {
                format: read_string(input, "format"),
                selector: read_string(input, "selector"),
                max_chars: read_u64(input, "maxChars").map(|v| v.min(u32::MAX as u64) as u32),
            },
            BrowserToolKind::Download => BrowserOperation::Download {
                wait_only: read_bool(input, "waitOnly", false),
                selector: read_string(input, "selector"),
                url: read_string(input, "url"),
                x: read_f64(input, "x"),
                y: read_f64(input, "y"),
                filename: read_string(input, "filename"),
            },
            BrowserToolKind::Clipboard => BrowserOperation::Clipboard {
                action: read_clipboard_action(input, "action")?,
                text: read_string(input, "text"),
                items: input.get("items").cloned(),
            },
            BrowserToolKind::Dialog => BrowserOperation::Dialog {
                action: read_dialog_action(input, "action")?,
                prompt_text: read_string(input, "promptText")
                    .or_else(|| read_string(input, "prompt_text")),
            },
            BrowserToolKind::Permission => {
                let action = read_permission_action(input, "action")?
                    .unwrap_or(BrowserPermissionAction::Grant);
                let origin = read_string(input, "origin");
                // `grant` and `set` are about ONE origin. Without it the host would have
                // to guess which site, and guessing changes what the browser remembers
                // for a site the caller never named.
                if origin.is_none() && action != BrowserPermissionAction::Reset {
                    return Err("`origin` is required for grant and set.".into());
                }
                BrowserOperation::Permission {
                    action,
                    origin,
                    permissions: read_string_array(input, "permissions"),
                    permission: read_string(input, "permission"),
                    setting: read_permission_setting(input, "setting")?,
                }
            }
            BrowserToolKind::MarkTab => BrowserOperation::MarkTab {
                status: read_tab_mark_status(input, "status")?,
            },
        };
        let mut request = BrowserRuntimeRequest::new(operation, target, Some(timeout_ms));
        request.metadata.insert(
            "toolName".into(),
            RuntimeJson::String(self.kind.name().into()),
        );
        Ok((request, timeout_ms))
    }

    /// Validate the URLs a navigation would visit, rejecting anything the browser
    /// must not open.
    ///
    /// Navigation does NOT ask for per-host network approval. Browsing the web is
    /// the entire purpose of this tool, so prompting once per new domain asks the
    /// user to confirm that a browser may reach the internet — a question with no
    /// meaningful "no", which trains people to click through every prompt and
    /// turns the approval into a rubber stamp. The consent that carries weight is
    /// granted elsewhere and stays: opening a browser at all is a user action, and
    /// driving the user's OWN Chrome (with their cookies and logins) requires them
    /// to connect it first. The built-in browser is a clean, signed-out Chromium
    /// that reaches nothing but the page it was pointed at.
    ///
    /// What still gets refused here is a target the browser has no business
    /// opening: a non-http(s) scheme (file:, chrome:, javascript: and friends) or
    /// an unparseable URL.
    async fn validate_navigation_targets(
        &self,
        request: &BrowserRuntimeRequest,
        _ctx: &ToolContext,
    ) -> Option<ToolResult> {
        let urls = match navigation_urls(&request.operation) {
            Ok(urls) => urls,
            Err(message) => {
                return Some(error_result(BrowserRuntimeError::new(
                    BrowserRuntimeErrorCode::InvalidRequest,
                    message,
                )));
            }
        };
        for url in urls {
            let url_usv = browser_url_to_web_idl_usv_string(&url);
            let parsed = match Url::parse(&url_usv) {
                Ok(parsed) => parsed,
                Err(err) => {
                    return Some(error_result(BrowserRuntimeError::new(
                        BrowserRuntimeErrorCode::InvalidRequest,
                        format!("Invalid browser navigation URL: {err}"),
                    )));
                }
            };
            if parsed.scheme() != "http" && parsed.scheme() != "https" {
                return Some(error_result(BrowserRuntimeError::new(
                    BrowserRuntimeErrorCode::InvalidRequest,
                    format!(
                        "Refusing to navigate browser to non-http(s) URL (scheme \"{}:\").",
                        parsed.scheme()
                    ),
                )));
            }
        }
        None
    }
}

#[async_trait]
impl Tool for BrowserTool {
    fn static_name(&self) -> &str {
        self.kind.name()
    }

    fn static_description(&self) -> &str {
        self.kind.description()
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn static_search_hint(&self) -> Option<&str> {
        Some(self.kind.search_hint())
    }

    fn max_result_size_chars(&self) -> Option<u64> {
        Some(200_000)
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        self.kind.is_read_only(_input)
    }

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    async fn check_permissions(
        &self,
        _input: &Json,
        _ctx: &ToolContext,
    ) -> Option<PermissionResult> {
        if self.kind.is_read_only(_input) {
            return None;
        }
        Some(PermissionResult::Ask {
            message: format!(
                "Allow {} to interact with the browser page?",
                self.kind.name()
            )
            .into(),
            updated_input: None,
            reason: PermissionReason::ToolSelfCheck,
            suggestions: None,
        })
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let runtime = match resolve_runtime(ctx) {
            Ok(runtime) => runtime,
            Err(error) => return Ok(error_result(error)),
        };
        let (request, timeout_ms) = match self.build_request(&input) {
            Ok(r) => r,
            Err(message) => {
                return Ok(error_result(BrowserRuntimeError::new(
                    BrowserRuntimeErrorCode::InvalidRequest,
                    message,
                )));
            }
        };
        if let Some(error) = self.validate_navigation_targets(&request, ctx).await {
            return Ok(error);
        }

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let persist = ctx.emit.clone();
        let emit: BrowserEventSink = Arc::new(move |event| {
            // Acquire callback responsibility when the producer emits, before
            // this existing synchronous bridge queues its pending future.
            let _ = tx.send(persist(ToolDomainEvent::Browser { event }));
        });
        let parent_signal = ctx.signal.clone();
        let call_signal = parent_signal.child_token();
        let run_signal = call_signal.clone();
        let run = async move {
            tokio::select! {
                _ = parent_signal.cancelled() => {
                    call_signal.cancel();
                    Err(BrowserRuntimeError::new(
                        BrowserRuntimeErrorCode::Cancelled,
                        "Browser operation cancelled.",
                    ))
                }
                _ = tokio::time::sleep(Duration::from_millis(timeout_ms)) => {
                    call_signal.cancel();
                    Err(BrowserRuntimeError::new(
                        BrowserRuntimeErrorCode::Timeout,
                        format!("Browser operation timed out after {timeout_ms} ms."),
                    ))
                }
                result = runtime.execute(request, run_signal, emit) => result,
            }
        };
        let forward = async {
            while let Some(persist) = rx.recv().await {
                persist.await;
            }
        };
        let (outcome, ()) = tokio::join!(run, forward);
        match outcome {
            Ok(response) => Ok(success_result(
                self.kind,
                response,
                ctx.vision != Some(false),
            )),
            Err(err) => Ok(error_result(err)),
        }
    }

    fn summarize(&self, _input: &Json) -> Option<RuntimeString> {
        match self.kind {
            BrowserToolKind::Navigate => Some("Opening browser page".into()),
            BrowserToolKind::Snapshot => Some("Reading browser page".into()),
            BrowserToolKind::Screenshot => Some("Capturing browser screenshot".into()),
            BrowserToolKind::Click => Some("Clicking in browser".into()),
            BrowserToolKind::Type => Some("Typing in browser".into()),
            BrowserToolKind::Eval => Some("Evaluating browser JavaScript".into()),
            BrowserToolKind::Console => Some("Reading browser console".into()),
            BrowserToolKind::Network => Some("Reading browser network log".into()),
            BrowserToolKind::ReadPage => Some("Reading browser page tree".into()),
            BrowserToolKind::Find => Some("Finding browser elements".into()),
            BrowserToolKind::FormInput => Some("Setting browser form input".into()),
            BrowserToolKind::Computer => Some("Using browser mouse or keyboard".into()),
            BrowserToolKind::Tabs => Some("Managing browser tabs".into()),
            BrowserToolKind::Locator => Some("Running browser locator action".into()),
            BrowserToolKind::PageText => Some("Extracting browser page text".into()),
            BrowserToolKind::Batch => Some("Running browser action batch".into()),
            BrowserToolKind::Resize => Some("Resizing browser viewport".into()),
            BrowserToolKind::Close => Some("Closing browser target".into()),
            BrowserToolKind::GifCreator => Some("Capturing browser GIF".into()),
            BrowserToolKind::UploadImage => Some("Uploading browser image".into()),
            BrowserToolKind::FileUpload => Some("Uploading browser file".into()),
            BrowserToolKind::ShortcutsList => Some("Listing browser shortcuts".into()),
            BrowserToolKind::ShortcutsExecute => Some("Executing browser shortcut".into()),
            BrowserToolKind::SwitchBrowser => Some("Switching connected browser".into()),
            BrowserToolKind::ListConnectedBrowsers => Some("Listing connected browsers".into()),
            BrowserToolKind::SelectBrowser => Some("Selecting connected browser".into()),
            BrowserToolKind::ResolveTab => Some("Resolving referenced browser tab".into()),
            BrowserToolKind::ClaimTab => Some("Claiming user browser tab".into()),
            BrowserToolKind::History => Some("Reading browser history".into()),
            BrowserToolKind::TabsContent => Some("Extracting temporary tab content".into()),
            BrowserToolKind::ContentExport => Some("Exporting browser content".into()),
            BrowserToolKind::Download => Some("Downloading browser content".into()),
            BrowserToolKind::Clipboard => Some("Using browser clipboard".into()),
            BrowserToolKind::Dialog => Some("Handling browser dialog".into()),
            BrowserToolKind::MarkTab => Some("Marking browser tab".into()),
            BrowserToolKind::Permission => Some("Setting a site's browser permissions".into()),
        }
    }
}

fn success_result(
    kind: BrowserToolKind,
    response: BrowserRuntimeResponse,
    vision: bool,
) -> ToolResult {
    // A model without vision cannot look at a capture, so the image block is
    // withheld and the text says so; the rest of the reply still rides along.
    let failed = response.error.is_some();
    let capture = inline_image(&response);
    let image = if vision { capture.clone() } else { None };
    let vision_dropped = !vision && capture.is_some();
    // A capture's text is what remains once the blob is out of it, because the
    // blob is going to the model as a PICTURE instead.
    let text = match &capture {
        Some(image) => image_text(kind, &response, image, vision),
        None => response_text(kind, &response),
    };
    let response_value = runtime_codec::encode_value(&response)
        .expect("BrowserRuntimeResponse exact codec must encode");
    let mut output = RuntimeObject::new();
    output.insert("ok", RuntimeJson::Bool(!failed));
    output.insert("operation", RuntimeJson::String(kind.name().into()));
    output.insert("response", response_value);
    let Some(image) = image else {
        let text = if vision_dropped {
            text.concat(&RuntimeString::from(
                " A capture was taken but not attached: this model cannot understand images. Tell the user to switch to a model with vision support for visual verification.",
            ))
        } else {
            text
        };
        return text_result(RuntimeJson::Object(output), text, failed);
    };
    // An image block is the ONLY channel a model can look through, and a
    // screenshot's entire purpose is to be looked at. Carrying it as text meant
    // `browser_screenshot` — whose own description says to use it for visual
    // verification — could not be visually verified: the base64 arrived as prose
    // the model cannot see, and being prose it counted against the tool-result
    // budget, so a large capture was spilled to a file on disk and replaced with a
    // 2000-character preview of base64. Image blocks are exempt from that budget
    // for exactly this reason.
    ToolResult {
        serializable: ToolResultSerializable {
            output: RuntimeJson::Object(output),
            model_content: vec![
                OutputBlock::Text(TextBlock {
                    text,
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                }),
                OutputBlock::Image(image),
            ],
            is_error: Some(failed),
            mcp_meta: None,
            extra_user_blocks: None,
        },
        side_effects: None,
    }
}

/// The image a browser answer carries inline, if it carries one.
///
/// The host hands a capture back as a `data:image/<mime>;base64,<data>` URL on the
/// response `value` (that is the wire shape both browsers answer), so this decodes
/// that URL into the block the model can actually see. `data:` URLs that are not
/// images are left alone: a pdf export rides the same key, and there is no document
/// block to put it in, so it stays a reference in the text.
fn inline_image(response: &BrowserRuntimeResponse) -> Option<ImageBlock> {
    let RuntimeJson::String(data_url) = response.value.as_ref()?.get("dataUrl")? else {
        return None;
    };
    let data_url = data_url.try_to_utf8().ok()?;
    let (declaration, base64) = data_url.strip_prefix("data:")?.split_once(',')?;
    let mime = declaration.strip_suffix(";base64")?;
    if !mime.starts_with("image/") || base64.is_empty() {
        return None;
    }
    Some(ImageBlock {
        mime_type: RuntimeString::from(mime),
        base64: RuntimeString::from(base64),
        detail: None,
        source_path: None,
    })
}

/// What to SAY about an answer whose payload is the attached image.
///
/// Anything the answer carried besides the image is still reported — the probe
/// overlay a `computer` capture returns, for instance — so attaching the picture
/// does not cost the caller the rest of the reply.
fn image_text(
    kind: BrowserToolKind,
    response: &BrowserRuntimeResponse,
    image: &ImageBlock,
    attached: bool,
) -> RuntimeString {
    let rest = response.value.as_ref().and_then(|value| match value {
        RuntimeJson::Object(object) => {
            let mut rest = object.clone();
            rest.remove("dataUrl");
            (!rest.is_empty()).then(|| RuntimeJson::Object(rest))
        }
        _ => None,
    });
    let mut text = RuntimeString::from(if attached {
        "Attached the "
    } else {
        "Captured an "
    })
    .concat(&image.mime_type)
    .concat(&RuntimeString::from(" capture from "))
    .concat(&RuntimeString::from(kind.name()));
    if let Some(url) = &response.url {
        text = text.concat(&RuntimeString::from(" of ")).concat(url);
    }
    text = text.concat(&RuntimeString::from("."));
    if let Some(RuntimeJson::String(path)) =
        response.value.as_ref().and_then(|value| value.get("path"))
    {
        text = RuntimeString::from("Saved: ")
            .concat(&runtime_json::stringify(&RuntimeJson::String(path.clone())))
            .concat(&RuntimeString::from("\n"))
            .concat(&text);
    }
    if let Some(message) = &response.message {
        text = text.concat(&RuntimeString::from("\n")).concat(message);
    }
    match rest {
        Some(rest) => text
            .concat(&RuntimeString::from("\n"))
            .concat(&runtime_json::stringify_pretty(&rest)),
        None => text,
    }
}

fn error_result(error: BrowserRuntimeError) -> ToolResult {
    let text = RuntimeString::from("Browser runtime error: ").concat(&error.message);
    let error_value =
        runtime_codec::encode_value(&error).expect("BrowserRuntimeError exact codec must encode");
    let mut output = RuntimeObject::new();
    output.insert("ok", RuntimeJson::Bool(false));
    output.insert("error", error_value);
    text_result(RuntimeJson::Object(output), text, true)
}

fn response_text(kind: BrowserToolKind, response: &BrowserRuntimeResponse) -> RuntimeString {
    if response.error.is_some() {
        return runtime_json::stringify_pretty(
            &runtime_codec::encode_value(response)
                .expect("Browser failure response codec must encode"),
        );
    }

    let diagnostic_entries = match kind {
        BrowserToolKind::Console => {
            Some(("console", runtime_codec::encode_value(&response.console)))
        }
        BrowserToolKind::Network => {
            Some(("network", runtime_codec::encode_value(&response.network)))
        }
        _ => None,
    };
    if let Some((name, entries)) = diagnostic_entries {
        let mut diagnostics = RuntimeObject::new();
        diagnostics.insert(name, entries.expect("Browser diagnostic codec must encode"));
        for (key, value) in [
            ("count", response.count.map(RuntimeJson::from)),
            ("total", response.total.map(RuntimeJson::from)),
            ("truncated", response.truncated.map(RuntimeJson::from)),
            ("evicted", response.evicted.map(RuntimeJson::from)),
            ("message", response.message.clone().map(RuntimeJson::String)),
        ] {
            if let Some(value) = value {
                diagnostics.insert(key, value);
            }
        }
        return runtime_json::stringify_pretty(&RuntimeJson::Object(diagnostics));
    }
    if let Some(message) = &response.message {
        return message.clone();
    }
    if let Some(text) = &response.text {
        return text.clone();
    }
    if let Some(snapshot) = &response.snapshot {
        if let Some(text) = &snapshot.text {
            return text.clone();
        }
        if !snapshot.elements.is_empty() {
            let elements = runtime_codec::encode_value(&snapshot.elements)
                .expect("BrowserElementSnapshot exact codec must encode");
            return runtime_json::stringify_pretty(&elements);
        }
    }
    if !response.artifacts.is_empty() {
        let refs: Vec<RuntimeString> = response
            .artifacts
            .iter()
            .map(|a| {
                a.path
                    .clone()
                    .or_else(|| a.uri.clone())
                    .unwrap_or_else(|| a.id.clone())
            })
            .collect();
        return RuntimeString::from("Browser artifact(s): ")
            .concat(&join_runtime_strings(&refs, ", "));
    }
    if let Some(value) = &response.value {
        return runtime_json::stringify_pretty(value);
    }
    match kind {
        BrowserToolKind::Navigate => response
            .url
            .as_ref()
            .map(|url| {
                RuntimeString::from("Browser navigated to ")
                    .concat(url)
                    .concat(&RuntimeString::from("."))
            })
            .unwrap_or_else(|| "Browser navigation completed.".into()),
        BrowserToolKind::Close => "Browser target closed.".into(),
        _ => runtime_json::stringify_pretty(
            &runtime_codec::encode_value(response)
                .expect("BrowserRuntimeResponse exact codec must encode"),
        ),
    }
}

fn join_runtime_strings(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let separator = RuntimeString::from(separator);
    let mut output = RuntimeString::default();
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            output = output.concat(&separator);
        }
        output = output.concat(value);
    }
    output
}

fn read_element_criteria(input: &RuntimeJson) -> BrowserElementCriteria {
    BrowserElementCriteria {
        role: read_string(input, "role"),
        name: read_string(input, "name"),
        label: read_string(input, "label"),
        placeholder: read_string(input, "placeholder"),
        test_id: read_string(input, "testId"),
        query: read_string(input, "query"),
        exact: input.get("exact").and_then(RuntimeJson::as_bool),
        nth: input.get("nth").and_then(RuntimeJson::as_i64),
    }
}

fn has_element_criteria(input: &RuntimeJson) -> bool {
    [
        "ref",
        "refId",
        "role",
        "name",
        "label",
        "placeholder",
        "testId",
        "query",
    ]
    .iter()
    .any(|key| read_string(input, key).is_some())
}

fn input_schema(kind: BrowserToolKind) -> JsonSchema {
    let mut props = common_props();
    let required: Vec<&str> = match kind {
        BrowserToolKind::Navigate => {
            props.insert("url".to_string(), string_prop("URL to navigate to"));
            props.insert(
                "waitUntil".to_string(),
                enum_prop(
                    &["commit", "dom_content_loaded", "load", "network_idle"],
                    "Load state to wait for",
                ),
            );
            props.insert("viewport".to_string(), viewport_prop());
            vec!["url"]
        }
        BrowserToolKind::Snapshot => {
            props.insert(
                "selector".to_string(),
                string_prop("Optional CSS selector to inspect"),
            );
            props.insert(
                "includeText".to_string(),
                bool_prop("Include rendered text in the snapshot"),
            );
            props.insert(
                "includeLinks".to_string(),
                bool_prop("Include discovered links in the snapshot"),
            );
            Vec::new()
        }
        BrowserToolKind::Screenshot => {
            props.insert(
                "selector".to_string(),
                string_prop("Optional CSS selector to screenshot"),
            );
            props.insert("fullPage".to_string(), bool_prop("Capture the full page"));
            props.insert("save".to_string(), json!({"type":"boolean", "default":true, "description":"Save the screenshot and return its absolute path; false is transient"}));
            props.insert(
                "format".to_string(),
                enum_prop(&["png", "jpeg"], "Screenshot image format"),
            );
            props.insert(
                "quality".to_string(),
                integer_prop("JPEG quality from 0 to 100"),
            );
            Vec::new()
        }
        BrowserToolKind::Click => {
            props.insert("selector".to_string(), string_prop("CSS selector to click"));
            props.insert("x".to_string(), number_prop("Viewport x coordinate"));
            props.insert("y".to_string(), number_prop("Viewport y coordinate"));
            props.insert(
                "button".to_string(),
                enum_prop(&["left", "middle", "right"], "Mouse button"),
            );
            props.insert("clickCount".to_string(), integer_prop("Number of clicks"));
            props.insert(
                "force".to_string(),
                bool_prop(
                    "Act even when the pre-click checks say the element is unreachable \
                     (aria-disabled, a zero-size box, something drawn over it). The \
                     answer says which path it took rather than claiming a verified click.",
                ),
            );
            props.insert(
                "modifiers".to_string(),
                array_string_prop("Keyboard modifiers such as Shift, Meta, Alt, Control"),
            );
            Vec::new()
        }
        BrowserToolKind::Type => {
            props.insert(
                "selector".to_string(),
                string_prop("CSS selector to type into"),
            );
            props.insert("text".to_string(), string_prop("Text to type"));
            props.insert("clear".to_string(), bool_prop("Clear existing value first"));
            props.insert(
                "pressEnter".to_string(),
                bool_prop("Press Enter after typing the text"),
            );
            props.insert(
                "key".to_string(),
                string_prop(
                    "A named key to press on this element after any text: Escape, Tab, \
                     ArrowDown, cmd+Enter. Goes to the element named here rather than \
                     to whatever holds focus.",
                ),
            );
            props.insert(
                "delayMs".to_string(),
                integer_prop("Delay between keystrokes"),
            );
            vec!["text"]
        }
        BrowserToolKind::Eval => {
            props.insert(
                "script".to_string(),
                string_prop("JavaScript source to evaluate"),
            );
            props.insert(
                "args".to_string(),
                json!({ "description": "JSON argument value" }),
            );
            props.insert(
                "awaitPromise".to_string(),
                bool_prop("Await a returned Promise before resolving"),
            );
            vec!["script"]
        }
        BrowserToolKind::Console => {
            props.insert(
                "levels".to_string(),
                array_enum_prop(
                    &["debug", "info", "warning", "error", "log"],
                    "Console levels to include",
                ),
            );
            props.insert(
                "sinceMs".to_string(),
                integer_prop("Only entries after this timestamp"),
            );
            props.insert(
                "limit".to_string(),
                integer_prop("Maximum entries to return"),
            );
            props.insert(
                "clear".to_string(),
                bool_prop("Clear buffered entries after reading"),
            );
            Vec::new()
        }
        BrowserToolKind::Network => {
            props.insert(
                "sinceMs".to_string(),
                integer_prop("Only entries after this timestamp"),
            );
            props.insert(
                "limit".to_string(),
                integer_prop("Maximum entries to return"),
            );
            props.insert(
                "includeBodies".to_string(),
                bool_prop("Include request/response bodies when the host permits it"),
            );
            props.insert("urlFilter".to_string(), string_prop("Substring URL filter"));
            Vec::new()
        }
        BrowserToolKind::ReadPage => {
            props.insert(
                "filter".to_string(),
                enum_prop(
                    &["interactive", "all"],
                    "Elements to include in the page tree",
                ),
            );
            props.insert(
                "depth".to_string(),
                integer_prop("Maximum DOM tree depth to traverse"),
            );
            props.insert(
                "refId".to_string(),
                string_prop("Optional parent element reference to focus on"),
            );
            props.insert(
                "maxChars".to_string(),
                integer_prop("Maximum response text characters"),
            );
            Vec::new()
        }
        BrowserToolKind::Find => {
            props.insert(
                "query".to_string(),
                string_prop(
                    "Rendered text, an accessible name or label, or a single role word \
                     such as button. NOT a description of an element: for \"the blue \
                     button at the bottom\" use browser_locator with role/text/within.",
                ),
            );
            props.insert(
                "limit".to_string(),
                integer_prop("Maximum matching elements to return"),
            );
            vec!["query"]
        }
        BrowserToolKind::FormInput => {
            props.insert(
                "ref".to_string(),
                string_prop("Element reference from browser_read_page or browser_find"),
            );
            props.insert(
                "value".to_string(),
                json!({
                    "description": "Value to set; strings/numbers set text/selects, booleans set checkboxes.",
                    "type": ["string", "number", "boolean"]
                }),
            );
            vec!["ref", "value"]
        }
        BrowserToolKind::Computer => {
            props.insert(
                "action".to_string(),
                enum_prop(
                    &[
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
                    ],
                    "Mouse, keyboard, wait, screenshot, zoom, or scroll action",
                ),
            );
            props.insert(
                "coordinate".to_string(),
                number_tuple_prop("Viewport coordinate [x, y]"),
            );
            props.insert(
                "keys".to_string(),
                array_string_prop(
                    "For the key action: the combination as its parts, [\"cmd\", \"a\"]. \
                     Without it a key action with no `text` presses Enter.",
                ),
            );
            props.insert(
                "path".to_string(),
                json!({
                    "type": "array",
                    "description": "For left_click_drag: the points the drag passes THROUGH, \
                                    as [[x, y], ...]. A control that follows the pointer, such \
                                    as a slider, a canvas or a reorder handle, does not react \
                                    to a jump from the start to the end.",
                    "items": { "type": "array", "items": { "type": "number" } }
                }),
            );
            props.insert(
                "scrollX".to_string(),
                number_prop("For scroll: signed pixels horizontally, instead of a direction."),
            );
            props.insert(
                "scrollY".to_string(),
                number_prop("For scroll: signed pixels vertically, instead of a direction."),
            );
            props.insert(
                "startCoordinate".to_string(),
                number_tuple_prop("Start coordinate [x, y] for drag"),
            );
            props.insert(
                "text".to_string(),
                string_prop("Text to type or key to press"),
            );
            props.insert("duration".to_string(), number_prop("Duration in seconds"));
            props.insert(
                "scrollDirection".to_string(),
                enum_prop(&["up", "down", "left", "right"], "Scroll direction"),
            );
            props.insert("amount".to_string(), number_prop("Scroll amount in pixels"));
            props.insert(
                "refId".to_string(),
                string_prop("Element reference for scroll_to or target-aware actions"),
            );
            props.insert("ref".to_string(), string_prop("Alias for refId"));
            props.insert(
                "save".to_string(),
                bool_prop("Save screenshot/zoom as artifact"),
            );
            vec!["action"]
        }
        BrowserToolKind::Tabs => {
            props.insert(
                "action".to_string(),
                enum_prop(
                    &[
                        "list",
                        "user",
                        "new",
                        "get",
                        "selected",
                        "close",
                        "finalize",
                        "name_session",
                    ],
                    "Tab/session action",
                ),
            );
            props.insert("url".to_string(), string_prop("Optional URL for a new tab"));
            props.insert(
                "name".to_string(),
                string_prop("Session name for name_session action"),
            );
            Vec::new()
        }
        BrowserToolKind::Locator => {
            props.insert(
                "selector".to_string(),
                string_prop("CSS selector. Omit it when naming the element another way."),
            );
            props.insert(
                "text".to_string(),
                string_prop("Name it by its rendered text."),
            );
            props.insert(
                "role".to_string(),
                string_prop("Name it by accessible role, such as button, link or textbox."),
            );
            props.insert("label".to_string(), string_prop("Name it by its label."));
            props.insert(
                "placeholder".to_string(),
                string_prop("Name it by its placeholder."),
            );
            props.insert(
                "testId".to_string(),
                string_prop("Name it by data-testid, which survives re-renders and copy changes."),
            );
            props.insert(
                "ref".to_string(),
                string_prop("A ref a previous locator call returned."),
            );
            props.insert(
                "action".to_string(),
                enum_prop(
                    &[
                        "click",
                        "double_click",
                        "type",
                        "fill",
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
                        "select_option",
                    ],
                    "Locator action",
                ),
            );
            props.insert(
                "value".to_string(),
                json!({ "description": "Action value, such as text, key, or select option." }),
            );
            props.insert(
                "options".to_string(),
                json!({
                    "type": "object",
                    "description": "Playwright-style action options such as timeoutMs, force, state, button, modifiers, exact, within, has, hasText, hasNot, hasNotText, limit.",
                    "additionalProperties": true
                }),
            );
            // `selector` is NOT required: naming by role, text, label, placeholder or
            // testId is the durable way, and requiring a selector alongside made those
            // unusable.
            vec!["action"]
        }
        BrowserToolKind::PageText => {
            props.insert(
                "maxChars".to_string(),
                integer_prop("Maximum response text characters"),
            );
            Vec::new()
        }
        BrowserToolKind::Batch => {
            props.insert(
                "actions".to_string(),
                json!({
                    "type": "array",
                    "items": { "type": "object", "additionalProperties": true },
                    "description": "Sequential browser operation objects using the same operation.type names."
                }),
            );
            vec!["actions"]
        }
        BrowserToolKind::Resize => {
            props.insert("viewport".to_string(), viewport_prop());
            props.insert(
                "reset".to_string(),
                bool_prop(
                    "Put the viewport BACK instead of setting one. An override that is \
                     never cleared leaves the user's own tab at whatever size it was \
                     narrowed to; call this before finishing.",
                ),
            );
            // Neither is required on its own: a size sets, `reset` clears, and asking
            // for neither is refused by name.
            Vec::new()
        }
        BrowserToolKind::Close => {
            props.insert(
                "target".to_string(),
                enum_prop(&["tab", "session", "all"], "What to close"),
            );
            Vec::new()
        }
        BrowserToolKind::GifCreator => {
            props.insert(
                "filename".to_string(),
                string_prop("Optional GIF artifact filename"),
            );
            props.insert(
                "durationSeconds".to_string(),
                number_prop("Capture duration in seconds"),
            );
            props.insert("fps".to_string(), integer_prop("Frames per second"));
            props.insert("fullPage".to_string(), bool_prop("Capture the full page"));
            Vec::new()
        }
        BrowserToolKind::UploadImage => {
            props.insert("path".to_string(), string_prop("Absolute image file path"));
            props.insert(
                "selector".to_string(),
                string_prop("Optional upload/drop target selector"),
            );
            props.insert(
                "prompt".to_string(),
                string_prop("Optional instruction for how the image should be used"),
            );
            vec!["path"]
        }
        BrowserToolKind::FileUpload => {
            props.insert(
                "paths".to_string(),
                array_string_prop("Absolute file paths to set on the file input"),
            );
            props.insert(
                "selector".to_string(),
                string_prop("Optional file input selector"),
            );
            props.insert(
                "refId".to_string(),
                string_prop("Optional element reference for the file input"),
            );
            props.insert("ref".to_string(), string_prop("Alias for refId"));
            vec!["paths"]
        }
        BrowserToolKind::ShortcutsList => {
            props.insert(
                "query".to_string(),
                string_prop("Optional shortcut/workflow name filter"),
            );
            Vec::new()
        }
        BrowserToolKind::ShortcutsExecute => {
            props.insert("name".to_string(), string_prop("Shortcut or workflow name"));
            props.insert(
                "waitForCompletion".to_string(),
                bool_prop("Wait for the workflow to complete if the runtime supports it"),
            );
            vec!["name"]
        }
        BrowserToolKind::SwitchBrowser => {
            props.insert(
                "prompt".to_string(),
                string_prop("Optional prompt shown while selecting a browser"),
            );
            props.insert(
                "waitMs".to_string(),
                integer_prop("Maximum time to wait for browser selection"),
            );
            Vec::new()
        }
        BrowserToolKind::ListConnectedBrowsers => {
            props.insert(
                "localOnly".to_string(),
                bool_prop("Only list browsers that appear to be on this computer"),
            );
            Vec::new()
        }
        BrowserToolKind::ResolveTab => {
            props.insert(
                "mention".to_string(),
                string_prop("Complete plugin:// tab-v1 mention URI selected by the user"),
            );
            vec!["mention"]
        }
        BrowserToolKind::ClaimTab => vec!["browserId", "tabId"],
        BrowserToolKind::SelectBrowser => {
            props.insert(
                "deviceId".to_string(),
                string_prop("Connected browser device id"),
            );
            vec!["deviceId"]
        }
        BrowserToolKind::History => {
            props.insert(
                "queries".to_string(),
                array_string_prop("Optional history search queries"),
            );
            props.insert(
                "query".to_string(),
                string_prop("Single history search query"),
            );
            props.insert("limit".to_string(), integer_prop("Maximum history entries"));
            props.insert(
                "from".to_string(),
                string_prop("ISO lower-bound visit timestamp"),
            );
            props.insert(
                "to".to_string(),
                string_prop("ISO upper-bound visit timestamp"),
            );
            Vec::new()
        }
        BrowserToolKind::TabsContent => {
            props.insert(
                "urls".to_string(),
                array_string_prop("URLs to load in temporary tabs"),
            );
            props.insert(
                "contentType".to_string(),
                enum_prop(&["text", "markdown", "html"], "Content representation"),
            );
            props.insert(
                "maxChars".to_string(),
                integer_prop("Maximum content characters"),
            );
            props.insert(
                "waitUntil".to_string(),
                enum_prop(
                    &["commit", "dom_content_loaded", "load", "network_idle"],
                    "Navigation wait condition",
                ),
            );
            vec!["urls"]
        }
        BrowserToolKind::ContentExport => {
            props.insert(
                "format".to_string(),
                enum_prop(&["text", "markdown", "html"], "Export format"),
            );
            props.insert(
                "selector".to_string(),
                string_prop("Optional root selector"),
            );
            props.insert(
                "maxChars".to_string(),
                integer_prop("Maximum content characters"),
            );
            Vec::new()
        }
        BrowserToolKind::Download => {
            props.insert(
                "selector".to_string(),
                string_prop("Link/media selector to download"),
            );
            props.insert("url".to_string(), string_prop("Direct URL to download"));
            props.insert("x".to_string(), number_prop("Viewport x coordinate"));
            props.insert("y".to_string(), number_prop("Viewport y coordinate"));
            props.insert(
                "filename".to_string(),
                string_prop("Optional artifact filename"),
            );
            props.insert(
                "waitOnly".to_string(),
                bool_prop(
                    "Wait for a download something else started rather than starting \
                     one. With a `url` and without this, that url is fetched.",
                ),
            );
            Vec::new()
        }
        BrowserToolKind::Clipboard => {
            props.insert(
                "action".to_string(),
                enum_prop(
                    &["read", "read_text", "write", "write_text"],
                    "Clipboard action",
                ),
            );
            props.insert("text".to_string(), string_prop("Text for write_text"));
            props.insert(
                "items".to_string(),
                json!({
                    "description": "Clipboard items for write.",
                    "type": "array",
                    "items": { "type": "object", "additionalProperties": true }
                }),
            );
            Vec::new()
        }
        BrowserToolKind::Dialog => {
            props.insert(
                "action".to_string(),
                enum_prop(&["get", "accept", "dismiss"], "Dialog action"),
            );
            props.insert(
                "promptText".to_string(),
                string_prop("Prompt text for accept"),
            );
            Vec::new()
        }
        BrowserToolKind::Permission => {
            props.insert(
                "action".to_string(),
                enum_prop(
                    &["grant", "set", "reset"],
                    "What to do with the permissions",
                ),
            );
            props.insert(
                "origin".to_string(),
                string_prop("http(s) origin, e.g. https://example.com. Needed for grant and set."),
            );
            props.insert(
                "permissions".to_string(),
                array_string_prop("For grant: geolocation, notifications, camera, microphone."),
            );
            props.insert(
                "permission".to_string(),
                string_prop("For set: the single permission name."),
            );
            props.insert(
                "setting".to_string(),
                enum_prop(
                    &["granted", "denied", "prompt"],
                    "For set: what to set it to",
                ),
            );
            vec!["action"]
        }
        BrowserToolKind::MarkTab => {
            props.insert(
                "status".to_string(),
                enum_prop(&["deliverable", "handoff"], "Tab disposition"),
            );
            Vec::new()
        }
    };
    if matches!(
        kind,
        BrowserToolKind::Screenshot
            | BrowserToolKind::Click
            | BrowserToolKind::Type
            | BrowserToolKind::UploadImage
            | BrowserToolKind::FileUpload
            | BrowserToolKind::Locator
    ) {
        for (key, description) in [
            ("ref", "Element reference from the latest page observation"),
            ("refId", "Element reference alias"),
            ("role", "Accessible role to match"),
            ("name", "Accessible element name to match"),
            ("label", "Form label to match"),
            ("placeholder", "Field placeholder to match"),
            ("testId", "Element test identifier to match"),
            (
                "query",
                "Visible target text to match; distinct from text to type",
            ),
        ] {
            props.insert(key.to_string(), string_prop(description));
        }
        props.insert(
            "exact".to_string(),
            bool_prop("Match the complete target name or text"),
        );
        props.insert(
            "nth".to_string(),
            json!({"type":"integer","minimum":-9007199254740991_i64,"maximum":9007199254740991_i64,"description":"Zero-based match index; negative indexes count from the end"}),
        );
    }

    object_schema(props, &required)
}

fn common_props() -> Map<String, Value> {
    let mut props = Map::new();
    props.insert(
        "sessionId".to_string(),
        string_prop("Browser session id; omitted means default session"),
    );
    props.insert(
        "tabId".to_string(),
        json!({
            "anyOf": [
                { "type": "string", "minLength": 1 },
                { "type": "integer", "minimum": 1, "maximum": 9007199254740991_u64 }
            ],
            "description": "Exact browser tab handle; accepts the numeric handle returned by resolve_tab or a legacy string id. Omitted means active/default tab; an invalid supplied handle is rejected."
        }),
    );
    props.insert(
        "browserId".to_string(),
        string_prop(
            "Which browser runs this call, from browser_list_connected_browsers \
             (the built-in browser, or the user's connected Chrome). Omitted means \
             the browser this session selected, which defaults to the built-in one. \
             Set it to run one call in a different browser without switching the \
             session",
        ),
    );
    props.insert(
        "timeoutMs".to_string(),
        integer_prop("Operation timeout in milliseconds"),
    );
    props
}

fn object_schema(props: Map<String, Value>, required: &[&str]) -> JsonSchema {
    let mut schema = Map::new();
    schema.insert("type".to_string(), json!("object"));
    schema.insert("properties".to_string(), Value::Object(props));
    if !required.is_empty() {
        schema.insert("required".to_string(), json!(required));
    }
    schema.insert("additionalProperties".to_string(), json!(false));
    schema
}

fn string_prop(description: &str) -> Value {
    json!({ "type": "string", "description": description })
}

fn bool_prop(description: &str) -> Value {
    json!({ "type": "boolean", "description": description })
}

fn integer_prop(description: &str) -> Value {
    json!({
        "type": "integer",
        "minimum": 0,
        "maximum": 9007199254740991_i64,
        "description": description,
    })
}

fn number_prop(description: &str) -> Value {
    json!({ "type": "number", "description": description })
}

fn number_tuple_prop(description: &str) -> Value {
    json!({
        "type": "array",
        "items": { "type": "number" },
        "minItems": 2,
        "maxItems": 2,
        "description": description,
    })
}

fn enum_prop(values: &[&str], description: &str) -> Value {
    json!({ "type": "string", "enum": values, "description": description })
}

fn array_string_prop(description: &str) -> Value {
    json!({
        "type": "array",
        "items": { "type": "string" },
        "description": description,
    })
}

fn array_enum_prop(values: &[&str], description: &str) -> Value {
    json!({
        "type": "array",
        "items": { "type": "string", "enum": values },
        "description": description,
    })
}

fn viewport_prop() -> Value {
    json!({
        "type": "object",
        "properties": {
            "width": { "type": "integer", "minimum": 1 },
            "height": { "type": "integer", "minimum": 1 },
            "deviceScaleFactor": { "type": "integer", "minimum": 1 },
            "isMobile": { "type": "boolean" }
        },
        "required": ["width", "height"],
        "additionalProperties": false,
        "description": "Optional viewport override"
    })
}

fn runtime_string_is_one_of(value: &RuntimeString, expected: &[&str]) -> bool {
    expected.iter().any(|expected| value == *expected)
}

fn unsupported_value(prefix: &str, value: &RuntimeString) -> RuntimeString {
    RuntimeString::from(prefix)
        .concat(value)
        .concat(&RuntimeString::from("`."))
}

fn read_string(input: &RuntimeJson, key: &str) -> Option<RuntimeString> {
    input
        .get(key)
        .and_then(RuntimeJson::as_str)
        .filter(|value| !value.is_empty())
        .cloned()
}

fn read_browser_tab_id(input: &RuntimeJson) -> Result<Option<RuntimeString>, RuntimeString> {
    let Some(value) = input.get("tabId") else {
        return Ok(None);
    };
    if let RuntimeJson::String(id) = value {
        if id
            .try_to_utf8()
            .ok()
            .is_some_and(|text| !text.trim().is_empty())
        {
            return Ok(Some(id.clone()));
        }
    } else if let Some(id) = value
        .as_u64()
        .filter(|id| *id > 0 && *id <= 9_007_199_254_740_991)
    {
        return Ok(Some(id.to_string().into()));
    }
    Err("`tabId` must be a non-empty string id or a positive safe integer; no default tab was selected.".into())
}

fn required_string(input: &RuntimeJson, key: &str) -> Result<RuntimeString, RuntimeString> {
    read_string(input, key)
        .ok_or_else(|| RuntimeString::from(format!("Missing required field `{key}`.")))
}

fn read_permission_action(
    input: &RuntimeJson,
    key: &str,
) -> Result<Option<BrowserPermissionAction>, RuntimeString> {
    read_optional_enum(
        input,
        key,
        &[
            ("grant", BrowserPermissionAction::Grant),
            ("set", BrowserPermissionAction::Set),
            ("reset", BrowserPermissionAction::Reset),
        ],
        "Unsupported permission action `",
    )
}

fn read_permission_setting(
    input: &RuntimeJson,
    key: &str,
) -> Result<Option<BrowserPermissionSetting>, RuntimeString> {
    read_optional_enum(
        input,
        key,
        &[
            ("granted", BrowserPermissionSetting::Granted),
            ("denied", BrowserPermissionSetting::Denied),
            ("prompt", BrowserPermissionSetting::Prompt),
        ],
        "Unsupported permission setting `",
    )
}

fn read_bool(input: &RuntimeJson, key: &str, default: bool) -> bool {
    input
        .get(key)
        .and_then(RuntimeJson::as_bool)
        .unwrap_or(default)
}

fn read_u64(input: &RuntimeJson, key: &str) -> Option<u64> {
    input.get(key).and_then(RuntimeJson::as_u64)
}

fn read_f64(input: &RuntimeJson, key: &str) -> Option<f64> {
    input.get(key).and_then(RuntimeJson::as_f64)
}

fn read_string_array(input: &RuntimeJson, key: &str) -> Option<Vec<RuntimeString>> {
    input.get(key).and_then(RuntimeJson::as_array).map(|items| {
        items
            .iter()
            .filter_map(RuntimeJson::as_str)
            .cloned()
            .collect()
    })
}

fn required_string_array(
    input: &RuntimeJson,
    key: &str,
) -> Result<Vec<RuntimeString>, RuntimeString> {
    let Some(items) = read_string_array(input, key) else {
        return Err(format!("Missing required field `{key}`.").into());
    };
    if items.is_empty() {
        return Err(format!("`{key}` must contain at least one string.").into());
    }
    Ok(items)
}

fn read_number_array(input: &RuntimeJson, key: &str) -> Result<Option<Vec<f64>>, RuntimeString> {
    let Some(value) = input.get(key) else {
        return Ok(None);
    };
    let Some(items) = value.as_array() else {
        return Err(format!("`{key}` must be an array.").into());
    };
    if items.len() != 2 {
        return Err(format!("`{key}` must contain exactly two numbers.").into());
    }
    let mut out = Vec::with_capacity(2);
    for item in items {
        let Some(value) = item.as_f64() else {
            return Err(format!("`{key}` must contain only numbers.").into());
        };
        out.push(value);
    }
    Ok(Some(out))
}

fn read_viewport(input: &RuntimeJson, key: &str) -> Result<Option<BrowserViewport>, RuntimeString> {
    let Some(v) = input.get(key) else {
        return Ok(None);
    };
    let Some(obj) = v.as_object() else {
        return Err(format!("`{key}` must be an object.").into());
    };
    let width = obj
        .get("width")
        .and_then(RuntimeJson::as_u64)
        .ok_or_else(|| RuntimeString::from("`viewport.width` is required."))?;
    let height = obj
        .get("height")
        .and_then(RuntimeJson::as_u64)
        .ok_or_else(|| RuntimeString::from("`viewport.height` is required."))?;
    Ok(Some(BrowserViewport {
        width: width.min(u32::MAX as u64) as u32,
        height: height.min(u32::MAX as u64) as u32,
        device_scale_factor: obj
            .get("deviceScaleFactor")
            .and_then(RuntimeJson::as_u64)
            .map(|v| v.min(u32::MAX as u64) as u32),
        is_mobile: obj.get("isMobile").and_then(RuntimeJson::as_bool),
    }))
}

fn read_optional_enum<T: Copy>(
    input: &RuntimeJson,
    key: &str,
    variants: &[(&str, T)],
    error_prefix: &str,
) -> Result<Option<T>, RuntimeString> {
    let Some(value) = read_string(input, key) else {
        return Ok(None);
    };
    variants
        .iter()
        .find_map(|(name, variant)| (value == *name).then_some(*variant))
        .map(Some)
        .ok_or_else(|| unsupported_value(error_prefix, &value))
}

fn read_wait_until(
    input: &RuntimeJson,
    key: &str,
) -> Result<Option<BrowserWaitUntil>, RuntimeString> {
    read_optional_enum(
        input,
        key,
        &[
            ("commit", BrowserWaitUntil::Commit),
            ("dom_content_loaded", BrowserWaitUntil::DomContentLoaded),
            ("load", BrowserWaitUntil::Load),
            ("network_idle", BrowserWaitUntil::NetworkIdle),
        ],
        "Unsupported waitUntil value `",
    )
}

fn read_screenshot_format(
    input: &RuntimeJson,
    key: &str,
) -> Result<Option<BrowserScreenshotFormat>, RuntimeString> {
    read_optional_enum(
        input,
        key,
        &[
            ("png", BrowserScreenshotFormat::Png),
            ("jpeg", BrowserScreenshotFormat::Jpeg),
        ],
        "Unsupported screenshot format `",
    )
}

fn read_mouse_button(
    input: &RuntimeJson,
    key: &str,
) -> Result<Option<BrowserMouseButton>, RuntimeString> {
    read_optional_enum(
        input,
        key,
        &[
            ("left", BrowserMouseButton::Left),
            ("middle", BrowserMouseButton::Middle),
            ("right", BrowserMouseButton::Right),
        ],
        "Unsupported mouse button `",
    )
}

fn read_console_levels(
    input: &RuntimeJson,
    key: &str,
) -> Result<Option<Vec<BrowserConsoleLevel>>, RuntimeString> {
    let Some(items) = read_string_array(input, key) else {
        return Ok(None);
    };
    let mut out = Vec::new();
    for item in items {
        let level = [
            ("debug", BrowserConsoleLevel::Debug),
            ("info", BrowserConsoleLevel::Info),
            ("warning", BrowserConsoleLevel::Warning),
            ("error", BrowserConsoleLevel::Error),
            ("log", BrowserConsoleLevel::Log),
        ]
        .into_iter()
        .find_map(|(name, level)| (item == name).then_some(level))
        .ok_or_else(|| unsupported_value("Unsupported console level `", &item))?;
        out.push(level);
    }
    Ok(Some(out))
}

fn read_close_target(
    input: &RuntimeJson,
    key: &str,
) -> Result<Option<BrowserCloseTarget>, RuntimeString> {
    read_optional_enum(
        input,
        key,
        &[
            ("tab", BrowserCloseTarget::Tab),
            ("session", BrowserCloseTarget::Session),
            ("all", BrowserCloseTarget::All),
        ],
        "Unsupported close target `",
    )
}

fn read_clipboard_action(
    input: &RuntimeJson,
    key: &str,
) -> Result<BrowserClipboardAction, RuntimeString> {
    Ok(read_optional_enum(
        input,
        key,
        &[
            ("read_text", BrowserClipboardAction::ReadText),
            ("read", BrowserClipboardAction::Read),
            ("write", BrowserClipboardAction::Write),
            ("write_text", BrowserClipboardAction::WriteText),
        ],
        "Unsupported clipboard action `",
    )?
    .unwrap_or(BrowserClipboardAction::ReadText))
}

fn read_dialog_action(
    input: &RuntimeJson,
    key: &str,
) -> Result<BrowserDialogAction, RuntimeString> {
    Ok(read_optional_enum(
        input,
        key,
        &[
            ("get", BrowserDialogAction::Get),
            ("accept", BrowserDialogAction::Accept),
            ("dismiss", BrowserDialogAction::Dismiss),
        ],
        "Unsupported dialog action `",
    )?
    .unwrap_or(BrowserDialogAction::Get))
}

fn read_tab_mark_status(
    input: &RuntimeJson,
    key: &str,
) -> Result<BrowserTabMarkStatus, RuntimeString> {
    Ok(read_optional_enum(
        input,
        key,
        &[
            ("handoff", BrowserTabMarkStatus::Handoff),
            ("deliverable", BrowserTabMarkStatus::Deliverable),
        ],
        "Unsupported tab mark status `",
    )?
    .unwrap_or(BrowserTabMarkStatus::Handoff))
}

fn read_read_page_filter(
    input: &RuntimeJson,
    key: &str,
) -> Result<Option<BrowserReadPageFilter>, RuntimeString> {
    read_optional_enum(
        input,
        key,
        &[
            ("interactive", BrowserReadPageFilter::Interactive),
            ("all", BrowserReadPageFilter::All),
        ],
        "Unsupported read page filter `",
    )
}

fn read_computer_action(
    input: &RuntimeJson,
    key: &str,
) -> Result<Option<BrowserComputerAction>, RuntimeString> {
    read_optional_enum(
        input,
        key,
        &[
            ("left_click", BrowserComputerAction::LeftClick),
            ("right_click", BrowserComputerAction::RightClick),
            ("double_click", BrowserComputerAction::DoubleClick),
            ("triple_click", BrowserComputerAction::TripleClick),
            ("type", BrowserComputerAction::Type),
            ("screenshot", BrowserComputerAction::Screenshot),
            ("wait", BrowserComputerAction::Wait),
            ("scroll", BrowserComputerAction::Scroll),
            ("key", BrowserComputerAction::Key),
            ("left_click_drag", BrowserComputerAction::LeftClickDrag),
            ("zoom", BrowserComputerAction::Zoom),
            ("scroll_to", BrowserComputerAction::ScrollTo),
            ("hover", BrowserComputerAction::Hover),
        ],
        "Unsupported computer action `",
    )
}

fn read_scroll_direction(
    input: &RuntimeJson,
    key: &str,
) -> Result<Option<BrowserScrollDirection>, RuntimeString> {
    read_optional_enum(
        input,
        key,
        &[
            ("up", BrowserScrollDirection::Up),
            ("down", BrowserScrollDirection::Down),
            ("left", BrowserScrollDirection::Left),
            ("right", BrowserScrollDirection::Right),
        ],
        "Unsupported scroll direction `",
    )
}

fn read_tabs_action(
    input: &RuntimeJson,
    key: &str,
) -> Result<Option<BrowserTabsAction>, RuntimeString> {
    read_optional_enum(
        input,
        key,
        &[
            ("list", BrowserTabsAction::List),
            ("user", BrowserTabsAction::User),
            ("new", BrowserTabsAction::New),
            ("get", BrowserTabsAction::Get),
            ("selected", BrowserTabsAction::Selected),
            ("close", BrowserTabsAction::Close),
            ("finalize", BrowserTabsAction::Finalize),
            ("name_session", BrowserTabsAction::NameSession),
        ],
        "Unsupported tabs action `",
    )
}

/// WHATWG URL input is a Web-IDL `USVString`: valid scalar values are retained
/// and each unpaired UTF-16 surrogate becomes U+FFFD at this terminal parser
/// boundary. The BrowserOperation itself keeps the original RuntimeString.
fn browser_url_to_web_idl_usv_string(value: &RuntimeString) -> String {
    value
        .code_points()
        .map(|point| char::from_u32(point).unwrap_or(char::REPLACEMENT_CHARACTER))
        .collect()
}

fn navigation_urls(operation: &BrowserOperation) -> Result<Vec<RuntimeString>, RuntimeString> {
    let mut urls = Vec::new();
    match operation {
        BrowserOperation::Navigate { url, .. } => urls.push(url.clone()),
        BrowserOperation::Tabs {
            action: BrowserTabsAction::New,
            url: Some(url),
            ..
        } => urls.push(url.clone()),
        BrowserOperation::TabsContent {
            urls: content_urls, ..
        } => urls.extend(content_urls.iter().cloned()),
        BrowserOperation::Download { url: Some(url), .. } => urls.push(url.clone()),
        BrowserOperation::Batch { actions } => collect_batch_navigation_urls(actions, &mut urls)?,
        _ => {}
    }
    Ok(urls)
}

fn collect_batch_navigation_urls(
    actions: &[RuntimeJson],
    urls: &mut Vec<RuntimeString>,
) -> Result<(), RuntimeString> {
    let mut pending: Vec<(&RuntimeJson, String)> = actions
        .iter()
        .enumerate()
        .rev()
        .map(|(index, action)| (action, format!("actions[{index}]")))
        .collect();
    while let Some((action, path)) = pending.pop() {
        let Some(object) = action.as_object() else {
            return Err(format!("Browser batch {path} must be an object.").into());
        };
        let Some(kind) = object.get("type").and_then(RuntimeJson::as_str) else {
            return Err(format!("Browser batch {path}.type must be a string.").into());
        };
        if kind == "navigate" {
            urls.push(batch_string_field(object, "url", &path)?.clone());
        } else if kind == "tabs"
            && object
                .get("action")
                .and_then(RuntimeJson::as_str)
                .is_some_and(|action| action == "new")
        {
            if let Some(url) = batch_optional_string_field(object, "url", &path)? {
                urls.push(url.clone());
            }
        } else if kind == "tabs_content" {
            let Some(items) = object.get("urls").and_then(RuntimeJson::as_array) else {
                return Err(format!("Browser batch {path}.urls must be an array.").into());
            };
            if items.is_empty() {
                return Err(
                    format!("Browser batch {path}.urls must contain at least one string.").into(),
                );
            }
            for (index, item) in items.iter().enumerate() {
                let Some(url) = item.as_str() else {
                    return Err(
                        format!("Browser batch {path}.urls[{index}] must be a string.").into(),
                    );
                };
                urls.push(url.clone());
            }
        } else if kind == "download" {
            if let Some(url) = batch_optional_string_field(object, "url", &path)? {
                urls.push(url.clone());
            }
        } else if kind == "batch" {
            let Some(nested) = object.get("actions").and_then(RuntimeJson::as_array) else {
                return Err(format!("Browser batch {path}.actions must be an array.").into());
            };
            pending.extend(
                nested
                    .iter()
                    .enumerate()
                    .rev()
                    .map(|(index, action)| (action, format!("{path}.actions[{index}]"))),
            );
        }
    }
    Ok(())
}

fn batch_string_field<'a>(
    object: &'a RuntimeObject,
    field: &str,
    path: &str,
) -> Result<&'a RuntimeString, RuntimeString> {
    object
        .get(field)
        .and_then(RuntimeJson::as_str)
        .ok_or_else(|| format!("Browser batch {path}.{field} must be a string.").into())
}

fn batch_optional_string_field<'a>(
    object: &'a RuntimeObject,
    field: &str,
    path: &str,
) -> Result<Option<&'a RuntimeString>, RuntimeString> {
    match object.get(field) {
        None | Some(RuntimeJson::Null) => Ok(None),
        Some(RuntimeJson::String(value)) => Ok(Some(value)),
        Some(_) => {
            Err(format!("Browser batch {path}.{field} must be a string when provided.").into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::browser::{
        attest_spawn_free, BrowserExecutor, BrowserRuntime, BrowserRuntimeEvent, BrowserSnapshot,
        BrowserWaitUntil,
    };
    use crate::schemas::content::OutputBlock;
    // Only the tests install a network-approval handler now: navigation itself no
    // longer consults one, and the tests assert exactly that.
    use crate::tools::builtins::test_ctx::make_ctx;
    use crate::types::abort::AbortSignal;
    use crate::types::tool::NetworkAccessRequest;
    use std::sync::Mutex;

    macro_rules! runtime_json {
        ($($json:tt)+) => {{
            runtime_json::parse(&RuntimeString::from(stringify!($($json)+)))
                .expect("test JSON literal must parse as exact runtime JSON")
        }};
    }

    struct RecordingRuntime {
        requests: Mutex<Vec<BrowserRuntimeRequest>>,
        response: BrowserRuntimeResponse,
        event: Option<BrowserRuntimeEvent>,
        wait_for_cancel: bool,
    }

    impl RecordingRuntime {
        fn new(response: BrowserRuntimeResponse) -> Self {
            RecordingRuntime {
                requests: Mutex::new(Vec::new()),
                response,
                event: None,
                wait_for_cancel: false,
            }
        }
    }

    fn assert_inner_json_preserves_lone_surrogate(text: &RuntimeString) {
        assert!(text.contains(r#"\ud800"#), "{text:?}");
        assert!(!text.contains("\u{fdd0}"), "{text:?}");
        let mut outer = RuntimeObject::new();
        outer.insert("text", RuntimeJson::String(text.clone()));
        let outer = runtime_json::stringify(&RuntimeJson::Object(outer));
        assert!(outer.contains(r#"\\ud800"#), "{outer:?}");
        assert!(!outer.contains("\u{fdd0}"), "{outer:?}");
    }

    #[test]
    fn json_response_text_preserves_lone_surrogate_as_inner_json_escape() {
        let lone = runtime_json::parse(&RuntimeString::from(r#""\ud800""#)).unwrap();
        let lone_text = lone.as_str().unwrap().clone();

        let mut value = RuntimeObject::new();
        value.insert("lone", lone.clone());

        let value_response = BrowserRuntimeResponse {
            value: Some(RuntimeJson::Object(value)),
            ..Default::default()
        };
        assert_inner_json_preserves_lone_surrogate(&response_text(
            BrowserToolKind::Eval,
            &value_response,
        ));

        let snapshot_response = BrowserRuntimeResponse {
            snapshot: Some(BrowserSnapshot {
                elements: vec![crate::browser::BrowserElementSnapshot {
                    selector: None,
                    role: None,
                    name: None,
                    text: Some(lone_text.clone()),
                    x: None,
                    y: None,
                    width: None,
                    height: None,
                    visible: None,
                    disabled: None,
                }],
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_inner_json_preserves_lone_surrogate(&response_text(
            BrowserToolKind::Snapshot,
            &snapshot_response,
        ));

        let fallback_response = BrowserRuntimeResponse {
            title: Some(lone_text),
            ..Default::default()
        };
        assert_inner_json_preserves_lone_surrogate(&response_text(
            BrowserToolKind::Snapshot,
            &fallback_response,
        ));
    }

    #[test]
    fn a_capture_reaches_the_model_as_an_image_it_can_actually_see() {
        let capture = |data_url: &str, extra: Option<(&str, RuntimeJson)>| {
            let mut value = RuntimeObject::new();
            value.insert(
                "dataUrl",
                RuntimeJson::String(RuntimeString::from(data_url)),
            );
            if let Some((key, entry)) = extra {
                value.insert(key, entry);
            }
            BrowserRuntimeResponse {
                url: Some(RuntimeString::from("https://shop.test/cart")),
                value: Some(RuntimeJson::Object(value)),
                ..Default::default()
            }
        };

        // A screenshot is the one answer whose POINT is to be looked at, and the
        // only channel a model can look through is an image block. Carried as text
        // the base64 was unreadable AND large enough to be spilled to disk as a
        // base64 preview, so the capture was worse than absent: it claimed success.
        let shot = success_result(
            BrowserToolKind::Screenshot,
            capture("data:image/jpeg;base64,QUJD", None),
            true,
        );
        let blocks = &shot.serializable.model_content;
        assert_eq!(
            blocks.len(),
            2,
            "a capture is a line of text plus the picture"
        );
        let OutputBlock::Image(image) = &blocks[1] else {
            panic!("the capture must ride an image block: {blocks:?}");
        };
        assert_eq!(image.mime_type, RuntimeString::from("image/jpeg"));
        assert_eq!(image.base64, RuntimeString::from("QUJD"));
        // The text says what was attached and where from, and does NOT repeat the
        // blob: text is what counts against the result budget.
        let OutputBlock::Text(text) = &blocks[0] else {
            panic!("expected a leading text block: {blocks:?}");
        };
        assert!(text.text.contains("image/jpeg"), "{:?}", text.text);
        assert!(
            text.text.contains("https://shop.test/cart"),
            "{:?}",
            text.text
        );
        assert!(
            !text.text.contains("QUJD"),
            "the base64 must not ride the text too"
        );
        // The structured output still carries the whole response, untouched.
        assert!(shot.serializable.output.get("response").is_some());

        // Anything the answer carried BESIDES the image is still reported, so
        // attaching the picture does not cost the caller the rest of the reply.
        let annotated = success_result(
            BrowserToolKind::Computer,
            capture(
                "data:image/png;base64,QQ==",
                Some(("annotated", runtime_json!({"hit": "button#buy"}))),
            ),
            true,
        );
        let OutputBlock::Text(text) = &annotated.serializable.model_content[0] else {
            panic!("expected a leading text block");
        };
        assert!(text.text.contains("button#buy"), "{:?}", text.text);

        // A `data:` URL that is not an image stays text: a pdf export rides the
        // same key and there is no document block to put it in, so inventing an
        // image block for it would hand the model a corrupt picture.
        let pdf = success_result(
            BrowserToolKind::ContentExport,
            capture("data:application/pdf;base64,QQ==", None),
            true,
        );
        assert_eq!(pdf.serializable.model_content.len(), 1);
        assert!(matches!(
            pdf.serializable.model_content[0],
            OutputBlock::Text(_)
        ));

        // An answer with no inline data is unchanged: one text block, as before.
        let plain = success_result(
            BrowserToolKind::Navigate,
            BrowserRuntimeResponse {
                url: Some(RuntimeString::from("https://shop.test/")),
                ..Default::default()
            },
            true,
        );
        assert_eq!(plain.serializable.model_content.len(), 1);
    }

    #[test]
    fn browser_request_and_batch_traversal_preserve_exact_runtime_strings() {
        let url = RuntimeString::from("https://example.test/")
            .concat(&RuntimeString::from_utf16(vec![0xd800]));
        let mut navigate_input = RuntimeObject::new();
        navigate_input.insert("url", RuntimeJson::String(url.clone()));
        let (request, _) = BrowserTool::new(BrowserToolKind::Navigate)
            .build_request(&RuntimeJson::Object(navigate_input))
            .unwrap();
        let BrowserOperation::Navigate {
            url: request_url, ..
        } = &request.operation
        else {
            panic!("expected navigate operation")
        };
        assert_eq!(request_url, &url);
        assert_eq!(request_url.as_utf16().last(), Some(&0xd800));

        let mut action = RuntimeObject::new();
        action.insert("type", RuntimeJson::String("navigate".into()));
        action.insert("url", RuntimeJson::String(url.clone()));
        let mut batch_input = RuntimeObject::new();
        batch_input.insert(
            "actions",
            RuntimeJson::Array(vec![RuntimeJson::Object(action)]),
        );
        let (batch_request, _) = BrowserTool::new(BrowserToolKind::Batch)
            .build_request(&RuntimeJson::Object(batch_input))
            .unwrap();
        assert_eq!(
            navigation_urls(&batch_request.operation).unwrap(),
            vec![url]
        );
    }

    #[test]
    fn browser_success_and_error_results_encode_exact_runtime_values() {
        let lone = RuntimeString::from_utf16(vec![0xdc00]);
        let mut value = RuntimeObject::new();
        value.insert("lone", RuntimeJson::String(lone.clone()));
        let success = success_result(
            BrowserToolKind::Eval,
            BrowserRuntimeResponse {
                value: Some(RuntimeJson::Object(value.clone())),
                ..Default::default()
            },
            true,
        );
        assert_eq!(
            success
                .serializable
                .output
                .get("response")
                .and_then(|response| response.get("value"))
                .and_then(|value| value.get("lone"))
                .and_then(RuntimeJson::as_str),
            Some(&lone)
        );

        let error = error_result(BrowserRuntimeError {
            code: BrowserRuntimeErrorCode::HostError,
            message: lone.clone(),
            details: Some(RuntimeJson::Object(value)),
        });
        assert_eq!(
            error
                .serializable
                .output
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(RuntimeJson::as_str),
            Some(&lone)
        );
        let OutputBlock::Text(text) = &error.serializable.model_content[0] else {
            panic!("expected exact browser error text")
        };
        assert_eq!(
            text.text,
            RuntimeString::from("Browser runtime error: ").concat(&lone)
        );
        assert_eq!(text.text.as_utf16().last(), Some(&0xdc00));
    }

    #[async_trait]
    unsafe impl BrowserExecutor for RecordingRuntime {
        async fn execute(
            &self,
            request: BrowserRuntimeRequest,
            signal: AbortSignal,
            emit: BrowserEventSink,
        ) -> Result<BrowserRuntimeResponse, BrowserRuntimeError> {
            self.requests.lock().unwrap().push(request);
            if let Some(event) = &self.event {
                emit(event.clone());
            }
            if self.wait_for_cancel {
                signal.cancelled().await;
                return Err(BrowserRuntimeError::new(
                    BrowserRuntimeErrorCode::Cancelled,
                    "cancelled by test",
                ));
            }
            Ok(self.response.clone())
        }
    }

    fn spawn_free<T>(executor: Arc<T>) -> Arc<dyn BrowserRuntime>
    where
        T: BrowserExecutor + 'static,
    {
        // SAFETY: this recording executor is an in-memory test double and has
        // no local process authority.
        unsafe { attest_spawn_free(executor) }
    }

    #[tokio::test]
    async fn navigate_forwards_typed_request_and_browser_events() {
        let runtime = Arc::new(RecordingRuntime {
            event: Some(BrowserRuntimeEvent::NavigationStarted {
                url: "https://example.com".into(),
                session_id: Some("s1".into()),
                tab_id: Some("t1".into()),
            }),
            ..RecordingRuntime::new(BrowserRuntimeResponse {
                session_id: Some("s1".into()),
                tab_id: Some("t1".into()),
                url: Some("https://example.com".into()),
                title: Some("Example".into()),
                ..Default::default()
            })
        });
        let mut ctx = make_ctx("/tmp");
        ctx.browser = Some(spawn_free(runtime.clone()));
        let events = Arc::new(Mutex::new(Vec::new()));
        let seen = events.clone();
        ctx.emit = std::sync::Arc::new(move |event| {
            seen.lock().unwrap().push(event);
            Box::pin(async {})
        });

        let tool = BrowserTool::new(BrowserToolKind::Navigate);
        let result = tool
            .call(
                runtime_json!({
                    "url": "https://example.com",
                    "sessionId": "s1",
                    "tabId": "t1",
                    "waitUntil": "dom_content_loaded",
                    "timeoutMs": 5000
                }),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(result.serializable.is_error, Some(false));
        let requests = runtime.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].target.session_id, Some("s1".into()));
        assert_eq!(requests[0].target.tab_id, Some("t1".into()));
        assert!(requests[0].permissions.navigate);
        assert!(requests[0].permissions.read_page);
        assert_eq!(requests[0].timeout_ms, Some(5000));
        match &requests[0].operation {
            BrowserOperation::Navigate {
                url, wait_until, ..
            } => {
                assert_eq!(url, "https://example.com");
                assert_eq!(*wait_until, Some(BrowserWaitUntil::DomContentLoaded));
            }
            other => panic!("unexpected operation: {other:?}"),
        }
        assert!(matches!(
            events.lock().unwrap().as_slice(),
            [ToolDomainEvent::Browser {
                event: BrowserRuntimeEvent::NavigationStarted { .. }
            }]
        ));
    }

    /// Navigating must NOT ask for per-host network approval. Browsing the web is
    /// what this tool is for, so a prompt per new domain is a question with no
    /// meaningful "no": it trains people to click through every prompt and turns
    /// the approval into a rubber stamp. The consent that carries weight is granted
    /// elsewhere and stays — opening a browser is a user action, and driving the
    /// user's own Chrome requires connecting it first.
    #[tokio::test]
    async fn navigate_does_not_ask_for_per_host_network_approval() {
        let runtime = Arc::new(RecordingRuntime::new(BrowserRuntimeResponse::default()));
        let asked = Arc::new(Mutex::new(Vec::new()));
        let mut ctx = make_ctx("/tmp");
        ctx.browser = Some(spawn_free(runtime.clone()));
        // A handler that refuses everything: if navigation still consulted it, the
        // call would fail and nothing would reach the runtime.
        ctx.request_network_access = Some(Arc::new({
            let asked = asked.clone();
            move |req: NetworkAccessRequest| {
                asked.lock().unwrap().push(req.url);
                Box::pin(async { false })
            }
        }));

        let tool = BrowserTool::new(BrowserToolKind::Navigate);
        let result = tool
            .call(runtime_json!({ "url": "https://example.com" }), &ctx, None)
            .await
            .unwrap();

        assert_eq!(result.serializable.is_error, Some(false));
        assert!(
            asked.lock().unwrap().is_empty(),
            "navigation must not consult the network-approval handler"
        );
        assert_eq!(runtime.requests.lock().unwrap().len(), 1);
    }

    /// Dropping the prompt must not drop URL validation: a scheme the browser has
    /// no business opening is still refused, and refused before the runtime runs.
    #[tokio::test]
    async fn navigate_still_refuses_a_non_http_scheme_before_the_runtime() {
        let runtime = Arc::new(RecordingRuntime::new(BrowserRuntimeResponse::default()));
        let mut ctx = make_ctx("/tmp");
        ctx.browser = Some(spawn_free(runtime.clone()));

        let tool = BrowserTool::new(BrowserToolKind::Navigate);
        // Written out one by one because the runtime-JSON literal takes no variable
        // interpolation.
        let file_url = tool
            .call(runtime_json!({ "url": "file:///etc/passwd" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(
            file_url.serializable.is_error,
            Some(true),
            "file: must be refused"
        );
        let js_url = tool
            .call(runtime_json!({ "url": "javascript:alert(1)" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(
            js_url.serializable.is_error,
            Some(true),
            "javascript: must be refused"
        );
        assert!(runtime.requests.lock().unwrap().is_empty());
    }

    /// Multi-URL operations are not a loophole either: they run without asking per
    /// host.
    #[tokio::test]
    async fn tabs_content_runs_without_asking_per_url() {
        let runtime = Arc::new(RecordingRuntime::new(BrowserRuntimeResponse::default()));
        let asked = Arc::new(Mutex::new(Vec::new()));
        let mut ctx = make_ctx("/tmp");
        ctx.browser = Some(spawn_free(runtime.clone()));
        ctx.request_network_access = Some(Arc::new({
            let asked = asked.clone();
            move |req: NetworkAccessRequest| {
                asked.lock().unwrap().push(req.url);
                Box::pin(async { true })
            }
        }));

        let tool = BrowserTool::new(BrowserToolKind::TabsContent);
        let result = tool
            .call(
                runtime_json!({
                    "urls": [
                        "https://one.example/a",
                        "http://two.example:8080/b",
                        "https://three.example/c"
                    ]
                }),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(result.serializable.is_error, Some(false));
        assert!(
            asked.lock().unwrap().is_empty(),
            "no per-URL network prompt"
        );
        assert_eq!(runtime.requests.lock().unwrap().len(), 1);
    }

    /// A batch runs its nested navigations without prompting, and one bad scheme
    /// anywhere in the tree still blocks the whole batch before the runtime sees it.
    #[tokio::test]
    async fn batch_runs_without_prompting_and_a_bad_nested_scheme_still_blocks_it() {
        let runtime = Arc::new(RecordingRuntime::new(BrowserRuntimeResponse::default()));
        let asked = Arc::new(Mutex::new(Vec::new()));
        let mut ctx = make_ctx("/tmp");
        ctx.browser = Some(spawn_free(runtime.clone()));
        ctx.request_network_access = Some(Arc::new({
            let asked = asked.clone();
            move |req: NetworkAccessRequest| {
                asked.lock().unwrap().push(req.url);
                Box::pin(async { true })
            }
        }));

        let tool = BrowserTool::new(BrowserToolKind::Batch);
        let allowed = tool
            .call(
                runtime_json!({
                    "actions": [
                        { "type": "navigate", "url": "https://one.example/a" },
                        {
                            "type": "tabs_content",
                            "urls": ["https://two.example/b", "https://three.example/c"]
                        },
                        {
                            "type": "batch",
                            "actions": [
                                {
                                    "type": "tabs",
                                    "action": "new",
                                    "url": "http://four.example:8080/d"
                                }
                            ]
                        }
                    ]
                }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(allowed.serializable.is_error, Some(false));
        assert!(
            asked.lock().unwrap().is_empty(),
            "a batch must not prompt per host"
        );

        // A file: URL buried in a nested batch still refuses the whole call.
        let refused = tool
            .call(
                runtime_json!({
                    "actions": [
                        { "type": "navigate", "url": "https://one.example/a" },
                        {
                            "type": "batch",
                            "actions": [
                                { "type": "download", "url": "file:///etc/passwd" }
                            ]
                        }
                    ]
                }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(refused.serializable.is_error, Some(true));
    }

    #[tokio::test]
    async fn nested_invalid_url_is_rejected_before_any_approval_or_runtime() {
        let runtime = Arc::new(RecordingRuntime::new(BrowserRuntimeResponse::default()));
        let approvals = Arc::new(Mutex::new(Vec::new()));
        let mut ctx = make_ctx("/tmp");
        ctx.browser = Some(spawn_free(runtime.clone()));
        ctx.request_network_access = Some(Arc::new({
            let approvals = approvals.clone();
            move |req: NetworkAccessRequest| {
                approvals.lock().unwrap().push(req.url);
                Box::pin(async { true })
            }
        }));

        let tool = BrowserTool::new(BrowserToolKind::Batch);
        let result = tool
            .call(
                runtime_json!({
                    "actions": [{
                        "type": "tabs_content",
                        "urls": ["https://valid.example/a", "file:///private/etc/passwd"]
                    }]
                }),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(result.serializable.is_error, Some(true));
        assert!(approvals.lock().unwrap().is_empty());
        assert!(runtime.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn snapshot_returns_text_from_runtime_response() {
        let runtime = Arc::new(RecordingRuntime::new(BrowserRuntimeResponse {
            snapshot: Some(BrowserSnapshot {
                text: Some("Rendered page text".into()),
                ..Default::default()
            }),
            ..Default::default()
        }));
        let mut ctx = make_ctx("/tmp");
        ctx.browser = Some(spawn_free(runtime));

        let tool = BrowserTool::new(BrowserToolKind::Snapshot);
        let result = tool.call(runtime_json!({}), &ctx, None).await.unwrap();

        assert_eq!(result.serializable.is_error, Some(false));
        match &result.serializable.model_content[0] {
            OutputBlock::Text(text) => assert_eq!(text.text, "Rendered page text"),
            other => panic!("expected text output, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn eval_is_not_read_only_and_asks_for_permission() {
        let ctx = make_ctx("/tmp");
        let tool = BrowserTool::new(BrowserToolKind::Eval);

        assert!(!tool.is_read_only(&runtime_json!({ "script": "1 + 1" })));
        assert!(matches!(
            tool.check_permissions(&runtime_json!({ "script": "1 + 1" }), &ctx)
                .await,
            Some(PermissionResult::Ask { .. })
        ));
    }

    #[tokio::test]
    async fn close_is_not_read_only_and_asks_for_permission() {
        let ctx = make_ctx("/tmp");
        let tool = BrowserTool::new(BrowserToolKind::Close);

        assert!(!tool.is_read_only(&runtime_json!({ "target": "tab" })));
        assert!(matches!(
            tool.check_permissions(&runtime_json!({ "target": "tab" }), &ctx)
                .await,
            Some(PermissionResult::Ask { .. })
        ));
    }

    #[tokio::test]
    async fn click_requires_selector_or_coordinates_before_runtime_call() {
        let runtime = Arc::new(RecordingRuntime::new(BrowserRuntimeResponse::default()));
        let mut ctx = make_ctx("/tmp");
        ctx.browser = Some(spawn_free(runtime.clone()));

        let tool = BrowserTool::new(BrowserToolKind::Click);
        let result = tool.call(runtime_json!({}), &ctx, None).await.unwrap();

        assert_eq!(result.serializable.is_error, Some(true));
        assert!(runtime.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn runtime_timeout_is_reported_and_cancels_call_signal() {
        let runtime = Arc::new(RecordingRuntime {
            wait_for_cancel: true,
            ..RecordingRuntime::new(BrowserRuntimeResponse::default())
        });
        let mut ctx = make_ctx("/tmp");
        ctx.browser = Some(spawn_free(runtime));

        let tool = BrowserTool::new(BrowserToolKind::Snapshot);
        let result = tool
            .call(runtime_json!({ "timeoutMs": 1 }), &ctx, None)
            .await
            .unwrap();

        assert_eq!(result.serializable.is_error, Some(true));
        assert_eq!(
            result.serializable.output["error"]["code"],
            runtime_json!("timeout")
        );
    }

    #[tokio::test]
    async fn rich_browser_tools_forward_structured_requests() {
        let runtime = Arc::new(RecordingRuntime::new(BrowserRuntimeResponse {
            value: Some(runtime_json!({ "ok": true })),
            ..Default::default()
        }));
        let mut ctx = make_ctx("/tmp");
        ctx.browser = Some(spawn_free(runtime.clone()));

        BrowserTool::new(BrowserToolKind::ReadPage)
            .call(
                runtime_json!({
                    "filter": "interactive",
                    "depth": 3,
                    "refId": "ref_1",
                    "maxChars": 2000
                }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        BrowserTool::new(BrowserToolKind::Find)
            .call(
                runtime_json!({ "query": "submit button", "limit": 5 }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        BrowserTool::new(BrowserToolKind::FormInput)
            .call(
                runtime_json!({ "ref": "ref_2", "value": "Ada" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        BrowserTool::new(BrowserToolKind::Computer)
            .call(
                runtime_json!({
                    "action": "left_click",
                    "coordinate": [12, 34]
                }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        BrowserTool::new(BrowserToolKind::Tabs)
            .call(runtime_json!({ "action": "new" }), &ctx, None)
            .await
            .unwrap();
        BrowserTool::new(BrowserToolKind::Locator)
            .call(
                runtime_json!({
                    "selector": "#name",
                    "action": "fill",
                    "value": "Grace"
                }),
                &ctx,
                None,
            )
            .await
            .unwrap();

        let requests = runtime.requests.lock().unwrap();
        assert!(matches!(
            requests[0].operation,
            BrowserOperation::ReadPage {
                filter: Some(crate::browser::BrowserReadPageFilter::Interactive),
                depth: Some(3),
                ..
            }
        ));
        assert!(matches!(
            requests[1].operation,
            BrowserOperation::Find { ref query, limit: Some(5) }
                if query == "submit button"
        ));
        assert!(matches!(
            requests[2].operation,
            BrowserOperation::FormInput { ref reference, .. } if reference == "ref_2"
        ));
        assert!(matches!(
            requests[3].operation,
            BrowserOperation::Computer {
                action: crate::browser::BrowserComputerAction::LeftClick,
                ..
            }
        ));
        assert!(matches!(
            requests[4].operation,
            BrowserOperation::Tabs {
                action: crate::browser::BrowserTabsAction::New,
                ..
            }
        ));
        assert!(matches!(
            requests[5].operation,
            BrowserOperation::Locator { ref selector, ref action, .. }
                if selector == "#name" && action == "fill"
        ));
    }

    #[tokio::test]
    async fn read_only_status_tracks_rich_browser_action_inputs() {
        let ctx = make_ctx("/tmp");
        assert!(BrowserTool::new(BrowserToolKind::ReadPage).is_read_only(&runtime_json!({})));
        assert!(BrowserTool::new(BrowserToolKind::Find)
            .is_read_only(&runtime_json!({ "query": "search" })));
        assert!(BrowserTool::new(BrowserToolKind::Computer)
            .is_read_only(&runtime_json!({ "action": "screenshot" })));
        assert!(!BrowserTool::new(BrowserToolKind::Computer)
            .is_read_only(&runtime_json!({ "action": "left_click", "coordinate": [1, 2] })));
        assert!(BrowserTool::new(BrowserToolKind::Locator)
            .is_read_only(&runtime_json!({ "selector": "button", "action": "count" })));
        assert!(!BrowserTool::new(BrowserToolKind::Locator)
            .is_read_only(&runtime_json!({ "selector": "button", "action": "click" })));

        assert!(matches!(
            BrowserTool::new(BrowserToolKind::Computer)
                .check_permissions(
                    &runtime_json!({ "action": "left_click", "coordinate": [1, 2] }),
                    &ctx
                )
                .await,
            Some(PermissionResult::Ask { .. })
        ));
    }

    /// Semantic element names reach the host through the locator options contract.
    #[tokio::test]
    async fn a_locator_may_name_an_element_without_a_css_selector() {
        for key in LOCATOR_NAME_KEYS {
            let runtime = Arc::new(RecordingRuntime::new(BrowserRuntimeResponse {
                value: Some(runtime_json!({ "ok": true })),
                ..Default::default()
            }));
            let mut ctx = make_ctx("/tmp");
            ctx.browser = Some(spawn_free(runtime.clone()));
            let mut input = RuntimeObject::new();
            input.insert("action".to_string(), runtime_json!("count"));
            input.insert((*key).to_string(), runtime_json!("Save"));
            BrowserTool::new(BrowserToolKind::Locator)
                .call(RuntimeJson::Object(input), &ctx, None)
                .await
                .unwrap_or_else(|error| panic!("locator by {key}: {error:?}"));
            let requests = runtime.requests.lock().unwrap();
            assert_eq!(requests.len(), 1, "{key}");
            match &requests[0].operation {
                BrowserOperation::Locator {
                    selector, options, ..
                } => {
                    assert!(
                        selector.is_empty(),
                        "{key}: a selector was invented: {selector:?}"
                    );
                    assert_eq!(
                        options.get(*key).and_then(RuntimeJson::as_str),
                        Some(&RuntimeString::from("Save")),
                        "{key} did not reach the runtime: {options:?}"
                    );
                }
                other => panic!("{key}: wrong operation {other:?}"),
            }
        }

        // A selector still works on its own, and naming NOTHING is refused by name
        // rather than being sent as a locator that matches the whole page.
        let runtime = Arc::new(RecordingRuntime::new(BrowserRuntimeResponse {
            value: Some(runtime_json!({ "ok": true })),
            ..Default::default()
        }));
        let mut ctx = make_ctx("/tmp");
        ctx.browser = Some(spawn_free(runtime.clone()));
        let result = BrowserTool::new(BrowserToolKind::Locator)
            .call(runtime_json!({ "action": "count" }), &ctx, None)
            .await
            .expect("an answered failure");
        let text = format!("{:?}", result.serializable);
        assert!(text.contains("Name the element"), "{text}");
        assert!(
            runtime.requests.lock().unwrap().is_empty(),
            "a nameless locator was sent anyway"
        );
    }

    /// The fields that make an action POSSIBLE reach the runtime.
    ///
    /// Each of these is a capability the host implements and the engine's typed
    /// operation had no room for, so it was unreachable through the tool the model
    /// reaches for first — a narrower door in front of a wider room. They are not
    /// conveniences: without `force` an agent facing a control its own page has made
    /// unreachable has no move left; without `keys` a key action falls back to Enter,
    /// so select-all and paste cannot be asked for; without `path` a drag is a jump
    /// the followed-pointer controls ignore; without `reset` the user's own tab keeps
    /// whatever size it was narrowed to; without `waitOnly` the click-then-wait shape
    /// cannot say "do not start anything".
    #[tokio::test]
    async fn the_fields_that_make_an_action_possible_reach_the_runtime() {
        async fn sent(kind: BrowserToolKind, input: RuntimeJson) -> BrowserOperation {
            let runtime = Arc::new(RecordingRuntime::new(BrowserRuntimeResponse {
                value: Some(runtime_json!({ "ok": true })),
                ..Default::default()
            }));
            let mut ctx = make_ctx("/tmp");
            ctx.browser = Some(spawn_free(runtime.clone()));
            BrowserTool::new(kind)
                .call(input, &ctx, None)
                .await
                .unwrap_or_else(|error| panic!("{kind:?}: {error:?}"));
            let requests = runtime.requests.lock().unwrap();
            assert_eq!(requests.len(), 1, "{kind:?}");
            requests[0].operation.clone()
        }

        match sent(
            BrowserToolKind::Click,
            runtime_json!({ "selector": "#pay", "force": true }),
        )
        .await
        {
            BrowserOperation::Click { force, .. } => {
                assert!(force, "force did not reach the runtime")
            }
            other => panic!("wrong operation {other:?}"),
        }

        match sent(
            BrowserToolKind::Type,
            runtime_json!({ "selector": "#q", "text": "draft", "key": "Escape" }),
        )
        .await
        {
            BrowserOperation::Type { key, .. } => {
                assert_eq!(
                    key,
                    Some(RuntimeString::from("Escape")),
                    "key did not reach it"
                )
            }
            other => panic!("wrong operation {other:?}"),
        }

        match sent(
            BrowserToolKind::Computer,
            runtime_json!({ "action": "key", "keys": ["cmd", "a"] }),
        )
        .await
        {
            BrowserOperation::Computer { keys, .. } => assert_eq!(
                keys,
                Some(vec![RuntimeString::from("cmd"), RuntimeString::from("a")]),
                "keys did not reach it"
            ),
            other => panic!("wrong operation {other:?}"),
        }

        match sent(
            BrowserToolKind::Computer,
            runtime_json!({ "action": "left_click_drag", "coordinate": [10.0, 10.0],
                            "path": [[10.0, 10.0], [40.0, 10.0], [80.0, 10.0]] }),
        )
        .await
        {
            BrowserOperation::Computer { path, .. } => {
                let points = path.expect("path did not reach it");
                assert_eq!(
                    points.as_array().map(|list| list.len()),
                    Some(3),
                    "{points:?}"
                );
            }
            other => panic!("wrong operation {other:?}"),
        }

        match sent(
            BrowserToolKind::Computer,
            runtime_json!({ "action": "scroll", "scrollX": 220.0, "scrollY": -40.0 }),
        )
        .await
        {
            BrowserOperation::Computer {
                scroll_x, scroll_y, ..
            } => {
                assert_eq!(scroll_x, Some(220.0));
                assert_eq!(scroll_y, Some(-40.0));
            }
            other => panic!("wrong operation {other:?}"),
        }

        // `reset` needs no size, which is the point: a size was required, so putting
        // the viewport back could not be asked for at all.
        match sent(BrowserToolKind::Resize, runtime_json!({ "reset": true })).await {
            BrowserOperation::Resize { viewport, reset } => {
                assert!(reset, "reset did not reach it");
                assert!(viewport.is_none(), "a size was invented: {viewport:?}");
            }
            other => panic!("wrong operation {other:?}"),
        }

        match sent(
            BrowserToolKind::Download,
            runtime_json!({ "url": "https://example.com/a.pdf", "waitOnly": true }),
        )
        .await
        {
            BrowserOperation::Download { wait_only, .. } => {
                assert!(wait_only, "waitOnly did not reach it")
            }
            other => panic!("wrong operation {other:?}"),
        }

        // And a resize that names NEITHER is refused rather than sent as a no-op.
        let runtime = Arc::new(RecordingRuntime::new(BrowserRuntimeResponse::default()));
        let mut ctx = make_ctx("/tmp");
        ctx.browser = Some(spawn_free(runtime.clone()));
        let refused = BrowserTool::new(BrowserToolKind::Resize)
            .call(runtime_json!({}), &ctx, None)
            .await
            .expect("an answered failure");
        assert!(
            format!("{:?}", refused.serializable).contains("reset"),
            "the refusal has to name the way out: {:?}",
            refused.serializable
        );
        assert!(runtime.requests.lock().unwrap().is_empty());
    }

    /// A site's browser permissions are reachable at all.
    ///
    /// The host implements this on BOTH of its browsers and there was no tool for it
    /// here, so a permission prompt was a wall rather than a step: browsing never
    /// raises an approval the agent can answer, and `dialog` answers JavaScript
    /// dialogs, not a permission chip. Anything behind such a prompt — a map asking
    /// for location, a notification opt-in, a call asking for the camera — could not be
    /// completed through the engine's own tools at all.
    ///
    /// The operation is appended LAST in the wire enum, so every earlier variant keeps
    /// the tag it had; the golden schema diff for this change is 76 added lines and
    /// nothing moved.
    #[tokio::test]
    async fn a_sites_browser_permissions_can_be_set_at_all() {
        assert!(
            create_browser_tools()
                .iter()
                .any(|tool| tool.name() == "browser_permission"),
            "the tool has to exist for the model to reach it"
        );

        let runtime = Arc::new(RecordingRuntime::new(BrowserRuntimeResponse {
            value: Some(runtime_json!({ "ok": true })),
            ..Default::default()
        }));
        let mut ctx = make_ctx("/tmp");
        ctx.browser = Some(spawn_free(runtime.clone()));

        BrowserTool::new(BrowserToolKind::Permission)
            .call(
                runtime_json!({ "action": "grant", "origin": "https://example.com",
                                "permissions": ["geolocation", "notifications"] }),
                &ctx,
                None,
            )
            .await
            .expect("a grant is answered");
        BrowserTool::new(BrowserToolKind::Permission)
            .call(
                runtime_json!({ "action": "set", "origin": "https://example.com",
                                "permission": "camera", "setting": "denied" }),
                &ctx,
                None,
            )
            .await
            .expect("a set is answered");
        // `reset` needs no origin: clearing everything this session granted is a
        // legitimate ask, and requiring an origin for it would make the cleanup step
        // harder than the change it undoes.
        BrowserTool::new(BrowserToolKind::Permission)
            .call(runtime_json!({ "action": "reset" }), &ctx, None)
            .await
            .expect("a reset is answered");

        // The snapshot is read inside a block so the guard's SCOPE ends here:
        // an explicit `drop` frees the value but leaves the binding alive to the
        // end of the function, which still counts as holding a lock across the
        // await below.
        {
            let requests = runtime.requests.lock().unwrap();
            assert_eq!(requests.len(), 3);
            match &requests[0].operation {
                BrowserOperation::Permission {
                    action,
                    origin,
                    permissions,
                    ..
                } => {
                    assert_eq!(*action, BrowserPermissionAction::Grant);
                    assert_eq!(
                        origin.as_ref(),
                        Some(&RuntimeString::from("https://example.com"))
                    );
                    assert_eq!(
                        permissions.as_ref().map(|list| list.len()),
                        Some(2),
                        "the permission names have to travel: {permissions:?}"
                    );
                }
                other => panic!("wrong operation {other:?}"),
            }
            match &requests[1].operation {
                BrowserOperation::Permission {
                    action,
                    permission,
                    setting,
                    ..
                } => {
                    assert_eq!(*action, BrowserPermissionAction::Set);
                    assert_eq!(permission.as_ref(), Some(&RuntimeString::from("camera")));
                    assert_eq!(*setting, Some(BrowserPermissionSetting::Denied));
                }
                other => panic!("wrong operation {other:?}"),
            }
            // Changing what a browser REMEMBERS for a site outlives the call, so the
            // capability a policy withholds to prevent it has to be declared.
            assert!(
                requests[2].permissions.session_state,
                "{:?}",
                requests[2].permissions
            );
        }

        // A grant with no origin is refused rather than applied to a guess.
        let refused = BrowserTool::new(BrowserToolKind::Permission)
            .call(runtime_json!({ "action": "grant" }), &ctx, None)
            .await
            .expect("an answered failure");
        assert!(
            format!("{:?}", refused.serializable).contains("origin"),
            "{:?}",
            refused.serializable
        );
    }

    #[test]
    fn browser_tab_mentions_reuse_numeric_handles_and_reject_invalid_supplied_targets() {
        let tool = BrowserTool::new(BrowserToolKind::Snapshot);
        for (input, expected) in [
            (
                runtime_json!({"browserId":"chrome:profile-1","tabId":42}),
                "42",
            ),
            (
                runtime_json!({"browserId":"chrome:profile-1","tabId":"42"}),
                "42",
            ),
            (runtime_json!({"tabId":"tab-1"}), "tab-1"),
        ] {
            let (request, _) = tool.build_request(&input).unwrap();
            assert_eq!(
                request.target.tab_id.as_ref(),
                Some(&RuntimeString::from(expected))
            );
        }
        for input in [
            runtime_json!({"tabId":null}),
            runtime_json!({"tabId":true}),
            runtime_json!({"tabId":0}),
            runtime_json!({"tabId":-1}),
            runtime_json!({"tabId":1.5}),
            runtime_json!({"tabId":9007199254740992}),
            runtime_json!({"tabId":""}),
            runtime_json!({"tabId":"   "}),
        ] {
            assert!(
                tool.build_request(&input).is_err(),
                "invalid explicit target must not become the active tab: {input:?}"
            );
        }
        assert!(tool
            .build_request(&runtime_json!({}))
            .unwrap()
            .0
            .target
            .tab_id
            .is_none());

        let claim = BrowserTool::new(BrowserToolKind::ClaimTab);
        for input in [
            runtime_json!({"tabId":42}),
            runtime_json!({"browserId":null,"tabId":42}),
            runtime_json!({"browserId":true,"tabId":42}),
            runtime_json!({"browserId":"","tabId":42}),
            runtime_json!({"browserId":"   ","tabId":42}),
        ] {
            assert!(
                claim.build_request(&input).is_err(),
                "a claim must not default to the current browser: {input:?}"
            );
        }
    }

    #[tokio::test]
    async fn browser_tab_mentions_reach_the_host_with_identity_and_ownership_permissions() {
        let runtime = Arc::new(RecordingRuntime::new(BrowserRuntimeResponse::default()));
        let mut ctx = make_ctx("/tmp");
        ctx.browser = Some(spawn_free(runtime.clone()));
        let tools = create_browser_tools();
        for (name, input) in [
            (
                "browser_resolve_tab",
                runtime_json!({"mention":"plugin://browser@klaus-bundled?mention=tab-v1&browserId=s1&tabId=42&title=Orders&url=https%3A%2F%2Fshop.test%2Forders"}),
            ),
            (
                "browser_claim_tab",
                runtime_json!({"browserId":"chrome:profile-1","tabId":"42"}),
            ),
            (
                "browser_tabs",
                runtime_json!({"browserId":"chrome:profile-1","action":"user"}),
            ),
        ] {
            let tool = tools.iter().find(|tool| tool.name() == name).unwrap();
            tool.call(input, &ctx, None).await.unwrap();
        }
        let requests = runtime.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(matches!(
            requests[0].operation,
            BrowserOperation::ResolveTab { .. }
        ));
        assert!(requests[0].permissions.manage_tabs && requests[0].permissions.session_state);
        assert!(matches!(requests[1].operation, BrowserOperation::ClaimTab));
        assert_eq!(
            requests[1].target.browser_id.as_ref(),
            Some(&RuntimeString::from("chrome:profile-1"))
        );
        assert_eq!(
            requests[1].target.tab_id.as_ref(),
            Some(&RuntimeString::from("42"))
        );
        assert!(matches!(
            requests[2].operation,
            BrowserOperation::Tabs {
                action: BrowserTabsAction::User,
                ..
            }
        ));
    }

    #[test]
    fn browser_tools_include_chrome_mcp_parity_surface() {
        let names = create_browser_tools()
            .into_iter()
            .map(|tool| tool.name())
            .collect::<Vec<_>>();

        for expected in [
            "browser_gif_creator",
            "browser_upload_image",
            "browser_file_upload",
            "browser_shortcuts_list",
            "browser_shortcuts_execute",
            "browser_switch_browser",
            "browser_list_connected_browsers",
            "browser_select_browser",
            "browser_history",
            "browser_tabs_content",
            "browser_content_export",
            "browser_download",
            "browser_clipboard",
            "browser_dialog",
            "browser_mark_tab",
        ] {
            assert!(
                names.iter().any(|name| name == expected),
                "missing {expected}; tools: {names:?}"
            );
        }
    }

    #[tokio::test]
    async fn chrome_mcp_parity_tools_forward_structured_requests() {
        let runtime = Arc::new(RecordingRuntime::new(BrowserRuntimeResponse {
            value: Some(runtime_json!({ "ok": true })),
            ..Default::default()
        }));
        let mut ctx = make_ctx("/tmp");
        ctx.browser = Some(spawn_free(runtime.clone()));

        BrowserTool::new(BrowserToolKind::GifCreator)
            .call(
                runtime_json!({ "filename": "capture.gif", "durationSeconds": 1.5, "fps": 4 }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        BrowserTool::new(BrowserToolKind::UploadImage)
            .call(
                runtime_json!({ "path": "/tmp/image.png", "selector": "#drop" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        BrowserTool::new(BrowserToolKind::FileUpload)
            .call(
                runtime_json!({ "paths": ["/tmp/a.txt", "/tmp/b.txt"], "ref": "ref_7" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        BrowserTool::new(BrowserToolKind::ShortcutsList)
            .call(runtime_json!({ "query": "workflow" }), &ctx, None)
            .await
            .unwrap();
        BrowserTool::new(BrowserToolKind::ShortcutsExecute)
            .call(
                runtime_json!({ "name": "summarize", "waitForCompletion": true }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        BrowserTool::new(BrowserToolKind::ListConnectedBrowsers)
            .call(runtime_json!({ "localOnly": true }), &ctx, None)
            .await
            .unwrap();
        BrowserTool::new(BrowserToolKind::SelectBrowser)
            .call(runtime_json!({ "deviceId": "device-1" }), &ctx, None)
            .await
            .unwrap();
        BrowserTool::new(BrowserToolKind::SwitchBrowser)
            .call(
                runtime_json!({ "prompt": "Pick Chrome", "waitMs": 120000 }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        BrowserTool::new(BrowserToolKind::History)
            .call(
                runtime_json!({ "queries": ["docs"], "limit": 3 }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        BrowserTool::new(BrowserToolKind::TabsContent)
            .call(
                runtime_json!({ "urls": ["https://example.test/"], "contentType": "text" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        BrowserTool::new(BrowserToolKind::ContentExport)
            .call(runtime_json!({ "format": "html" }), &ctx, None)
            .await
            .unwrap();
        BrowserTool::new(BrowserToolKind::Download)
            .call(
                runtime_json!({ "url": "https://example.test/file.txt", "filename": "file.txt" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        BrowserTool::new(BrowserToolKind::Clipboard)
            .call(
                runtime_json!({ "action": "write_text", "text": "Copied" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        BrowserTool::new(BrowserToolKind::Dialog)
            .call(
                runtime_json!({ "action": "accept", "promptText": "ok" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        BrowserTool::new(BrowserToolKind::MarkTab)
            .call(runtime_json!({ "status": "deliverable" }), &ctx, None)
            .await
            .unwrap();

        let requests = runtime.requests.lock().unwrap();
        assert!(matches!(
            requests[0].operation,
            BrowserOperation::GifCreator {
                ref filename,
                duration_seconds: Some(1.5),
                fps: Some(4),
                ..
            } if filename.as_ref().is_some_and(|value| value == "capture.gif")
        ));
        assert!(matches!(
            requests[1].operation,
            BrowserOperation::UploadImage { ref path, ref selector, .. }
                if path == "/tmp/image.png"
                    && selector.as_ref().is_some_and(|value| value == "#drop")
        ));
        assert!(matches!(
            requests[2].operation,
            BrowserOperation::FileUpload { ref paths, ref ref_id, .. }
                if paths == &vec![RuntimeString::from("/tmp/a.txt"), RuntimeString::from("/tmp/b.txt")]
                    && ref_id.as_ref().is_some_and(|value| value == "ref_7")
        ));
        assert!(matches!(
            requests[3].operation,
            BrowserOperation::ShortcutsList { ref query }
                if query.as_ref().is_some_and(|value| value == "workflow")
        ));
        assert!(matches!(
            requests[4].operation,
            BrowserOperation::ShortcutsExecute { ref name, wait_for_completion: true }
                if name == "summarize"
        ));
        assert!(matches!(
            requests[5].operation,
            BrowserOperation::ListConnectedBrowsers { local_only: true }
        ));
        assert!(matches!(
            requests[6].operation,
            BrowserOperation::SelectBrowser { ref device_id } if device_id == "device-1"
        ));
        assert!(matches!(
            requests[7].operation,
            BrowserOperation::SwitchBrowser { ref prompt, wait_ms: Some(120000) }
                if prompt.as_ref().is_some_and(|value| value == "Pick Chrome")
        ));
        assert!(matches!(
            requests[8].operation,
            BrowserOperation::History { ref queries, limit: Some(3), .. }
                if queries == &vec![RuntimeString::from("docs")]
        ));
        assert!(matches!(
            requests[9].operation,
            BrowserOperation::TabsContent { ref urls, ref content_type, .. }
                if urls == &vec![RuntimeString::from("https://example.test/")]
                    && content_type.as_ref().is_some_and(|value| value == "text")
        ));
        assert!(matches!(
            requests[10].operation,
            BrowserOperation::ContentExport { ref format, .. }
                if format.as_ref().is_some_and(|value| value == "html")
        ));
        assert!(matches!(
            requests[11].operation,
            BrowserOperation::Download { ref url, ref filename, .. }
                if url.as_ref().is_some_and(|value| value == "https://example.test/file.txt")
                    && filename.as_ref().is_some_and(|value| value == "file.txt")
        ));
        assert!(matches!(
            requests[12].operation,
            BrowserOperation::Clipboard {
                action: crate::browser::BrowserClipboardAction::WriteText,
                ref text,
                ..
            } if text.as_ref().is_some_and(|value| value == "Copied")
        ));
        assert!(matches!(
            requests[13].operation,
            BrowserOperation::Dialog {
                action: crate::browser::BrowserDialogAction::Accept,
                ref prompt_text,
            } if prompt_text.as_ref().is_some_and(|value| value == "ok")
        ));
        assert!(matches!(
            requests[14].operation,
            BrowserOperation::MarkTab {
                status: crate::browser::BrowserTabMarkStatus::Deliverable,
            }
        ));
        assert!(requests[1].permissions.file_transfer);
        assert!(requests[2].permissions.file_transfer);
        assert!(requests[11].permissions.file_transfer);
        assert!(requests[12].permissions.interact);
        assert!(requests[14].permissions.manage_tabs);
        assert!(requests[4].permissions.interact);
        assert!(requests[7].permissions.session_state);
    }
}
