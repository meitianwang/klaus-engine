---
name: control-browser
description: Control the bundled Klaus browser runtime through browser tools or its Rust-hosted client.
when_to_use: Use when a task requires navigating, inspecting, finding elements, filling forms, uploading files/images, running browser shortcuts, using mouse/keyboard browser actions, evaluating JavaScript, reading console/network logs, managing tabs or connected browsers, recording GIF artifacts, or capturing screenshots in a controlled browser.
allowed-tools: browser_navigate,browser_snapshot,browser_screenshot,browser_click,browser_type,browser_eval,browser_console,browser_network,browser_read_page,browser_find,browser_form_input,browser_computer,browser_tabs,browser_locator,browser_page_text,browser_batch,browser_resize,browser_close,browser_gif_creator,browser_upload_image,browser_file_upload,browser_shortcuts_list,browser_shortcuts_execute,browser_switch_browser,browser_list_connected_browsers,browser_select_browser,browser_resolve_tab,browser_claim_tab,browser_history,browser_tabs_content,browser_content_export,browser_download,browser_clipboard,browser_dialog,browser_mark_tab
---

The bundled browser runtime exposes the model-facing `browser_*` tools when the
host injects `PlaywrightBrowserRuntime` or another browser runtime root.
Read `docs/browser-safety.md`, `docs/confirmations.md`, `docs/file-uploads.md`,
`docs/downloads.md`, `docs/tab-cleanup.md`, and `docs/visibility.md` when a task
touches those policies.
Use the `extension` backend when the task needs the user's real Chrome tabs,
profile cookies, logged-in sessions, or browser extension state. The built-in
Chrome connector uses the token- and Origin-authenticated local
WebSocket bridge directly. The Rust `klaus-browser-native-host` binary remains
only as compatibility tooling for older external extensions.

The locked browser client is loaded only by the Rust-owned sandbox and receives
a context-bound transport capability. Do not import it from standalone Node or
provide process, sidecar, cwd, or environment options.

Prefer `readPage()` or `browser_read_page` before interacting with a page.
Use `find()`/`browser_find` to get element refs, then `formInput()` or
`computer()` when selector-only interaction is brittle. Use `fileUpload()` or
`browser_file_upload` for file inputs instead of clicking upload buttons that
open native file pickers. Use screenshots and `gifCreator()`/`browser_gif_creator`
for visual verification, and console/network calls when diagnosing page behavior;
the extension backend captures those entries and file upload injection through
Chrome debugger protocol.

Use `browser_tabs_content` for temporary background content extraction,
`browser_content_export` for current-tab export artifacts, `browser_download`
for linked/media downloads, `browser_clipboard` for browser-session clipboard
work, `browser_dialog` for JavaScript alerts, confirms, and prompts, and
`browser_mark_tab` when cleanup policy should keep a tab as a deliverable or
handoff.

When using the Rust-hosted browser client, the first-stage Playwright API is
available through `tab.playwright`: use `waitForEvent("download")`,
`waitForEvent("filechooser")`, `waitForLoadState`, `waitForURL`,
`waitForTimeout`, `frameLocator`, `domSnapshot`, `elementInfo`,
`elementScreenshot`, and chained locator helpers such as `filter`, `all`,
`last`, `getAttribute`, `innerText`, `isEnabled`, and `setChecked`. The
browser-client also exposes `tab.cua` coordinate helpers and
`tab.dom_cua` DOM helpers for click, double-click, scroll, keypress, type, and
downloads.

For a user-selected tab mention, call `browser_resolve_tab` with the complete
mention URI. Use the exact returned browserId/tabId on subsequent operations.
The resolver verifies the original title and URL and claims an external user
tab; stale references must remain explicit errors. Never open a replacement.
The client equivalent is `browser.tabs.get({ mention: completeMentionUri })`.
