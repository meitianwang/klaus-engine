# Klaus Bundled Browser Runtime

This plugin ships hash-attested browser assets and a Rust-owned broker that
starts the bundled sidecar with pinned Playwright 1.61.1. The Rust runtime owns
the model-facing protocol, exact RuntimeJson transport, permissions, process
tree, timeouts, cancellation, cleanup, and artifacts. Host npm modules,
standalone client loading, and caller-selected sidecar paths are not supported.

The locked browser client is evaluated only in the Rust-owned sandbox and
acquires its context-bound `BrowserHostTransportV1` capability there. Embedders
configure the SDK browser runtime through the engine; model-authored code uses
`agent.browsers` and never receives process, argv, cwd, environment, or package
resolution authority.

The sidecar speaks newline-delimited JSON over stdio. Each request carries the
same `BrowserRuntimeRequest` shape used by the `browser_*` tools, and each final
response returns a `BrowserRuntimeResponse`.

## Backends

The default backend launches an isolated Chromium instance:

```bash
KLAUS_BROWSER_BACKEND=launch
```

To attach to an existing Chrome/Chromium DevTools endpoint:

```bash
KLAUS_BROWSER_BACKEND=cdp
KLAUS_BROWSER_CDP_ENDPOINT=http://127.0.0.1:9222
```

To use a persistent Chrome profile managed by the host:

```bash
KLAUS_BROWSER_BACKEND=persistent
KLAUS_BROWSER_USER_DATA_DIR=/path/to/profile
KLAUS_BROWSER_CHANNEL=chrome
```

To operate the user's already-open Chrome profile and logged-in tabs, run the
extension backend and load the bundled Chrome extension:

```bash
KLAUS_BROWSER_BACKEND=extension
KLAUS_BROWSER_NATIVE_BRIDGE_SOCKET=~/.klaus/browser/native-host.sock
KLAUS_BROWSER_EXTENSION_BRIDGE_PORT=18765
KLAUS_BROWSER_EXTENSION_BRIDGE_TOKEN=<random-high-entropy-token>
KLAUS_BROWSER_EXTENSION_CONNECT_TIMEOUT_MS=5000
KLAUS_BROWSER_EXTENSION_HEARTBEAT_MS=15000
KLAUS_BROWSER_EXTENSION_STALE_MS=45000
```

Load `plugins/klaus-bundled/browser/extension` as an unpacked Chrome extension.
The extension has the stable id `gjamccelebgpogpbiceajehcajghphnb` and
connects directly to the token- and Origin-authenticated local WebSocket bridge.
The optional Rust `klaus-browser-native-host` compatibility binary connects through
`~/.klaus/browser/native-host.sock` by default; that socket lives under a
0700 directory and is chmodded to 0600 on Unix platforms. If the native socket is unavailable, that compatibility host can fall back to the local
WebSocket bridge at `ws://127.0.0.1:18765/klaus-browser`. The fallback is
disabled unless `KLAUS_BROWSER_EXTENSION_BRIDGE_TOKEN` is configured. Store the
same value as `bridgeToken` in the extension's local storage (or include it in
the stored `bridgeUrl`); the bridge also requires the bundled extension Origin.
In both paths it
sends a stable device id and forwards tab, DOM, screenshot, JavaScript, form,
mouse, keyboard, locator, page text, console, network, file upload, GIF
artifact, shortcut, browser selection, and batch operations to the active Chrome
profile. Use `KLAUS_BROWSER_EXTENSION_PATH` when the extension lives outside
the plugin directory.

Before relying on a real user Chrome profile, hosts should run:

```bash
klaus-browser check-extension-installed --json
klaus-browser install-native-host-manifest --json
klaus-browser check-native-host-manifest --json
klaus-browser chrome-is-running --json
klaus-browser installed-browsers --json
klaus-browser open-chrome-window --dry-run --json
```

The browser stack has three layers: the engine sidecar exposes the
secure local bridge plus fallback WebSocket bridge, the Chrome extension owns
the real browser/profile context, and the native host gives Chrome a durable,
profile-installed route into the local agent process.

The extension bridge keeps a pool of connected extension instances, records
transport/connectedAt/lastSeenAt metadata, sends heartbeat pings, removes stale
clients, and times out individual extension calls. `listConnectedBrowsers`,
`selectBrowser`, and `switchBrowser` operate on that bridge pool rather than
blindly forwarding to the currently selected extension.

## Runtime API

`setupBrowserRuntime({ globals })` installs `agent.browsers`. The default
browser exposes:

- `browser.documentation()` for runtime guidance and API details.
- `browser.capabilities.list()` and `tab.capabilities.list()` for discovery.
- `browser.tabs.list/get/new/selected/finalize()` for tab/session control.
- `browser.tabs.content({ urls, contentType })` for temporary background
  extraction without changing the selected tab.
- `browser.user.openTabs()` and `browser.user.claimTab()` for host-visible tabs.
- `browser.user.history({ queries, limit, from, to })` for runtime-visible
  browsing history.
- `browser.listConnectedBrowsers()`, `browser.selectBrowser(deviceId)`, and
  `browser.switchBrowser()` for Chrome extension instance selection.

A browser tab selected in the user's `@` menu carries a complete `plugin://`
`tab-v1` mention. Pass that URI unchanged to the owning host:

```javascript
const browser = await agent.browsers.getDefault();
const tab = await browser.tabs.get({ mention: completeMentionUri });
```

The host validates the original browser instance, tab handle, title and URL,
then claims an external user tab. A changed or closed tab fails explicitly;
never substitute another tab or navigate a replacement. The returned tab binds
all subsequent operations to its resolved `browserId` and `tabId`. Stable
external browsers can also be selected with
`agent.browsers.get('chrome:<profileInstanceId>')`; `user.openTabs()` lists user
tabs and `user.claimTab(tabInfo)` performs the explicit ownership transfer.
Hosts that expose the capability through MCP provide `resolve_tab` with
`{ mention: completeMentionUri }`, reachable by the typed `browser_resolve_tab`
tool and by Code Mode as `tools.mcp__klaus-browser__resolve_tab`.

Tabs expose basic page operations plus richer first-stage controls:

- `tab.readPage()` returns an accessibility-style tree with stable `ref` ids.
- `tab.find(query)` returns matching elements with refs.
- `tab.formInput(ref, value)` sets inputs, checkboxes, selects, or editable text.
- `tab.fileUpload(paths, { selector | ref })` uploads one or more files through
  a file input without opening a native picker. The Playwright backend uses
  `locator.setInputFiles`; the Chrome extension backend uses Chrome debugger
  protocol `DOM.setFileInputFiles`.
- `tab.uploadImage(path, { selector })` is the image-specific convenience form
  of file upload.
- `tab.gifCreator({ filename, durationSeconds, fps, fullPage })` captures a
  recording artifact. With a local `ffmpeg` binary it encodes captured frames to
  GIF; otherwise it returns a GIF artifact plus frame artifacts so hosts can
  encode/export later.
- `tab.shortcuts.list()` and `tab.shortcuts.execute(name)` expose browser/page
  shortcut workflows. The extension backend lists Chrome extension commands and
  dispatches shortcut execution into the page context.
- `tab.content.export()` and `tab.content.exportGsuite(type)` export current
  page content to a host artifact or a content string, depending on backend.
- `tab.downloadMedia({ selector | url | coordinate, filename })` downloads
  linked or media content. The Playwright backend captures real browser
  download events or direct URLs; the extension backend uses Chrome downloads.
- `tab.clipboard.read/readText/write/writeText` operate on the browser-session
  clipboard with a runtime fallback when a page blocks the Clipboard API.
- `tab.getJsDialog()` returns the active JavaScript dialog with `accept()` and
  `dismiss()` helpers when the backend can observe dialogs.
- `tab.markDeliverable()` and `tab.markHandoff()` mark tabs for cleanup policy.
- `tab.title()` and `tab.url()` return current tab metadata.
- `tab.computer({ action, ... })` supports click, hover, drag, scroll, key,
  type, wait, screenshot, and zoom actions.
- `tab.playwright.locator(selector)` supports common locator actions including
  click, fill, type, press, count, textContent, allTextContents, isVisible,
  waitFor, screenshot, check, uncheck, and selectOption.
- `tab.console()` and `tab.network()` read entries captured from Chrome
  debugger protocol when the extension backend controls a real Chrome profile.
- `tab.playwright.waitForEvent("download")` returns a download helper with
  `path()` and `suggestedFilename`; the Playwright sidecar saves the download as
  a runtime artifact.
- `tab.playwright.waitForEvent("filechooser")` returns a file chooser helper
  with `isMultiple()` and `setFiles(files)`. Start the promise before the click
  that opens the chooser, then set files on the returned helper.
- `tab.playwright.waitForLoadState()`, `waitForURL()`, and `waitForTimeout()`
  provide the first-stage browser client wait helpers.
- `tab.playwright.frameLocator(selector)` creates frame-scoped locators.
- `tab.playwright.domSnapshot()`, `elementInfo({ selector | x, y })`, and
  `elementScreenshot({ selector | x, y })` expose DOM inspection and focused
  screenshot helpers used by browser automation workflows.
- `tab.playwright.locator(selector)` supports chained locator helpers including
  `first`, `last`, `nth`, `filter`, `and`, `or`, `all`, nested `locator`,
  `getByText`, `getByRole`, `getByLabel`, `getByPlaceholder`, `getByTestId`,
  `getAttribute`, `innerText`, `isEnabled`, `isVisible`, `textContent`,
  `allTextContents`, `click`, `dblclick`, `fill`, `type`, `press`, `waitFor`,
  `check`, `uncheck`, `setChecked`, `selectOption`, `screenshot`, and
  `downloadMedia`.
- `tab.cua` exposes coordinate-based click, double-click, drag, keypress, move,
  scroll, type, and download helpers over the browser computer-action runtime.
- `tab.dom_cua` exposes DOM-oriented click, double-click, visible DOM snapshot,
  keypress, scroll, type, and download helpers over selectors/refs.

## Safety And Limits

Do not click file inputs to open native file pickers; use `fileUpload` with a
selector or ref. The sidecar rejects missing files and enforces
`KLAUS_BROWSER_MAX_UPLOAD_BYTES` (default 10 MB) before handing files to
Playwright. In the extension backend the host process is responsible for path
policy, and Chrome receives the selected paths only through debugger protocol.

GIF creation captures real frames. `KLAUS_BROWSER_FFMPEG` can point at a
specific encoder binary; `KLAUS_BROWSER_GIF_ENCODER=none` disables ffmpeg and
keeps the frame-backed fallback.
