const DEFAULT_BRIDGE_URL = 'ws://127.0.0.1:18765/klaus-browser';
const DEVICE_ID_KEY = 'klausBrowserDeviceId';
const RECONNECT_ALARM = 'klaus-browser-reconnect';
const DEBUGGER_PROTOCOL_VERSION = '1.3';
const MAX_DIAGNOSTIC_ENTRIES = 500;

let socket = null;
let reconnectTimer = null;
let reconnectDelayMs = 500;
let localDeviceId = null;
const refsByTab = new Map();
const attachedDebuggerTabs = new Set();
const diagnosticsByTab = new Map();
const dialogsByTab = new Map();
const tabMarks = new Map();
const fileChoosers = new Map();
let debuggerListenersInstalled = false;
let extensionClipboardText = '';
let extensionClipboardItems = [];
let nextFileChooserSeq = 1;

connect();

chrome.runtime.onStartup?.addListener(connect);
chrome.runtime.onInstalled?.addListener(connect);
chrome.alarms?.onAlarm?.addListener((alarm) => {
  if (alarm.name === RECONNECT_ALARM) {
    void connect();
  }
});

async function connect() {
  if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) {
    return;
  }
  await connectWebSocket();
}

async function connectWebSocket() {
  const bridgeUrl = await readBridgeUrl();
  if (!bridgeUrl) {
    scheduleReconnect();
    return;
  }
  socket = new WebSocket(bridgeUrl);
  socket.addEventListener('open', async () => {
    reconnectDelayMs = 500;
    localDeviceId = await deviceId();
    send(await helloMessage());
  });
  socket.addEventListener('message', (event) => {
    void handleBridgeMessage(event.data);
  });
  socket.addEventListener('close', () => {
    socket = null;
    scheduleReconnect();
  });
  socket.addEventListener('error', () => {
    socket = null;
    scheduleReconnect();
  });
}

function scheduleReconnect() {
  if (reconnectTimer) return;
  chrome.alarms?.create?.(RECONNECT_ALARM, {
    delayInMinutes: Math.max(1 / 60, reconnectDelayMs / 60_000),
  });
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    reconnectDelayMs = Math.min(reconnectDelayMs * 1.8, 10_000);
    void connect();
  }, reconnectDelayMs);
}

async function helloMessage() {
  return {
    type: 'hello',
    deviceId: localDeviceId || await deviceId(),
    name: await browserName(),
    platform: navigator.platform || 'unknown',
    version: chrome.runtime.getManifest().version,
    capabilities: [
      'tabs',
      'read_page',
      'find',
      'form_input',
      'computer',
      'screenshot',
      'eval',
      'console',
      'network',
      'locator',
      'page_text',
      'gif_creator',
      'upload_image',
      'file_upload',
      'shortcuts',
      'browser_selection',
    ],
  };
}

async function readBridgeUrl() {
  const stored = await chrome.storage.local.get(['bridgeUrl', 'bridgeToken']);
  const url = new URL(stored.bridgeUrl || DEFAULT_BRIDGE_URL);
  if (!url.searchParams.has('token') && stored.bridgeToken) {
    url.searchParams.set('token', stored.bridgeToken);
  }
  return url.href;
}

async function deviceId() {
  const stored = await chrome.storage.local.get([DEVICE_ID_KEY]);
  if (stored[DEVICE_ID_KEY]) return stored[DEVICE_ID_KEY];
  const value = crypto.randomUUID();
  await chrome.storage.local.set({ [DEVICE_ID_KEY]: value });
  return value;
}

async function browserName() {
  const [active] = await chrome.tabs.query({ active: true, lastFocusedWindow: true });
  return active?.windowId ? `Chrome window ${active.windowId}` : 'Chrome';
}

async function handleBridgeMessage(raw) {
  let message;
  try {
    message = typeof raw === 'string' ? JSON.parse(raw) : raw;
  } catch (_error) {
    return;
  }
  if (message.type === 'ping') {
    send({ type: 'pong', ts: message.ts || Date.now() });
    return;
  }
  if (message.type !== 'call') return;
  try {
    const response = await executeRequest(message.request);
    send({ type: 'response', id: message.id, ok: true, response });
  } catch (error) {
    send({
      type: 'response',
      id: message.id,
      ok: false,
      error: normalizeError(error),
    });
  }
}

function send(message) {
  if (!socket || socket.readyState !== WebSocket.OPEN) return;
  socket.send(JSON.stringify(message));
}

async function executeRequest(request) {
  if (!request?.operation) throw new BrowserError('invalid_request', 'Missing browser operation.');
  const op = request.operation;
  switch (op.type) {
    case 'tabs':
      return tabs(request, op);
    case 'navigate':
      return navigate(request, op);
    case 'snapshot':
      return snapshot(request, op);
    case 'read_page':
      return readPage(request, op);
    case 'find':
      return findElements(request, op);
    case 'form_input':
      return formInput(request, op);
    case 'screenshot':
      return screenshot(request, op);
    case 'click':
      return click(request, op);
    case 'type':
      return typeText(request, op);
    case 'eval':
      return evaluate(request, op);
    case 'computer':
      return computer(request, op);
    case 'locator':
      return locatorAction(request, op);
    case 'page_text':
      return pageTextOperation(request, op);
    case 'batch':
      return batch(request, op);
    case 'resize':
      return resize(request, op);
    case 'console':
      return consoleEntries(request, op);
    case 'network':
      return networkEntries(request, op);
    case 'gif_creator':
      return gifCreator(request, op);
    case 'upload_image':
      return uploadImage(request, op);
    case 'file_upload':
      return fileUpload(request, op);
    case 'shortcuts_list':
      return shortcutsList(request, op);
    case 'shortcuts_execute':
      return shortcutsExecute(request, op);
    case 'list_connected_browsers':
      return listConnectedBrowsers(request, op);
    case 'select_browser':
      return selectBrowser(request, op);
    case 'switch_browser':
      return switchBrowser(request, op);
    case 'history':
      return historyEntries(request, op);
    case 'tabs_content':
      return tabsContent(request, op);
    case 'content_export':
      return contentExport(request, op);
    case 'download':
      return downloadMedia(request, op);
    case 'clipboard':
      return clipboardOperation(request, op);
    case 'dialog':
      return dialogOperation(request, op);
    case 'mark_tab':
      return markTab(request, op);
    case 'playwright_wait_for_event':
      return playwrightWaitForEvent(request, op);
    case 'playwright_file_chooser_set_files':
      return playwrightFileChooserSetFiles(request, op);
    case 'playwright_wait_for_load_state':
      return playwrightWaitForLoadState(request, op);
    case 'playwright_wait_for_url':
      return playwrightWaitForUrl(request, op);
    case 'playwright_dom_snapshot':
      return playwrightDomSnapshot(request, op);
    case 'playwright_element_info':
      return playwrightElementInfo(request, op);
    case 'playwright_element_screenshot':
      return playwrightElementScreenshot(request, op);
    case 'close':
      return closeTarget(request, op);
    default:
      throw new BrowserError('invalid_request', `Unsupported browser operation: ${op.type}`);
  }
}

async function tabs(request, op) {
  const action = op.action || 'list';
  if (action === 'new') {
    const tab = await chrome.tabs.create({ url: op.url || 'about:blank', active: true });
    return baseResponse(request, { tabId: tabId(tab.id), url: tab.url || null, title: tab.title || null, value: tabInfo(tab) });
  }
  if (action === 'selected') {
    const tab = await currentTab();
    return baseResponse(request, { tabId: tabId(tab.id), value: tabInfo(tab) });
  }
  if (action === 'get') {
    const tab = await tabForRequest(request);
    return baseResponse(request, { tabId: tabId(tab.id), value: tabInfo(tab) });
  }
  if (action === 'close') {
    const tab = await tabForRequest(request);
    await chrome.tabs.remove(tab.id);
    return baseResponse(request, { message: 'tab closed', value: [] });
  }
  if (action === 'finalize') {
    const selected = await currentTab();
    const all = await chrome.tabs.query({});
    const closeIds = all
      .filter((tab) => tab.windowId === selected.windowId && tab.id !== selected.id)
      .map((tab) => tab.id)
      .filter(Boolean);
    if (closeIds.length > 0) await chrome.tabs.remove(closeIds);
    return baseResponse(request, { tabId: tabId(selected.id), message: 'tabs finalized', value: [tabInfo(selected)] });
  }
  if (action === 'name_session') {
    return baseResponse(request, { message: 'session named', value: { name: op.name || null } });
  }
  const all = await chrome.tabs.query({});
  return baseResponse(request, { value: all.map(tabInfo), text: JSON.stringify(all.map(tabInfo), null, 2) });
}

async function navigate(request, op) {
  const tab = await tabForRequest(request);
  await chrome.tabs.update(tab.id, { url: op.url, active: true });
  await waitForTabLoad(tab.id, request.timeoutMs || 30_000);
  const updated = await chrome.tabs.get(tab.id);
  return baseResponse(request, {
    tabId: tabId(tab.id),
    url: updated.url || op.url,
    title: updated.title || null,
    text: await pageText(updated.id).catch(() => null),
    events: [{ type: 'navigation_completed', url: updated.url || op.url, title: updated.title || null }],
  });
}

async function snapshot(request, op) {
  const tab = await tabForRequest(request);
  const result = await runInPage(tab.id, pageSnapshotScript, [op.selector || null, op.includeText !== false, op.includeLinks !== false]);
  rememberRefs(tab.id, result.elements || []);
  return baseResponse(request, { tabId: tabId(tab.id), snapshot: result, text: result.text || null });
}

async function readPage(request, op) {
  const tab = await tabForRequest(request);
  const result = await runInPage(tab.id, pageSnapshotScript, [selectorForRef(tab.id, op.refId), true, true]);
  const elements = rememberRefs(tab.id, result.elements || []);
  const filtered = op.filter === 'interactive' ? elements.filter((element) => element.interactive !== false) : elements;
  const tree = renderElements(filtered, op.maxChars || 50_000);
  return baseResponse(request, {
    tabId: tabId(tab.id),
    value: { tabId: tabId(tab.id), text: result.text || '', elements: filtered, tree },
    text: tree,
  });
}

async function findElements(request, op) {
  const tab = await tabForRequest(request);
  const result = await runInPage(tab.id, pageSnapshotScript, [null, true, true]);
  const elements = rememberRefs(tab.id, result.elements || []);
  const query = String(op.query || '').toLowerCase();
  const matches = elements
    .map((element) => ({ element, score: scoreElement(element, query) }))
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, op.limit || 20)
    .map((item) => item.element);
  return baseResponse(request, { tabId: tabId(tab.id), value: { query: op.query, matches, count: matches.length }, text: renderElements(matches, 20_000) });
}

async function formInput(request, op) {
  const tab = await tabForRequest(request);
  const selector = selectorForRef(tab.id, op.ref || op.refId || op.reference);
  if (!selector) throw new BrowserError('not_found', `Unknown element ref: ${op.ref || op.reference}`);
  await runInPage(tab.id, formInputScript, [selector, op.value]);
  return baseResponse(request, { tabId: tabId(tab.id), message: 'form input completed', value: op.value });
}

async function screenshot(request, op) {
  const tab = await tabForRequest(request);
  await chrome.tabs.update(tab.id, { active: true });
  const options = { format: op.format === 'jpeg' ? 'jpeg' : 'png' };
  if (Number.isInteger(op.quality)) options.quality = op.quality;
  const dataUrl = await chrome.tabs.captureVisibleTab(tab.windowId, options);
  return screenshotResponse(request, tab, dataUrl, op.format === 'jpeg' ? 'image/jpeg' : 'image/png');
}

async function click(request, op) {
  const tab = await tabForRequest(request);
  if (op.selector) {
    await runInPage(tab.id, clickSelectorScript, [op.selector]);
  } else {
    await dispatchMouse(tab.id, op.x, op.y, op.button || 'left', op.clickCount || 1);
  }
  return baseResponse(request, { tabId: tabId(tab.id), message: 'click completed' });
}

async function typeText(request, op) {
  const tab = await tabForRequest(request);
  if (op.selector) {
    await runInPage(tab.id, formInputScript, [op.selector, op.text, { append: !op.clear }]);
  } else {
    await attachDebugger(tab.id);
    await chrome.debugger.sendCommand({ tabId: tab.id }, 'Input.insertText', { text: op.text });
  }
  if (op.pressEnter) {
    await pressKey(tab.id, 'Enter');
  }
  return baseResponse(request, { tabId: tabId(tab.id), message: 'type completed' });
}

async function evaluate(request, op) {
  const tab = await tabForRequest(request);
  const value = await runInPage(tab.id, evalScript, [op.script, op.args ?? null]);
  return baseResponse(request, { tabId: tabId(tab.id), value });
}

async function computer(request, op) {
  const tab = await tabForRequest(request);
  switch (op.action) {
    case 'left_click':
    case 'right_click':
    case 'double_click':
    case 'triple_click':
      await dispatchMouse(tab.id, op.coordinate?.[0], op.coordinate?.[1], op.action === 'right_click' ? 'right' : 'left', op.action === 'double_click' ? 2 : op.action === 'triple_click' ? 3 : 1);
      break;
    case 'hover':
      await dispatchMouseMove(tab.id, op.coordinate?.[0], op.coordinate?.[1]);
      break;
    case 'type':
      await attachDebugger(tab.id);
      await chrome.debugger.sendCommand({ tabId: tab.id }, 'Input.insertText', { text: String(op.text || '') });
      break;
    case 'key':
      await pressKey(tab.id, String(op.text || 'Enter'));
      break;
    case 'scroll':
      await dispatchWheel(tab.id, op.amount || 500, op.scrollDirection || 'down');
      break;
    case 'wait':
      await new Promise((resolve) => setTimeout(resolve, Math.max(0, Math.min(10_000, (op.duration || 0) * 1000))));
      break;
    case 'screenshot':
    case 'zoom':
      return screenshot(request, { format: 'png' });
    case 'scroll_to': {
      const selector = selectorForRef(tab.id, op.refId || op.ref);
      if (!selector) throw new BrowserError('not_found', 'scroll_to requires a known ref.');
      await runInPage(tab.id, scrollToScript, [selector]);
      break;
    }
    default:
      throw new BrowserError('invalid_request', `Unsupported computer action: ${op.action}`);
  }
  return baseResponse(request, { tabId: tabId(tab.id), message: 'computer action completed', value: { action: op.action } });
}

async function locatorAction(request, op) {
  const tab = await tabForRequest(request);
  const value = await runInPage(tab.id, locatorScript, [op.selector, op.action, op.value ?? null, op.options || {}]);
  return baseResponse(request, { tabId: tabId(tab.id), message: `locator ${op.action} completed`, value });
}

async function pageTextOperation(request, op) {
  const tab = await tabForRequest(request);
  const text = clip(await pageText(tab.id), op.maxChars || 50_000);
  return baseResponse(request, { tabId: tabId(tab.id), text, value: { text } });
}

async function consoleEntries(request, op) {
  const tab = await tabForRequest(request);
  await ensureDiagnostics(tab.id);
  const state = diagnosticsState(tab.id);
  if (op.clear) {
    state.console = [];
  }
  const levels = new Set((op.levels || []).map((level) => String(level).toLowerCase()));
  let entries = state.console.filter((entry) => {
    if (levels.size > 0 && !levels.has(entry.level)) return false;
    if (Number.isFinite(op.sinceMs) && entry.timestampMs < op.sinceMs) return false;
    return true;
  });
  entries = entries.slice(-(op.limit || 100));
  return baseResponse(request, {
    tabId: tabId(tab.id),
    console: entries,
    value: entries,
    text: renderConsoleEntries(entries),
  });
}

async function networkEntries(request, op) {
  const tab = await tabForRequest(request);
  await ensureDiagnostics(tab.id);
  const state = diagnosticsState(tab.id);
  if (op.clear) {
    state.network = [];
    state.networkByRequestId.clear();
  }
  let entries = state.network.filter((entry) => {
    if (Number.isFinite(op.sinceMs) && entry.startedAtMs < op.sinceMs) return false;
    if (op.urlFilter && !String(entry.url || '').includes(op.urlFilter)) return false;
    return true;
  });
  entries = entries.slice(-(op.limit || 100));
  if (op.includeBodies) {
    entries = await withResponseBodies(tab.id, entries);
  }
  return baseResponse(request, {
    tabId: tabId(tab.id),
    network: entries,
    value: entries,
    text: renderNetworkEntries(entries),
  });
}

async function gifCreator(request, op) {
  const tab = await tabForRequest(request);
  await chrome.tabs.update(tab.id, { active: true });
  const dataUrl = await chrome.tabs.captureVisibleTab(tab.windowId, { format: 'png' });
  const artifact = {
    id: `chrome-gif-${Date.now()}`,
    kind: 'gif',
    uri: minimalGifDataUrl(),
    mimeType: 'image/gif',
    metadata: {
      source: 'chrome-extension',
      tabId: tabId(tab.id),
      frames: 1,
      encoded: false,
      frameDataUrls: [dataUrl],
      filename: op.filename || null,
      fullPage: !!op.fullPage,
    },
  };
  return baseResponse(request, {
    tabId: tabId(tab.id),
    message: 'gif frame captured',
    artifacts: [artifact],
    value: { artifact, frames: [{ uri: dataUrl, mimeType: 'image/png' }] },
  });
}

async function uploadImage(request, op) {
  return fileUpload(request, {
    ...op,
    paths: [op.path],
    selector: op.selector,
  });
}

async function fileUpload(request, op) {
  const tab = await tabForRequest(request);
  const files = validateUploadPaths(op.paths || []);
  const selector = op.selector || selectorForRef(tab.id, op.refId || op.ref || op.reference);
  if (!selector) {
    throw new BrowserError('not_found', 'file_upload requires selector or known ref.');
  }
  await setFileInputFiles(tab.id, selector, files);
  return baseResponse(request, {
    tabId: tabId(tab.id),
    message: 'file upload completed',
    value: { selector, files, count: files.length },
  });
}

async function shortcutsList(request, op) {
  const tab = await tabForRequest(request);
  let commands = [];
  if (chrome.commands?.getAll) {
    commands = await chrome.commands.getAll();
  }
  const query = String(op.query || '').trim().toLowerCase();
  const shortcuts = commands
    .filter((command) => command && command.name)
    .map((command, index) => ({
      id: command.name || `shortcut-${index + 1}`,
      command: command.name || `shortcut-${index + 1}`,
      description: command.description || null,
      shortcut: command.shortcut || null,
      isWorkflow: false,
    }))
    .filter((shortcut) => {
      if (!query) return true;
      return [shortcut.id, shortcut.command, shortcut.description, shortcut.shortcut]
        .filter(Boolean)
        .join(' ')
        .toLowerCase()
        .includes(query);
    });
  return baseResponse(request, {
    tabId: tabId(tab.id),
    value: shortcuts,
    text: JSON.stringify(shortcuts, null, 2),
  });
}

async function shortcutsExecute(request, op) {
  const tab = await tabForRequest(request);
  const name = String(op.name || '').trim();
  if (!name) throw new BrowserError('invalid_request', 'shortcuts_execute requires name.');
  const listed = await shortcutsList(request, { query: null });
  const shortcut = (listed.value || []).find((item) => item.command === name || item.id === name);
  if (!shortcut) {
    throw new BrowserError('not_found', `Shortcut not found: ${name}`);
  }
  await runInPage(tab.id, shortcutExecuteScript, [name, shortcut, !!op.waitForCompletion]);
  return baseResponse(request, {
    tabId: tabId(tab.id),
    message: 'shortcut execution started',
    value: { ...shortcut, started: true },
  });
}

async function listConnectedBrowsers(request, _op) {
  const id = localDeviceId || await deviceId();
  const value = [{
    deviceId: id,
    name: await browserName(),
    platform: navigator.platform || 'unknown',
    version: chrome.runtime.getManifest().version,
    capabilities: (await helloMessage()).capabilities,
    isLocal: true,
    selected: true,
    connected: true,
  }];
  return baseResponse(request, { sessionId: id, value, text: JSON.stringify(value, null, 2) });
}

async function selectBrowser(request, op) {
  const id = localDeviceId || await deviceId();
  const requested = String(op.deviceId || '').trim();
  if (!requested) throw new BrowserError('invalid_request', 'select_browser requires deviceId.');
  if (requested !== id) {
    throw new BrowserError(
      'not_found',
      `No connected browser has deviceId "${requested}". Call list_connected_browsers to see currently connected browsers.`,
    );
  }
  return baseResponse(request, {
    sessionId: id,
    message: `Connected to browser "${await browserName()}".`,
    value: { deviceId: id, name: await browserName(), isLocal: true, selected: true },
  });
}

async function switchBrowser(request, op) {
  const waitMs = Number.isFinite(op.waitMs) ? Math.max(0, Math.min(op.waitMs, 120_000)) : 0;
  if (waitMs > 0) {
    await new Promise((resolve) => setTimeout(resolve, waitMs));
  }
  const id = localDeviceId || await deviceId();
  return baseResponse(request, {
    sessionId: id,
    message: `Connected to browser "${await browserName()}".`,
    value: { deviceId: id, name: await browserName(), isLocal: true, selected: true, prompt: op.prompt || null },
  });
}

async function historyEntries(request, op) {
  let entries = [];
  if (chrome.history?.search) {
    const queries = Array.isArray(op.queries) && op.queries.length > 0 ? op.queries : [''];
    const seen = new Set();
    for (const query of queries) {
      const found = await chrome.history.search({
        text: String(query || ''),
        startTime: op.from ? Date.parse(op.from) : 0,
        endTime: op.to ? Date.parse(op.to) : Date.now(),
        maxResults: op.limit || 100,
      });
      for (const item of found) {
        if (!item.url || seen.has(item.url)) continue;
        seen.add(item.url);
        entries.push({
          url: item.url,
          title: item.title || null,
          dateVisited: new Date(item.lastVisitTime || Date.now()).toISOString(),
        });
      }
    }
  } else {
    const tabs = await chrome.tabs.query({});
    entries = tabs
      .filter((tab) => tab.url)
      .map((tab) => ({
        url: tab.url,
        title: tab.title || null,
        dateVisited: new Date().toISOString(),
      }));
  }
  entries.sort((a, b) => Date.parse(b.dateVisited) - Date.parse(a.dateVisited));
  entries = entries.slice(0, op.limit || 100);
  return baseResponse(request, { value: entries, text: JSON.stringify(entries, null, 2) });
}

async function tabsContent(request, op) {
  if (!Array.isArray(op.urls) || op.urls.length === 0) {
    throw new BrowserError('invalid_request', 'tabs_content requires urls.');
  }
  const results = [];
  for (const url of op.urls) {
    let tab = null;
    try {
      tab = await chrome.tabs.create({ url, active: false });
      await waitForTabLoad(tab.id, request.timeoutMs || 30_000);
      const updated = await chrome.tabs.get(tab.id);
      results.push({
        url: updated.url || url,
        requestedUrl: url,
        title: updated.title || null,
        content: await contentFromTab(updated.id, op.contentType || op.content_type || 'text', op.maxChars || 50_000),
      });
    } catch (error) {
      results.push({
        url,
        requestedUrl: url,
        title: null,
        content: null,
        error: error?.message || String(error),
      });
    } finally {
      if (tab?.id != null) await chrome.tabs.remove(tab.id).catch(() => {});
    }
  }
  return baseResponse(request, { value: results, text: JSON.stringify(results, null, 2) });
}

async function contentExport(request, op) {
  const tab = await tabForRequest(request);
  const content = await contentFromTab(tab.id, op.format || 'text', op.maxChars || 200_000, op.selector || null);
  return baseResponse(request, {
    tabId: tabId(tab.id),
    message: 'content exported',
    value: {
      content,
      format: op.format || 'text',
      url: tab.url || null,
      title: tab.title || null,
    },
    text: content,
  });
}

async function downloadMedia(request, op) {
  const tab = await tabForRequest(request);
  let url = op.url || null;
  if (!url && op.selector) {
    url = await runInPage(tab.id, mediaUrlForSelectorScript, [op.selector]);
  }
  if (!url && Number.isFinite(op.x) && Number.isFinite(op.y)) {
    url = await runInPage(tab.id, mediaUrlForPointScript, [op.x, op.y]);
  }
  if (!url) {
    throw new BrowserError('not_found', 'download requires url, selector with href/src, or media coordinates.');
  }
  const resolved = new URL(url, tab.url || undefined).href;
  if (!chrome.downloads?.download) {
    throw new BrowserError('unavailable', 'Chrome downloads API is unavailable.');
  }
  const downloadId = await chrome.downloads.download({
    url: resolved,
    filename: op.filename || undefined,
    saveAs: false,
  });
  const artifact = {
    id: `chrome-download-${downloadId}`,
    kind: 'download',
    uri: resolved,
    mimeType: null,
    metadata: {
      source: 'chrome-extension',
      downloadId,
      tabId: tabId(tab.id),
      filename: op.filename || null,
    },
  };
  return baseResponse(request, {
    tabId: tabId(tab.id),
    message: 'download started',
    artifacts: [artifact],
    value: { artifact, downloadId },
    text: resolved,
  });
}

async function playwrightWaitForEvent(request, op) {
  const tab = await tabForRequest(request);
  const event = String(op.event || '');
  if (event === 'download') {
    if (!chrome.downloads?.onCreated) {
      throw new BrowserError('unavailable', 'Chrome downloads API is unavailable.');
    }
    const item = await waitForChromeDownload(request.timeoutMs || 30_000);
    const artifact = {
      id: `chrome-download-${item.id}`,
      kind: 'download',
      uri: item.url || null,
      path: item.filename || null,
      mimeType: item.mime || null,
      metadata: {
        source: 'chrome-extension-wait-event',
        downloadId: item.id,
        tabId: tabId(tab.id),
      },
    };
    return baseResponse(request, {
      tabId: tabId(tab.id),
      artifacts: [artifact],
      value: {
        event,
        path: item.filename || null,
        suggestedFilename: item.filename ? item.filename.split(/[\\/]/).pop() : null,
        url: item.url || null,
        artifact,
      },
    });
  }
  if (event === 'filechooser') {
    const chooserId = `filechooser-${nextFileChooserSeq++}`;
    fileChoosers.set(chooserId, {
      tabId: tab.id,
      selector: op.selector || 'input[type="file"]',
      createdAt: Date.now(),
    });
    return baseResponse(request, {
      tabId: tabId(tab.id),
      value: {
        event,
        chooserId,
        isMultiple: false,
      },
    });
  }
  throw new BrowserError('invalid_request', `Unsupported Playwright event: ${event}`);
}

async function playwrightFileChooserSetFiles(request, op) {
  const chooserId = op.chooserId || op.id;
  const chooser = fileChoosers.get(chooserId);
  if (!chooser) {
    throw new BrowserError('not_found', `Unknown or expired file chooser: ${chooserId}`);
  }
  const files = validateUploadPaths(op.files || op.paths || []);
  await setFileInputFiles(chooser.tabId, op.selector || chooser.selector || 'input[type="file"]', files);
  fileChoosers.delete(chooserId);
  return baseResponse(request, {
    tabId: tabId(chooser.tabId),
    message: 'file chooser files set',
    value: { chooserId, files, count: files.length },
  });
}

async function playwrightWaitForLoadState(request, _op) {
  const tab = await tabForRequest(request);
  await waitForTabLoad(tab.id, request.timeoutMs || 30_000);
  const updated = await chrome.tabs.get(tab.id);
  return baseResponse(request, {
    tabId: tabId(tab.id),
    url: updated.url || null,
    title: updated.title || null,
    value: { state: updated.status === 'complete' ? 'load' : updated.status || 'unknown' },
  });
}

async function playwrightWaitForUrl(request, op) {
  const tab = await tabForRequest(request);
  if (!op.url) throw new BrowserError('invalid_request', 'waitForURL requires url.');
  const expected = String(op.url);
  const deadline = Date.now() + (request.timeoutMs || 30_000);
  let updated = tab;
  while (Date.now() <= deadline) {
    updated = await chrome.tabs.get(tab.id);
    if (urlMatches(updated.url || '', expected)) {
      return baseResponse(request, {
        tabId: tabId(tab.id),
        url: updated.url || null,
        title: updated.title || null,
        value: { url: updated.url || null },
      });
    }
    await sleep(50);
  }
  throw new BrowserError('timeout', `Timed out waiting for URL: ${expected}`);
}

async function playwrightDomSnapshot(request, op) {
  const tab = await tabForRequest(request);
  const text = await runInPage(tab.id, domSnapshotScript, [!!op.visibleOnly, op.maxChars || 100_000]);
  return baseResponse(request, { tabId: tabId(tab.id), text, value: { text } });
}

async function playwrightElementInfo(request, op) {
  const tab = await tabForRequest(request);
  const value = await runInPage(tab.id, elementInfoScript, [
    op.selector || null,
    Number.isFinite(op.x) ? op.x : null,
    Number.isFinite(op.y) ? op.y : null,
  ]);
  return baseResponse(request, { tabId: tabId(tab.id), value, text: JSON.stringify(value, null, 2) });
}

async function playwrightElementScreenshot(request, op) {
  const tab = await tabForRequest(request);
  return screenshotResponse(request, tab, await chrome.tabs.captureVisibleTab(tab.windowId, { format: op.format || 'png' }), `image/${op.format || 'png'}`);
}

async function clipboardOperation(request, op) {
  const tab = await tabForRequest(request);
  const action = op.action || 'read_text';
  if (action === 'write' || action === 'write_text') {
    const text = op.text ?? clipboardTextFromItems(op.items) ?? '';
    extensionClipboardText = String(text);
    extensionClipboardItems = op.items ? normalizeClipboardItems(op.items) : clipboardItemsForText(String(text));
    await runInPage(tab.id, clipboardWriteTextScript, [String(text)]).catch(() => {});
    return baseResponse(request, { tabId: tabId(tab.id), message: 'clipboard written', value: { text: String(text) } });
  }
  const text = await runInPage(tab.id, clipboardReadTextScript, []).catch(() => extensionClipboardText);
  extensionClipboardText = text ?? extensionClipboardText;
  const items = extensionClipboardItems.length > 0 ? extensionClipboardItems : clipboardItemsForText(extensionClipboardText);
  if (action === 'read') {
    return baseResponse(request, { tabId: tabId(tab.id), value: items, text: extensionClipboardText });
  }
  return baseResponse(request, { tabId: tabId(tab.id), value: extensionClipboardText, text: extensionClipboardText });
}

async function dialogOperation(request, op) {
  const tab = await tabForRequest(request);
  await ensurePageDialogs(tab.id);
  const entry = dialogsByTab.get(tab.id) || null;
  const action = op.action || 'get';
  if (action === 'get') {
    return baseResponse(request, { tabId: tabId(tab.id), value: entry });
  }
  if (!entry) {
    throw new BrowserError('not_found', 'No active JavaScript dialog is available for this tab.');
  }
  await chrome.debugger.sendCommand({ tabId: tab.id }, 'Page.handleJavaScriptDialog', {
    accept: action === 'accept',
    promptText: op.promptText || op.prompt_text || '',
  });
  dialogsByTab.delete(tab.id);
  return baseResponse(request, { tabId: tabId(tab.id), message: `dialog ${action} completed`, value: entry });
}

async function markTab(request, op) {
  const tab = await tabForRequest(request);
  const value = {
    status: op.status || 'handoff',
    markedAt: new Date().toISOString(),
    url: tab.url || null,
    title: tab.title || null,
  };
  tabMarks.set(tab.id, value);
  return baseResponse(request, { tabId: tabId(tab.id), message: `tab marked ${value.status}`, value });
}

async function batch(request, op) {
  const results = [];
  for (const action of op.actions || []) {
    results.push(await executeRequest({ ...request, operation: action }));
  }
  return baseResponse(request, { value: { results }, message: `batch completed ${results.length} action(s)` });
}

async function resize(request, op) {
  const tab = await tabForRequest(request);
  await chrome.windows.update(tab.windowId, { width: op.viewport.width, height: op.viewport.height });
  return baseResponse(request, { tabId: tabId(tab.id), message: 'viewport resized', value: op.viewport });
}

async function closeTarget(request, op) {
  if ((op.target || 'tab') === 'all') {
    const tabs = await chrome.tabs.query({});
    await chrome.tabs.remove(tabs.map((tab) => tab.id).filter(Boolean));
    return baseResponse(request, { message: 'closed all tabs' });
  }
  const tab = await tabForRequest(request);
  await chrome.tabs.remove(tab.id);
  return baseResponse(request, { message: 'closed tab' });
}

async function currentTab() {
  const [active] = await chrome.tabs.query({ active: true, lastFocusedWindow: true });
  if (active) return active;
  const [fallback] = await chrome.tabs.query({ active: true });
  if (fallback) return fallback;
  throw new BrowserError('not_found', 'No active Chrome tab is available.');
}

async function tabForRequest(request) {
  const raw = request?.target?.tabId;
  if (raw) {
    const id = parseTabId(raw);
    if (id != null) return chrome.tabs.get(id);
  }
  return currentTab();
}

function parseTabId(value) {
  if (typeof value === 'number') return value;
  const match = String(value).match(/(\d+)$/);
  return match ? Number.parseInt(match[1], 10) : null;
}

function tabId(id) {
  return `chrome-${id}`;
}

function tabInfo(tab) {
  return {
    id: tabId(tab.id),
    type: 'page',
    title: tab.title || null,
    url: tab.url || null,
    selected: !!tab.active,
    windowId: tab.windowId,
  };
}

function waitForChromeDownload(timeoutMs) {
  return new Promise((resolve, reject) => {
    let settled = false;
    const timer = setTimeout(() => {
      cleanup();
      reject(new BrowserError('timeout', `Timed out after ${timeoutMs}ms waiting for Chrome download.`));
    }, timeoutMs);
    function cleanup() {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      chrome.downloads?.onCreated?.removeListener(onCreated);
    }
    function onCreated(item) {
      cleanup();
      resolve(item);
    }
    chrome.downloads.onCreated.addListener(onCreated);
  });
}

function urlMatches(actual, expected) {
  if (actual === expected) return true;
  if (expected.includes('*')) {
    const pattern = `^${expected.split('*').map(escapeRegExp).join('.*')}$`;
    return new RegExp(pattern).test(actual);
  }
  return actual.includes(expected);
}

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function baseResponse(request, values) {
  return {
    sessionId: request?.target?.sessionId || localDeviceId || 'chrome-session',
    tabId: values.tabId || request?.target?.tabId || null,
    ...values,
  };
}

function screenshotResponse(request, tab, dataUrl, mimeType) {
  return baseResponse(request, {
    tabId: tabId(tab.id),
    artifacts: [{
      id: `chrome-screenshot-${Date.now()}`,
      kind: 'screenshot',
      uri: dataUrl,
      mimeType,
      metadata: { source: 'chrome-extension', tabId: tabId(tab.id) },
    }],
  });
}

async function waitForTabLoad(tabIdValue, timeoutMs) {
  const current = await chrome.tabs.get(tabIdValue);
  if (current.status === 'complete') return;
  await new Promise((resolve) => {
    const timer = setTimeout(done, timeoutMs);
    function done() {
      clearTimeout(timer);
      chrome.tabs.onUpdated.removeListener(listener);
      resolve();
    }
    function listener(changedTabId, info) {
      if (changedTabId === tabIdValue && info.status === 'complete') done();
    }
    chrome.tabs.onUpdated.addListener(listener);
  });
}

async function runInPage(tabIdValue, func, args) {
  const [result] = await chrome.scripting.executeScript({
    target: { tabId: tabIdValue },
    func,
    args,
    world: 'MAIN',
  });
  return result?.result;
}

function validateUploadPaths(paths) {
  if (!Array.isArray(paths) || paths.length === 0) {
    throw new BrowserError('invalid_request', 'file_upload requires at least one path.');
  }
  return paths.map((item) => {
    const value = String(item || '').trim();
    if (!value) throw new BrowserError('invalid_request', 'file_upload path cannot be empty.');
    return value;
  });
}

async function setFileInputFiles(tabIdValue, selector, files) {
  await attachDebugger(tabIdValue);
  const target = { tabId: tabIdValue };
  const expression = `document.querySelector(${JSON.stringify(selector)})`;
  const evaluated = await chrome.debugger.sendCommand(target, 'Runtime.evaluate', {
    expression,
    objectGroup: 'klaus-file-upload',
    includeCommandLineAPI: false,
    silent: false,
  });
  const objectId = evaluated?.result?.objectId;
  if (!objectId) {
    throw new BrowserError('not_found', `File input not found: ${selector}`);
  }
  const requested = await chrome.debugger.sendCommand(target, 'DOM.requestNode', { objectId });
  const nodeId = requested?.nodeId;
  if (!nodeId) {
    throw new BrowserError('not_found', `Could not resolve file input node: ${selector}`);
  }
  await chrome.debugger.sendCommand(target, 'DOM.setFileInputFiles', { nodeId, files });
  await chrome.debugger.sendCommand(target, 'Runtime.releaseObject', { objectId }).catch(() => {});
}

async function pageText(tabIdValue) {
  return runInPage(tabIdValue, () => document.body?.innerText || document.documentElement?.innerText || '', []);
}

async function contentFromTab(tabIdValue, contentType, maxChars, selector = null) {
  const normalized = String(contentType || 'text').toLowerCase();
  if (normalized === 'html') {
    return clip(await runInPage(tabIdValue, htmlContentScript, [selector]), maxChars);
  }
  const text = selector
    ? await runInPage(tabIdValue, elementTextScript, [selector])
    : await pageText(tabIdValue);
  return clip(textToMarkdown(text), maxChars);
}

async function ensurePageDialogs(tabIdValue) {
  await attachDebugger(tabIdValue);
  await chrome.debugger.sendCommand({ tabId: tabIdValue }, 'Page.enable').catch(() => {});
}

function rememberRefs(tabIdValue, elements) {
  const refs = new Map();
  const enriched = (elements || []).map((element, index) => {
    const ref = element.ref || `ref_${index + 1}`;
    if (element.selector) refs.set(ref, element.selector);
    return { ref, ...element };
  });
  refsByTab.set(tabIdValue, refs);
  return enriched;
}

function selectorForRef(tabIdValue, ref) {
  if (!ref) return null;
  if (String(ref).startsWith('#') || String(ref).startsWith('.') || String(ref).includes('[')) return ref;
  return refsByTab.get(tabIdValue)?.get(ref) || null;
}

function renderElements(elements, maxChars) {
  return clip((elements || []).map((element) => {
    return `[${element.ref}] ${element.role || element.tagName || 'element'} ${element.name || ''} ${element.text || ''} ${element.selector || ''}`.trim();
  }).join('\n'), maxChars);
}

function scoreElement(element, query) {
  const haystack = [element.ref, element.selector, element.role, element.name, element.text, element.tagName].filter(Boolean).join(' ').toLowerCase();
  if (!haystack) return 0;
  if (haystack.includes(query)) return 50;
  return query.split(/\s+/).filter((part) => haystack.includes(part)).length * 5;
}

async function attachDebugger(tabIdValue) {
  installDebuggerListeners();
  if (attachedDebuggerTabs.has(tabIdValue)) return;
  await chrome.debugger.attach({ tabId: tabIdValue }, DEBUGGER_PROTOCOL_VERSION);
  attachedDebuggerTabs.add(tabIdValue);
}

async function ensureDiagnostics(tabIdValue) {
  await attachDebugger(tabIdValue);
  const state = diagnosticsState(tabIdValue);
  if (state.enabled) return;
  const target = { tabId: tabIdValue };
  await chrome.debugger.sendCommand(target, 'Runtime.enable').catch(() => {});
  await chrome.debugger.sendCommand(target, 'Log.enable').catch(() => {});
  await chrome.debugger.sendCommand(target, 'Network.enable').catch(() => {});
  state.enabled = true;
}

function installDebuggerListeners() {
  if (debuggerListenersInstalled || !chrome.debugger?.onEvent?.addListener) return;
  chrome.debugger.onEvent.addListener(onDebuggerEvent);
  chrome.debugger.onDetach?.addListener?.((source) => {
    const detachedTabId = source?.tabId;
    if (detachedTabId == null) return;
    attachedDebuggerTabs.delete(detachedTabId);
    diagnosticsByTab.delete(detachedTabId);
    dialogsByTab.delete(detachedTabId);
  });
  debuggerListenersInstalled = true;
}

function diagnosticsState(tabIdValue) {
  let state = diagnosticsByTab.get(tabIdValue);
  if (!state) {
    state = {
      enabled: false,
      console: [],
      network: [],
      networkByRequestId: new Map(),
    };
    diagnosticsByTab.set(tabIdValue, state);
  }
  return state;
}

function onDebuggerEvent(source, method, params = {}) {
  const sourceTabId = source?.tabId;
  if (sourceTabId == null) return;
  const state = diagnosticsState(sourceTabId);
  switch (method) {
    case 'Runtime.consoleAPICalled':
      pushLimited(state.console, consoleEntryFromRuntime(params));
      break;
    case 'Log.entryAdded':
      pushLimited(state.console, consoleEntryFromLog(params.entry || {}));
      break;
    case 'Network.requestWillBeSent':
      recordNetworkRequest(state, params);
      break;
    case 'Network.responseReceived':
      recordNetworkResponse(state, params);
      break;
    case 'Network.loadingFinished':
      recordNetworkFinished(state, params);
      break;
    case 'Network.loadingFailed':
      recordNetworkFailed(state, params);
      break;
    case 'Page.javascriptDialogOpening':
      dialogsByTab.set(sourceTabId, {
        type: normalizeDialogType(params.type),
        message: params.message || '',
        defaultValue: params.defaultPrompt || '',
        openedAt: new Date().toISOString(),
      });
      break;
    default:
      break;
  }
}

function consoleEntryFromRuntime(params) {
  return {
    level: normalizeConsoleLevel(params.type),
    text: renderRemoteObjects(params.args || []),
    timestampMs: normalizeTimestampMs(params.timestamp),
    url: params.stackTrace?.callFrames?.[0]?.url || null,
    lineNumber: params.stackTrace?.callFrames?.[0]?.lineNumber ?? null,
    columnNumber: params.stackTrace?.callFrames?.[0]?.columnNumber ?? null,
    source: 'runtime',
  };
}

function consoleEntryFromLog(entry) {
  return {
    level: normalizeConsoleLevel(entry.level),
    text: String(entry.text || ''),
    timestampMs: normalizeTimestampMs(entry.timestamp),
    url: entry.url || null,
    lineNumber: entry.lineNumber ?? null,
    source: entry.source || 'log',
  };
}

function recordNetworkRequest(state, params) {
  const requestId = String(params.requestId || '');
  if (!requestId) return;
  let entry = state.networkByRequestId.get(requestId);
  if (!entry) {
    entry = {
      id: requestId,
      url: params.request?.url || params.documentURL || null,
      method: params.request?.method || null,
      status: null,
      statusText: null,
      mimeType: null,
      requestHeaders: params.request?.headers || null,
      responseHeaders: null,
      type: params.type || null,
      startedAtMs: normalizeTimestampMs(params.wallTime || params.timestamp),
      endedAtMs: null,
      encodedDataLength: null,
      errorText: null,
    };
    state.networkByRequestId.set(requestId, entry);
    pushLimited(state.network, entry);
  } else {
    entry.url = params.request?.url || entry.url;
    entry.method = params.request?.method || entry.method;
    entry.requestHeaders = params.request?.headers || entry.requestHeaders;
  }
}

function recordNetworkResponse(state, params) {
  const entry = networkEntryForUpdate(state, params.requestId);
  if (!entry) return;
  entry.url = params.response?.url || entry.url;
  entry.status = params.response?.status ?? entry.status;
  entry.statusText = params.response?.statusText || entry.statusText;
  entry.mimeType = params.response?.mimeType || entry.mimeType;
  entry.responseHeaders = params.response?.headers || entry.responseHeaders;
  entry.fromDiskCache = params.response?.fromDiskCache ?? false;
  entry.remoteIPAddress = params.response?.remoteIPAddress || null;
  entry.endedAtMs = normalizeTimestampMs(params.timestamp);
}

function recordNetworkFinished(state, params) {
  const entry = networkEntryForUpdate(state, params.requestId);
  if (!entry) return;
  entry.endedAtMs = normalizeTimestampMs(params.timestamp);
  entry.encodedDataLength = params.encodedDataLength ?? entry.encodedDataLength;
}

function recordNetworkFailed(state, params) {
  const entry = networkEntryForUpdate(state, params.requestId);
  if (!entry) return;
  entry.endedAtMs = normalizeTimestampMs(params.timestamp);
  entry.errorText = params.errorText || 'failed';
  entry.canceled = !!params.canceled;
}

function networkEntryForUpdate(state, requestId) {
  const key = String(requestId || '');
  if (!key) return null;
  let entry = state.networkByRequestId.get(key);
  if (!entry) {
    entry = {
      id: key,
      url: null,
      method: null,
      status: null,
      statusText: null,
      mimeType: null,
      requestHeaders: null,
      responseHeaders: null,
      type: null,
      startedAtMs: Date.now(),
      endedAtMs: null,
      encodedDataLength: null,
      errorText: null,
    };
    state.networkByRequestId.set(key, entry);
    pushLimited(state.network, entry);
  }
  return entry;
}

async function withResponseBodies(tabIdValue, entries) {
  const target = { tabId: tabIdValue };
  const enriched = [];
  for (const entry of entries) {
    const copy = { ...entry };
    if (entry.id && entry.status != null) {
      try {
        const body = await chrome.debugger.sendCommand(target, 'Network.getResponseBody', { requestId: entry.id });
        copy.body = body?.body ?? null;
        copy.base64Encoded = !!body?.base64Encoded;
      } catch (_error) {
        copy.bodyUnavailable = true;
      }
    }
    enriched.push(copy);
  }
  return enriched;
}

function pushLimited(entries, entry) {
  entries.push(entry);
  while (entries.length > MAX_DIAGNOSTIC_ENTRIES) {
    entries.shift();
  }
}

function normalizeConsoleLevel(level) {
  const value = String(level || 'log').toLowerCase();
  if (value === 'warn' || value === 'warning') return 'warning';
  if (value === 'error' || value === 'debug' || value === 'info') return value;
  return 'log';
}

function normalizeTimestampMs(value) {
  if (!Number.isFinite(value)) return Date.now();
  return value > 10_000_000_000 ? value : Math.round(value * 1000);
}

function normalizeDialogType(value) {
  const type = String(value || 'alert').toLowerCase();
  if (['alert', 'confirm', 'prompt', 'beforeunload'].includes(type)) return type;
  return 'alert';
}

function renderRemoteObjects(args) {
  return args.map((arg) => {
    if ('value' in arg) return String(arg.value);
    if (arg.description) return String(arg.description);
    if (arg.unserializableValue) return String(arg.unserializableValue);
    return String(arg.type || '');
  }).join(' ');
}

function renderConsoleEntries(entries) {
  return entries.map((entry) => {
    const location = entry.url ? ` ${entry.url}${entry.lineNumber != null ? `:${entry.lineNumber}` : ''}` : '';
    return `[${entry.level}]${location} ${entry.text}`.trim();
  }).join('\n');
}

function renderNetworkEntries(entries) {
  return entries.map((entry) => {
    const status = entry.status == null ? entry.errorText || 'pending' : String(entry.status);
    return `${entry.method || 'GET'} ${status} ${entry.url || ''}`.trim();
  }).join('\n');
}

async function dispatchMouse(tabIdValue, x, y, button, clickCount) {
  if (!Number.isFinite(x) || !Number.isFinite(y)) throw new BrowserError('invalid_request', 'Mouse action requires coordinate.');
  await attachDebugger(tabIdValue);
  const target = { tabId: tabIdValue };
  const btn = button === 'right' ? 'right' : button === 'middle' ? 'middle' : 'left';
  await chrome.debugger.sendCommand(target, 'Input.dispatchMouseEvent', { type: 'mousePressed', x, y, button: btn, clickCount });
  await chrome.debugger.sendCommand(target, 'Input.dispatchMouseEvent', { type: 'mouseReleased', x, y, button: btn, clickCount });
}

async function dispatchMouseMove(tabIdValue, x, y) {
  if (!Number.isFinite(x) || !Number.isFinite(y)) throw new BrowserError('invalid_request', 'Mouse move requires coordinate.');
  await attachDebugger(tabIdValue);
  await chrome.debugger.sendCommand({ tabId: tabIdValue }, 'Input.dispatchMouseEvent', { type: 'mouseMoved', x, y });
}

async function dispatchWheel(tabIdValue, amount, direction) {
  await attachDebugger(tabIdValue);
  const deltaX = direction === 'left' ? -amount : direction === 'right' ? amount : 0;
  const deltaY = direction === 'up' ? -amount : direction === 'down' ? amount : 0;
  await chrome.debugger.sendCommand({ tabId: tabIdValue }, 'Input.dispatchMouseEvent', { type: 'mouseWheel', x: 1, y: 1, deltaX, deltaY });
}

async function pressKey(tabIdValue, key) {
  await attachDebugger(tabIdValue);
  await chrome.debugger.sendCommand({ tabId: tabIdValue }, 'Input.dispatchKeyEvent', { type: 'keyDown', key });
  await chrome.debugger.sendCommand({ tabId: tabIdValue }, 'Input.dispatchKeyEvent', { type: 'keyUp', key });
}

function clip(text, max = 50_000) {
  const value = String(text || '');
  return value.length <= max ? value : `${value.slice(0, max)}\n[truncated]`;
}

function normalizeError(error) {
  return {
    code: error.code || 'host_error',
    message: error.message || 'Chrome extension error.',
    details: error.details || null,
  };
}

class BrowserError extends Error {
  constructor(code, message, details = null) {
    super(message);
    this.code = code;
    this.details = details;
  }
}

function pageSnapshotScript(selector, includeText, includeLinks) {
  const root = selector ? document.querySelector(selector) : document;
  if (!root) return { html: null, text: null, elements: [] };
  const cssEscape = (value) => globalThis.CSS?.escape ? globalThis.CSS.escape(value) : String(value).replace(/[^a-zA-Z0-9_-]/g, '\\$&');
  const selectorFor = (el) => {
    if (el.id) return `#${cssEscape(el.id)}`;
    const name = el.getAttribute('name');
    if (name) return `${el.tagName.toLowerCase()}[name="${name.replaceAll('"', '\\"')}"]`;
    const aria = el.getAttribute('aria-label');
    if (aria) return `${el.tagName.toLowerCase()}[aria-label="${aria.replaceAll('"', '\\"')}"]`;
    return el.tagName.toLowerCase();
  };
  const elements = includeLinks
    ? Array.from(root.querySelectorAll('a,button,input,textarea,select,[role],[aria-label],[contenteditable="true"]')).slice(0, 200).map((el, index) => {
      const rect = el.getBoundingClientRect();
      const style = getComputedStyle(el);
      return {
        ref: `ref_${index + 1}`,
        selector: selectorFor(el),
        role: el.getAttribute('role') || el.tagName.toLowerCase(),
        name: el.getAttribute('aria-label') || el.getAttribute('name') || el.getAttribute('placeholder') || null,
        text: (el.innerText || el.value || el.textContent || '').trim().slice(0, 500) || null,
        tagName: el.tagName.toLowerCase(),
        x: rect.x,
        y: rect.y,
        width: rect.width,
        height: rect.height,
        visible: rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none',
        disabled: Boolean(el.disabled || el.getAttribute('aria-disabled') === 'true'),
        interactive: true,
      };
    })
    : [];
  return { html: root.documentElement ? document.documentElement.outerHTML : root.outerHTML, text: includeText ? document.body?.innerText || '' : null, elements };
}

function domSnapshotScript(visibleOnly, maxChars) {
  if (!visibleOnly) return (document.documentElement?.outerHTML || '').slice(0, maxChars);
  const elements = Array.from(document.querySelectorAll('body *')).slice(0, 500);
  const lines = [];
  for (const el of elements) {
    const rect = el.getBoundingClientRect();
    const style = getComputedStyle(el);
    if (rect.width <= 0 || rect.height <= 0 || style.visibility === 'hidden' || style.display === 'none') continue;
    const label = `${el.tagName.toLowerCase()}${el.id ? `#${el.id}` : ''}`;
    const text = (el.innerText || el.textContent || '').trim().replace(/\s+/g, ' ').slice(0, 120);
    lines.push(`${label}${text ? ` ${text}` : ''}`);
  }
  return lines.join('\n').slice(0, maxChars);
}

function elementInfoScript(selector, x, y) {
  const candidates = selector
    ? Array.from(document.querySelectorAll(selector)).slice(0, 20)
    : [document.elementFromPoint(x || 0, y || 0)].filter(Boolean);
  return candidates.map((el) => {
    const rect = el.getBoundingClientRect();
    return {
      selector: el.id ? `#${globalThis.CSS?.escape ? CSS.escape(el.id) : el.id}` : el.tagName.toLowerCase(),
      tagName: el.tagName.toLowerCase(),
      text: (el.innerText || el.textContent || '').trim(),
      role: el.getAttribute('role'),
      x: rect.x,
      y: rect.y,
      width: rect.width,
      height: rect.height,
      visible: rect.width > 0 && rect.height > 0,
    };
  });
}

function formInputScript(selector, value, options = {}) {
  const el = document.querySelector(selector);
  if (!el) throw new Error(`Element not found: ${selector}`);
  const str = value == null ? '' : String(value);
  if (el instanceof HTMLInputElement && ['checkbox', 'radio'].includes(el.type)) {
    el.checked = Boolean(value);
  } else if (el instanceof HTMLSelectElement) {
    el.value = str;
  } else if ('value' in el) {
    el.value = options.append ? `${el.value || ''}${str}` : str;
  } else {
    el.textContent = options.append ? `${el.textContent || ''}${str}` : str;
  }
  el.dispatchEvent(new InputEvent('input', { bubbles: true, inputType: 'insertText', data: str }));
  el.dispatchEvent(new Event('change', { bubbles: true }));
  return true;
}

function clickSelectorScript(selector) {
  const el = document.querySelector(selector);
  if (!el) throw new Error(`Element not found: ${selector}`);
  el.click();
  return true;
}

function scrollToScript(selector) {
  const el = document.querySelector(selector);
  if (!el) throw new Error(`Element not found: ${selector}`);
  el.scrollIntoView({ block: 'center', inline: 'center' });
  return true;
}

function evalScript(source, args) {
  try {
    return Function('args', `return (${source});`)(args);
  } catch (_expressionError) {
    return Function('args', source)(args);
  }
}

function locatorScript(selector, action, value, options) {
  const all = Array.from(document.querySelectorAll(selector));
  const el = all[0];
  if (!el && !['count', 'all_text_contents'].includes(action)) throw new Error(`Element not found: ${selector}`);
  switch (action) {
    case 'count':
      return all.length;
    case 'text':
    case 'text_content':
      return el.textContent;
    case 'inner_text':
      return el.innerText || el.textContent;
    case 'get_attribute':
      return el.getAttribute(String(value || ''));
    case 'all_text_contents':
      return all.map((item) => item.textContent);
    case 'is_enabled':
      return !(el.disabled || el.getAttribute('aria-disabled') === 'true');
    case 'is_visible': {
      const rect = el.getBoundingClientRect();
      const style = getComputedStyle(el);
      return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
    }
    case 'click':
    case 'double_click':
      el.click();
      return true;
    case 'fill':
      return formInputScript(selector, value, { append: false });
    case 'type':
      return formInputScript(selector, value, { append: true });
    case 'press':
      el.dispatchEvent(new KeyboardEvent('keydown', { key: String(value || 'Enter'), bubbles: true }));
      el.dispatchEvent(new KeyboardEvent('keyup', { key: String(value || 'Enter'), bubbles: true }));
      return true;
    case 'wait_for':
      return true;
    case 'check':
      el.checked = true;
      el.dispatchEvent(new Event('change', { bubbles: true }));
      return true;
    case 'uncheck':
      el.checked = false;
      el.dispatchEvent(new Event('change', { bubbles: true }));
      return true;
    case 'set_checked':
      el.checked = Boolean(value);
      el.dispatchEvent(new Event('change', { bubbles: true }));
      return true;
    case 'select_option':
      el.value = Array.isArray(value) ? value[0] : value;
      el.dispatchEvent(new Event('change', { bubbles: true }));
      return el.value;
    default:
      throw new Error(`Unsupported locator action: ${action}`);
  }
}

function shortcutExecuteScript(name, shortcut, waitForCompletion) {
  globalThis.dispatchEvent?.(
    new CustomEvent('klaus-shortcut-execute', {
      detail: { name, shortcut, waitForCompletion },
    }),
  );
  return true;
}

function minimalGifDataUrl() {
  return 'data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==';
}

function clipboardReadTextScript() {
  return navigator.clipboard?.readText?.() || '';
}

function clipboardWriteTextScript(text) {
  return navigator.clipboard?.writeText?.(text);
}

function htmlContentScript(selector) {
  const root = selector ? document.querySelector(selector) : document.documentElement;
  return root ? root.outerHTML : '';
}

function elementTextScript(selector) {
  const root = selector ? document.querySelector(selector) : document.body || document.documentElement;
  return root?.innerText || root?.textContent || '';
}

function mediaUrlForSelectorScript(selector) {
  const el = document.querySelector(selector);
  if (!el) return null;
  return mediaUrlForElement(el);
}

function mediaUrlForPointScript(x, y) {
  const el = document.elementFromPoint(x, y);
  return el ? mediaUrlForElement(el) : null;
}

function mediaUrlForElement(el) {
  const target = el.closest?.('a[href]') || el;
  return target.href || target.src || target.currentSrc || target.getAttribute?.('href') || target.getAttribute?.('src') || null;
}

function textToMarkdown(text) {
  return String(text || '')
    .split('\n')
    .map((line) => line.trimEnd())
    .join('\n')
    .trim();
}

function clipboardItemsForText(text) {
  return [
    {
      entries: [
        {
          type: 'text/plain',
          text,
        },
      ],
      presentationStyle: 'inline',
    },
  ];
}

function normalizeClipboardItems(items) {
  return Array.isArray(items) ? items : [];
}

function clipboardTextFromItems(items) {
  if (!Array.isArray(items)) return null;
  for (const item of items) {
    for (const entry of item.entries || []) {
      if (typeof entry.text === 'string') return entry.text;
      if (typeof entry.value === 'string') return entry.value;
    }
  }
  return null;
}
