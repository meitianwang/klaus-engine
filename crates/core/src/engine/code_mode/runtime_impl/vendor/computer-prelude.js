// Unified computer-use control surface for the REPL context (project-authored
// companion; no third-party code).
//
// Layers one hierarchical `computer` global over the session's bound host
// tools: a cross-surface inventory (native apps + connected browsers with
// their tabs), app handles for window-level control, a desktop handle for the
// full-desktop surface, and tab handles that route browser targets through the
// browser channel (element-index accessibility actions, tab @-mention
// resolution). Underlying tool functions are resolved at call time, so tools
// that are bound, rebound or loaded later in the session work the same as ones
// present from the start. When a surface is unbound, every helper fails with
// an actionable message instead of a TypeError, and the inventory isolates a
// failing surface into `errors` without dropping the other one.
(function () {
  if (globalThis.__klausComputerPreludeInstalled) return;
  Object.defineProperty(globalThis, '__klausComputerPreludeInstalled', {
    value: true, configurable: true, writable: true, enumerable: false,
  });

  const CU_PREFIX = 'mcp__computer_use__';
  const BROWSER_HOST_PREFIX = 'mcp__klaus_browser__';
  const BROWSER_NATIVE_PREFIX = 'browser_';

  function cuOp(name) {
    const fn = globalThis[CU_PREFIX + name];
    if (typeof fn !== 'function') {
      throw new Error(
        `Computer-use operation "${name}" is not bound in this REPL session. The computer-use ` +
        'surface is unavailable (it is disabled for this session, or the tool is deferred - ' +
        'load it with tool_search first).');
    }
    return fn;
  }

  function browserOp(name) {
    let fn = globalThis[BROWSER_HOST_PREFIX + name];
    if (typeof fn !== 'function') fn = globalThis[BROWSER_NATIVE_PREFIX + name];
    if (typeof fn !== 'function') {
      throw new Error(
        `Browser operation "${name}" is not bound in this REPL session. The host browser ` +
        'tools are unavailable (the browser surface is off, or the tool is deferred - ' +
        'load it with tool_search first).');
    }
    return fn;
  }

  // Merge argument objects left to right, skipping undefined and null so an
  // omitted sugar parameter never reaches the wire as a literal null.
  function merge() {
    const out = {};
    for (const part of arguments) {
      if (!part) continue;
      for (const key of Object.keys(part)) {
        if (part[key] !== undefined && part[key] !== null) out[key] = part[key];
      }
    }
    return out;
  }

  // Host browser operations answer with a { value } envelope inside their
  // parsed JSON result; native app tools answer with plain text or bare
  // objects. Unwrap defensively so either shape reaches callers intact.
  function unwrap(result) {
    if (result && typeof result === 'object' && !Array.isArray(result) && 'value' in result) return result.value;
    return result;
  }

  function reasonText(reason) {
    if (reason instanceof Error) return reason.message;
    if (typeof reason === 'string') return reason;
    try { return JSON.stringify(reason); } catch { return String(reason); }
  }

  const DOCS = [
    'computer - unified control surface for native apps and browser tabs.',
    '',
    'Inventory:',
    '  computer.getState()              One inventory across surfaces: {apps, browsers, errors?}.',
    '                                   apps: the native app list, as the app surface reports it.',
    '                                   browsers: connected browser surfaces, each with its tab list',
    '                                   ({id, title, url, active?}) when the host provides one.',
    '                                   A surface that fails or is unbound lands in errors without',
    '                                   dropping the other surface.',
    '',
    'Native apps (window-level control):',
    '  computer.getApp(app)             Handle for one app (display name, exact path, or bundle id).',
    '    app.state(opts?)               Settled screenshot + accessibility state. opts: {disable_diff}.',
    '    app.click(params)              {element_index} or {x, y}; plus mouse_button, click_count,',
    '                                   key (held combo), duration (hold seconds).',
    '    app.type(text)                 Type literal text into the app.',
    '    app.key(key, opts?)            Keysym chord such as \'Return\' or \'super+c\'. opts: {duration}.',
    '    app.paste(text, opts?)         Clipboard paste without submitting. opts: {format:\'text\'|\'md\'|\'html\'}.',
    '    app.scroll(params)             {direction, pages?, element_index? | x?, y?}.',
    '    app.drag(params)               {from_x, from_y, to_x, to_y}.',
    '    app.setValue(index, value)     Set an exposed value by element index.',
    '    app.selectText(index, text, opts?)   opts: {prefix, suffix, selection}.',
    '    app.secondaryAction(index, name)     Only names the element actually exposes.',
    '  computer.desktop                 Full-desktop surface (Windows; lazy - unbound elsewhere).',
    '    .screenshot()                  One stitched screenshot; coordinates are its pixels.',
    '    .click({x, y}) / .move({x, y}) / .scroll({x, y, delta_x, delta_y})',
    '    .type(text) / .key(key, opts?) / .paste(text, opts?)',
    '    .drag(path)                    Multi-point path of {x, y}.',
    '    .dragHandle(op, {x, y})        Hold-move-release across calls: \'start\' / \'move_to\' / \'end\'.',
    '',
    'Browser tabs (DOM-level control):',
    '  computer.getTab(target, opts?)   Tab handle with element-index actions under tab.ax.*',
    '                                   (click/type/setValue/paste/key/scroll/selectText/',
    '                                   secondaryAction/drag) plus tab.content, tab.clipboard,',
    '                                   tab.dev and the page operations.',
    '                                   target: a tab @-mention URI (pass the complete plugin://',
    '                                   URL from the user message verbatim), {url}, or a tab id.',
    '                                   opts: {browser: browserId}.',
    '  computer.getBrowser(id)          Browser handle: {id, tabs(), select()}.',
    '  The `browser` global carries the session-wide page surface (navigate, find, eval,',
    '  content, dev, tabs, ...); computer.getTab handles add browser scoping.',
    '',
    'Discipline:',
    '  - Drive web content through the browser channel (element indices, real browser input)',
    '    instead of pixel-clicking a browser window.',
    '  - Indices belong to the state that returned them; re-read state after any action.',
    '  - A stale tab mention must be re-selected by the user; never substitute another tab.',
    '  - Flat tool functions remain available: mcp__computer_use__<tool>({...}) and',
    '    mcp__klaus_browser__<operation>({...}).',
  ].join('\n');

  let noted = false;
  function note() {
    if (noted) return;
    noted = true;
    console.log('computer API: computer.getState() inventories native apps and browser tabs; '
      + 'computer.getApp(name) and computer.getTab(mention|url|tabId) return control handles; '
      + 'full reference: computer.docs()');
  }

  // ---- inventory ------------------------------------------------------------

  async function getState() {
    note();
    const [appsResult, browsersResult] = await Promise.allSettled([
      (async () => cuOp('list_apps')({}))(),
      (async () => browserOp('list_connected_browsers')({ tabs: true }))(),
    ]);
    const errors = [];
    if (appsResult.status === 'rejected') errors.push(`Native apps: ${reasonText(appsResult.reason)}`);
    if (browsersResult.status === 'rejected') errors.push(`Browsers: ${reasonText(browsersResult.reason)}`);
    const apps = appsResult.status === 'fulfilled' ? appsResult.value : [];
    let browsers = [];
    if (browsersResult.status === 'fulfilled') {
      const payload = unwrap(browsersResult.value);
      if (payload && typeof payload === 'object' && Array.isArray(payload.browsers)) browsers = payload.browsers;
      if (payload && typeof payload === 'object' && Array.isArray(payload.errors)) {
        for (const entry of payload.errors) {
          const row = entry && typeof entry === 'object' ? entry : { message: reasonText(entry) };
          errors.push(`Browsers: ${row.browserId !== undefined ? `${row.browserId}: ` : ''}${reasonText(row.message ?? row)}`);
        }
      }
    }
    return { apps, browsers, ...(errors.length ? { errors } : {}) };
  }

  // ---- native app handles ---------------------------------------------------

  function getApp(app) {
    if ((typeof app !== 'string' && typeof app !== 'number') || String(app).trim() === '') {
      throw new Error('computer.getApp(app) requires an app display name, exact path, or bundle id.');
    }
    const target = String(app);
    const scope = (name, args) => { note(); return cuOp(name)(merge({ app: target }, args)); };
    return {
      app: target,
      state: (opts) => scope('get_app_state', opts),
      click: (params) => scope('click', params),
      type: (text, params) => scope('type_text', merge({ text }, params)),
      key: (key, params) => scope('press_key', merge({ key }, params)),
      paste: (text, params) => scope('paste', merge({ text }, params)),
      scroll: (params) => scope('scroll', params),
      drag: (params) => scope('drag', params),
      setValue: (elementIndex, value, params) => scope('set_value', merge({ element_index: elementIndex, value }, params)),
      selectText: (elementIndex, text, params) => scope('select_text', merge({ element_index: elementIndex, text }, params)),
      secondaryAction: (elementIndex, action, params) => scope('perform_secondary_action', merge({ element_index: elementIndex, action }, params)),
    };
  }

  function makeDesktop() {
    const scope = (name, args) => { note(); return cuOp(name)(args || {}); };
    return {
      screenshot: (params) => scope('get_desktop_screenshot', params),
      click: (params) => scope('desktop_click', params),
      move: (params) => scope('desktop_move', params),
      scroll: (params) => scope('desktop_scroll', params),
      type: (text, params) => scope('desktop_type_text', merge({ text }, params)),
      key: (key, params) => scope('desktop_press_key', merge({ key }, params)),
      paste: (text, params) => scope('desktop_paste', merge({ text }, params)),
      drag: (path, params) => scope('desktop_drag', merge({ path }, params)),
      dragHandle: (op, params) => scope('desktop_drag_handle', merge({ op }, params)),
    };
  }

  // ---- browser tab handles --------------------------------------------------

  function tabHandle(tabId, browserId, meta) {
    const base = globalThis.browser;
    if (!base || typeof base.getTab !== 'function') {
      throw new Error(
        'The browser control surface is not installed in this REPL session; ' +
        'computer.getTab cannot build a tab handle.');
    }
    const scoped = browserId === undefined ? base.getTab(tabId) : base.getTab(tabId, { browserId });
    return Object.assign(scoped, {
      browserId: browserId === undefined ? null : browserId,
      ...(meta.title !== undefined ? { title: meta.title } : {}),
      ...(meta.url !== undefined ? { url: meta.url } : {}),
      ...(meta.mention !== undefined ? { mention: meta.mention } : {}),
    });
  }

  async function inventoryBrowsers() {
    const payload = unwrap(await browserOp('list_connected_browsers')({ tabs: true }));
    return payload && typeof payload === 'object' && Array.isArray(payload.browsers) ? payload.browsers : [];
  }

  async function getTab(target, opts) {
    note();
    const wanted = opts && opts.browser !== undefined && opts.browser !== null ? String(opts.browser)
      : target && typeof target === 'object' && target.browser !== undefined && target.browser !== null ? String(target.browser)
      : undefined;
    const mention = typeof target === 'string' && target.startsWith('plugin://') ? target
      : target && typeof target === 'object' && typeof target.mention === 'string' ? target.mention
      : null;
    if (mention) {
      const resolved = unwrap(await browserOp('resolve_tab')({ mention }));
      if (!resolved || typeof resolved !== 'object' || resolved.tabId === undefined || resolved.browserId === undefined) {
        throw new Error('resolve_tab returned an unexpected result; the tab mention could not be resolved.');
      }
      return tabHandle(resolved.tabId, String(resolved.browserId), {
        ...(resolved.title !== undefined ? { title: resolved.title } : {}),
        ...(resolved.url !== undefined ? { url: resolved.url } : {}),
        mention,
      });
    }
    const url = target && typeof target === 'object' && typeof target.url === 'string' ? target.url : null;
    if (url) {
      const browsers = await inventoryBrowsers();
      for (const browser of browsers) {
        const id = String(browser.browserId ?? browser.id ?? '');
        if (wanted !== undefined && id !== wanted) continue;
        const tabs = Array.isArray(browser.tabs) ? browser.tabs : [];
        for (const tab of tabs) {
          if (tab && typeof tab === 'object' && tab.url === url) {
            return tabHandle(tab.id, id, {
              ...(tab.title !== undefined ? { title: tab.title } : {}),
              url,
            });
          }
        }
      }
      throw new Error(
        `No open tab matches the URL ${JSON.stringify(url)}` +
        `${wanted !== undefined ? ` in browser ${JSON.stringify(wanted)}` : ''}. ` +
        'Call computer.getState() for the current tab inventory; never navigate another tab as a substitute.');
    }
    let rawId = target && typeof target === 'object' ? target.tabId : target;
    if (typeof rawId === 'string' && /^[1-9][0-9]*$/.test(rawId)) rawId = Number(rawId);
    if (typeof rawId !== 'number' || !Number.isSafeInteger(rawId) || rawId < 1) {
      throw new Error(
        'computer.getTab expects a tab @-mention URI (the complete plugin:// URL), {url}, ' +
        'or a positive integer tab id with optional {browser: browserId}.');
    }
    return tabHandle(rawId, wanted, {});
  }

  function getBrowser(id) {
    if ((typeof id !== 'string' && typeof id !== 'number') || String(id).trim() === '') {
      throw new Error('computer.getBrowser(id) requires a browser id from computer.getState().');
    }
    const target = String(id);
    return {
      id: target,
      tabs: async () => {
        note();
        const browsers = await inventoryBrowsers();
        const found = browsers.find(entry => String(entry.browserId ?? entry.id ?? '') === target);
        if (!found) {
          throw new Error(`Browser ${JSON.stringify(target)} is not connected. Call computer.getState() for the current inventory.`);
        }
        return Array.isArray(found.tabs) ? found.tabs : [];
      },
      select: () => { note(); return browserOp('select_browser')({ browserId: target }); },
    };
  }

  const computer = {
    docs: () => { note(); return DOCS; },
    getState,
    getApp,
    getTab,
    getBrowser,
    desktop: makeDesktop(),
  };
  Object.defineProperty(globalThis, 'computer', {
    value: computer, configurable: true, writable: true, enumerable: true,
  });
})();
