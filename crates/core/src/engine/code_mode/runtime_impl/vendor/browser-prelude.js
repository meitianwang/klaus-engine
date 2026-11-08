// Browser control surface for the REPL context (project-authored companion;
// no third-party code).
//
// Wraps the session's bound host browser tool functions in a hierarchical
// API: a `browser` global for session-wide operations and persistent tab
// handles for per-tab work. Underlying tool functions are resolved at call
// time, so tools that are bound, rebound or loaded later in the session work
// the same as ones present from the start. When no browser tool is bound,
// every helper fails with an actionable message instead of a TypeError.
(function () {
  if (globalThis.__klausBrowserPreludeInstalled) return;
  Object.defineProperty(globalThis, '__klausBrowserPreludeInstalled', {
    value: true, configurable: true, writable: true, enumerable: false,
  });

  const HOST_PREFIX = 'mcp__klaus_browser__';
  const NATIVE_PREFIX = 'browser_';

  function op(name) {
    let fn = globalThis[HOST_PREFIX + name];
    if (typeof fn !== 'function') fn = globalThis[NATIVE_PREFIX + name];
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

  const DOCS = [
    'browser - hierarchical browser control for the REPL.',
    '',
    'Observe (accessibility-first):',
    '  browser.getState(opts?)          Accessibility text of the active tab with integer element',
    '                                   indices. Defaults to a diff from the previous state (~ changed,',
    '                                   + added, removed ID ranges).',
    "                                   opts: {mode:'state'|'screenshot'|'both', disableDiffing, fast, maxChars}.",
    '  browser.screenshot(opts?)        JPEG capture - proof of what the user would see.',
    '  ax.write(opts?)                  getState and echo the state to the transcript output',
    '                                   (available as browser.ax.write and tab.ax.write).',
    '',
    'Act (by element index from the latest state):',
    '  browser.getTab(tabId?)           Persistent handle for one tab (omitted: the active tab).',
    '  tab.ax.click(index, opts?)       Real input click. opts: {clickCount, mouseButton, key (held',
    "                                   combo such as 'shift+a'), duration (hold seconds 0-30), timeoutMs}.",
    '  tab.ax.type(index, text)         Type into an element; index omitted types into the focused element.',
    '  tab.ax.setValue(index, value)    Set an input/textarea/select/contenteditable value directly.',
    "  tab.ax.paste(text, opts?)        Virtual-clipboard paste. opts: {format:'text'|'md'|'html', elementIndex}.",
    "  tab.ax.key(key, opts?)           xdotool-style key or combo: 'Return', 'super+c', 'KP_0'.",
    '                                   opts: {duration, elementIndex}.',
    "  tab.ax.scroll(opts)              {elementIndex or point:[x,y], direction:'up'|'down'|'left'|'right', pages}.",
    "  tab.ax.selectText(index, text, opts?)  {prefix, suffix, selectionType:'text'|'cursor_before'|'cursor_after'}.",
    "  tab.ax.secondaryAction(index, name)    Only names the state exposes, e.g. 'Scroll to Visible'.",
    '  tab.ax.drag(point, to)           Drag [x,y] to [x,y].',
    '  browser.action(name, params) / tab.ax.action(name, params)   Generic escape hatch.',
    '',
    'Session-wide variants act on the active tab; every one also exists on tab handles:',
    '  browser.navigate(url) / back() / forward() / reload()',
    '  browser.click(opts) / type(text, opts) / find(query) / eval(script) / waitFor(opts)',
    '  browser.content.text() / .read() / .snapshot() / .export(opts)',
    '  browser.content.youtubeTranscript(opts?)  Caption transcript of the current YouTube watch',
    '                                   page; opts: {language}. Also on tab.content.',
    '  browser.clipboard.read() / .write(text)',
    '  browser.dev.console(opts) / .network(opts) / .cdp(method, params)',
    '  browser.tabs.list() / .new(url) / .select(tabId) / .close(opts) / .selected()',
    '  browser.capabilities() / browser.listBrowsers() / browser.selectBrowser(opts)',
    '',
    'Discipline:',
    '  - Indices belong to the state that returned them. After any action or navigation,',
    '    call getState() again instead of reusing old indices.',
    '  - Batch independent steps into one REPL call; keep dependent steps sequential.',
    '  - Do not sleep: the runtime waits for the page to settle before capturing.',
    '  - A failed action returns the latest accessibility state with the error;',
    '    re-derive indices from it instead of retrying blindly.',
    '  - The flat tool functions remain available: mcp__klaus_browser__<operation>({...}).',
  ].join('\n');

  let noted = false;
  function note() {
    if (noted) return;
    noted = true;
    console.log('browser API: browser.getState() observes the page with element indices; '
      + 'browser.getTab(id) returns a tab handle with tab.ax.click(index)/type/scroll and more; '
      + 'full reference: browser.docs()');
  }

  function call(name, args) {
    note();
    return op(name)(args || {});
  }

  function contentGroup(scope) {
    return {
      text: (opts) => scope('page_text', opts),
      read: (opts) => scope('read_page', opts),
      snapshot: (opts) => scope('snapshot', opts),
      export: (opts) => scope('content_export', opts),
      youtubeTranscript: (opts) => scope('content_export', merge({ format: 'youtube_transcript' }, opts)),
    };
  }

  function clipboardGroup(scope) {
    return {
      read: (opts) => scope('clipboard', merge({ action: 'read' }, opts)),
      write: (text, opts) => scope('clipboard', merge({ action: 'write', text }, opts)),
    };
  }

  function devGroup(scope) {
    return {
      console: (opts) => scope('console', opts),
      network: (opts) => scope('network', opts),
      cdp: (method, params, opts) => scope('cdp', merge({ method, params }, opts)),
    };
  }

  function axGroup(scope) {
    return {
      getState: (opts) => scope('ax_state', opts),
      write: async (opts) => {
        const state = await scope('ax_state', opts);
        console.log(typeof state === 'string' ? state : JSON.stringify(state, null, 2));
        return state;
      },
      action: (action, params) => scope('ax_action', merge({ action }, params)),
      click: (elementIndex, opts) => scope('ax_action', merge({ action: 'click', elementIndex }, opts)),
      type: (elementIndex, text, opts) => scope('ax_action', merge({ action: 'type_text', elementIndex, text }, opts)),
      setValue: (elementIndex, value, opts) => scope('ax_action', merge({ action: 'set_value', elementIndex, value }, opts)),
      paste: (text, opts) => scope('ax_action', merge({ action: 'paste', text }, opts)),
      key: (key, opts) => scope('ax_action', merge({ action: 'press_key', key }, opts)),
      scroll: (opts) => scope('ax_action', merge({ action: 'scroll' }, opts)),
      selectText: (elementIndex, text, opts) => scope('ax_action', merge({ action: 'select_text', elementIndex, text }, opts)),
      secondaryAction: (elementIndex, name, opts) => scope('ax_action', merge({ action: 'secondary_action', elementIndex, name }, opts)),
      drag: (point, to, opts) => scope('ax_action', merge({ action: 'drag', point, to }, opts)),
    };
  }

  // Operations that exist both session-wide (active tab) and per tab handle.
  function pageGroup(scope) {
    return {
      navigate: (url, opts) => scope('navigate', merge({ url }, opts)),
      back: (opts) => scope('back', opts),
      forward: (opts) => scope('forward', opts),
      reload: (opts) => scope('reload', opts),
      eval: (script, opts) => scope('eval', merge({ script }, opts)),
      screenshot: (opts) => scope('screenshot', opts),
      find: (query, opts) => scope('find', merge({ query }, opts)),
      click: (opts) => scope('click', opts),
      type: (text, opts) => scope('type', merge({ text }, opts)),
      formInput: (fields, opts) => scope('form_input', merge({ fields }, opts)),
      selectOption: (opts) => scope('select_option', opts),
      setChecked: (checked, opts) => scope('set_checked', merge({ checked }, opts)),
      computer: (action, opts) => scope('computer', merge({ action }, opts)),
      waitFor: (opts) => scope('wait_for', opts),
      locator: (opts) => scope('locator', opts),
      elementInfo: (opts) => scope('element_info', opts),
      download: (opts) => scope('download', opts),
      upload: (files, opts) => scope('file_upload', merge({ files }, opts)),
      dialog: (opts) => scope('dialog', opts),
      permission: (opts) => scope('permission', opts),
      resize: (opts) => scope('resize', opts),
      visibility: (opts) => scope('visibility', opts),
      pageAssets: (opts) => scope('page_assets', opts),
      webmcp: (opts) => scope('webmcp', opts),
      batch: (operations, opts) => scope('batch', merge({ operations }, opts)),
    };
  }

  function makeTab(tabId) {
    const scope = (name, args) => call(name, merge(args, tabId === undefined ? null : { tabId }));
    const tab = {
      id: tabId === undefined ? null : tabId,
      ax: axGroup(scope),
      content: contentGroup(scope),
      clipboard: clipboardGroup(scope),
      dev: devGroup(scope),
    };
    return Object.assign(tab, pageGroup(scope));
  }

  const sessionScope = (name, args) => call(name, args);
  const browser = {
    docs: () => { note(); return DOCS; },
    getState: (opts) => call('ax_state', opts),
    action: (action, params) => call('ax_action', merge({ action }, params)),
    getTab: (tabId) => makeTab(tabId),
    tabs: {
      list: () => call('tabs', { action: 'list' }),
      new: (url, opts) => call('new_tab', merge(url ? { url } : null, opts)),
      select: (tabId) => call('tabs', { action: 'select', tabId }),
      selected: () => call('tabs', { action: 'selected' }),
      close: (opts) => call('close', opts),
    },
    capabilities: () => call('capabilities', {}),
    listBrowsers: () => call('list_connected_browsers', {}),
    selectBrowser: (opts) => call('select_browser', opts),
    ax: axGroup(sessionScope),
    content: contentGroup(sessionScope),
    clipboard: clipboardGroup(sessionScope),
    dev: devGroup(sessionScope),
  };
  Object.assign(browser, pageGroup(sessionScope));
  Object.defineProperty(globalThis, 'browser', {
    value: browser, configurable: true, writable: true, enumerable: true,
  });
})();
