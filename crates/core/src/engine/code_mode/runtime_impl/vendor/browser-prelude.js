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
    '  browser.getState(mode?, opts?)   Accessibility text of the active tab with integer element',
    '                                   indices. Defaults to a diff from the previous state (~ changed,',
    '                                   + added, removed ID ranges).',
    "                                   mode: 'state'|'screenshot'|'both'; opts: {disableDiffing, fast, maxChars}.",
    '  browser.screenshot(opts?)        JPEG capture - proof of what the user would see.',
    '  ax.write(mode?, opts?)           Capture state and print it to stdout without duplicating',
    '                                   it in the completion value (on browser.ax and tab.ax).',
    '  ax.get(mode?, opts?)             Return the state value without printing to stdout.',
    '',
    'Act (by element index from the latest state):',
    '  browser.getTab(tabId?, opts?)    Persistent handle for one tab (omitted: the active tab).',
    '                                   opts: {browserId} scopes every call to one connected browser.',
    '  tab.ax.click(index, opts?)       Real input click on an element index or [x, y] point.',
    "                                   opts: {clickCount, mouseButton, key ('super', 'shift+a'), duration (0-30), timeoutMs}.",
    '  tab.ax.typeText(index, text)     Type into an element; null/omitted index types into the focused element.',
    '                                   Alias: tab.ax.type(index, text).',
    '  tab.ax.setValue(index, value)    Set an input/textarea/select/checkbox/radio/contenteditable value.',
    "  tab.ax.paste(index, text, opts?) Virtual-clipboard paste (or paste(text, opts)); null index uses focus.",
    "                                   opts: {format:'text'|'md'|'html', elementIndex}.",
    "  tab.ax.pressKey(index, key, opts?) xdotool-style key or combo ('Return', 'super+c', 'KP_0'); null index",
    '                                   uses focus. Alias: tab.ax.key(key, opts?). opts: {duration, elementIndex}.',
    "  tab.ax.scroll(target, dir, pages?) Scroll an element index or [x,y] point ('up'|'down'|'left'|'right'),",
    '                                   or pass an options object {elementIndex|point, direction, pages}.',
    "  tab.ax.selectText(index, text, opts?)  {prefix, suffix, selectionType:'text'|'cursor_before'|'cursor_after'}.",
    "  tab.ax.performSecondaryAction(index, name)  Only names the state exposes ('Expand', 'Collapse',",
    "                                   'Scroll to Visible'). Alias: tab.ax.secondaryAction(index, name).",
    '  tab.ax.drag(point, to)           Drag [x,y] to [x,y].',
    '  browser.action(name, params) / tab.ax.action(name, params)   Generic escape hatch.',
    '',
    'Session-wide variants act on the active tab; every one also exists on tab handles:',
    '  browser.navigate(url) / goto(url) / back() / forward() / reload()',
    '  browser.click(opts) / type(text, opts) / find(query) / eval(script) / waitFor(opts)',
    '  browser.content.text() / .read() / .snapshot() / .export(opts) / .exportGsuite(type)',
    '  browser.content.youtubeTranscript(opts?) / .exportYouTubeTranscript(opts?)',
    '  browser.clipboard.read() / .readText() / .write(text) / .writeText(text)',
    '  browser.dev.console(opts) / .logs(opts) / .network(opts) / .cdp(method, params)',
    '  browser.tabs.list() / .new(url) / .get(tabId) / .select(tabId) / .close(opts) / .selected() / .content(opts)',
    '  browser.capabilities() / browser.listBrowsers() / browser.selectBrowser(opts) / browser.nameSession(name)',
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
    const youtubeTranscript = (opts) => scope('content_export', merge({ format: 'youtube_transcript' }, opts));
    return {
      text: (opts) => scope('page_text', opts),
      read: (opts) => scope('read_page', opts),
      snapshot: (opts) => scope('snapshot', opts),
      export: (opts) => scope('content_export', opts),
      exportGsuite: (gsuite, opts) => scope('content_export', merge({ gsuite }, opts)),
      youtubeTranscript,
      exportYouTubeTranscript: youtubeTranscript,
    };
  }

  function clipboardGroup(scope) {
    const read = (opts) => scope('clipboard', merge({ action: 'read' }, opts));
    const write = (text, opts) => scope('clipboard', merge({ action: 'write', text }, opts));
    return {
      read,
      readText: read,
      write,
      writeText: write,
    };
  }

  function devGroup(scope) {
    const consoleLogs = (opts) => scope('console', opts);
    return {
      console: consoleLogs,
      logs: consoleLogs,
      network: (opts) => scope('network', opts),
      cdp: (method, params, opts) => scope('cdp', merge({ method, params }, opts)),
    };
  }

  function axArgs(modeOrOpts, maybeOpts) {
    if (typeof modeOrOpts === 'string') return merge({ mode: modeOrOpts }, maybeOpts);
    return merge(modeOrOpts, maybeOpts);
  }

  function axGroup(scope) {
    const getState = (modeOrOpts, maybeOpts) => scope('ax_state', axArgs(modeOrOpts, maybeOpts));
    const typeText = (first, second, third) => {
      if (typeof first === 'string' && (second === undefined || (second !== null && typeof second === 'object'))) {
        return scope('ax_action', merge({ action: 'type_text', text: first }, second));
      }
      return scope('ax_action', merge({ action: 'type_text', elementIndex: first, text: second }, third));
    };
    const pressKey = (first, second, third) => {
      if ((typeof first === 'number' || first === null || first === undefined) && typeof second === 'string') {
        return scope('ax_action', merge({ action: 'press_key', elementIndex: first, key: second }, third));
      }
      return scope('ax_action', merge({ action: 'press_key', key: first }, second));
    };
    const paste = (first, second, third) => {
      if ((typeof first === 'number' || first === null || first === undefined) && typeof second === 'string') {
        return scope('ax_action', merge({ action: 'paste', elementIndex: first, text: second }, third));
      }
      return scope('ax_action', merge({ action: 'paste', text: first }, second));
    };
    const scroll = (first, direction, pages, opts) => {
      if (first && typeof first === 'object' && !Array.isArray(first)) {
        return scope('ax_action', merge({ action: 'scroll' }, first));
      }
      const targetPart = Array.isArray(first) ? { point: first } : { elementIndex: first };
      const pagesPart = typeof pages === 'number' ? { pages } : pages;
      return scope('ax_action', merge({ action: 'scroll', direction }, targetPart, pagesPart, opts));
    };
    const secondaryAction = (elementIndex, name, opts) =>
      scope('ax_action', merge({ action: 'secondary_action', elementIndex, name }, opts));
    return {
      getState,
      get: (modeOrOpts, maybeOpts) => scope('ax_state', merge(axArgs(modeOrOpts, maybeOpts), { emitImage: false })),
      write: async (modeOrOpts, maybeOpts) => {
        const args = axArgs(modeOrOpts, maybeOpts);
        const state = await scope('ax_state', args);
        if (args.mode !== 'screenshot') {
          console.log(typeof state === 'string' ? state : JSON.stringify(state, null, 2));
        }
      },
      action: (action, params) => scope('ax_action', merge({ action }, params)),
      click: (target, opts) => {
        if (Array.isArray(target)) return scope('ax_action', merge({ action: 'click', point: target }, opts));
        if (target && typeof target === 'object') return scope('ax_action', merge({ action: 'click' }, target, opts));
        return scope('ax_action', merge({ action: 'click', elementIndex: target }, opts));
      },
      type: typeText,
      typeText,
      setValue: (elementIndex, value, opts) => scope('ax_action', merge({ action: 'set_value', elementIndex, value }, opts)),
      paste,
      key: pressKey,
      pressKey,
      scroll,
      selectText: (elementIndex, text, opts) => scope('ax_action', merge({ action: 'select_text', elementIndex, text }, opts)),
      secondaryAction,
      performSecondaryAction: secondaryAction,
      drag: (point, to, opts) => scope('ax_action', merge({ action: 'drag', point, to }, opts)),
    };
  }

  function cuaGroup(scope) {
    const doubleClick = (opts) => scope('computer', merge({ action: 'double_click' }, opts));
    return {
      click: (opts) => scope('computer', merge({ action: 'click' }, opts)),
      double_click: doubleClick,
      doubleClick,
      move: (opts) => scope('computer', merge({ action: 'mouse_move' }, opts)),
      drag: (opts) => scope('computer', merge({ action: 'drag' }, opts)),
      scroll: (opts) => scope('computer', merge({ action: 'scroll' }, opts)),
      type: (opts) => scope('computer', merge({ action: 'type' }, typeof opts === 'string' ? { text: opts } : opts)),
      keypress: (opts) => scope('computer', merge({ action: 'key' }, typeof opts === 'string' || Array.isArray(opts) ? { keys: opts } : opts)),
      downloadMedia: (opts) => scope('download', opts),
    };
  }

  // Operations that exist both session-wide (active tab) and per tab handle.
  function pageGroup(scope) {
    const navigate = (url, opts) => scope('navigate', merge({ url }, opts));
    return {
      navigate,
      goto: navigate,
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
      getJsDialog: (opts) => scope('dialog', merge({ action: 'get' }, opts)),
      permission: (opts) => scope('permission', opts),
      resize: (opts) => scope('resize', opts),
      visibility: (opts) => scope('visibility', opts),
      pageAssets: (opts) => scope('page_assets', opts),
      webmcp: (opts) => scope('webmcp', opts),
      markDeliverable: (opts) => scope('mark_tab', merge({ status: 'deliverable' }, opts)),
      markHandoff: (opts) => scope('mark_tab', merge({ status: 'handoff' }, opts)),
      batch: (operations, opts) => scope('batch', merge({ operations }, opts)),
    };
  }

  function makeTab(tabId, browserId) {
    const scope = (name, args) => call(name, merge(args,
      tabId === undefined ? null : { tabId },
      browserId === undefined ? null : { browserId }));
    const tab = {
      id: tabId === undefined ? null : tabId,
      browserId: browserId === undefined ? null : browserId,
      ax: axGroup(scope),
      cua: cuaGroup(scope),
      content: contentGroup(scope),
      clipboard: clipboardGroup(scope),
      dev: devGroup(scope),
      close: (opts) => scope('close', merge({ target: 'tab' }, opts)),
    };
    return Object.assign(tab, pageGroup(scope));
  }

  const sessionScope = (name, args) => call(name, args);
  const getTab = (tabId, opts) => makeTab(tabId,
    opts && opts.browserId !== undefined && opts.browserId !== null ? String(opts.browserId) : undefined);
  const browser = {
    docs: () => { note(); return DOCS; },
    documentation: () => { note(); return Promise.resolve(DOCS); },
    getState: (modeOrOpts, maybeOpts) => call('ax_state', axArgs(modeOrOpts, maybeOpts)),
    action: (action, params) => call('ax_action', merge({ action }, params)),
    getTab,
    tabs: {
      list: (opts) => call('tabs', merge({ action: 'list' }, opts)),
      new: (url, opts) => call('new_tab', merge(url ? { url } : null, opts)),
      get: getTab,
      select: (tabId, opts) => call('tabs', merge({ action: 'select', tabId }, opts)),
      selected: (opts) => call('tabs', merge({ action: 'selected' }, opts)),
      content: (opts) => call('read_urls', opts),
      close: (opts) => call('close', opts),
    },
    user: {
      openTabs: (opts) => call('tabs_content', opts),
      claimTab: (tab, opts) => call('claim_tab', merge(
        tab && typeof tab === 'object' ? { tabId: tab.id ?? tab.tabId } : { tabId: tab }, opts)),
      getTabContext: (tab, opts) => call('tabs', merge({ action: 'get' },
        tab && typeof tab === 'object' ? { tabId: tab.id ?? tab.tabId } : tab !== undefined ? { tabId: tab } : null, opts)),
    },
    history: (opts) => call('history', opts),
    nameSession: (name, opts) => call('tabs', merge({ action: 'name_session', name }, opts)),
    capabilities: (opts) => call('capabilities', opts),
    listBrowsers: (opts) => call('list_connected_browsers', opts),
    selectBrowser: (opts) => call('select_browser', typeof opts === 'string' ? { browserId: opts } : opts),
    ax: axGroup(sessionScope),
    cua: cuaGroup(sessionScope),
    content: contentGroup(sessionScope),
    clipboard: clipboardGroup(sessionScope),
    dev: devGroup(sessionScope),
  };
  Object.assign(browser, pageGroup(sessionScope));
  Object.defineProperty(globalThis, 'browser', {
    value: browser, configurable: true, writable: true, enumerable: true,
  });
})();
