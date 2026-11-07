import {
  acquireBrowserHostTransportV1,
  assertBrowserHostTransportV1,
  isBrowserRuntimeProxyV1,
  browserHostDefaults,
  browserDocumentationText,
} from 'klaus:browser-host-transport';

/* KLAUS_EXACT_BROWSER_RUNTIME_JSON_CODEC_V1_BEGIN c6f61faf5be383d322b7acfba238b82e79b20c2be16a136060be57b2b535557a */
function createExactBrowserRuntimeJsonCodecV1(isProxy) {
  'use strict';

  const ArrayCtor = Array;
  const ArrayPrototype = Array.prototype;
  const ErrorCtor = Error;
  const ObjectPrototype = Object.prototype;
  const SetCtor = Set;
  const Uint8ArrayCtor = Uint8Array;
  const Uint8ArrayPrototype = Uint8Array.prototype;
  const arrayJoin = Array.prototype.join;
  const arrayPush = Array.prototype.push;
  const arraySlice = Array.prototype.slice;
  const arrayIsArray = Array.isArray;
  const defineProperty = Object.defineProperty;
  const freeze = Object.freeze;
  const getOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
  const getPrototypeOf = Object.getPrototypeOf;
  const numberIsFinite = Number.isFinite;
  const numberIsInteger = Number.isInteger;
  const numberIsSafeInteger = Number.isSafeInteger;
  const numberToString = Number.prototype.toString;
  const NumberCtor = Number;
  const objectIs = Object.is;
  const ownKeys = Reflect.ownKeys;
  const reflectApply = Reflect.apply;
  const setAdd = Set.prototype.add;
  const setDelete = Set.prototype.delete;
  const setHas = Set.prototype.has;
  const stringCharCodeAt = String.prototype.charCodeAt;
  const stringSlice = String.prototype.slice;
  const stringFromCharCode = String.fromCharCode;
  const stringFromCodePoint = String.fromCodePoint;
  const StringCtor = String;

  const ENCODE_MESSAGE = 'Browser request contains a value outside exact RuntimeJson.';
  const DECODE_MESSAGE = 'Invalid canonical browser RuntimeJson bytes.';
  const HEX = '0123456789abcdef';

  function hostError(message) {
    const error = new ErrorCtor(message);
    defineProperty(error, 'code', {
      value: 'host_error',
      writable: true,
      enumerable: true,
      configurable: true,
    });
    return error;
  }

  function fail() {
    throw new ErrorCtor('invalid exact RuntimeJson image');
  }

  function push(array, ...values) {
    reflectApply(arrayPush, array, values);
  }

  function pushAscii(output, text) {
    for (let index = 0; index < text.length; index += 1) {
      push(output, reflectApply(stringCharCodeAt, text, [index]));
    }
  }

  function pushHexEscape(output, unit) {
    pushAscii(output, '\\u');
    push(
      output,
      reflectApply(stringCharCodeAt, HEX, [(unit >>> 12) & 15]),
      reflectApply(stringCharCodeAt, HEX, [(unit >>> 8) & 15]),
      reflectApply(stringCharCodeAt, HEX, [(unit >>> 4) & 15]),
      reflectApply(stringCharCodeAt, HEX, [unit & 15]),
    );
  }

  function pushScalarUtf8(output, scalar) {
    if (scalar <= 0x7f) {
      push(output, scalar);
    } else if (scalar <= 0x7ff) {
      push(output, 0xc0 | (scalar >>> 6), 0x80 | (scalar & 0x3f));
    } else if (scalar <= 0xffff) {
      push(
        output,
        0xe0 | (scalar >>> 12),
        0x80 | ((scalar >>> 6) & 0x3f),
        0x80 | (scalar & 0x3f),
      );
    } else {
      push(
        output,
        0xf0 | (scalar >>> 18),
        0x80 | ((scalar >>> 12) & 0x3f),
        0x80 | ((scalar >>> 6) & 0x3f),
        0x80 | (scalar & 0x3f),
      );
    }
  }

  function writeString(output, value) {
    push(output, 0x22);
    for (let index = 0; index < value.length; index += 1) {
      const unit = reflectApply(stringCharCodeAt, value, [index]);
      if (unit >= 0xd800 && unit <= 0xdbff && index + 1 < value.length) {
        const low = reflectApply(stringCharCodeAt, value, [index + 1]);
        if (low >= 0xdc00 && low <= 0xdfff) {
          pushScalarUtf8(output, 0x10000 + ((unit - 0xd800) << 10) + (low - 0xdc00));
          index += 1;
          continue;
        }
      }
      if (unit >= 0xd800 && unit <= 0xdfff) pushHexEscape(output, unit);
      else if (unit === 0x22) pushAscii(output, '\\"');
      else if (unit === 0x5c) pushAscii(output, '\\\\');
      else if (unit === 0x08) pushAscii(output, '\\b');
      else if (unit === 0x0c) pushAscii(output, '\\f');
      else if (unit === 0x0a) pushAscii(output, '\\n');
      else if (unit === 0x0d) pushAscii(output, '\\r');
      else if (unit === 0x09) pushAscii(output, '\\t');
      else if (unit <= 0x1f) pushHexEscape(output, unit);
      else pushScalarUtf8(output, unit);
    }
    push(output, 0x22);
  }

  function inheritedToJsonHook(value) {
    let prototype = getPrototypeOf(value);
    while (prototype !== null) {
      const descriptor = getOwnPropertyDescriptor(prototype, 'toJSON');
      if (descriptor) {
        return !('value' in descriptor) || typeof descriptor.value === 'function';
      }
      prototype = getPrototypeOf(prototype);
    }
    return false;
  }

  function stableDataDescriptor(object, key, snapshot) {
    const current = getOwnPropertyDescriptor(object, key);
    return !!(
      current &&
      'value' in current &&
      current.enumerable === snapshot.enumerable &&
      current.configurable === snapshot.configurable &&
      current.writable === snapshot.writable &&
      objectIs(current.value, snapshot.value)
    );
  }

  function writeValue(output, value, active) {
    if (value === null) {
      pushAscii(output, 'null');
      return;
    }
    if (value === true) {
      pushAscii(output, 'true');
      return;
    }
    if (value === false) {
      pushAscii(output, 'false');
      return;
    }
    if (typeof value === 'number') {
      if (!numberIsFinite(value)) pushAscii(output, 'null');
      else if (value === 0) push(output, 0x30);
      else pushAscii(output, reflectApply(numberToString, value, []));
      return;
    }
    if (typeof value === 'string') {
      writeString(output, value);
      return;
    }
    if (typeof value !== 'object' || isProxy(value)) fail();
    if (reflectApply(setHas, active, [value])) fail();
    reflectApply(setAdd, active, [value]);
    try {
      if (inheritedToJsonHook(value)) fail();
      const ownToJson = getOwnPropertyDescriptor(value, 'toJSON');
      if (ownToJson && (!('value' in ownToJson) || typeof ownToJson.value === 'function')) fail();
      if (arrayIsArray(value)) {
        if (getPrototypeOf(value) !== ArrayPrototype) fail();
        const lengthDescriptor = getOwnPropertyDescriptor(value, 'length');
        if (!lengthDescriptor || !('value' in lengthDescriptor)) fail();
        const length = lengthDescriptor.value;
        if (!numberIsSafeInteger(length) || length < 0 || length > 0xffff_ffff) fail();
        const keys = ownKeys(value);
        for (let keyIndex = 0; keyIndex < keys.length; keyIndex += 1) {
          const key = keys[keyIndex];
          if (typeof key !== 'string') fail();
          if (key === 'length') continue;
          const numeric = NumberCtor(key);
          if (!numberIsInteger(numeric) || numeric < 0 || numeric >= length || StringCtor(numeric) !== key) fail();
        }
        push(output, 0x5b);
        for (let index = 0; index < length; index += 1) {
          if (index !== 0) push(output, 0x2c);
          const key = StringCtor(index);
          const descriptor = getOwnPropertyDescriptor(value, key);
          if (!descriptor || !descriptor.enumerable || !('value' in descriptor)) fail();
          writeValue(output, descriptor.value, active);
          if (!stableDataDescriptor(value, key, descriptor)) fail();
        }
        if (!stableDataDescriptor(value, 'length', lengthDescriptor)) fail();
        push(output, 0x5d);
        return;
      }
      const prototype = getPrototypeOf(value);
      if (prototype !== ObjectPrototype && prototype !== null) fail();
      const entries = [];
      const keys = ownKeys(value);
      for (let keyIndex = 0; keyIndex < keys.length; keyIndex += 1) {
        const key = keys[keyIndex];
        if (typeof key !== 'string') fail();
        const descriptor = getOwnPropertyDescriptor(value, key);
        if (!descriptor) fail();
        if (!descriptor.enumerable) continue;
        if (!('value' in descriptor)) fail();
        push(entries, [key, descriptor]);
      }
      push(output, 0x7b);
      for (let index = 0; index < entries.length; index += 1) {
        if (index !== 0) push(output, 0x2c);
        const entry = entries[index];
        const key = entry[0];
        const descriptor = entry[1];
        writeString(output, key);
        push(output, 0x3a);
        writeValue(output, descriptor.value, active);
        if (!stableDataDescriptor(value, key, descriptor)) fail();
      }
      push(output, 0x7d);
    } finally {
      reflectApply(setDelete, active, [value]);
    }
  }

  function encode(value) {
    try {
      const output = [];
      writeValue(output, value, new SetCtor());
      const bytes = new Uint8ArrayCtor(output.length);
      for (let index = 0; index < output.length; index += 1) bytes[index] = output[index];
      return bytes;
    } catch (_error) {
      throw hostError(ENCODE_MESSAGE);
    }
  }

  function strictUtf8(bytes) {
    if (getPrototypeOf(bytes) !== Uint8ArrayPrototype) fail();
    if (bytes.length >= 3 && bytes[0] === 0xef && bytes[1] === 0xbb && bytes[2] === 0xbf) fail();
    const chunks = [];
    for (let index = 0; index < bytes.length;) {
      const first = bytes[index];
      let scalar;
      let width;
      if (first <= 0x7f) {
        scalar = first;
        width = 1;
      } else if (first >= 0xc2 && first <= 0xdf) {
        if (index + 1 >= bytes.length || (bytes[index + 1] & 0xc0) !== 0x80) fail();
        scalar = ((first & 0x1f) << 6) | (bytes[index + 1] & 0x3f);
        width = 2;
      } else if (first >= 0xe0 && first <= 0xef) {
        if (
          index + 2 >= bytes.length ||
          (bytes[index + 1] & 0xc0) !== 0x80 ||
          (bytes[index + 2] & 0xc0) !== 0x80 ||
          (first === 0xe0 && bytes[index + 1] < 0xa0) ||
          (first === 0xed && bytes[index + 1] >= 0xa0)
        ) fail();
        scalar = ((first & 15) << 12) | ((bytes[index + 1] & 0x3f) << 6) | (bytes[index + 2] & 0x3f);
        width = 3;
      } else if (first >= 0xf0 && first <= 0xf4) {
        if (
          index + 3 >= bytes.length ||
          (bytes[index + 1] & 0xc0) !== 0x80 ||
          (bytes[index + 2] & 0xc0) !== 0x80 ||
          (bytes[index + 3] & 0xc0) !== 0x80 ||
          (first === 0xf0 && bytes[index + 1] < 0x90) ||
          (first === 0xf4 && bytes[index + 1] >= 0x90)
        ) fail();
        scalar =
          ((first & 7) << 18) |
          ((bytes[index + 1] & 0x3f) << 12) |
          ((bytes[index + 2] & 0x3f) << 6) |
          (bytes[index + 3] & 0x3f);
        width = 4;
      } else {
        fail();
      }
      push(chunks, scalar <= 0xffff ? stringFromCharCode(scalar) : stringFromCodePoint(scalar));
      index += width;
    }
    return reflectApply(arrayJoin, chunks, ['']);
  }

  function parseJson(text) {
    let index = 0;

    function hexValue(unit) {
      if (unit >= 0x30 && unit <= 0x39) return unit - 0x30;
      if (unit >= 0x41 && unit <= 0x46) return unit - 0x41 + 10;
      if (unit >= 0x61 && unit <= 0x66) return unit - 0x61 + 10;
      fail();
    }

    function parseString() {
      if (reflectApply(stringCharCodeAt, text, [index]) !== 0x22) fail();
      index += 1;
      const units = [];
      while (index < text.length) {
        let unit = reflectApply(stringCharCodeAt, text, [index++]);
        if (unit === 0x22) {
          let value = '';
          for (let offset = 0; offset < units.length; offset += 0x2000) {
            const chunk = reflectApply(arraySlice, units, [offset, offset + 0x2000]);
            value += reflectApply(stringFromCharCode, null, chunk);
          }
          return value;
        }
        if (unit < 0x20) fail();
        if (unit !== 0x5c) {
          push(units, unit);
          continue;
        }
        if (index >= text.length) fail();
        unit = reflectApply(stringCharCodeAt, text, [index++]);
        if (unit === 0x22 || unit === 0x5c || unit === 0x2f) push(units, unit);
        else if (unit === 0x62) push(units, 0x08);
        else if (unit === 0x66) push(units, 0x0c);
        else if (unit === 0x6e) push(units, 0x0a);
        else if (unit === 0x72) push(units, 0x0d);
        else if (unit === 0x74) push(units, 0x09);
        else if (unit === 0x75) {
          if (index + 4 > text.length) fail();
          let value = 0;
          for (let count = 0; count < 4; count += 1) {
            value = (value << 4) | hexValue(reflectApply(stringCharCodeAt, text, [index++]));
          }
          push(units, value);
        } else fail();
      }
      fail();
    }

    function parseNumber() {
      const start = index;
      if (reflectApply(stringCharCodeAt, text, [index]) === 0x2d) index += 1;
      if (reflectApply(stringCharCodeAt, text, [index]) === 0x30) index += 1;
      else {
        const first = reflectApply(stringCharCodeAt, text, [index]);
        if (!(first >= 0x31 && first <= 0x39)) fail();
        while (true) {
          const unit = reflectApply(stringCharCodeAt, text, [index]);
          if (!(unit >= 0x30 && unit <= 0x39)) break;
          index += 1;
        }
      }
      if (reflectApply(stringCharCodeAt, text, [index]) === 0x2e) {
        index += 1;
        const first = reflectApply(stringCharCodeAt, text, [index]);
        if (!(first >= 0x30 && first <= 0x39)) fail();
        while (true) {
          const unit = reflectApply(stringCharCodeAt, text, [index]);
          if (!(unit >= 0x30 && unit <= 0x39)) break;
          index += 1;
        }
      }
      const exponent = reflectApply(stringCharCodeAt, text, [index]);
      if (exponent === 0x65 || exponent === 0x45) {
        index += 1;
        const sign = reflectApply(stringCharCodeAt, text, [index]);
        if (sign === 0x2b || sign === 0x2d) index += 1;
        const first = reflectApply(stringCharCodeAt, text, [index]);
        if (!(first >= 0x30 && first <= 0x39)) fail();
        while (true) {
          const unit = reflectApply(stringCharCodeAt, text, [index]);
          if (!(unit >= 0x30 && unit <= 0x39)) break;
          index += 1;
        }
      }
      return NumberCtor(reflectApply(stringSlice, text, [start, index]));
    }

    function parseValue() {
      const unit = reflectApply(stringCharCodeAt, text, [index]);
      if (unit === 0x22) return parseString();
      if (unit === 0x6e && reflectApply(stringSlice, text, [index, index + 4]) === 'null') {
        index += 4;
        return null;
      }
      if (unit === 0x74 && reflectApply(stringSlice, text, [index, index + 4]) === 'true') {
        index += 4;
        return true;
      }
      if (unit === 0x66 && reflectApply(stringSlice, text, [index, index + 5]) === 'false') {
        index += 5;
        return false;
      }
      if (unit === 0x5b) {
        index += 1;
        const array = new ArrayCtor();
        let next = 0;
        if (reflectApply(stringCharCodeAt, text, [index]) === 0x5d) {
          index += 1;
          return array;
        }
        for (;;) {
          defineProperty(array, StringCtor(next), {
            value: parseValue(),
            writable: true,
            enumerable: true,
            configurable: true,
          });
          next += 1;
          const separator = reflectApply(stringCharCodeAt, text, [index++]);
          if (separator === 0x5d) return array;
          if (separator !== 0x2c) fail();
        }
      }
      if (unit === 0x7b) {
        index += 1;
        const object = {};
        const seen = new SetCtor();
        if (reflectApply(stringCharCodeAt, text, [index]) === 0x7d) {
          index += 1;
          return object;
        }
        for (;;) {
          const key = parseString();
          if (reflectApply(setHas, seen, [key])) fail();
          reflectApply(setAdd, seen, [key]);
          if (reflectApply(stringCharCodeAt, text, [index++]) !== 0x3a) fail();
          defineProperty(object, key, {
            value: parseValue(),
            writable: true,
            enumerable: true,
            configurable: true,
          });
          const separator = reflectApply(stringCharCodeAt, text, [index++]);
          if (separator === 0x7d) return object;
          if (separator !== 0x2c) fail();
        }
      }
      if (unit === 0x2d || (unit >= 0x30 && unit <= 0x39)) return parseNumber();
      fail();
    }

    const value = parseValue();
    if (index !== text.length) fail();
    return value;
  }

  function decode(bytes) {
    try {
      const value = parseJson(strictUtf8(bytes));
      const canonical = encode(value);
      if (canonical.length !== bytes.length) fail();
      for (let index = 0; index < bytes.length; index += 1) {
        if (canonical[index] !== bytes[index]) fail();
      }
      return value;
    } catch (_error) {
      throw hostError(DECODE_MESSAGE);
    }
  }

  return freeze({ version: 1, encode, decode });
}
/* KLAUS_EXACT_BROWSER_RUNTIME_JSON_CODEC_V1_END c6f61faf5be383d322b7acfba238b82e79b20c2be16a136060be57b2b535557a */

const exactBrowserRuntimeJsonCodecV1 = createExactBrowserRuntimeJsonCodecV1(
  isBrowserRuntimeProxyV1,
);
const browserObjectHasOwn = Object.hasOwn;
const browserObjectFreeze = Object.freeze;
const browserObjectPrototype = Object.prototype;
const browserDefineProperty = Object.defineProperty;
const browserGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const browserGetPrototypeOf = Object.getPrototypeOf;
const browserOwnKeys = Reflect.ownKeys;

function browserHostError(message) {
  const error = new Error(message);
  error.code = 'host_error';
  return error;
}

function prepareBrowserHost(options) {
  if (
    options === null ||
    typeof options !== 'object' ||
    isBrowserRuntimeProxyV1(options)
  ) {
    throw browserHostError('Rust browser host transport is required');
  }
  for (const forbidden of ['nodePath', 'sidecarPath', 'cwd', 'env']) {
    if (browserObjectHasOwn(options, forbidden)) {
      throw browserHostError('Rust browser host transport is required');
    }
  }
  const capability = browserObjectHasOwn(options, 'transport')
    ? options.transport
    : acquireBrowserHostTransportV1();
  if (isBrowserRuntimeProxyV1(capability)) {
    throw browserHostError('Rust browser host transport is required');
  }
  assertBrowserHostTransportV1(capability);
  for (const forbidden of ['nodePath', 'sidecarPath', 'cwd', 'env']) {
    if (browserObjectHasOwn(capability, forbidden)) {
      throw browserHostError('Rust browser host transport is required');
    }
  }
  const backend = options.backend || browserHostDefaults.backend || 'launch';
  return {
    capability,
    backend,
    browserType: options.browserType || typeForBackend(backend),
    configuration: browserObjectFreeze({
      artifactDir: options.artifactDir,
      headless: options.headless !== false,
      backend,
      cdpEndpoint: options.cdpEndpoint,
      userDataDir: options.userDataDir,
      channel: options.channel,
      executablePath: options.executablePath,
    }),
  };
}

function compactBrowserRecordV1(record) {
  if (
    record === null ||
    typeof record !== 'object' ||
    isBrowserRuntimeProxyV1(record)
  ) return record;
  const prototype = browserGetPrototypeOf(record);
  if (prototype !== browserObjectPrototype && prototype !== null) return record;
  const compact = {};
  const keys = browserOwnKeys(record);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    if (typeof key !== 'string') return record;
    const descriptor = browserGetOwnPropertyDescriptor(record, key);
    if (!descriptor || !descriptor.enumerable) continue;
    if (!browserObjectHasOwn(descriptor, 'value')) return record;
    if (descriptor.value === undefined) continue;
    browserDefineProperty(compact, key, {
      value: descriptor.value,
      writable: true,
      enumerable: true,
      configurable: true,
    });
  }
  return compact;
}

export async function setupBrowserRuntime({ globals = globalThis, options = {} } = {}) {
  const registry = createBrowserRegistry(options);
  const agent = globals.agent || {};
  agent.browsers = registry;
  globals.agent = agent;
  return { agent, browsers: registry };
}

export function createBrowserRegistry(options = {}) {
  const host = prepareBrowserHost(options);
  const transport = new BrowserSidecarTransport(host.capability, host.configuration);
  const backend = host.backend;
  const browserType = host.browserType;
  const sharedTransport = transport;
  const browsers = new Map();
  const getBrowser = (browserId) => {
    if (!browsers.has(browserId)) browsers.set(browserId, createBrowser(browserId));
    return browsers.get(browserId);
  };
  const resolveMention = async (reference) => {
    if (!reference || typeof reference !== 'object' || typeof reference.mention !== 'string' || !reference.mention) {
      throw new Error('A complete tab mention URI is required.');
    }
    const result = await callBrowserOperation(sharedTransport, { type: 'resolve_tab', mention: reference.mention });
    const value = result.value;
    if (!value || typeof value.browserId !== 'string' || (value.browserId !== 'iab' && !/^chrome:[^\s]{1,121}$/.test(value.browserId)) || !Number.isSafeInteger(value.tabId) || value.tabId <= 0) {
      throw browserHostError('Browser host returned an invalid resolved tab reference.');
    }
    return value;
  };
  function createBrowser(browserId) {
    const tabs = new Map();
    const transport = {
      call: (request) => sharedTransport.call({ ...request, target: { browserId, ...request.target } }),
      dispose: () => sharedTransport.dispose(),
    };
    const getTab = (tabId = 'tab-1') => {
      if (!tabs.has(tabId)) {
        tabs.set(tabId, createTab(transport, tabId));
      }
      return tabs.get(tabId);
    };
    const browser = {
      browserId,
      id: browserId === 'iab' ? 'klaus-bundled' : browserId,
      type: browserId === 'iab' ? browserType : 'chrome',
      name: browserId === 'iab' ? 'Klaus Bundled Browser' : browserId,
      capabilities: createCapabilityCollection('browser'),
      user: {
        controlled: true,
        async openTabs() {
          const result = await callBrowserOperation(transport, { type: 'tabs', action: browserId === 'iab' ? 'list' : 'user' });
          return (result.value || []).map((tab) => ({ id: tab.id, browserId, title: tab.title || null, url: tab.url || null }));
        },
        async claimTab(tabInfo) {
          const id = typeof tabInfo === 'string' || typeof tabInfo === 'number' ? tabInfo : tabInfo?.id;
          if (id === undefined || id === null || id === '') throw new Error('claimTab requires a tab id.');
          await callBrowserOperation(transport, browserId === 'iab' ? { type: 'tabs', action: 'get' } : { type: 'claim_tab' }, { tabId: String(id) });
          return getTab(String(id));
        },
        async history(options = {}) {
          const result = await callBrowserOperation(
            transport,
            {
              type: 'history',
              queries: options.queries || (options.query ? [options.query] : []),
              limit: options.limit,
              from: options.from instanceof Date ? options.from.toISOString() : options.from,
              to: options.to instanceof Date ? options.to.toISOString() : options.to,
            },
            {},
            options,
          );
          return result.value || [];
        },
      },
      tabs: {
        async getDefault() {
          return getTab('tab-1');
        },
        async list() {
          const result = await callBrowserOperation(transport, { type: 'tabs', action: 'list' });
          return (result.value || [getTab('tab-1').descriptor()]).map((item) => ({
            id: item.id,
            type: item.type || 'page',
            url: item.url || null,
            selected: !!item.selected,
          }));
        },
        async get(id) {
          if (id && typeof id === 'object') {
            const resolved = await resolveMention(id);
            return getBrowser(resolved.browserId).__getResolvedTab(String(resolved.tabId));
          }
          await callBrowserOperation(transport, { type: 'tabs', action: 'get' }, { tabId: String(id) });
          return getTab(String(id));
        },
        async new(options = {}) {
          const result = await callBrowserOperation(transport, {
            type: 'tabs',
            action: 'new',
            url: typeof options === 'string' ? options : options.url,
          });
          return getTab(result.tabId || result.value?.id || 'tab-1');
        },
        async selected() {
          const result = await callBrowserOperation(transport, { type: 'tabs', action: 'selected' });
          return getTab(result.value?.id || result.tabId || 'tab-1');
        },
        async finalize(options = {}) {
          return callBrowserOperation(transport, { type: 'tabs', action: 'finalize', ...options });
        },
        async content(options = {}) {
          const result = await callBrowserOperation(
            transport,
            {
              type: 'tabs_content',
              urls: options.urls || [],
              contentType: options.contentType || 'text',
              maxChars: options.maxChars,
              waitUntil: options.waitUntil,
            },
            {},
            options,
          );
          return result.value || [];
        },
      },
      async documentation() {
        return browserDocumentation();
      },
      async nameSession(name) {
        return callBrowserOperation(transport, { type: 'tabs', action: 'name_session', name });
      },
      async listConnectedBrowsers(options = {}) {
        return callBrowserOperation(
          transport,
          { type: 'list_connected_browsers', localOnly: !!options.localOnly },
          {},
          options,
        );
      },
      async selectBrowser(deviceId, options = {}) {
        return callBrowserOperation(
          transport,
          { type: 'select_browser', deviceId },
          {},
          options,
        );
      },
      async switchBrowser(options = {}) {
        return callBrowserOperation(
          transport,
          { type: 'switch_browser', prompt: options.prompt, waitMs: options.waitMs },
          {},
          options,
        );
      },
      async close(target = 'all') {
        return getTab().close({ target });
      },
      async dispose() {
        await transport.dispose();
      },
    };

    Object.defineProperty(browser, '__getResolvedTab', { value: getTab });
    return browser;
  }
  const browser = getBrowser('iab');

  return {
    async get(id) {
      if (id && typeof id === 'object') return getBrowser((await resolveMention(id)).browserId);
      if (!id || id === 'browser' || id === 'klaus-bundled' || id === 'iab') return browser;
      if (typeof id === 'string' && /^chrome:[^\s]{1,121}$/.test(id)) return getBrowser(id);
      throw new Error(`Unknown browser id: ${id}`);
    },
    async getDefault() {
      return browser;
    },
    async getForUrl() {
      return browser;
    },
    async list() {
      const result = await callBrowserOperation(sharedTransport, { type: 'list_connected_browsers', localOnly: true });
      const rows = Array.isArray(result.value) ? result.value : result.value?.browsers || [];
      const available = new Map([['iab', browser]]);
      for (const row of rows) {
        const id = row.browserId || row.id;
        if (typeof id === 'string' && /^chrome:[^\s]{1,121}$/.test(id) && row.available !== false) {
          const connected = getBrowser(id);
          if (typeof row.name === 'string' && row.name) connected.name = row.name;
          available.set(id, connected);
        }
      }
      return Promise.all([...available.values()].map(async (connected) => ({
        id: connected.id,
        browserId: connected.browserId,
        type: connected.type,
        name: connected.name,
        capabilities: {
          browser: await connected.capabilities.list(),
          tab: await createCapabilityCollection('tab').list(),
        },
        metadata: { backend },
      })));
    },
    async dispose() {
      await transport.dispose();
    },
  };
}

function createTab(transport, tabId = 'tab-1') {
  const target = { tabId: String(tabId) };
  const call = (operation, options = {}) =>
    transport.call({
      operation,
      target,
      permissions: permissionsFor(operation),
      timeoutMs: options.timeoutMs || options.timeout || 30_000,
      metadata: options.metadata || {},
    });

  return {
    id: tabId,
    type: 'page',
    capabilities: createCapabilityCollection('tab'),
    descriptor() {
      return { id: tabId, type: 'page' };
    },
    async title(options = {}) {
      const result = await call({ type: 'tabs', action: 'get' }, options);
      return result.value?.title || result.title || null;
    },
    async url(options = {}) {
      const result = await call({ type: 'tabs', action: 'get' }, options);
      return result.value?.url || result.url || null;
    },
    async markDeliverable(options = {}) {
      return call({ type: 'mark_tab', status: 'deliverable' }, options);
    },
    async markHandoff(options = {}) {
      return call({ type: 'mark_tab', status: 'handoff' }, options);
    },
    async getJsDialog(options = {}) {
      const result = await call({ type: 'dialog', action: 'get' }, options);
      return result.value ? createDialog(call, result.value) : undefined;
    },
    content: {
      async export(options = {}) {
        const result = await call(
          {
            type: 'content_export',
            format: options.format,
            selector: options.selector,
            maxChars: options.maxChars,
          },
          options,
        );
        return result.value?.path || result.value?.content || result.text || '';
      },
      async exportGsuite(type, options = {}) {
        const result = await call(
          {
            type: 'content_export',
            format: type,
            selector: options.selector,
            maxChars: options.maxChars,
          },
          options,
        );
        return result.value?.path || result.value?.content || result.text || '';
      },
    },
    clipboard: {
      async read(options = {}) {
        const result = await call({ type: 'clipboard', action: 'read' }, options);
        return result.value || [];
      },
      async readText(options = {}) {
        const result = await call({ type: 'clipboard', action: 'read_text' }, options);
        return result.text || result.value || '';
      },
      async write(items, options = {}) {
        return call({ type: 'clipboard', action: 'write', items }, options);
      },
      async writeText(text, options = {}) {
        return call({ type: 'clipboard', action: 'write_text', text }, options);
      },
    },
    async goto(url, options = {}) {
      return call(
        {
          type: 'navigate',
          url,
          waitUntil: options.waitUntil,
          viewport: options.viewport,
        },
        options,
      );
    },
    async snapshot(options = {}) {
      return call(
        {
          type: 'snapshot',
          selector: options.selector,
          includeText: options.includeText ?? true,
          includeLinks: options.includeLinks ?? true,
        },
        options,
      );
    },
    async screenshot(options = {}) {
      return call(
        {
          type: 'screenshot',
          selector: options.selector,
          fullPage: !!options.fullPage,
          format: options.format,
          quality: options.quality,
          save: options.save !== false,
        },
        options,
      );
    },
    async gifCreator(options = {}) {
      return call(
        {
          type: 'gif_creator',
          filename: options.filename,
          durationSeconds: options.durationSeconds,
          fps: options.fps,
          fullPage: !!options.fullPage,
        },
        options,
      );
    },
    async downloadMedia(options = {}) {
      return call(
        {
          type: 'download',
          selector: options.selector,
          url: options.url,
          x: options.x ?? options.coordinate?.[0],
          y: options.y ?? options.coordinate?.[1],
          filename: options.filename,
        },
        options,
      );
    },
    async download(options = {}) {
      return this.downloadMedia(options);
    },
    async fileUpload(paths, options = {}) {
      return call(
        {
          type: 'file_upload',
          paths: Array.isArray(paths) ? paths : [paths],
          selector: options.selector,
          refId: options.refId || options.ref,
        },
        options,
      );
    },
    async uploadImage(path, options = {}) {
      return call(
        {
          type: 'upload_image',
          path,
          selector: options.selector,
          prompt: options.prompt,
        },
        options,
      );
    },
    shortcuts: {
      async list(options = {}) {
        return call({ type: 'shortcuts_list', query: options.query }, options);
      },
      async execute(name, options = {}) {
        return call(
          {
            type: 'shortcuts_execute',
            name,
            waitForCompletion: !!options.waitForCompletion,
          },
          options,
        );
      },
    },
    async click(selectorOrPoint, options = {}) {
      const operation =
        typeof selectorOrPoint === 'string'
          ? { type: 'click', selector: selectorOrPoint }
          : { type: 'click', x: selectorOrPoint?.x, y: selectorOrPoint?.y };
      return call({ ...operation, button: options.button, clickCount: options.clickCount, modifiers: options.modifiers }, options);
    },
    async type(selectorOrText, textOrOptions, maybeOptions = {}) {
      const hasSelector = typeof textOrOptions === 'string';
      const options = hasSelector ? maybeOptions : textOrOptions || {};
      return call(
        {
          type: 'type',
          selector: hasSelector ? selectorOrText : undefined,
          text: hasSelector ? textOrOptions : selectorOrText,
          clear: !!options.clear,
          pressEnter: !!options.pressEnter,
          delayMs: options.delayMs,
        },
        options,
      );
    },
    async evaluate(script, argsOrOptions, maybeOptions = {}) {
      const hasArgs = arguments.length >= 2 && !isOptionsOnly(argsOrOptions);
      const options = hasArgs ? maybeOptions : argsOrOptions || {};
      return call(
        {
          type: 'eval',
          script,
          args: hasArgs ? argsOrOptions : undefined,
          awaitPromise: options.awaitPromise ?? true,
        },
        options,
      );
    },
    async console(options = {}) {
      return call(
        {
          type: 'console',
          levels: options.levels,
          sinceMs: options.sinceMs,
          limit: options.limit,
          clear: !!options.clear,
        },
        options,
      );
    },
    async network(options = {}) {
      return call(
        {
          type: 'network',
          sinceMs: options.sinceMs,
          limit: options.limit,
          includeBodies: !!options.includeBodies,
          urlFilter: options.urlFilter,
        },
        options,
      );
    },
    async close(options = {}) {
      return call({ type: 'close', target: options.target || 'tab' }, options);
    },
    async readPage(options = {}) {
      return call({
        type: 'read_page',
        filter: options.filter,
        depth: options.depth,
        refId: options.refId || options.ref,
        maxChars: options.maxChars,
      }, options);
    },
    async find(query, options = {}) {
      return call({ type: 'find', query, limit: options.limit }, options);
    },
    async formInput(ref, value, options = {}) {
      return call({ type: 'form_input', ref, value }, options);
    },
    async computer(actionOrOptions, options = {}) {
      const operation =
        typeof actionOrOptions === 'string'
          ? { type: 'computer', action: actionOrOptions, ...options }
          : { type: 'computer', ...(actionOrOptions || {}) };
      return call(operation, options);
    },
    cua: {
      async click(options = {}) {
        return call({ type: 'computer', action: 'left_click', coordinate: pointFromOptions(options) }, options);
      },
      async double_click(options = {}) {
        return call({ type: 'computer', action: 'double_click', coordinate: pointFromOptions(options) }, options);
      },
      async drag(options = {}) {
        return call(
          {
            type: 'computer',
            action: 'left_click_drag',
            startCoordinate: options.startCoordinate || options.start || options.from,
            coordinate: options.coordinate || options.end || options.to,
          },
          options,
        );
      },
      async keypress(options = {}) {
        return call({ type: 'computer', action: 'key', text: options.text || options.key || options.keys }, options);
      },
      async move(options = {}) {
        return call({ type: 'computer', action: 'hover', coordinate: pointFromOptions(options) }, options);
      },
      async scroll(options = {}) {
        return call(
          {
            type: 'computer',
            action: 'scroll',
            coordinate: pointFromOptions(options),
            scrollDirection: options.direction || scrollDirectionFromDeltas(options),
            amount: options.amount || Math.abs(options.deltaY || options.dy || options.deltaX || options.dx || 500),
          },
          options,
        );
      },
      async type(options = {}) {
        return call({ type: 'computer', action: 'type', text: options.text || '' }, options);
      },
      async downloadMedia(options = {}) {
        return call(
          {
            type: 'download',
            selector: options.selector,
            url: options.url,
            x: options.x ?? options.coordinate?.[0],
            y: options.y ?? options.coordinate?.[1],
            filename: options.filename,
          },
          options,
        );
      },
    },
    dom_cua: {
      async click(options = {}) {
        return call({ type: 'click', selector: selectorFromDomOptions(options) }, options);
      },
      async double_click(options = {}) {
        return call(
          { type: 'locator', selector: selectorFromDomOptions(options), action: 'double_click', options },
          options,
        );
      },
      async get_visible_dom(options = {}) {
        return call({ type: 'playwright_dom_snapshot', visibleOnly: true, maxChars: options.maxChars }, options);
      },
      async keypress(options = {}) {
        return call({ type: 'computer', action: 'key', text: options.text || options.key || options.keys }, options);
      },
      async scroll(options = {}) {
        return call(
          {
            type: 'computer',
            action: 'scroll',
            coordinate: pointFromOptions(options),
            scrollDirection: options.direction || scrollDirectionFromDeltas(options),
            amount: options.amount || Math.abs(options.deltaY || options.dy || options.deltaX || options.dx || 500),
          },
          options,
        );
      },
      async type(options = {}) {
        return call({ type: 'type', selector: selectorFromDomOptions(options), text: options.text || '' }, options);
      },
      async downloadMedia(options = {}) {
        return call(
          {
            type: 'download',
            selector: options.selector || options.ref,
            url: options.url,
            x: options.x ?? options.coordinate?.[0],
            y: options.y ?? options.coordinate?.[1],
            filename: options.filename,
          },
          options,
        );
      },
    },
    async pageText(options = {}) {
      return call({ type: 'page_text', maxChars: options.maxChars }, options);
    },
    async resize(viewport, options = {}) {
      return call({ type: 'resize', viewport }, options);
    },
    async batch(actions, options = {}) {
      return call({ type: 'batch', actions }, options);
    },
    async back(options = {}) {
      return this.evaluate('history.back()', options);
    },
    async forward(options = {}) {
      return this.evaluate('history.forward()', options);
    },
    async reload(options = {}) {
      return this.evaluate('location.reload()', options);
    },
    playwright: {
      async evaluate(script, args, options = {}) {
        return call({ type: 'eval', script, args, awaitPromise: true }, options);
      },
      async waitForEvent(event, options = {}) {
        const result = await call({ type: 'playwright_wait_for_event', event }, options);
        if (event === 'download') {
          return createDownload(result.value || {});
        }
        if (event === 'filechooser') {
          return createFileChooser(call, result.value || {});
        }
        return result.value;
      },
      async waitForLoadState(options = {}) {
        const state = typeof options === 'string' ? options : options.state || options.waitUntil;
        return call({ type: 'playwright_wait_for_load_state', state }, typeof options === 'string' ? {} : options);
      },
      async waitForTimeout(timeoutMs) {
        await new Promise((resolve) => setTimeout(resolve, Math.max(0, Number(timeoutMs) || 0)));
      },
      async waitForURL(url, options = {}) {
        return call({ type: 'playwright_wait_for_url', url: String(url), waitUntil: options.waitUntil }, options);
      },
      frameLocator(frameSelector) {
        return createFrameLocator(call, frameSelector);
      },
      async domSnapshot(options = {}) {
        const result = await call({ type: 'playwright_dom_snapshot', maxChars: options.maxChars }, options);
        return result.text || result.value?.text || '';
      },
      async elementInfo(options = {}) {
        const result = await call(
          {
            type: 'playwright_element_info',
            x: options.x ?? options.coordinate?.[0],
            y: options.y ?? options.coordinate?.[1],
            selector: options.selector,
          },
          options,
        );
        return result.value || [];
      },
      async elementScreenshot(options = {}) {
        const result = await call(
          {
            type: 'playwright_element_screenshot',
            x: options.x ?? options.coordinate?.[0],
            y: options.y ?? options.coordinate?.[1],
            selector: options.selector,
            format: options.format || 'png',
          },
          options,
        );
        return result.artifacts?.[0] || result.value;
      },
      getByText(text, options = {}) {
        return createLocator(call, `:text(${JSON.stringify(String(text))})`, { exact: !!options.exact });
      },
      getByRole(role, options = {}) {
        const name = options.name ? `:has-text(${JSON.stringify(String(options.name))})` : '';
        return createLocator(call, `[role="${cssEscape(String(role))}"]${name}`, { exact: !!options.exact });
      },
      getByLabel(text, options = {}) {
        return createLocator(call, `label:has-text(${JSON.stringify(String(text))})`, { exact: !!options.exact });
      },
      getByPlaceholder(text, options = {}) {
        return createLocator(call, `[placeholder="${cssEscape(String(text))}"]`, { exact: !!options.exact });
      },
      getByTestId(testId) {
        return createLocator(call, `[data-testid="${cssEscape(String(testId))}"]`);
      },
      locator(selector) {
        return createLocator(call, selector);
      },
    },
  };
}

function createFrameLocator(call, frameSelector) {
  return {
    frameSelector,
    frameLocator(childFrameSelector) {
      return createFrameLocator(call, `${frameSelector} ${childFrameSelector}`);
    },
    locator(selector) {
      return createLocator(call, selector, { frameSelector });
    },
    getByText(text, options = {}) {
      return createLocator(call, `:text(${JSON.stringify(String(text))})`, { frameSelector, exact: !!options.exact });
    },
    getByRole(role, options = {}) {
      const name = options.name ? `:has-text(${JSON.stringify(String(options.name))})` : '';
      return createLocator(call, `[role="${cssEscape(String(role))}"]${name}`, { frameSelector, exact: !!options.exact });
    },
    getByLabel(text, options = {}) {
      return createLocator(call, `label:has-text(${JSON.stringify(String(text))})`, { frameSelector, exact: !!options.exact });
    },
    getByPlaceholder(text, options = {}) {
      return createLocator(call, `[placeholder="${cssEscape(String(text))}"]`, { frameSelector, exact: !!options.exact });
    },
    getByTestId(testId) {
      return createLocator(call, `[data-testid="${cssEscape(String(testId))}"]`, { frameSelector });
    },
  };
}

function createLocator(call, selector, state = {}) {
  const locatorState = {
    selector,
    frameSelector: state.frameSelector,
    nthIndex: state.nthIndex,
    filter: state.filter,
    andSelector: state.andSelector,
    orSelector: state.orSelector,
  };
  const invoke = (action, value, options = {}) =>
    call({ type: 'locator', ...locatorState, action, value, options }, options);
  const derive = (patch = {}) => createLocator(call, selector, { ...locatorState, ...patch });
  return {
    selector: locatorState.selector,
    frameSelector: locatorState.frameSelector,
    first() {
      return derive({ nthIndex: 0 });
    },
    nth(index) {
      return derive({ nthIndex: index });
    },
    last() {
      return derive({ nthIndex: -1 });
    },
    locator(childSelector) {
      return createLocator(call, `${selector} ${childSelector}`, locatorState);
    },
    getByText(text, options = {}) {
      return createLocator(call, `${selector} :text(${JSON.stringify(String(text))})`, { ...locatorState, exact: !!options.exact });
    },
    getByRole(role, options = {}) {
      const name = options.name ? `:has-text(${JSON.stringify(String(options.name))})` : '';
      return createLocator(call, `${selector} [role="${cssEscape(String(role))}"]${name}`, { ...locatorState, exact: !!options.exact });
    },
    getByLabel(text, options = {}) {
      return createLocator(call, `${selector} label:has-text(${JSON.stringify(String(text))})`, { ...locatorState, exact: !!options.exact });
    },
    getByPlaceholder(text, options = {}) {
      return createLocator(call, `${selector} [placeholder="${cssEscape(String(text))}"]`, { ...locatorState, exact: !!options.exact });
    },
    getByTestId(testId) {
      return createLocator(call, `${selector} [data-testid="${cssEscape(String(testId))}"]`, locatorState);
    },
    filter(options = {}) {
      return derive({ filter: normalizeLocatorFilter(options) });
    },
    and(locator) {
      return derive({ andSelector: locator?.selector || String(locator) });
    },
    or(locator) {
      return derive({ orSelector: locator?.selector || String(locator) });
    },
    async all() {
      const count = await invoke('count');
      return Array.from({ length: Number(count.value ?? count) || 0 }, (_, index) => this.nth(index));
    },
    async click(options = {}) {
      return invoke('click', undefined, options);
    },
    async dblclick(options = {}) {
      return invoke('double_click', undefined, options);
    },
    async type(value, options = {}) {
      return invoke('type', value, options);
    },
    async fill(value, options = {}) {
      return invoke('fill', value, options);
    },
    async innerText(options = {}) {
      return invoke('inner_text', undefined, options);
    },
    async getAttribute(name, options = {}) {
      return invoke('get_attribute', name, options);
    },
    async isEnabled(options = {}) {
      return invoke('is_enabled', undefined, options);
    },
    async press(value, options = {}) {
      return invoke('press', value, options);
    },
    async count(options = {}) {
      return invoke('count', undefined, options);
    },
    async textContent(options = {}) {
      return invoke('text_content', undefined, options);
    },
    async allTextContents(options = {}) {
      return invoke('all_text_contents', undefined, options);
    },
    async isVisible(options = {}) {
      return invoke('is_visible', undefined, options);
    },
    async waitFor(options = {}) {
      return invoke('wait_for', undefined, options);
    },
    async screenshot(options = {}) {
      return invoke('screenshot', undefined, options);
    },
    async downloadMedia(options = {}) {
      return call(
        {
          type: 'download',
          selector,
          filename: options.filename,
        },
        options,
      );
    },
    async check(options = {}) {
      return invoke('check', undefined, options);
    },
    async uncheck(options = {}) {
      return invoke('uncheck', undefined, options);
    },
    async setChecked(value, options = {}) {
      return invoke('set_checked', !!value, options);
    },
    async selectOption(value, options = {}) {
      return invoke('select_option', value, options);
    },
  };
}

function normalizeLocatorFilter(options = {}) {
  const filter = {};
  if (options.hasText != null) filter.hasText = String(options.hasText);
  if (options.hasNotText != null) filter.hasNotText = String(options.hasNotText);
  if (options.has?.selector) filter.has = options.has.selector;
  if (options.hasNot?.selector) filter.hasNot = options.hasNot.selector;
  return filter;
}

function createDownload(descriptor = {}) {
  return {
    suggestedFilename: descriptor.suggestedFilename || descriptor.filename || null,
    url: descriptor.url || null,
    async path() {
      return descriptor.path || null;
    },
  };
}

function createFileChooser(call, descriptor = {}) {
  return {
    id: descriptor.chooserId || descriptor.id || null,
    isMultiple() {
      return !!descriptor.isMultiple;
    },
    async setFiles(files, options = {}) {
      return call(
        {
          type: 'playwright_file_chooser_set_files',
          chooserId: descriptor.chooserId || descriptor.id,
          files: Array.isArray(files) ? files : [files],
        },
        options,
      );
    },
  };
}

function pointFromOptions(options = {}) {
  if (Array.isArray(options.coordinate)) return options.coordinate;
  if (Number.isFinite(options.x) && Number.isFinite(options.y)) return [options.x, options.y];
  return options.point || [0, 0];
}

function selectorFromDomOptions(options = {}, fallback = undefined) {
  if (options.selector) return options.selector;
  if (options.ref) return options.ref;
  if (options.node_id != null) return `[data-node-id="${cssEscape(String(options.node_id))}"]`;
  if (options.nodeId != null) return `[data-node-id="${cssEscape(String(options.nodeId))}"]`;
  if (fallback) return fallback;
  throw new Error('DOM CUA operation requires selector, ref, node_id, or nodeId.');
}

function scrollDirectionFromDeltas(options = {}) {
  const dx = options.deltaX ?? options.dx ?? 0;
  const dy = options.deltaY ?? options.dy ?? 0;
  if (Math.abs(dx) > Math.abs(dy)) return dx < 0 ? 'left' : 'right';
  return dy < 0 ? 'up' : 'down';
}

function createDialog(call, descriptor) {
  return {
    ...descriptor,
    async accept(text = '', options = {}) {
      return call({ type: 'dialog', action: 'accept', promptText: text }, options);
    },
    async dismiss(options = {}) {
      return call({ type: 'dialog', action: 'dismiss' }, options);
    },
  };
}

function createCapabilityCollection(scope) {
  const browserCapabilities = [
    { id: 'tabs', description: 'Create, select, list, finalize, and close browser tabs.' },
    { id: 'documentation', description: 'Read runtime API and troubleshooting documentation.' },
    { id: 'user-tabs', description: 'Inspect and claim tabs managed by the browser runtime.' },
    { id: 'playwright', description: 'Use the bundled Playwright/CDP-backed browser runtime.' },
    { id: 'computer', description: 'Drive browser mouse, keyboard, screenshots, scroll, and waits.' },
    { id: 'browser-selection', description: 'List, select, or switch connected browser instances.' },
    { id: 'history', description: 'Read browser history exposed by the backend.' },
  ];
  const tabCapabilities = [
    { id: 'playwright', description: 'Use Playwright-style evaluate and locator helpers.' },
    { id: 'accessibility-tree', description: 'Read page trees with stable element refs.' },
    { id: 'computer', description: 'Drive mouse, keyboard, scroll, screenshots, and waits.' },
    { id: 'forms', description: 'Set form input values by element ref.' },
    { id: 'console', description: 'Read captured browser console entries.' },
    { id: 'network', description: 'Read captured browser network entries.' },
    { id: 'file-upload', description: 'Upload files or images through file input elements.' },
    { id: 'shortcuts', description: 'List and execute browser shortcuts or workflows.' },
    { id: 'gif-recording', description: 'Capture browser recording artifacts.' },
    { id: 'clipboard', description: 'Read and write browser session clipboard text/items.' },
    { id: 'content-export', description: 'Export page content to a host artifact or string.' },
    { id: 'dialogs', description: 'Inspect and resolve JavaScript dialogs.' },
  ];
  const items = scope === 'browser' ? browserCapabilities : tabCapabilities;
  return {
    async list() {
      return [...items];
    },
    async get(id) {
      const item = items.find((capability) => capability.id === id);
      if (!item) {
        throw new Error(`Unknown ${scope} capability: ${id}`);
      }
      return {
        ...item,
        async documentation() {
          return `${item.id}: ${item.description}\n\n${browserDocumentation()}`;
        },
      };
    },
  };
}

function typeForBackend(backend) {
  switch (backend) {
    case 'extension':
      return 'extension';
    case 'cdp':
      return 'cdp';
    default:
      return 'iab';
  }
}

function callBrowserOperation(transport, operation, target = {}, options = {}) {
  return transport.call({
    operation,
    target,
    permissions: permissionsFor(operation),
    timeoutMs: options.timeoutMs || options.timeout || 30_000,
    metadata: options.metadata || {},
  });
}

function browserDocumentation() {
  return browserDocumentationText;
}

function cssEscape(value) {
  return value.replaceAll('\\', '\\\\').replaceAll('"', '\\"');
}

class BrowserSidecarTransport {
  constructor(transport, configuration) {
    this.transport = transport;
    this.configuration = configuration;
    this.seq = 0;
    this.pending = new Map();
    this.endpoint = null;
    this.generation = 0;
  }

  call(request) {
    const generation = this.ensureStarted();
    const id = 'browser-client-' + (++this.seq);
    const envelopeOptions = this.configuration.artifactDir === undefined
      ? { headless: this.configuration.headless }
      : { artifactDir: this.configuration.artifactDir, headless: this.configuration.headless };
    const normalizedRequest = {
      ...request,
      operation: compactBrowserRecordV1(request.operation),
      target: compactBrowserRecordV1(request.target),
    };
    const encoded = exactBrowserRuntimeJsonCodecV1.encode({
      id,
      request: normalizedRequest,
      options: envelopeOptions,
    });
    return new Promise((resolve, reject) => {
      this.pending.set(id, { generation, resolve, reject, events: [] });
      try {
        this.transport.send(encoded, (error) => {
          const pending = this.pending.get(id);
          if (
            !pending ||
            pending.generation !== generation ||
            this.endpoint?.generation !== generation
          ) return;
          if (error) {
            this.pending.delete(id);
            pending.reject(error);
          }
        });
      } catch (error) {
        const pending = this.pending.get(id);
        if (pending?.generation === generation) {
          this.pending.delete(id);
          pending.reject(error);
        }
      }
    });
  }

  ensureStarted() {
    if (this.endpoint?.phase === 'Registered') return this.endpoint.generation;
    const endpoint = {
      generation: ++this.generation,
      phase: 'Registering',
    };
    this.endpoint = endpoint;
    try {
      this.transport.open(this.configuration, {
        message: (bytes) => this.handleMessage(endpoint.generation, bytes),
        close: (_error) => this.handleClose(endpoint.generation),
      });
      if (this.endpoint === endpoint) endpoint.phase = 'Registered';
      return endpoint.generation;
    } catch (error) {
      if (this.endpoint === endpoint) this.endpoint = null;
      throw error;
    }
  }

  handleMessage(generation, bytes) {
    if (this.endpoint?.generation !== generation || this.endpoint.phase !== 'Registered') return;
    let message;
    try {
      message = exactBrowserRuntimeJsonCodecV1.decode(bytes);
    } catch (_error) {
      return;
    }
    if (!message || typeof message !== 'object' || typeof message.id !== 'string') return;
    const pending = this.pending.get(message.id);
    if (!pending || pending.generation !== generation) return;
    if (browserObjectHasOwn(message, 'event')) {
      pending.events.push(message.event);
      return;
    }
    if (typeof message.ok !== 'boolean') return;
    this.pending.delete(message.id);
    if (message.ok) {
      pending.resolve({ ...message.response, streamedEvents: pending.events });
    } else {
      const error = new Error(message.error?.message || 'Browser sidecar error.');
      error.code = message.error?.code || 'host_error';
      error.details = message.error?.details;
      pending.reject(error);
    }
  }

  handleClose(generation) {
    if (this.endpoint?.generation !== generation || this.endpoint.phase === 'Disposing') return;
    this.endpoint = null;
    const error = new Error('Browser sidecar exited.');
    for (const [id, pending] of this.pending) {
      if (pending.generation !== generation) continue;
      this.pending.delete(id);
      pending.reject(error);
    }
  }

  async dispose() {
    const endpoint = this.endpoint;
    if (!endpoint) return;
    endpoint.phase = 'Disposing';
    try {
      this.transport.close();
    } finally {
      for (const [id, pending] of this.pending) {
        if (pending.generation === endpoint.generation) this.pending.delete(id);
      }
      if (this.endpoint === endpoint) this.endpoint = null;
    }
  }
}

function permissionsFor(operation) {
  const type = operation.type;
  return {
    navigate: ['navigate'].includes(type),
    readPage: [
      'navigate',
      'snapshot',
      'screenshot',
      'click',
      'type',
      'eval',
      'read_page',
      'find',
      'form_input',
      'computer',
      'locator',
      'page_text',
      'resize',
      'gif_creator',
      'upload_image',
      'file_upload',
      'shortcuts_list',
      'history',
      'tabs_content',
      'content_export',
      'clipboard',
      'dialog',
      'download',
      'playwright_wait_for_event',
      'playwright_wait_for_load_state',
      'playwright_wait_for_url',
      'playwright_dom_snapshot',
      'playwright_element_info',
      'playwright_element_screenshot',
    ].includes(type),
    captureScreenshot: ['screenshot', 'computer', 'gif_creator', 'playwright_element_screenshot'].includes(type),
    interact: ['click', 'type', 'form_input', 'computer', 'locator', 'batch', 'file_upload', 'upload_image', 'shortcuts_execute', 'clipboard', 'dialog', 'download', 'playwright_file_chooser_set_files'].includes(type),
    javascriptEval: type === 'eval',
    readConsole: type === 'console',
    readNetwork: type === 'network',
    manageTabs: ['tabs', 'close', 'batch', 'switch_browser', 'select_browser', 'resolve_tab', 'claim_tab', 'mark_tab'].includes(type),
    sessionState: ['navigate', 'click', 'type', 'eval', 'close', 'tabs', 'form_input', 'computer', 'locator', 'resize', 'file_upload', 'upload_image', 'shortcuts_list', 'shortcuts_execute', 'switch_browser', 'select_browser', 'resolve_tab', 'claim_tab', 'list_connected_browsers', 'history', 'tabs_content', 'clipboard', 'dialog', 'mark_tab', 'download', 'playwright_wait_for_event', 'playwright_file_chooser_set_files', 'playwright_wait_for_load_state', 'playwright_wait_for_url', 'playwright_dom_snapshot', 'playwright_element_info', 'playwright_element_screenshot'].includes(type),
    fileTransfer: (type === 'screenshot' && operation.save !== false) || (type === 'computer' && operation.save === true) || ['batch', 'file_upload', 'upload_image', 'content_export', 'download', 'playwright_wait_for_event', 'playwright_file_chooser_set_files'].includes(type),
  };
}

function isOptionsOnly(value) {
  return (
    value &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    ('timeout' in value || 'timeoutMs' in value || 'awaitPromise' in value || 'metadata' in value)
  );
}
