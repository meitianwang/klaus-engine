#!/usr/bin/env node

import { createHash } from 'node:crypto'
import {
  chmodSync,
  existsSync,
  lstatSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  renameSync,
  rmSync,
  writeFileSync,
} from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { exactBrowserBrokerSourceV1 } from './exact-browser-broker-source.mjs'

const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..')
export const SOURCE_ROOT = 'plugins/klaus-bundled/browser'
export const SOURCE_LOCK = 'bundled-browser-assets.lock.json'
export const CANDIDATE_ROOT = 'crates/sdk/tests/fixtures/exact-browser-v1'
export const CANDIDATE_TREE = 'browser'
export const CANDIDATE_LOCK = 'bundled-browser-assets.lock.json'
export const PUBLISHED_MIRROR_ROOT = 'crates/sdk/assets/klaus-bundled/browser'
export const PUBLISHED_EMBEDDED_LOCK = 'crates/sdk/assets/bundled-browser-assets.lock.json'
export const PUBLISHED_SOURCE_MANIFEST = 'ASSET-SOURCES.sha256'

const BROWSER_RUNTIME_TRANSPORT_V1 = Object.freeze({
  owner: 'rust',
  version: 1,
  clientModule: 'klaus:browser-host-transport',
  wire: 'canonical-runtime-json-utf8',
  playwright: '1.61.1-pinned',
  standaloneNode: false,
})

const UTILITY_SHIMS = [
  'check-extension-installed',
  'check-native-host-manifest',
  'install-native-host-manifest',
  'chrome-is-running',
  'installed-browsers',
  'open-chrome-window',
]

function sha256(bytes) {
  return createHash('sha256').update(bytes).digest('hex')
}

function u32be(value) {
  const bytes = Buffer.allocUnsafe(4)
  bytes.writeUInt32BE(value)
  return bytes
}

function u64be(value) {
  const bytes = Buffer.allocUnsafe(8)
  bytes.writeBigUInt64BE(BigInt(value))
  return bytes
}

function pathSetDigest(files) {
  const hash = createHash('sha256')
  hash.update(Buffer.from('KLAUS_BROWSER_PATH_SET_V1\0', 'ascii'))
  for (const file of files) {
    const path = Buffer.from(file.path, 'utf8')
    hash.update(u32be(path.length))
    hash.update(path)
  }
  return hash.digest('hex')
}

function treeDigest(files) {
  const hash = createHash('sha256')
  hash.update(Buffer.from('KLAUS_BROWSER_ASSET_TREE_V1\0', 'ascii'))
  for (const file of files) {
    const path = Buffer.from(file.path, 'utf8')
    hash.update(u32be(path.length))
    hash.update(path)
    hash.update(Buffer.from(file.mode, 'ascii'))
    hash.update(u64be(file.size))
    hash.update(Buffer.from(file.sha256, 'hex'))
  }
  return hash.digest('hex')
}

function migrationDescriptorSource(from, subcommand, directMessage) {
  return `#!/usr/bin/env node
import { pathToFileURL } from 'node:url';

export const migration = Object.freeze({
  version: 1,
  from: ${JSON.stringify(from)},
  command: 'klaus-browser',
  subcommand: ${JSON.stringify(subcommand)},
  directExitCode: 64,
});

export default migration;

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  process.stderr.write(${JSON.stringify(`${directMessage}\n`)});
  process.exitCode = migration.directExitCode;
}
`
}

function renderUtilityShim(subcommand) {
  const from = `scripts/${subcommand}.mjs`
  return migrationDescriptorSource(
    from,
    subcommand,
    `This browser utility moved to the Rust CLI: klaus-browser ${subcommand}.`,
  )
}

function renderNativeHostShim() {
  return migrationDescriptorSource(
    'runtime/native-host.mjs',
    'install-native-host-manifest',
    'Legacy JavaScript native host disabled; run klaus-browser install-native-host-manifest.',
  )
}

function spliceFunctionRange(source, startToken, endToken, replacement, label) {
  const start = source.indexOf(startToken)
  const end = source.indexOf(endToken, start + startToken.length)
  if (start === -1 || end === -1 || end <= start) {
    throw new Error(`cannot locate ${label} in service-worker source`)
  }
  return `${source.slice(0, start)}${replacement}${source.slice(end)}`
}

function renderWebSocketOnlyServiceWorker(sourceBytes) {
  let source = sourceBytes.toString('utf8')
  for (const declaration of [
    "const NATIVE_HOST_NAME = 'com.klaus.browser';\n",
    'let nativePort = null;\n',
    'let nativeUnavailableUntil = 0;\n',
  ]) {
    if (!source.includes(declaration)) {
      throw new Error(`service-worker migration anchor drifted: ${declaration.trim()}`)
    }
    source = source.replace(declaration, '')
  }
  source = spliceFunctionRange(
    source,
    'async function connect() {',
    'async function connectWebSocket() {',
    `async function connect() {
  if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) {
    return;
  }
  await connectWebSocket();
}

`,
    'native/WebSocket selection',
  )
  source = spliceFunctionRange(
    source,
    'function send(message) {',
    'async function executeRequest(request) {',
    `function send(message) {
  if (!socket || socket.readyState !== WebSocket.OPEN) return;
  socket.send(JSON.stringify(message));
}

`,
    'bridge send selection',
  )
  for (const forbidden of ['connectNative', 'nativePort', 'nativeUnavailableUntil']) {
    if (source.includes(forbidden)) {
      throw new Error(`service-worker retained native messaging authority: ${forbidden}`)
    }
  }
  return source
}

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

function exactRuntimeJsonCodecBlock() {
  const factory = createExactBrowserRuntimeJsonCodecV1.toString()
  const digest = sha256(Buffer.from(factory, 'utf8'))
  return `/* KLAUS_EXACT_BROWSER_RUNTIME_JSON_CODEC_V1_BEGIN ${digest} */
${factory}
/* KLAUS_EXACT_BROWSER_RUNTIME_JSON_CODEC_V1_END ${digest} */`
}

function renderBrowserClient(sourceBytes) {
  let source = sourceBytes.toString('utf8')
  const setupAnchor = 'export async function setupBrowserRuntime({ globals = globalThis, options = {} } = {}) {'
  const setupIndex = source.indexOf(setupAnchor)
  if (setupIndex === -1) throw new Error('browser-client setup anchor drifted')
  source = `import {
  acquireBrowserHostTransportV1,
  assertBrowserHostTransportV1,
  isBrowserRuntimeProxyV1,
  browserHostDefaults,
  browserDocumentationText,
} from 'klaus:browser-host-transport';

${exactRuntimeJsonCodecBlock()}

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

${source.slice(setupIndex)}`

  const originalRegistryPrefix = `export function createBrowserRegistry(options = {}) {
  const transport = new BrowserSidecarTransport(options);
  const backend = options.backend || process.env.KLAUS_BROWSER_BACKEND || 'launch';
  const browserType = options.browserType || typeForBackend(backend);`
  const migratedRegistryPrefix = `export function createBrowserRegistry(options = {}) {
  const host = prepareBrowserHost(options);
  const transport = new BrowserSidecarTransport(host.capability, host.configuration);
  const backend = host.backend;
  const browserType = host.browserType;`
  if (!source.includes(originalRegistryPrefix)) {
    throw new Error('browser-client registry transport anchor drifted')
  }
  source = source.replace(originalRegistryPrefix, migratedRegistryPrefix)

  const documentationStart = source.indexOf('function browserDocumentation() {')
  const documentationEnd = source.indexOf('function cssEscape(value) {', documentationStart)
  if (documentationStart === -1 || documentationEnd === -1) {
    throw new Error('browser-client documentation anchor drifted')
  }
  source = `${source.slice(0, documentationStart)}function browserDocumentation() {
  return browserDocumentationText;
}

${source.slice(documentationEnd)}`

  const transportStart = source.indexOf('class BrowserSidecarTransport {')
  const transportEnd = source.indexOf('function permissionsFor(type) {', transportStart)
  if (transportStart === -1 || transportEnd === -1) {
    throw new Error('browser-client transport anchor drifted')
  }
  const transportSource = `class BrowserSidecarTransport {
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

`
  source = `${source.slice(0, transportStart)}${transportSource}${source.slice(transportEnd)}`

  for (const forbidden of [
    "node:child_process",
    "node:fs",
    "node:path",
    "node:readline",
    "node:url",
    'process.',
    'defaultSidecarPath',
  ]) {
    if (source.includes(forbidden)) {
      throw new Error(`browser-client retained host authority: ${forbidden}`)
    }
  }
  return source
}

function renderPlaywrightSidecar(sourceBytes) {
  let source = sourceBytes.toString('utf8')
  const childProcessImport = "import { spawn } from 'node:child_process';\n"
  if (!source.includes(childProcessImport)) {
    throw new Error('playwright-sidecar child_process import anchor drifted')
  }
  source = source.replace(
    childProcessImport,
    "import { EventEmitter } from 'node:events';\nimport { Duplex, Readable } from 'node:stream';\n",
  )
  const importAnchor = "import { fileURLToPath, pathToFileURL } from 'node:url';\n"
  if (!source.includes(importAnchor)) {
    throw new Error('playwright-sidecar import anchor drifted')
  }
  source = source.replace(
    importAnchor,
    `${importAnchor}import { types as browserNodeUtilTypes } from 'node:util';\n\n${exactRuntimeJsonCodecBlock()}\n\nconst exactBrowserRuntimeJsonCodecV1 = createExactBrowserRuntimeJsonCodecV1(\n  browserNodeUtilTypes.isProxy,\n);\n\nfunction decodeBrowserSidecarLineV1(line) {\n  const encoded = Buffer.from(line, 'utf8');\n  const bytes = new Uint8Array(encoded.length);\n  for (let index = 0; index < encoded.length; index += 1) bytes[index] = encoded[index];\n  return exactBrowserRuntimeJsonCodecV1.decode(bytes);\n}\n`,
  )
  const requireAnchor = 'const requireFromHere = createRequire(import.meta.url);\n'
  if (!source.includes(requireAnchor)) {
    throw new Error('playwright-sidecar private require anchor drifted')
  }
  source = source.replace(
    requireAnchor,
    `${requireAnchor}\n${exactBrowserBrokerSourceV1()}\n`,
  )
  const cwdRequireAnchor = "const requireFromCwd = createRequire(path.join(process.cwd(), 'package.json'));\n"
  if (!source.includes(cwdRequireAnchor)) {
    throw new Error('playwright-sidecar cwd require anchor drifted')
  }
  source = source.replace(cwdRequireAnchor, '')

  const concurrentOriginal = `function isConcurrentRequestLine(line) {
  try {
    const message = JSON.parse(line);
    return message?.request?.operation?.type === 'playwright_wait_for_event';
  } catch (_error) {
    return false;
  }
}`
  const concurrentMigrated = `function isConcurrentRequestLine(line) {
  try {
    const message = decodeBrowserSidecarLineV1(line);
    return message?.request?.operation?.type === 'playwright_wait_for_event';
  } catch (_error) {
    return false;
  }
}`
  if (!source.includes(concurrentOriginal)) {
    throw new Error('playwright-sidecar concurrent request decoder anchor drifted')
  }
  source = source.replace(concurrentOriginal, concurrentMigrated)

  const handleOriginal = `  try {
    message = JSON.parse(line);
  } catch (error) {`
  const handleMigrated = `  try {
    message = decodeBrowserSidecarLineV1(line);
  } catch (error) {`
  if (!source.includes(handleOriginal)) {
    throw new Error('playwright-sidecar request decoder anchor drifted')
  }
  source = source.replace(handleOriginal, handleMigrated)

  const lineProtocolStart = source.indexOf(
    "const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });",
  )
  const lineProtocolEnd = source.indexOf('function isConcurrentRequestLine(line) {', lineProtocolStart)
  if (lineProtocolStart === -1 || lineProtocolEnd === -1) {
    throw new Error('playwright-sidecar line protocol startup anchor drifted')
  }
  const lineProtocol = `let queue = Promise.resolve();

function startApplicationLineProtocolV1() {
  const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
  rl.on('line', (line) => {
    if (isConcurrentRequestLine(line)) {
      void handleLine(line).catch((error) => {
        write({
          id: 'unknown',
          ok: false,
          error: normalizeError(error, 'host_error'),
        });
      });
      return;
    }
    queue = queue.then(() => handleLine(line)).catch((error) => {
      write({
        id: 'unknown',
        ok: false,
        error: normalizeError(error, 'host_error'),
      });
    });
  });
  process.on('SIGTERM', () => shutdown().finally(() => process.exit(0)));
  process.on('SIGINT', () => shutdown().finally(() => process.exit(0)));
}

`
  source = `${source.slice(0, lineProtocolStart)}${lineProtocol}${source.slice(lineProtocolEnd)}`

  const loaderStart = source.indexOf('async function loadPlaywright() {')
  const loaderEnd = source.indexOf('function instrumentPage(page, tabId) {', loaderStart)
  if (loaderStart === -1 || loaderEnd === -1) {
    throw new Error('playwright-sidecar package loader anchor drifted')
  }
  const verifiedLoader = `let verifiedPlaywrightLoaderV1 = null;

function installVerifiedPlaywrightLoaderV1(loader) {
  if (verifiedPlaywrightLoaderV1 !== null || typeof loader !== 'function') {
    throw new BrowserSidecarError(
      'unavailable',
      'Pinned Playwright 1.61.1 asset is missing or corrupt',
    );
  }
  verifiedPlaywrightLoaderV1 = loader;
}

const browserBrokerV1 = await bootstrapPinnedBrowserBrokerV1();
startApplicationLineProtocolV1();

async function loadPlaywright() {
  if (verifiedPlaywrightLoaderV1 === null) {
    throw new BrowserSidecarError(
      'unavailable',
      'Pinned Playwright 1.61.1 asset is missing or corrupt',
    );
  }
  try {
    const mod = await verifiedPlaywrightLoaderV1();
    const normalized = mod?.chromium ? mod : mod?.default?.chromium ? mod.default : null;
    if (normalized?.chromium) return normalized;
  } catch (_error) {
    // The verified package boundary exposes one stable error without host paths.
  }
  throw new BrowserSidecarError(
    'unavailable',
    'Pinned Playwright 1.61.1 asset is missing or corrupt',
  );
}

`
  source = `${source.slice(0, loaderStart)}${verifiedLoader}${source.slice(loaderEnd)}`

  const ffmpegStart = source.indexOf('async function encodeGifWithFfmpeg(framePaths, filePath, fps) {')
  const ffmpegEnd = source.indexOf('function minimalGifBuffer() {', ffmpegStart)
  if (ffmpegStart === -1 || ffmpegEnd === -1) {
    throw new Error('playwright-sidecar FFmpeg process anchor drifted')
  }
  const brokeredFfmpeg = `async function encodeGifWithFfmpeg(framePaths, filePath, fps) {
  if (framePaths.length === 0) {
    throw new Error('no frames to encode');
  }
  const listPath = path.join(path.dirname(filePath), \`gif-input-\${Date.now()}-\${Math.random().toString(16).slice(2)}.txt\`);
  const normalizedFps = Math.max(1, fps);
  const duration = 1 / normalizedFps;
  const lines = [];
  for (const framePath of framePaths) {
    lines.push(\`file '\${framePath.replaceAll("'", "'\\\\''")}'\`);
    lines.push(\`duration \${duration}\`);
  }
  lines.push(\`file '\${framePaths[framePaths.length - 1].replaceAll("'", "'\\\\''")}'\`);
  await fsp.writeFile(listPath, \`\${lines.join('\\n')}\\n\`);
  try {
    await runBrokeredFfmpegV1(listPath, filePath, normalizedFps);
    return { encoded: true, encoder: 'ffmpeg' };
  } finally {
    await fsp.unlink(listPath).catch(() => {});
  }
}

function runBrokeredFfmpegV1(listPath, filePath, fps) {
  return new Promise((resolve, reject) => {
    const child = browserBrokerV1.operations.BrowserFfmpegEncode({ listPath, filePath, fps });
    let stderr = '';
    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString('utf8');
    });
    child.once('error', reject);
    child.once('exit', (code, signal) => {
      if (code === 0) resolve();
      else reject(new Error(\`ffmpeg failed with \${signal || code}: \${stderr.trim()}\`));
    });
  });
}

`
  source = `${source.slice(0, ffmpegStart)}${brokeredFfmpeg}${source.slice(ffmpegEnd)}`

  const writeOriginal = `function write(value) {
  process.stdout.write(\`${'${JSON.stringify(value)}'}\\n\`);
}`
  const writeMigrated = `function write(value) {
  const encoded = exactBrowserRuntimeJsonCodecV1.encode(value);
  const framed = new Uint8Array(encoded.length + 1);
  for (let index = 0; index < encoded.length; index += 1) framed[index] = encoded[index];
  framed[encoded.length] = 10;
  process.stdout.write(framed);
}`
  if (!source.includes(writeOriginal)) {
    throw new Error('playwright-sidecar response encoder anchor drifted')
  }
  source = source.replace(writeOriginal, writeMigrated)
  for (const forbidden of [
    'KLAUS_BROWSER_PLAYWRIGHT_MODULE',
    'requireFromCwd',
    "import('playwright')",
    'npm install playwright',
    "node:child_process",
    'function runProcess(',
  ]) {
    if (source.includes(forbidden)) {
      throw new Error(`playwright-sidecar retained host package authority: ${forbidden}`)
    }
  }
  return source
}

function replaceDocumentRange(source, start, end, replacement, label) {
  const startIndex = source.indexOf(start)
  const endIndex = source.indexOf(end, startIndex + start.length)
  if (startIndex === -1 || endIndex === -1) {
    throw new Error(`browser documentation anchor drifted: ${label}`)
  }
  return `${source.slice(0, startIndex)}${replacement}${source.slice(endIndex)}`
}

function renderPlaywrightDocumentation(sourceBytes) {
  let source = sourceBytes.toString('utf8')
  source = replaceDocumentRange(
    source,
    'This plugin ships a Node sidecar',
    'The sidecar speaks newline-delimited JSON over stdio.',
    `This plugin ships hash-attested browser assets and a Rust-owned broker that
starts the bundled sidecar with pinned Playwright 1.61.1. The Rust runtime owns
the model-facing protocol, exact RuntimeJson transport, permissions, process
tree, timeouts, cancellation, cleanup, and artifacts. Host npm modules,
standalone client loading, and caller-selected sidecar paths are not supported.

The locked browser client is evaluated only in the Rust-owned sandbox and
acquires its context-bound \`BrowserHostTransportV1\` capability there. Embedders
configure the SDK browser runtime through the engine; model-authored code uses
\`agent.browsers\` and never receives process, argv, cwd, environment, or package
resolution authority.

`,
    'host Playwright installation and self-spawn example',
  )
  source = source.replace(
    concatLines([
      'The extension has the stable id `gjamccelebgpogpbiceajehcajghphnb`. It first',
      'tries the Chrome native messaging host `com.klaus.browser`. The native host',
      'connects to the sidecar through a local socket at',
    ]),
    concatLines([
      'The extension has the stable id `gjamccelebgpogpbiceajehcajghphnb` and',
      'connects directly to the token- and Origin-authenticated local WebSocket bridge.',
      'The optional Rust `klaus-browser-native-host` compatibility binary connects through',
    ]),
  )
  source = source.replace(
    'If the native socket\nis unavailable, the native host and extension can fall back to the local\nWebSocket bridge',
    'If the native socket is unavailable, that compatibility host can fall back to the local\nWebSocket bridge',
  )
  const oldCommands = `node plugins/klaus-bundled/browser/scripts/check-extension-installed.mjs --json
node plugins/klaus-bundled/browser/scripts/install-native-host-manifest.mjs --json
node plugins/klaus-bundled/browser/scripts/check-native-host-manifest.mjs --json
node plugins/klaus-bundled/browser/scripts/chrome-is-running.mjs --json
node plugins/klaus-bundled/browser/scripts/installed-browsers.mjs --json
node plugins/klaus-bundled/browser/scripts/open-chrome-window.mjs --dry-run --json`
  const newCommands = `klaus-browser check-extension-installed --json
klaus-browser install-native-host-manifest --json
klaus-browser check-native-host-manifest --json
klaus-browser chrome-is-running --json
klaus-browser installed-browsers --json
klaus-browser open-chrome-window --dry-run --json`
  if (!source.includes(oldCommands)) {
    throw new Error('browser documentation utility command anchor drifted')
  }
  source = source.replace(oldCommands, newCommands)
  return source
}

function concatLines(lines) {
  return `${lines.join('\n')}\n`
}

function renderBrowserSkill(sourceBytes) {
  let source = sourceBytes.toString('utf8')
  source = source.replace(
    'description: Control the bundled Klaus browser runtime through browser tools or the Node browser-client.',
    'description: Control the bundled Klaus browser runtime through browser tools or its Rust-hosted client.',
  )
  source = replaceDocumentRange(
    source,
    'Use the `extension` backend',
    'Prefer `readPage()`',
    `Use the \`extension\` backend when the task needs the user's real Chrome tabs,
profile cookies, logged-in sessions, or browser extension state. The built-in
Chrome connector uses the token- and Origin-authenticated local
WebSocket bridge directly. The Rust \`klaus-browser-native-host\` binary remains
only as compatibility tooling for older external extensions.

The locked browser client is loaded only by the Rust-owned sandbox and receives
a context-bound transport capability. Do not import it from standalone Node or
provide process, sidecar, cwd, or environment options.

`,
    'skill standalone Node client guidance',
  )
  source = source.replaceAll('Node browser-client', 'Rust-hosted browser client')
  return source
}

function renderBrowserApi(sourceBytes) {
  const api = JSON.parse(sourceBytes.toString('utf8'))
  api.nativeHost = 'klaus-browser-native-host'
  api.backends.launch = 'Launch pinned Chromium through the Rust-owned Playwright broker.'
  api.backends.extension = 'Connect to the bundled extension through the authenticated local WebSocket bridge.'
  api.extension.nativeHost = 'klaus-browser-native-host'
  api.extension.builtInTransport = 'authenticated-websocket'
  api.extension.scripts = api.extension.scripts.map((shim) => ({
    shim,
    directExitCode: 64,
    command: `klaus-browser ${shim.slice(shim.lastIndexOf('/') + 1, -4)}`,
  }))
  api.runtimeTransport = { ...BROWSER_RUNTIME_TRANSPORT_V1 }
  return `${JSON.stringify(api, null, 2)}\n`
}

function renderBrowserPluginManifest(sourceBytes) {
  const manifest = JSON.parse(sourceBytes.toString('utf8'))
  manifest.description = 'Klaus bundled browser runtime with a Rust-owned pinned Playwright broker and authenticated Chrome extension bridge.'
  return `${JSON.stringify(manifest, null, 2)}\n`
}

function candidateBytes(path, sourceBytes) {
  if (path === 'runtime/native-host.mjs') {
    return Buffer.from(renderNativeHostShim(), 'utf8')
  }
  if (path === 'extension/service-worker.js') {
    return Buffer.from(renderWebSocketOnlyServiceWorker(sourceBytes), 'utf8')
  }
  if (path === 'scripts/browser-client.mjs') {
    return Buffer.from(renderBrowserClient(sourceBytes), 'utf8')
  }
  if (path === 'runtime/playwright-sidecar.mjs') {
    return Buffer.from(renderPlaywrightSidecar(sourceBytes), 'utf8')
  }
  if (path === 'docs/playwright.md') {
    return Buffer.from(renderPlaywrightDocumentation(sourceBytes), 'utf8')
  }
  if (path === 'docs/api.json') {
    return Buffer.from(renderBrowserApi(sourceBytes), 'utf8')
  }
  if (path === 'skills/control-browser/SKILL.md') {
    return Buffer.from(renderBrowserSkill(sourceBytes), 'utf8')
  }
  if (path === '.codex-plugin/plugin.json') {
    return Buffer.from(renderBrowserPluginManifest(sourceBytes), 'utf8')
  }
  const utility = UTILITY_SHIMS.find((name) => path === `scripts/${name}.mjs`)
  if (utility) {
    return Buffer.from(renderUtilityShim(utility), 'utf8')
  }
  return sourceBytes
}

export function createExactBrowserCandidateState({ repoRoot = REPO_ROOT } = {}) {
  const repository = resolve(repoRoot)
  const sourceLockBytes = readFileSync(join(repository, SOURCE_LOCK), 'utf8')
  const sourceLock = JSON.parse(sourceLockBytes)
  if (sourceLock.version !== 1 || sourceLock.sourceRoot !== SOURCE_ROOT) {
    throw new Error('canonical bundled browser source lock identity drifted')
  }
  if (!Array.isArray(sourceLock.files) || sourceLock.files.length !== 24) {
    throw new Error('canonical bundled browser source lock must contain 24 files')
  }
  let checkedCandidateLock = null
  const checkedCandidateLockPath = join(repository, CANDIDATE_ROOT, CANDIDATE_LOCK)
  if (existsSync(checkedCandidateLockPath)) {
    checkedCandidateLock = JSON.parse(readFileSync(checkedCandidateLockPath, 'utf8'))
  }
  const sourceEntries = sourceLock.files.map((sourceFile) => {
    const sourceBytes = readFileSync(join(repository, SOURCE_ROOT, sourceFile.path))
    if (sourceBytes.length !== sourceFile.size || sha256(sourceBytes) !== sourceFile.sha256) {
      throw new Error(`canonical bundled browser source bytes drifted: ${sourceFile.path}`)
    }
    return { sourceFile, sourceBytes }
  })
  const apiEntry = sourceEntries.find(({ sourceFile }) => sourceFile.path === 'docs/api.json')
  if (!apiEntry) throw new Error('canonical bundled browser source has no API contract')
  const api = JSON.parse(apiEntry.sourceBytes.toString('utf8'))
  const sourceAlreadyExact = Object.hasOwn(api, 'runtimeTransport')
  if (sourceAlreadyExact) {
    const transport = api.runtimeTransport
    if (
      transport === null || typeof transport !== 'object' || Array.isArray(transport)
      || Object.keys(transport).length !== Object.keys(BROWSER_RUNTIME_TRANSPORT_V1).length
      || Object.entries(BROWSER_RUNTIME_TRANSPORT_V1).some(([key, value]) => transport[key] !== value)
    ) {
      throw new Error('canonical bundled browser runtime transport contract is unsupported')
    }
  }
  // A published runtime stays published when its locked source bytes change.
  // The candidate fixture is output evidence, not the input-format discriminator:
  // using its old tree hash here reran legacy string/Node migrations on V1 objects.
  const entries = sourceEntries.map(({ sourceFile, sourceBytes }) => {
    const bytes = sourceAlreadyExact ? sourceBytes : candidateBytes(sourceFile.path, sourceBytes)
    return {
      path: sourceFile.path,
      mode: sourceFile.mode,
      bytes,
      size: bytes.length,
      sha256: sha256(bytes),
    }
  })
  const files = entries.map(({ path, mode, size, sha256: digest }) => ({
    path,
    size,
    mode,
    sha256: digest,
  }))
  const lock = {
    version: 1,
    sourceTreeSha256: sourceAlreadyExact
      && checkedCandidateLock?.version === 1
      && checkedCandidateLock.root === CANDIDATE_TREE
      ? checkedCandidateLock.sourceTreeSha256
      : sourceLock.treeSha256,
    root: CANDIDATE_TREE,
    files,
    pathSetSha256: pathSetDigest(files),
    treeSha256: treeDigest(files),
  }
  return {
    entries,
    lock,
    lockBytes: `${JSON.stringify(lock)}\n`,
  }
}

function publishedState(candidateState) {
  const files = candidateState.lock.files.map((file) => ({ ...file }))
  const lock = {
    version: 1,
    sourceRoot: SOURCE_ROOT,
    mirrorRoot: PUBLISHED_MIRROR_ROOT,
    files,
    pathSetSha256: candidateState.lock.pathSetSha256,
    treeSha256: candidateState.lock.treeSha256,
  }
  return {
    lock,
    lockBytes: `${JSON.stringify(lock)}\n`,
    sourceManifest: files.map((file) => `${file.sha256}  ${file.path}\n`).join(''),
  }
}

function writePublishedTree(root, entries, sourceManifest = null) {
  mkdirSync(root, { recursive: true, mode: 0o700 })
  for (const entry of entries) {
    const path = join(root, entry.path)
    mkdirSync(dirname(path), { recursive: true })
    writeFileSync(path, entry.bytes, { mode: entry.mode === '100755' ? 0o755 : 0o644 })
    if (process.platform !== 'win32') {
      chmodSync(path, entry.mode === '100755' ? 0o755 : 0o644)
    }
  }
  if (sourceManifest !== null) {
    writeFileSync(join(root, PUBLISHED_SOURCE_MANIFEST), sourceManifest, { mode: 0o644 })
  }
}

function replaceWithRollback(replacements) {
  const applied = []
  try {
    for (const replacement of replacements) {
      const backup = `${replacement.target}.bak-${process.pid}`
      rmSync(backup, { recursive: true, force: true })
      if (existsSync(replacement.target)) renameSync(replacement.target, backup)
      try {
        renameSync(replacement.staged, replacement.target)
      } catch (error) {
        if (existsSync(backup)) renameSync(backup, replacement.target)
        throw error
      }
      applied.push({ ...replacement, backup })
    }
  } catch (error) {
    for (const replacement of applied.reverse()) {
      rmSync(replacement.target, { recursive: true, force: true })
      if (existsSync(replacement.backup)) renameSync(replacement.backup, replacement.target)
    }
    throw error
  }
  for (const replacement of applied) {
    rmSync(replacement.backup, { recursive: true, force: true })
  }
}

export function promoteExactBrowserCandidate({
  repoRoot = REPO_ROOT,
  state = assertExactBrowserCandidateState({ repoRoot }),
} = {}) {
  const repository = resolve(repoRoot)
  const published = publishedState(state)
  const sourceStage = join(repository, `${SOURCE_ROOT}.stage-${process.pid}`)
  const mirrorStage = join(repository, `${PUBLISHED_MIRROR_ROOT}.stage-${process.pid}`)
  const lockStage = join(repository, `${SOURCE_LOCK}.stage-${process.pid}`)
  const embeddedLockStage = join(repository, `${PUBLISHED_EMBEDDED_LOCK}.stage-${process.pid}`)
  for (const path of [sourceStage, mirrorStage, lockStage, embeddedLockStage]) {
    rmSync(path, { recursive: true, force: true })
  }
  try {
    writePublishedTree(sourceStage, state.entries)
    writePublishedTree(mirrorStage, state.entries, published.sourceManifest)
    writeFileSync(lockStage, published.lockBytes, { mode: 0o644 })
    writeFileSync(embeddedLockStage, published.lockBytes, { mode: 0o644 })
    replaceWithRollback([
      { staged: sourceStage, target: join(repository, SOURCE_ROOT) },
      { staged: mirrorStage, target: join(repository, PUBLISHED_MIRROR_ROOT) },
      { staged: lockStage, target: join(repository, SOURCE_LOCK) },
      { staged: embeddedLockStage, target: join(repository, PUBLISHED_EMBEDDED_LOCK) },
    ])
  } finally {
    for (const path of [sourceStage, mirrorStage, lockStage, embeddedLockStage]) {
      rmSync(path, { recursive: true, force: true })
    }
  }
  return published
}

function collectRelativeFiles(root, directory = root, output = []) {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const absolute = join(directory, entry.name)
    const relative = absolute.slice(root.length + 1).split('\\').join('/')
    if (entry.isSymbolicLink()) throw new Error(`candidate tree contains symlink: ${relative}`)
    if (entry.isDirectory()) collectRelativeFiles(root, absolute, output)
    else if (entry.isFile()) output.push(relative)
    else throw new Error(`candidate tree contains nonregular entry: ${relative}`)
  }
  return output.sort((left, right) => Buffer.compare(Buffer.from(left), Buffer.from(right)))
}

export function assertExactBrowserCandidateState({
  repoRoot = REPO_ROOT,
  state = createExactBrowserCandidateState({ repoRoot }),
} = {}) {
  const root = join(resolve(repoRoot), CANDIDATE_ROOT)
  const tree = join(root, CANDIDATE_TREE)
  const actualPaths = collectRelativeFiles(tree)
  const expectedPaths = state.entries.map((entry) => entry.path)
  if (JSON.stringify(actualPaths) !== JSON.stringify(expectedPaths)) {
    throw new Error('exact browser candidate path set drifted')
  }
  for (const entry of state.entries) {
    const path = join(tree, entry.path)
    const metadata = lstatSync(path)
    if (!metadata.isFile() || metadata.isSymbolicLink()) {
      throw new Error(`exact browser candidate is not a regular file: ${entry.path}`)
    }
    const bytes = readFileSync(path)
    if (!bytes.equals(entry.bytes)) {
      throw new Error(`exact browser candidate bytes drifted: ${entry.path}`)
    }
    if (process.platform !== 'win32') {
      const expectedMode = entry.mode === '100755' ? 0o755 : 0o644
      if ((metadata.mode & 0o777) !== expectedMode) {
        throw new Error(`exact browser candidate mode drifted: ${entry.path}`)
      }
    }
  }
  if (readFileSync(join(root, CANDIDATE_LOCK), 'utf8') !== state.lockBytes) {
    throw new Error('exact browser candidate lock drifted')
  }
  return state
}

function writeCandidate(state) {
  const target = join(REPO_ROOT, CANDIDATE_ROOT)
  const temporary = `${target}.tmp-${process.pid}`
  rmSync(temporary, { recursive: true, force: true })
  mkdirSync(join(temporary, CANDIDATE_TREE), { recursive: true, mode: 0o700 })
  try {
    for (const entry of state.entries) {
      const path = join(temporary, CANDIDATE_TREE, entry.path)
      mkdirSync(dirname(path), { recursive: true })
      writeFileSync(path, entry.bytes, { mode: entry.mode === '100755' ? 0o755 : 0o644 })
      if (process.platform !== 'win32') {
        chmodSync(path, entry.mode === '100755' ? 0o755 : 0o644)
      }
    }
    writeFileSync(join(temporary, CANDIDATE_LOCK), state.lockBytes, { mode: 0o644 })
    rmSync(target, { recursive: true, force: true })
    renameSync(temporary, target)
  } finally {
    rmSync(temporary, { recursive: true, force: true })
  }
}

function main() {
  const [mode, ...rest] = process.argv.slice(2)
  if (rest.length !== 0 || !['--check', '--write', '--promote'].includes(mode)) {
    throw new Error('usage: node scripts/exact-browser-candidate.mjs --check|--write|--promote')
  }
  const state = createExactBrowserCandidateState()
  if (mode === '--write') writeCandidate(state)
  else if (mode === '--promote') promoteExactBrowserCandidate({ state })
  else assertExactBrowserCandidateState({ state })
  process.stdout.write(
    `exact browser candidate ${mode === '--write' ? 'written' : mode === '--promote' ? 'promoted' : 'OK'}: ${state.lock.files.length} files, tree ${state.lock.treeSha256}\n`,
  )
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try {
    main()
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`)
    process.exitCode = 1
  }
}
