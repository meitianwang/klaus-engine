#!/usr/bin/env node
import crypto from 'node:crypto';
import { EventEmitter } from 'node:events';
import { Duplex, Readable } from 'node:stream';
import { createRequire } from 'node:module';
import readline from 'node:readline';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import http from 'node:http';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { types as browserNodeUtilTypes } from 'node:util';

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
  browserNodeUtilTypes.isProxy,
);

// The exact codec accepts a plain Uint8Array and nothing else: its `strictUtf8` guard
// compares the prototype against `Uint8Array.prototype`, so a Buffer or a Buffer slice is
// rejected before a single byte is read. Everything that arrives off a pipe or is carved
// out of a frame is a Buffer, so each decode has to copy into a plain view first.
function exactBrowserRuntimeBytesV1(view) {
  const bytes = new Uint8Array(view.length);
  for (let index = 0; index < view.length; index += 1) bytes[index] = view[index];
  return bytes;
}

function decodeBrowserSidecarLineV1(line) {
  return exactBrowserRuntimeJsonCodecV1.decode(
    exactBrowserRuntimeBytesV1(Buffer.from(line, 'utf8')),
  );
}

const requireFromHere = createRequire(import.meta.url);

const KLAUS_BROWSER_BROKER_SYNC_MAX_V1 = 16 * 1024 * 1024;
const KLAUS_BROWSER_BROKER_MUX_MAX_V1 = 256 * 1024 * 1024;
const KLAUS_BROWSER_BROKER_ENV_ARG_V1 = '--klaus-browser-broker-env-key=';
const KLAUS_BROWSER_PIPE_ERROR_V1 = 'Browser broker native pipe binding failed.';
const KLAUS_BROWSER_READY_ERROR_V1 = 'Browser broker package readiness failed.';
const KLAUS_BROWSER_RUNTIME_ROOT_V1 = new URL('../../playwright-runtime-v1/', import.meta.url);
const KLAUS_BROWSER_PRELUDE_V1 = new URL('playwright-process-prelude.mjs', KLAUS_BROWSER_RUNTIME_ROOT_V1);
const KLAUS_BROWSER_PLAYWRIGHT_V1 = new URL('node_modules/playwright/index.mjs', KLAUS_BROWSER_RUNTIME_ROOT_V1);

const BrowserSyncKindV1 = Object.freeze({
  PlaywrightLaunch: 1,
  PlaywrightTreeKill: 2,
  PlaywrightMacOsSwVers: 3,
  PlaywrightLinuxLdd: 4,
  PlaywrightLinuxLdconfig: 5,
  PlaywrightWindowsWinldd: 6,
  BrowserFfmpegEncode: 7,
});

const BrowserMuxKindV1 = Object.freeze({
  StdoutData: 1,
  StderrData: 2,
  ExtraData: 3,
  Write: 4,
  WriteAck: 5,
  ReadCredit: 6,
  HalfClose: 7,
  StreamError: 8,
  StreamClose: 9,
  CloseHooksReady: 10,
  Exit: 11,
  Close: 12,
  CleanupComplete: 13,
  BeginGracefulClose: 14,
  GracefulCloseResult: 15,
  BeginForceClose: 16,
  ForceCloseResult: 17,
});

function browserBrokerFailureV1(message) {
  const error = new Error(message);
  error.code = 'host_error';
  return error;
}

function readExactFdV1(fd, length) {
  const output = Buffer.allocUnsafe(length);
  let offset = 0;
  while (offset < output.length) {
    let count;
    try {
      count = fs.readSync(fd, output, offset, output.length - offset, null);
    } catch (error) {
      if (error?.code === 'EINTR') continue;
      throw error;
    }
    if (count === 0) throw browserBrokerFailureV1(KLAUS_BROWSER_READY_ERROR_V1);
    offset += count;
  }
  return output;
}

function writeAllFdV1(fd, bytes) {
  let offset = 0;
  while (offset < bytes.length) {
    let count;
    try {
      count = fs.writeSync(fd, bytes, offset, bytes.length - offset);
    } catch (error) {
      if (error?.code === 'EINTR') continue;
      throw error;
    }
    if (count === 0) throw browserBrokerFailureV1(KLAUS_BROWSER_READY_ERROR_V1);
    offset += count;
  }
}

function parseUnixPackagePermitV1(record) {
  if (
    record.length !== 48 ||
    record.subarray(0, 4).toString('ascii') !== 'KBU1' ||
    record[4] !== 1 || record[5] !== 3 || record[6] !== 2 || record[7] !== 0
  ) throw browserBrokerFailureV1(KLAUS_BROWSER_READY_ERROR_V1);
  return Object.freeze({
    generation: record.readBigUInt64BE(8),
    token: Buffer.from(record.subarray(16, 48)),
  });
}

function unixPackageReadyV1(permit) {
  const record = Buffer.allocUnsafe(48);
  record.write('KBU1', 0, 'ascii');
  record[4] = 2;
  record[5] = 2;
  record[6] = 3;
  record[7] = 0;
  record.writeBigUInt64BE(permit.generation, 8);
  permit.token.copy(record, 16);
  return record;
}

function parseWindowsBrokerV1() {
  const switches = process.argv.slice(2).filter((argument) =>
    argument.startsWith(KLAUS_BROWSER_BROKER_ENV_ARG_V1));
  if (switches.length !== 1) throw browserBrokerFailureV1(KLAUS_BROWSER_PIPE_ERROR_V1);
  const key = switches[0].slice(KLAUS_BROWSER_BROKER_ENV_ARG_V1.length);
  if (!/^KLAUS_INTERNAL_BROWSER_BROKER_[0-9A-F]{64}$/.test(key)) {
    throw browserBrokerFailureV1(KLAUS_BROWSER_PIPE_ERROR_V1);
  }
  const aliases = Object.keys(process.env).filter((name) => name.toUpperCase() === key);
  if (aliases.length !== 1 || aliases[0] !== key || typeof process.env[key] !== 'string') {
    throw browserBrokerFailureV1(KLAUS_BROWSER_PIPE_ERROR_V1);
  }
  const bootstrap = process.env[key];
  delete process.env[key];
  const addon = requireFromHere('./klaus_browser_pipe.node');
  if (
    addon === null || typeof addon !== 'object' ||
    Object.keys(addon).join(',') !== 'openWindowsBrokerPipesV1' ||
    typeof addon.openWindowsBrokerPipesV1 !== 'function'
  ) throw browserBrokerFailureV1(KLAUS_BROWSER_PIPE_ERROR_V1);
  const pipes = addon.openWindowsBrokerPipesV1(bootstrap);
  const keys = ['version', 'generation', 'writeSync', 'readSyncExact', 'startMux', 'writeMux', 'markPackageReady', 'close'];
  if (
    pipes === null || typeof pipes !== 'object' || Object.keys(pipes).join(',') !== keys.join(',') ||
    pipes.version !== 1 || typeof pipes.generation !== 'bigint' ||
    keys.slice(2).some((name) => typeof pipes[name] !== 'function')
  ) throw browserBrokerFailureV1(KLAUS_BROWSER_PIPE_ERROR_V1);
  return pipes;
}

function exactOwnKeysV1(value, keys) {
  return value !== null && typeof value === 'object' && !Array.isArray(value) &&
    Object.keys(value).join(',') === keys.join(',');
}

function brokerErrorDescriptorV1(value) {
  if (!exactOwnKeysV1(value, ['name', 'message', 'code', 'errno', 'syscall', 'path'])) {
    throw browserBrokerFailureV1('Invalid browser broker response.');
  }
  const error = new Error(value.message);
  error.name = value.name;
  for (const key of ['code', 'errno', 'syscall', 'path']) {
    if (value[key] !== null) error[key] = value[key];
  }
  return error;
}

function orderedEnvironmentV1(environment) {
  if (environment === undefined || environment === null) return null;
  const rows = [];
  for (const key of Object.keys(environment)) rows.push([key, environment[key]]);
  return rows;
}

function projectRuntimeJsonV1(value, active = new Set()) {
  if (value === undefined || value === null) return null;
  if (typeof value === 'string' || typeof value === 'boolean' || typeof value === 'number') return value;
  if (typeof value !== 'object' || browserNodeUtilTypes.isProxy(value) || active.has(value)) {
    throw browserBrokerFailureV1('Invalid pinned Playwright source option record.');
  }
  active.add(value);
  try {
    if (Array.isArray(value)) {
      const output = [];
      for (let index = 0; index < value.length; index += 1) {
        if (!Object.hasOwn(value, index)) throw browserBrokerFailureV1('Invalid pinned Playwright source option record.');
        output.push(projectRuntimeJsonV1(value[index], active));
      }
      return output;
    }
    if (Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null) {
      throw browserBrokerFailureV1('Invalid pinned Playwright source option record.');
    }
    const output = {};
    for (const key of Object.keys(value)) {
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor || !Object.hasOwn(descriptor, 'value') || !descriptor.enumerable) {
        throw browserBrokerFailureV1('Invalid pinned Playwright source option record.');
      }
      const entry = descriptor.value;
      if (typeof entry === 'function' || typeof entry === 'symbol' || typeof entry === 'bigint') {
        throw browserBrokerFailureV1('Invalid pinned Playwright source option record.');
      }
      Object.defineProperty(output, key, {
        value: projectRuntimeJsonV1(entry, active),
        writable: true,
        enumerable: true,
        configurable: true,
      });
    }
    return output;
  } finally {
    active.delete(value);
  }
}

function launchPayloadV1(command, args, options) {
  return {
    command,
    args: args ?? [],
    options: {
      detached: options?.detached ?? false,
      cwd: options?.cwd ?? null,
      env: orderedEnvironmentV1(options?.env),
      shell: options?.shell ?? null,
      stdio: options?.stdio ?? null,
    },
    sourceOptions: projectRuntimeJsonV1(options?.__klausSourceOptionsV1 ?? null),
    tempDirectories: projectRuntimeJsonV1(options?.__klausTempDirectoriesV1 ?? []),
    mode: options?.__klausModeV1 ?? null,
    plannerTrace: projectRuntimeJsonV1(options?.__klausPlannerTraceV1 ?? null),
  };
}

class BrokerReadableV1 extends Readable {
  constructor(owner, streamId) {
    super({ highWaterMark: 16 * 1024 });
    this.owner = owner;
    this.streamId = streamId;
    this.lastDataSequence = 0n;
    this.creditOutstanding = false;
  }
  _read() {
    if (this.owner.spawnFailed) {
      this.push(null);
      return;
    }
    if (this.creditOutstanding || this.destroyed) return;
    this.creditOutstanding = true;
    this.owner.broker.sendMux(this.owner.objectId, this.streamId, BrowserMuxKindV1.ReadCredit, {
      sequenceClass: 'credit', correlation: this.lastDataSequence, credit: 1, payload: Buffer.alloc(0),
    });
  }
  accept(sequence, payload) {
    if (!this.creditOutstanding) throw browserBrokerFailureV1('Invalid browser broker mux credit.');
    this.creditOutstanding = false;
    this.lastDataSequence = sequence;
    if (this.push(Buffer.from(payload))) this._read();
  }
  acceptClose() { this.push(null); }
}

class BrokerDuplexV1 extends Duplex {
  constructor(owner, streamId) {
    super({ readableHighWaterMark: 16 * 1024, writableHighWaterMark: 16 * 1024 });
    this.owner = owner;
    this.streamId = streamId;
    this.lastDataSequence = 0n;
    this.creditOutstanding = false;
    this.writeCallbacks = new Map();
    this.finalCallback = null;
  }
  _read() {
    if (this.owner.spawnFailed) {
      this.push(null);
      return;
    }
    if (this.creditOutstanding || this.destroyed) return;
    this.creditOutstanding = true;
    this.owner.broker.sendMux(this.owner.objectId, this.streamId, BrowserMuxKindV1.ReadCredit, {
      sequenceClass: 'credit', correlation: this.lastDataSequence, credit: 1, payload: Buffer.alloc(0),
    });
  }
  _write(chunk, _encoding, callback) {
    if (this.owner.spawnFailed) {
      callback(browserBrokerFailureV1('Browser broker child did not start.'));
      return;
    }
    const sequence = this.owner.broker.sendMux(
      this.owner.objectId, this.streamId, BrowserMuxKindV1.Write,
      { sequenceClass: 'write', correlation: 0n, credit: 0, payload: Buffer.from(chunk) },
    );
    this.writeCallbacks.set(sequence, callback);
  }
  _final(callback) {
    if (this.owner.spawnFailed) {
      callback();
      return;
    }
    this.finalCallback = callback;
    this.owner.broker.sendMux(this.owner.objectId, this.streamId, BrowserMuxKindV1.HalfClose, {
      sequenceClass: 'control', correlation: 0n, credit: 0, payload: Buffer.alloc(0),
    });
  }
  _destroy(error, callback) {
    if (this.owner.spawnFailed) {
      callback(error);
      return;
    }
    this.owner.broker.sendMux(this.owner.objectId, this.streamId, BrowserMuxKindV1.StreamClose, {
      sequenceClass: 'control', correlation: 0n, credit: 0, payload: Buffer.alloc(0),
    });
    callback(error);
  }
  acceptData(sequence, payload) {
    if (!this.creditOutstanding) throw browserBrokerFailureV1('Invalid browser broker mux credit.');
    this.creditOutstanding = false;
    this.lastDataSequence = sequence;
    if (this.push(Buffer.from(payload))) this._read();
  }
  acceptWriteAck(correlation) {
    const callback = this.writeCallbacks.get(correlation);
    if (!callback) throw browserBrokerFailureV1('Invalid browser broker write acknowledgement.');
    this.writeCallbacks.delete(correlation);
    callback();
  }
  acceptClose(correlation) {
    if (this.finalCallback && correlation !== 0n) {
      const callback = this.finalCallback;
      this.finalCallback = null;
      callback();
    }
    this.push(null);
  }
}

class BrowserBrokerProxyV1 {
  constructor(broker, objectId, pid, kind) {
    this.broker = broker;
    this.objectId = objectId;
    // A pidless spawn failure owns no child, and the wire admits nothing but Close and
    // CleanupComplete for such an object. The stdio the caller still gets handed has to
    // stay inert: one credit request or one write from it is rejected as an invalid spawn
    // outcome, which fails the whole broker instead of just that stream.
    this.spawnFailed = pid === undefined;
    this.events = new EventEmitter();
    this.stdout = kind === 'playwright' ? new BrokerReadableV1(this, 1) : null;
    this.stderr = new BrokerReadableV1(this, 2);
    this.extra3 = kind === 'playwright' ? new BrokerDuplexV1(this, 3) : null;
    this.extra4 = kind === 'playwright' ? new BrokerDuplexV1(this, 4) : null;
    const stdio = kind === 'playwright'
      ? [null, this.stdout, this.stderr, this.extra3, this.extra4]
      : [null, null, this.stderr];
    this.public = Object.freeze({
      pid,
      killed: false,
      stdin: null,
      stdout: this.stdout,
      stderr: this.stderr,
      stdio: Object.freeze(stdio),
      on: this.events.on.bind(this.events),
      once: this.events.once.bind(this.events),
    });
  }
}

class ExactBrowserBrokerV1 {
  constructor(transport) {
    this.transport = transport;
    this.generation = transport.generation;
    this.nextRequestId = 1n;
    this.outgoingSequences = new Map();
    this.incomingSequences = new Map();
    this.objects = new Map();
    this.proxyObjects = new WeakMap();
    this.closeHooks = new Map();
    this.muxBuffer = Buffer.alloc(0);
    this.closed = false;
  }
  readSyncExact(length) {
    return Buffer.from(this.transport.readSyncExact(length));
  }
  requestSync(kind, payload) {
    if (this.closed) throw browserBrokerFailureV1('Browser broker is closed.');
    const requestId = this.nextRequestId++;
    const json = Buffer.from(exactBrowserRuntimeJsonCodecV1.encode(payload));
    const body = Buffer.allocUnsafe(20 + json.length);
    body.writeBigUInt64BE(this.generation, 0);
    body.writeBigUInt64BE(requestId, 8);
    body.writeUInt16BE(kind, 16);
    body.writeUInt16BE(0, 18);
    json.copy(body, 20);
    if (body.length > KLAUS_BROWSER_BROKER_SYNC_MAX_V1) {
      throw browserBrokerFailureV1('Browser broker sync frame is too large.');
    }
    const frame = Buffer.allocUnsafe(4 + body.length);
    frame.writeUInt32BE(body.length, 0);
    body.copy(frame, 4);
    this.transport.writeSync(frame);
    const prefix = this.readSyncExact(4);
    const remaining = prefix.readUInt32BE(0);
    if (remaining < 20 || remaining > KLAUS_BROWSER_BROKER_SYNC_MAX_V1) {
      throw browserBrokerFailureV1('Invalid browser broker response.');
    }
    const response = this.readSyncExact(remaining);
    if (
      response.readBigUInt64BE(0) !== this.generation ||
      response.readBigUInt64BE(8) !== requestId ||
      response.readUInt16BE(16) !== kind || response.readUInt16BE(18) !== 0
    ) throw browserBrokerFailureV1('Invalid browser broker response.');
    return exactBrowserRuntimeJsonCodecV1.decode(
      exactBrowserRuntimeBytesV1(response.subarray(20)),
    );
  }
  processResponse(response, kind) {
    if (kind === 'proxy') {
      if (!exactOwnKeysV1(response, ['ok', 'object_id', 'pid']) &&
          !exactOwnKeysV1(response, ['ok', 'object_id', 'error'])) {
        throw browserBrokerFailureV1('Invalid browser broker response.');
      }
      if (!/^[0-9a-f]{16}$/.test(response.object_id)) {
        throw browserBrokerFailureV1('Invalid browser broker response.');
      }
      return response;
    }
    if (!exactOwnKeysV1(response, ['ok', 'value', 'error'])) {
      throw browserBrokerFailureV1('Invalid browser broker response.');
    }
    if (!response.ok) throw brokerErrorDescriptorV1(response.error);
    return response.value;
  }
  createProxy(response, kind) {
    const descriptor = this.processResponse(response, 'proxy');
    const objectId = BigInt('0x' + descriptor.object_id);
    const proxy = new BrowserBrokerProxyV1(this, objectId, descriptor.ok ? descriptor.pid : undefined, kind);
    this.objects.set(objectId, proxy);
    this.proxyObjects.set(proxy.public, objectId);
    if (!descriptor.ok) {
      const error = brokerErrorDescriptorV1(descriptor.error);
      setImmediate(() => {
        proxy.events.emit('error', error);
        proxy.events.emit('close', null, null);
      });
    }
    return proxy.public;
  }
  operationSet() {
    const operations = Object.create(null);
    operations.PlaywrightLaunch = (command, args, options) =>
      this.createProxy(this.requestSync(BrowserSyncKindV1.PlaywrightLaunch, launchPayloadV1(command, args, options)), 'playwright');
    for (const name of [
      'PlaywrightTreeKill', 'PlaywrightMacOsSwVers', 'PlaywrightLinuxLdd',
      'PlaywrightLinuxLdconfig', 'PlaywrightWindowsWinldd',
    ]) {
      operations[name] = (...args) => this.processResponse(
        this.requestSync(BrowserSyncKindV1[name], { args }), 'value');
    }
    operations.BrowserFfmpegEncode = ({ listPath, filePath, fps }) => {
      const payload = { listPath, filePath, fps };
      return this.createProxy(this.requestSync(BrowserSyncKindV1.BrowserFfmpegEncode, payload), 'ffmpeg');
    };
    return Object.freeze(operations);
  }
  nextSequence(objectId, streamId, sequenceClass) {
    const key = [objectId, streamId, sequenceClass].join(':');
    const next = (this.outgoingSequences.get(key) ?? 0n) + 1n;
    this.outgoingSequences.set(key, next);
    return next;
  }
  sendMux(objectId, streamId, kind, { sequenceClass, correlation, credit, payload }) {
    const sequence = this.nextSequence(objectId, streamId, sequenceClass);
    const body = Buffer.allocUnsafe(40 + payload.length);
    body.writeBigUInt64BE(this.generation, 0);
    body.writeBigUInt64BE(objectId, 8);
    body.writeUInt16BE(streamId, 16);
    body.writeUInt16BE(kind, 18);
    body.writeBigUInt64BE(sequence, 20);
    body.writeBigUInt64BE(correlation, 28);
    body.writeUInt32BE(credit, 36);
    payload.copy(body, 40);
    if (body.length > KLAUS_BROWSER_BROKER_MUX_MAX_V1) {
      throw browserBrokerFailureV1('Browser broker mux frame is too large.');
    }
    const frame = Buffer.allocUnsafe(4 + body.length);
    frame.writeUInt32BE(body.length, 0);
    body.copy(frame, 4);
    this.transport.writeMux(frame, (error) => {
      if (error) this.fail(error);
    });
    return sequence;
  }
  acceptMux(bytes) {
    this.muxBuffer = Buffer.concat([this.muxBuffer, Buffer.from(bytes)]);
    for (;;) {
      if (this.muxBuffer.length < 4) return;
      const remaining = this.muxBuffer.readUInt32BE(0);
      if (remaining < 40 || remaining > KLAUS_BROWSER_BROKER_MUX_MAX_V1) {
        throw browserBrokerFailureV1('Invalid browser broker mux frame.');
      }
      if (this.muxBuffer.length < 4 + remaining) return;
      const body = this.muxBuffer.subarray(4, 4 + remaining);
      this.muxBuffer = this.muxBuffer.subarray(4 + remaining);
      this.dispatchMux(body);
    }
  }
  dispatchMux(body) {
    if (body.readBigUInt64BE(0) !== this.generation) {
      throw browserBrokerFailureV1('Invalid browser broker mux generation.');
    }
    const objectId = body.readBigUInt64BE(8);
    const streamId = body.readUInt16BE(16);
    const kind = body.readUInt16BE(18);
    const sequence = body.readBigUInt64BE(20);
    const correlation = body.readBigUInt64BE(28);
    const credit = body.readUInt32BE(36);
    const payload = body.subarray(40);
    if (credit !== 0 || objectId === 0n) throw browserBrokerFailureV1('Invalid browser broker mux frame.');
    const proxy = this.objects.get(objectId);
    if (!proxy) throw browserBrokerFailureV1('Invalid browser broker object.');
    if (kind === BrowserMuxKindV1.StdoutData && streamId === 1 && proxy.stdout) proxy.stdout.accept(sequence, payload);
    else if (kind === BrowserMuxKindV1.StderrData && streamId === 2) proxy.stderr.accept(sequence, payload);
    else if (kind === BrowserMuxKindV1.ExtraData && (streamId === 3 || streamId === 4)) proxy['extra' + streamId].acceptData(sequence, payload);
    else if (kind === BrowserMuxKindV1.WriteAck && (streamId === 3 || streamId === 4) && payload.length === 0) proxy['extra' + streamId].acceptWriteAck(correlation);
    else if (kind === BrowserMuxKindV1.StreamClose && streamId >= 1 && streamId <= 4 && payload.length === 0) {
      const stream = streamId === 1 ? proxy.stdout : streamId === 2 ? proxy.stderr : proxy['extra' + streamId];
      stream?.acceptClose(correlation);
    } else if (kind === BrowserMuxKindV1.Exit && streamId === 0) {
      const value = exactBrowserRuntimeJsonCodecV1.decode(exactBrowserRuntimeBytesV1(payload));
      if (!exactOwnKeysV1(value, ['code', 'signal'])) throw browserBrokerFailureV1('Invalid browser exit frame.');
      proxy.exitSequence = sequence;
      proxy.events.emit('exit', value.code, value.signal);
    } else if (kind === BrowserMuxKindV1.Close && streamId === 0 && payload.length === 0) {
      proxy.events.emit('close', null, null);
    } else if (kind === BrowserMuxKindV1.CleanupComplete && streamId === 0) {
      const value = exactBrowserRuntimeJsonCodecV1.decode(exactBrowserRuntimeBytesV1(payload));
      if (!exactOwnKeysV1(value, ['ok', 'error'])) throw browserBrokerFailureV1('Invalid browser cleanup frame.');
      this.closeHooks.delete(objectId);
      this.objects.delete(objectId);
    } else if (kind === BrowserMuxKindV1.BeginGracefulClose && streamId === 0 && payload.length === 0) {
      this.runCloseHook(proxy, 'graceful', BrowserMuxKindV1.GracefulCloseResult, sequence);
    } else if (kind === BrowserMuxKindV1.BeginForceClose && streamId === 0 && payload.length === 0) {
      this.runCloseHook(proxy, 'force', BrowserMuxKindV1.ForceCloseResult, sequence);
    } else throw browserBrokerFailureV1('Invalid browser broker mux frame.');
  }
  async runCloseHook(proxy, name, resultKind, correlation) {
    const hooks = this.closeHooks.get(proxy.objectId);
    let ok = true;
    let error = null;
    try {
      if (!hooks) throw new Error('Browser close hooks are unavailable.');
      await hooks[name]();
    } catch (cause) {
      ok = false;
      error = cause?.message || String(cause);
    }
    this.sendMux(proxy.objectId, 0, resultKind, {
      sequenceClass: 'control', correlation, credit: 0,
      payload: Buffer.from(exactBrowserRuntimeJsonCodecV1.encode({ ok, error })),
    });
  }
  registerCloseHooks(proxy, graceful, force) {
    const objectId = this.proxyObjects.get(proxy);
    if (objectId === undefined || typeof graceful !== 'function' || typeof force !== 'function' || this.closeHooks.has(objectId)) {
      throw browserBrokerFailureV1('Pinned Playwright close hook registration failed.');
    }
    this.closeHooks.set(objectId, Object.freeze({ graceful, force }));
    if (this.objects.get(objectId)?.spawnFailed) return;
    this.sendMux(objectId, 0, BrowserMuxKindV1.CloseHooksReady, {
      sequenceClass: 'control', correlation: 0n, credit: 0, payload: Buffer.alloc(0),
    });
  }
  fail(error) {
    if (this.closed) return;
    this.closed = true;
    for (const proxy of this.objects.values()) proxy.events.emit('error', error);
    this.transport.close();
  }
}

function createBrowserBrokerTransportV1() {
  if (process.platform === 'win32') {
    const pipes = parseWindowsBrokerV1();
    return Object.freeze({
      generation: pipes.generation,
      writeSync: (bytes) => pipes.writeSync(bytes),
      readSyncExact: (length) => pipes.readSyncExact(length),
      startMux: (handlers) => pipes.startMux(handlers),
      writeMux: (bytes, done) => pipes.writeMux(bytes, done),
      markPackageReady: () => new Promise((resolve, reject) => pipes.markPackageReady((error) => error ? reject(error) : resolve())),
      close: () => pipes.close(),
    });
  }
  if (process.argv.slice(2).some((argument) => argument.startsWith(KLAUS_BROWSER_BROKER_ENV_ARG_V1))) {
    throw browserBrokerFailureV1(KLAUS_BROWSER_READY_ERROR_V1);
  }
  const permit = parseUnixPackagePermitV1(readExactFdV1(6, 48));
  let muxClosed = false;
  return Object.freeze({
    generation: permit.generation,
    writeSync: (bytes) => writeAllFdV1(3, bytes),
    readSyncExact: (length) => readExactFdV1(4, length),
    startMux: (handlers) => {
      const readNext = () => {
        if (muxClosed) return;
        const buffer = Buffer.allocUnsafe(64 * 1024);
        fs.read(6, buffer, 0, buffer.length, null, (error, count) => {
          if (muxClosed) return;
          if (error) {
            if (error.code === 'EINTR') return readNext();
            handlers.close(error);
            return;
          }
          if (count === 0) {
            handlers.close();
            return;
          }
          handlers.bytes(buffer.subarray(0, count));
          readNext();
        });
      };
      readNext();
    },
    writeMux: (bytes, done) => {
      let offset = 0;
      const writeNext = () => {
        fs.write(5, bytes, offset, bytes.length - offset, (error, count) => {
          if (error) {
            if (error.code === 'EINTR') return writeNext();
            done(error);
            return;
          }
          if (count === 0) {
            done(browserBrokerFailureV1('Browser broker mux write failed.'));
            return;
          }
          offset += count;
          if (offset === bytes.length) done();
          else writeNext();
        });
      };
      writeNext();
    },
    markPackageReady: () => {
      writeAllFdV1(5, unixPackageReadyV1(permit));
      return Promise.resolve();
    },
    close: () => { muxClosed = true; },
  });
}

async function bootstrapPinnedBrowserBrokerV1() {
  let transport;
  try {
    transport = createBrowserBrokerTransportV1();
  } catch (_error) {
    const message = process.platform === 'win32' ? KLAUS_BROWSER_PIPE_ERROR_V1 : KLAUS_BROWSER_READY_ERROR_V1;
    process.stderr.write(message + '\n');
    process.exit(70);
  }
  const broker = new ExactBrowserBrokerV1(transport);
  const handlers = Object.freeze({
    bytes: (bytes) => { try { broker.acceptMux(bytes); } catch (error) { broker.fail(error); } },
    close: (error) => broker.fail(error || browserBrokerFailureV1('Browser broker mux closed.')),
  });
  try {
    const prelude = await import(KLAUS_BROWSER_PRELUDE_V1.href);
    const operations = broker.operationSet();
    Object.defineProperty(broker, 'operations', {
      value: operations, enumerable: false, configurable: false, writable: false,
    });
    prelude.installPinnedPlaywrightProcessPatchV1(
      operations,
      (proxy, graceful, force) => broker.registerCloseHooks(proxy, graceful, force),
    );
    installVerifiedPlaywrightLoaderV1(() => import(KLAUS_BROWSER_PLAYWRIGHT_V1.href));
    transport.startMux(handlers);
    await transport.markPackageReady();
    return broker;
  } catch (_error) {
    transport.close();
    process.stderr.write(KLAUS_BROWSER_READY_ERROR_V1 + '\n');
    process.exit(70);
  }
}


const state = {
  playwright: null,
  browser: null,
  context: null,
  pages: new Map(),
  selectedTabId: null,
  nextTabSeq: 1,
  sessionId: 'session-1',
  sessionName: null,
  console: [],
  network: [],
  networkSeq: 0,
  refs: new Map(),
  requestIds: new WeakMap(),
  pageInstrumented: new WeakSet(),
  extensionBridge: null,
  history: [],
  clipboard: { text: '', items: [] },
  dialogs: new Map(),
  tabMarks: new Map(),
  fileChoosers: new Map(),
  nextFileChooserSeq: 1,
};

const artifactDir =
  process.env.KLAUS_BROWSER_ARTIFACT_DIR ||
  path.join(process.cwd(), '.klaus', 'browser-artifacts');
const headless = process.env.KLAUS_BROWSER_HEADLESS !== '0';
const backend = process.env.KLAUS_BROWSER_BACKEND || 'launch';
const bundledExtensionOrigin = 'chrome-extension://gjamccelebgpogpbiceajehcajghphnb';

let queue = Promise.resolve();

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

function isConcurrentRequestLine(line) {
  try {
    const message = decodeBrowserSidecarLineV1(line);
    return message?.request?.operation?.type === 'playwright_wait_for_event';
  } catch (_error) {
    return false;
  }
}

async function handleLine(line) {
  let message;
  try {
    message = decodeBrowserSidecarLineV1(line);
  } catch (error) {
    write({
      id: 'unknown',
      ok: false,
      error: normalizeError(error, 'invalid_request', 'Invalid browser sidecar JSON request.'),
    });
    return;
  }

  const { id, request } = message;
  try {
    const response = await executeRequest(id, request);
    write({ id, ok: true, response });
  } catch (error) {
    write({ id, ok: false, error: normalizeError(error, codeForOperation(request?.operation)) });
  }
}

async function executeRequest(id, request) {
  if (!request || !request.operation) {
    throw new BrowserSidecarError('invalid_request', 'Browser request missing operation.');
  }
  if (backend === 'extension') {
    return executeExtensionRequest(id, request);
  }
  const op = request.operation;
  switch (op.type) {
    case 'navigate':
      return navigate(id, request, op);
    case 'snapshot':
      return snapshot(id, request, op);
    case 'screenshot':
      return screenshot(id, request, op);
    case 'click':
      return click(id, request, op);
    case 'type':
      return typeText(id, request, op);
    case 'eval':
      return evaluate(id, request, op);
    case 'console':
      return consoleEntries(id, request, op);
    case 'network':
      return networkEntries(id, request, op);
    case 'gif_creator':
      return gifCreator(id, request, op);
    case 'upload_image':
      return uploadImage(id, request, op);
    case 'file_upload':
      return fileUpload(id, request, op);
    case 'shortcuts_list':
      return shortcutsList(id, request, op);
    case 'shortcuts_execute':
      return shortcutsExecute(id, request, op);
    case 'switch_browser':
      return switchBrowser(id, request, op);
    case 'list_connected_browsers':
      return listConnectedBrowsers(id, request, op);
    case 'select_browser':
      return selectBrowser(id, request, op);
    case 'history':
      return historyEntries(id, request, op);
    case 'tabs_content':
      return tabsContent(id, request, op);
    case 'content_export':
      return contentExport(id, request, op);
    case 'download':
      return downloadMedia(id, request, op);
    case 'clipboard':
      return clipboardOperation(id, request, op);
    case 'dialog':
      return dialogOperation(id, request, op);
    case 'mark_tab':
      return markTab(id, request, op);
    case 'playwright_wait_for_event':
      return playwrightWaitForEvent(id, request, op);
    case 'playwright_file_chooser_set_files':
      return playwrightFileChooserSetFiles(id, request, op);
    case 'playwright_wait_for_load_state':
      return playwrightWaitForLoadState(id, request, op);
    case 'playwright_wait_for_url':
      return playwrightWaitForUrl(id, request, op);
    case 'playwright_dom_snapshot':
      return playwrightDomSnapshot(id, request, op);
    case 'playwright_element_info':
      return playwrightElementInfo(id, request, op);
    case 'playwright_element_screenshot':
      return playwrightElementScreenshot(id, request, op);
    case 'read_page':
      return readPage(id, request, op);
    case 'find':
      return findElements(id, request, op);
    case 'form_input':
      return formInput(id, request, op);
    case 'computer':
      return computer(id, request, op);
    case 'tabs':
      return tabs(id, request, op);
    case 'locator':
      return locatorAction(id, request, op);
    case 'page_text':
      return pageTextOperation(id, request, op);
    case 'batch':
      return batch(id, request, op);
    case 'resize':
      return resize(id, request, op);
    case 'close':
      return closeTarget(id, request, op);
    default:
      throw new BrowserSidecarError('invalid_request', `Unsupported browser operation: ${op.type}`);
  }
}

async function executeExtensionRequest(id, request) {
  const bridge = await ensureExtensionBridge();
  const op = request?.operation || {};
  sendEvent(id, {
    type: 'status',
    message: `Chrome extension bridge handling ${op.type || 'unknown'} operation.`,
  });
  if (op.type === 'list_connected_browsers') {
    return bridge.listConnectedBrowsers(request, op);
  }
  if (op.type === 'select_browser') {
    return bridge.selectBrowser(request, op);
  }
  if (op.type === 'switch_browser') {
    return bridge.switchBrowser(request, op);
  }
  const client =
    bridge.clientForRequest(request) ||
    (await bridge.waitForClient(Math.min(timeoutMs(request), extensionConnectTimeoutMs())));
  if (!client) {
    throw new BrowserSidecarError(
      'unavailable',
      `No Klaus Chrome extension is connected to ${bridge.nativeSocketPath} or ws://127.0.0.1:${bridge.port}. Install the native host manifest or load the bundled extension, then open Chrome with the target profile.`,
      { port: bridge.port, nativeSocketPath: bridge.nativeSocketPath, extensionPath: bundledExtensionPath() },
    );
  }
  const response = await client.call(request, timeoutMs(request));
  sendEvent(id, {
    type: 'status',
    message: `Chrome extension ${client.deviceId || 'selected'} completed ${op.type || 'operation'}.`,
  });
  return response;
}

async function ensureExtensionBridge() {
  if (state.extensionBridge) {
    return state.extensionBridge;
  }
  const port = Number.parseInt(process.env.KLAUS_BROWSER_EXTENSION_BRIDGE_PORT || '18765', 10);
  const bridge = new ExtensionBridge(Number.isFinite(port) ? port : 18765);
  await bridge.start();
  state.extensionBridge = bridge;
  return bridge;
}

function bundledExtensionPath() {
  if (process.env.KLAUS_BROWSER_EXTENSION_PATH) {
    return process.env.KLAUS_BROWSER_EXTENSION_PATH;
  }
  const scriptDir = path.dirname(fileURLToPath(import.meta.url));
  const sibling = path.join(scriptDir, 'extension');
  if (fs.existsSync(sibling)) {
    return sibling;
  }
  return path.resolve(scriptDir, '..', 'extension');
}

function extensionConnectTimeoutMs() {
  const parsed = Number.parseInt(process.env.KLAUS_BROWSER_EXTENSION_CONNECT_TIMEOUT_MS || '5000', 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : 5000;
}

function tokensEqual(provided, expected) {
  const providedHash = crypto.createHash('sha256').update(String(provided)).digest();
  const expectedHash = crypto.createHash('sha256').update(String(expected)).digest();
  return crypto.timingSafeEqual(providedHash, expectedHash);
}

function rejectWebSocketUpgrade(socket, status, reason) {
  socket.end(
    [
      `HTTP/1.1 ${status} ${reason}`,
      'Connection: close',
      'Content-Length: 0',
      '',
      '',
    ].join('\r\n'),
  );
}

class ExtensionBridge {
  constructor(port) {
    this.port = port;
    this.nativeSocketPath = process.env.KLAUS_BROWSER_NATIVE_BRIDGE_SOCKET || defaultNativeBridgeSocketPath();
    this.websocketToken = String(process.env.KLAUS_BROWSER_EXTENSION_BRIDGE_TOKEN || '').trim();
    this.allowedOrigins = new Set(
      String(process.env.KLAUS_BROWSER_EXTENSION_BRIDGE_ORIGINS || bundledExtensionOrigin)
        .split(',')
        .map((origin) => origin.trim())
        .filter(Boolean),
    );
    this.server = null;
    this.nativeServer = null;
    this.clients = new Map();
    this.selectedDeviceId = null;
    this.waiters = [];
    this.heartbeatTimer = null;
  }

  async start() {
    await this.startNativeServer();
    if (!this.websocketToken) {
      this.startHeartbeat();
      return;
    }
    this.server = http.createServer((_req, res) => {
      res.writeHead(404);
      res.end('Klaus browser extension bridge\n');
    });
    this.server.on('upgrade', (req, socket) => {
      const requestUrl = new URL(req.url || '/', `http://${req.headers.host || '127.0.0.1'}`);
      if (requestUrl.pathname !== '/klaus-browser') {
        rejectWebSocketUpgrade(socket, 404, 'Not Found');
        return;
      }
      if (!this.allowedOrigins.has(String(req.headers.origin || ''))) {
        rejectWebSocketUpgrade(socket, 403, 'Forbidden');
        return;
      }
      if (!tokensEqual(requestUrl.searchParams.get('token') || '', this.websocketToken)) {
        rejectWebSocketUpgrade(socket, 401, 'Unauthorized');
        return;
      }
      const key = req.headers['sec-websocket-key'];
      if (!key) {
        rejectWebSocketUpgrade(socket, 400, 'Bad Request');
        return;
      }
      const accept = crypto
        .createHash('sha1')
        .update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`)
        .digest('base64');
      socket.write(
        [
          'HTTP/1.1 101 Switching Protocols',
          'Upgrade: websocket',
          'Connection: Upgrade',
          `Sec-WebSocket-Accept: ${accept}`,
          '',
          '',
        ].join('\r\n'),
      );
      const client = new ExtensionBridgeClient(socket, this);
      client.start();
    });
    await new Promise((resolve, reject) => {
      this.server.once('error', reject);
      this.server.listen(this.port, '127.0.0.1', () => {
        this.server.off('error', reject);
        resolve();
      });
    });
    this.startHeartbeat();
  }

  async startNativeServer() {
    if (process.env.KLAUS_BROWSER_DISABLE_NATIVE_BRIDGE === '1') return;
    if (process.platform !== 'win32') {
      await fsp.mkdir(path.dirname(this.nativeSocketPath), { recursive: true, mode: 0o700 });
      await fsp.chmod(path.dirname(this.nativeSocketPath), 0o700).catch(() => {});
      if (fs.existsSync(this.nativeSocketPath)) {
        if (await nativeSocketAcceptsConnections(this.nativeSocketPath)) {
          throw new Error(`Klaus browser native bridge socket is already in use: ${this.nativeSocketPath}`);
        }
        await fsp.unlink(this.nativeSocketPath).catch((error) => {
          if (error?.code !== 'ENOENT') throw error;
        });
      }
    }
    this.nativeServer = net.createServer((socket) => {
      const client = new ExtensionBridgeNativeClient(socket, this);
      client.start();
    });
    await new Promise((resolve, reject) => {
      this.nativeServer.once('error', reject);
      this.nativeServer.listen(this.nativeSocketPath, () => {
        this.nativeServer.off('error', reject);
        resolve();
      });
    });
    if (process.platform !== 'win32') {
      await fsp.chmod(this.nativeSocketPath, 0o600).catch(() => {});
    }
  }

  register(client, hello = {}) {
    const deviceId = String(hello.deviceId || `extension-${this.clients.size + 1}`);
    client.deviceId = deviceId;
    client.name = hello.name || 'Chrome';
    client.platform = hello.platform || null;
    client.version = hello.version || null;
    client.capabilities = hello.capabilities || [];
    client.connectedAt = client.connectedAt || Date.now();
    client.lastSeenAt = Date.now();
    this.clients.set(deviceId, client);
    if (!this.selectedDeviceId || !this.clients.has(this.selectedDeviceId)) {
      this.selectedDeviceId = deviceId;
    }
    this.resolveWaiters(this.selectedClient());
  }

  unregister(client) {
    if (client.deviceId) {
      this.clients.delete(client.deviceId);
      if (this.selectedDeviceId === client.deviceId) {
        this.selectedDeviceId = this.clients.keys().next().value || null;
      }
    }
  }

  selectedClient() {
    if (this.selectedDeviceId && this.clients.has(this.selectedDeviceId)) {
      return this.clients.get(this.selectedDeviceId);
    }
    const first = this.clients.values().next().value || null;
    this.selectedDeviceId = first?.deviceId || null;
    return first;
  }

  clientForRequest(request) {
    const requested =
      request?.target?.sessionId ||
      request?.metadata?.browserId ||
      request?.metadata?.deviceId ||
      null;
    if (requested && this.clients.has(requested)) {
      this.selectedDeviceId = requested;
      return this.clients.get(requested);
    }
    return this.selectedClient();
  }

  waitForClient(timeout) {
    const selected = this.selectedClient();
    if (selected || timeout <= 0) {
      return Promise.resolve(selected);
    }
    return new Promise((resolve) => {
      const waiter = { resolve, timer: null };
      waiter.timer = setTimeout(() => {
        this.waiters = this.waiters.filter((item) => item !== waiter);
        resolve(null);
      }, timeout);
      this.waiters.push(waiter);
    });
  }

  resolveWaiters(client) {
    const waiters = this.waiters.splice(0);
    for (const waiter of waiters) {
      clearTimeout(waiter.timer);
      waiter.resolve(client);
    }
  }

  browserDescriptors() {
    return [...this.clients.values()].map((client, index) => ({
      deviceId: client.deviceId,
      name: client.name || `Chrome ${index + 1}`,
      platform: client.platform || null,
      version: client.version || null,
      capabilities: client.capabilities || [],
      isLocal: true,
      selected: client.deviceId === this.selectedDeviceId,
      connected: true,
      transport: client.transport || 'websocket',
      connectedAt: new Date(client.connectedAt || Date.now()).toISOString(),
      lastSeenAt: new Date(client.lastSeenAt || Date.now()).toISOString(),
    }));
  }

  listConnectedBrowsers(request, _op) {
    const browsers = this.browserDescriptors();
    return {
      sessionId: this.selectedDeviceId || null,
      tabId: request?.target?.tabId || null,
      value: browsers,
      text: JSON.stringify(browsers, null, 2),
    };
  }

  selectBrowser(request, op) {
    const deviceId = String(op.deviceId || '').trim();
    if (!deviceId) {
      throw new BrowserSidecarError('invalid_request', 'select_browser requires deviceId.');
    }
    if (!this.clients.has(deviceId)) {
      throw new BrowserSidecarError(
        'not_found',
        `No connected browser has deviceId "${deviceId}". Call list_connected_browsers to see currently connected browsers.`,
      );
    }
    this.selectedDeviceId = deviceId;
    const selected = this.clients.get(deviceId);
    return {
      sessionId: deviceId,
      tabId: request?.target?.tabId || null,
      message: `Connected to browser "${selected.name || deviceId}".`,
      value: this.browserDescriptors().find((browser) => browser.deviceId === deviceId) || null,
    };
  }

  async switchBrowser(request, op) {
    const waitMs = Number.isFinite(op.waitMs) ? Math.max(0, Math.min(op.waitMs, 120_000)) : 120_000;
    if (this.clients.size === 0 && waitMs > 0) {
      await this.waitForClient(waitMs);
    }
    const candidates = [...this.clients.values()];
    if (candidates.length === 0) {
      throw new BrowserSidecarError(
        'unavailable',
        'No browser responded within the timeout. Make sure Chrome is open with the Klaus extension installed, then try again.',
      );
    }
    const next =
      candidates.find((client) => client.deviceId !== this.selectedDeviceId) ||
      this.selectedClient() ||
      candidates[0];
    this.selectedDeviceId = next.deviceId;
    return {
      sessionId: next.deviceId,
      tabId: request?.target?.tabId || null,
      message: `Connected to browser "${next.name || next.deviceId}".`,
      value: this.browserDescriptors().find((browser) => browser.deviceId === next.deviceId) || null,
    };
  }

  async close() {
    this.resolveWaiters(null);
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    for (const client of this.clients.values()) {
      client.close();
    }
    this.clients.clear();
    if (this.server) {
      await new Promise((resolve) => this.server.close(resolve));
      this.server = null;
    }
    if (this.nativeServer) {
      await new Promise((resolve) => this.nativeServer.close(resolve));
      this.nativeServer = null;
    }
    if (process.platform !== 'win32') {
      await fsp.unlink(this.nativeSocketPath).catch(() => {});
    }
  }

  startHeartbeat() {
    if (this.heartbeatTimer) return;
    const intervalMs = Number.parseInt(process.env.KLAUS_BROWSER_EXTENSION_HEARTBEAT_MS || '15000', 10);
    const staleMs = Number.parseInt(process.env.KLAUS_BROWSER_EXTENSION_STALE_MS || '45000', 10);
    this.heartbeatTimer = setInterval(() => {
      const now = Date.now();
      for (const client of [...this.clients.values()]) {
        if (Number.isFinite(staleMs) && staleMs > 0 && now - (client.lastSeenAt || 0) > staleMs) {
          client.close();
          continue;
        }
        client.ping();
      }
    }, Number.isFinite(intervalMs) && intervalMs > 0 ? intervalMs : 15000);
    this.heartbeatTimer.unref?.();
  }
}

class ExtensionBridgeClient {
  constructor(socket, bridge) {
    this.socket = socket;
    this.bridge = bridge;
    this.buffer = Buffer.alloc(0);
    this.pending = new Map();
    this.seq = 0;
    this.deviceId = null;
    this.name = null;
    this.platform = null;
    this.transport = 'websocket';
    this.connectedAt = Date.now();
    this.lastSeenAt = Date.now();
  }

  start() {
    this.socket.on('data', (chunk) => this.onData(chunk));
    this.socket.on('close', () => this.close());
    this.socket.on('error', () => this.close());
  }

  onData(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    while (true) {
      const parsed = readWebSocketFrame(this.buffer);
      if (!parsed) return;
      this.buffer = this.buffer.subarray(parsed.consumed);
      if (parsed.opcode === 0x8) {
        this.close();
        return;
      }
      if (parsed.opcode !== 0x1) continue;
      let message;
      try {
        message = JSON.parse(parsed.payload.toString('utf8'));
      } catch (_error) {
        continue;
      }
      this.lastSeenAt = Date.now();
      this.handleMessage(message);
    }
  }

  handleMessage(message) {
    this.lastSeenAt = Date.now();
    if (message.type === 'hello') {
      this.bridge.register(this, message);
      return;
    }
    if (message.type === 'pong') {
      return;
    }
    if (message.type === 'response') {
      const pending = this.pending.get(message.id);
      if (!pending) return;
      clearTimeout(pending.timer);
      this.pending.delete(message.id);
      if (message.ok) {
        pending.resolve(message.response || {});
      } else {
        const error = message.error || {};
        pending.reject(new BrowserSidecarError(error.code || 'host_error', error.message || 'Chrome extension browser call failed.', error.details || null));
      }
    }
  }

  call(request, timeout) {
    const id = `extension-${++this.seq}`;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new BrowserSidecarError('timeout', `Chrome extension request timed out after ${timeout}ms.`));
      }, timeout);
      this.pending.set(id, { resolve, reject, timer });
      this.send({ type: 'call', id, request });
    });
  }

  ping() {
    this.send({ type: 'ping', ts: Date.now() });
  }

  send(message) {
    if (this.socket.destroyed || !this.socket.writable) return;
    try {
      this.socket.write(writeWebSocketFrame(Buffer.from(JSON.stringify(message), 'utf8')));
    } catch (_error) {
      this.close();
    }
  }

  close() {
    this.bridge.unregister(this);
    for (const [id, pending] of this.pending) {
      clearTimeout(pending.timer);
      pending.reject(new BrowserSidecarError('unavailable', 'Chrome extension disconnected.'));
      this.pending.delete(id);
    }
    this.socket.destroy();
  }
}

class ExtensionBridgeNativeClient extends ExtensionBridgeClient {
  constructor(socket, bridge) {
    super(socket, bridge);
    this.lineBuffer = '';
    this.transport = 'native-socket';
  }

  onData(chunk) {
    this.lineBuffer += chunk.toString('utf8');
    while (this.lineBuffer.includes('\n')) {
      const index = this.lineBuffer.indexOf('\n');
      const raw = this.lineBuffer.slice(0, index);
      this.lineBuffer = this.lineBuffer.slice(index + 1);
      if (!raw.trim()) continue;
      let message;
      try {
        message = JSON.parse(raw);
      } catch (_error) {
        continue;
      }
      this.lastSeenAt = Date.now();
      this.handleMessage(message);
    }
  }

  send(message) {
    if (this.socket.destroyed || !this.socket.writable) return;
    try {
      this.socket.write(`${JSON.stringify(message)}\n`);
    } catch (_error) {
      this.close();
    }
  }
}

function defaultNativeBridgeSocketPath() {
  if (process.platform === 'win32') {
    return '\\\\.\\pipe\\klaus-browser-native-host';
  }
  return path.join(os.homedir(), '.klaus', 'browser', 'native-host.sock');
}

function nativeSocketAcceptsConnections(socketPath) {
  return new Promise((resolve) => {
    const socket = net.connect({ path: socketPath });
    const timer = setTimeout(() => {
      socket.destroy();
      resolve(false);
    }, 200);
    socket.once('connect', () => {
      clearTimeout(timer);
      socket.destroy();
      resolve(true);
    });
    socket.once('error', () => {
      clearTimeout(timer);
      resolve(false);
    });
  });
}

function readWebSocketFrame(buffer) {
  if (buffer.length < 2) return null;
  const first = buffer[0];
  const second = buffer[1];
  const opcode = first & 0x0f;
  const masked = (second & 0x80) !== 0;
  let length = second & 0x7f;
  let offset = 2;
  if (length === 126) {
    if (buffer.length < offset + 2) return null;
    length = buffer.readUInt16BE(offset);
    offset += 2;
  } else if (length === 127) {
    if (buffer.length < offset + 8) return null;
    const high = buffer.readUInt32BE(offset);
    const low = buffer.readUInt32BE(offset + 4);
    length = high * 2 ** 32 + low;
    offset += 8;
  }
  let mask = null;
  if (masked) {
    if (buffer.length < offset + 4) return null;
    mask = buffer.subarray(offset, offset + 4);
    offset += 4;
  }
  if (buffer.length < offset + length) return null;
  const payload = Buffer.from(buffer.subarray(offset, offset + length));
  if (mask) {
    for (let i = 0; i < payload.length; i += 1) {
      payload[i] ^= mask[i % 4];
    }
  }
  return { opcode, payload, consumed: offset + length };
}

function writeWebSocketFrame(payload) {
  const length = payload.length;
  if (length < 126) {
    return Buffer.concat([Buffer.from([0x81, length]), payload]);
  }
  if (length < 65536) {
    const header = Buffer.alloc(4);
    header[0] = 0x81;
    header[1] = 126;
    header.writeUInt16BE(length, 2);
    return Buffer.concat([header, payload]);
  }
  const header = Buffer.alloc(10);
  header[0] = 0x81;
  header[1] = 127;
  header.writeUInt32BE(Math.floor(length / 2 ** 32), 2);
  header.writeUInt32BE(length >>> 0, 6);
  return Buffer.concat([header, payload]);
}

async function navigate(id, request, op) {
  const { page, tabId } = await pageFor(request, op.viewport);
  sendEvent(id, {
    type: 'navigation_started',
    url: op.url,
    sessionId: state.sessionId,
    tabId,
  });
  const response = await page.goto(op.url, {
    waitUntil: mapWaitUntil(op.waitUntil),
    timeout: timeoutMs(request),
  });
  const title = await page.title().catch(() => null);
  const text = await pageText(page).catch(() => null);
  sendEvent(id, {
    type: 'navigation_completed',
    url: page.url(),
    status: response?.status?.() ?? null,
    title,
  });
  recordHistory(page.url(), title);
  return baseResponse({
    url: page.url(),
    title,
    text,
  });
}

async function snapshot(id, request, op) {
  const { page } = await pageFor(request);
  let snap;
  if (op.selector) {
    const locator = page.locator(op.selector).first();
    await locator.waitFor({ state: 'attached', timeout: timeoutMs(request) });
    snap = {
      html: await locator.evaluate((el) => el.outerHTML).catch(() => null),
      text: op.includeText ? await locator.innerText({ timeout: timeoutMs(request) }).catch(() => null) : null,
      elements: op.includeLinks ? await elementSnapshots(page, op.selector) : [],
    };
  } else {
    snap = {
      html: await page.content(),
      text: op.includeText ? await pageText(page) : null,
      elements: await elementSnapshots(page, null),
    };
  }
  sendEvent(id, { type: 'dom_snapshot', snapshot: snap });
  return baseResponse({ snapshot: snap, text: snap.text ?? null });
}

async function screenshot(id, request, op) {
  const { page } = await pageFor(request);
  const artifact = await screenshotArtifact(page, request, op);
  const value = screenshotCaptures.get(artifact);
  if (artifact.path) sendEvent(id, { type: 'screenshot_captured', artifact });
  return baseResponse({ value, artifacts: artifact.path ? [artifact] : [] });
}

async function click(id, request, op) {
  const { page } = await pageFor(request);
  const button = op.button || 'left';
  const clickCount = op.clickCount || 1;
  const modifiers = op.modifiers || [];
  if (op.selector) {
    await page.locator(op.selector).first().click({
      button,
      clickCount,
      modifiers,
      timeout: timeoutMs(request),
    });
  } else if (Number.isFinite(op.x) && Number.isFinite(op.y)) {
    await page.mouse.click(op.x, op.y, { button, clickCount });
  } else {
    throw new BrowserSidecarError('invalid_request', 'Click requires selector or x/y coordinates.');
  }
  sendEvent(id, { type: 'status', message: 'Browser click completed.' });
  return baseResponse({ message: 'click completed', url: page.url(), title: await page.title().catch(() => null) });
}

async function typeText(id, request, op) {
  const { page } = await pageFor(request);
  const delay = op.delayMs ?? 0;
  if (op.selector) {
    const locator = page.locator(op.selector).first();
    if (op.clear) {
      await locator.fill('', { timeout: timeoutMs(request) });
    }
    await locator.type(op.text, { delay, timeout: timeoutMs(request) });
  } else {
    await page.keyboard.type(op.text, { delay });
  }
  if (op.pressEnter) {
    await page.keyboard.press('Enter');
  }
  sendEvent(id, { type: 'status', message: 'Browser type completed.' });
  return baseResponse({ message: 'type completed', url: page.url(), title: await page.title().catch(() => null) });
}

async function evaluate(id, request, op) {
  const { page } = await pageFor(request);
  const value = await page.evaluate(
    ({ source, args }) => {
      try {
        const fn = new Function('args', `return (${source});`);
        return fn(args);
      } catch (_expressionError) {
        const fn = new Function('args', source);
        return fn(args);
      }
    },
    { source: op.script, args: op.args ?? null },
  );
  sendEvent(id, { type: 'status', message: 'Browser eval completed.' });
  return baseResponse({ value, url: page.url(), title: await page.title().catch(() => null) });
}

async function consoleEntries(id, _request, op) {
  let entries = filterBySince(state.console, op.sinceMs);
  if (Array.isArray(op.levels) && op.levels.length > 0) {
    const levels = new Set(op.levels);
    entries = entries.filter((entry) => levels.has(entry.level));
  }
  entries = takeLimit(entries, op.limit);
  if (op.clear) {
    state.console = [];
  }
  for (const entry of entries) {
    sendEvent(id, { type: 'console', entry });
  }
  return baseResponse({ console: entries });
}

async function networkEntries(id, _request, op) {
  let entries = filterBySince(state.network, op.sinceMs);
  if (op.urlFilter) {
    entries = entries.filter((entry) => entry.url.includes(op.urlFilter));
  }
  entries = takeLimit(entries, op.limit);
  for (const entry of entries) {
    sendEvent(id, { type: 'network', entry });
  }
  return baseResponse({ network: entries });
}

async function gifCreator(id, request, op) {
  const { page, tabId } = await pageFor(request);
  await fsp.mkdir(artifactDir, { recursive: true });
  const durationSeconds = clampNumber(op.durationSeconds ?? op.duration_seconds ?? 1, 0.05, 60);
  const fps = Math.max(1, Math.min(30, Math.round(op.fps || 2)));
  const frameCount = Math.max(1, Math.min(120, Math.ceil(durationSeconds * fps)));
  const frameDelayMs = Math.max(0, Math.round((durationSeconds * 1000) / frameCount));
  const framesDir = path.join(artifactDir, `gif-frames-${Date.now()}-${Math.random().toString(16).slice(2)}`);
  await fsp.mkdir(framesDir, { recursive: true });
  const framePaths = [];
  const frameArtifacts = [];
  for (let index = 0; index < frameCount; index += 1) {
    const framePath = path.join(framesDir, `frame-${String(index + 1).padStart(4, '0')}.png`);
    await page.screenshot({
      path: framePath,
      fullPage: !!op.fullPage,
      type: 'png',
      timeout: timeoutMs(request),
    });
    const stat = await fsp.stat(framePath);
    framePaths.push(framePath);
    frameArtifacts.push({
      id: path.basename(framePath),
      kind: 'screenshot',
      uri: pathToFileURL(framePath).href,
      path: framePath,
      mimeType: 'image/png',
      sizeBytes: stat.size,
      metadata: { frameIndex: index, tabId, source: 'gif_creator' },
    });
    if (index + 1 < frameCount && frameDelayMs > 0) {
      await page.waitForTimeout(frameDelayMs).catch(
        () => new Promise((resolve) => setTimeout(resolve, frameDelayMs)),
      );
    }
  }
  const fileName = gifFileName(op.filename);
  const filePath = path.join(artifactDir, fileName);
  const encoded = await encodeGif(framePaths, filePath, fps);
  const stat = await fsp.stat(filePath);
  const artifact = {
    id: path.basename(filePath),
    kind: 'gif',
    uri: pathToFileURL(filePath).href,
    path: filePath,
    mimeType: 'image/gif',
    sizeBytes: stat.size,
    metadata: {
      frames: frameCount,
      fps,
      durationSeconds,
      fullPage: !!op.fullPage,
      encoder: encoded.encoder,
      encoded: encoded.encoded,
      framesDirectory: framesDir,
      frameArtifacts: frameArtifacts.map((frame) => ({
        id: frame.id,
        path: frame.path,
        uri: frame.uri,
      })),
    },
  };
  sendEvent(id, { type: 'artifact', artifact });
  return baseResponse({
    message: encoded.encoded ? 'gif exported' : 'gif frames captured',
    artifacts: [artifact, ...frameArtifacts],
    value: { artifact, frames: frameArtifacts, encoded },
  });
}

async function uploadImage(id, request, op) {
  return fileUpload(id, request, {
    ...op,
    paths: [op.path],
    selector: op.selector,
  });
}

async function fileUpload(id, request, op) {
  const { page, tabId } = await pageFor(request);
  const files = await validateUploadPaths(op.paths || []);
  const selector = op.selector || selectorForRef(tabId, op.refId || op.ref || op.reference);
  if (!selector) {
    throw new BrowserSidecarError('not_found', 'file_upload requires selector or known refId.');
  }
  const locator = page.locator(selector).first();
  if (typeof locator.setInputFiles !== 'function') {
    throw new BrowserSidecarError('unavailable', 'The selected browser runtime does not support file uploads.');
  }
  await locator.setInputFiles(files, { timeout: timeoutMs(request) });
  sendEvent(id, { type: 'status', message: `Uploaded ${files.length} file(s) to ${selector}.` });
  return baseResponse({
    message: 'file upload completed',
    value: {
      selector,
      files,
      count: files.length,
    },
  });
}

async function shortcutsList(_id, request, op) {
  const { page } = await pageFor(request);
  const raw = await page
    .evaluate(() => {
      const value = globalThis.__klausShortcuts || globalThis.klausShortcuts || [];
      return typeof value === 'function' ? value() : value;
    })
    .catch(() => []);
  const query = String(op.query || '').trim().toLowerCase();
  const shortcuts = normalizeShortcuts(raw).filter((shortcut) => {
    if (!query) return true;
    return [shortcut.id, shortcut.command, shortcut.description]
      .filter(Boolean)
      .join(' ')
      .toLowerCase()
      .includes(query);
  });
  return baseResponse({ value: shortcuts, text: JSON.stringify(shortcuts, null, 2) });
}

async function shortcutsExecute(id, request, op) {
  const { page } = await pageFor(request);
  const value = await page.evaluate(
    ({ name, waitForCompletion }) => {
      const executor = globalThis.__klausExecuteShortcut || globalThis.klausExecuteShortcut;
      if (typeof executor === 'function') {
        return executor(name, { waitForCompletion });
      }
      const raw = globalThis.__klausShortcuts || globalThis.klausShortcuts || [];
      const shortcuts = typeof raw === 'function' ? raw() : raw;
      const list = Array.isArray(shortcuts) ? shortcuts : [];
      const shortcut = list.find((item) => {
        if (!item) return false;
        return item.id === name || item.command === name || item.name === name;
      });
      if (!shortcut) {
        return { executed: false, name, reason: 'not_found' };
      }
      globalThis.dispatchEvent?.(
        new CustomEvent('klaus-shortcut-execute', {
          detail: { name, shortcut, waitForCompletion },
        }),
      );
      return { executed: true, name, command: shortcut.command || shortcut.name || shortcut.id, shortcut };
    },
    { name: op.name, waitForCompletion: !!op.waitForCompletion },
  );
  if (value?.executed === false) {
    throw new BrowserSidecarError('not_found', `Shortcut not found: ${op.name}`, value);
  }
  sendEvent(id, { type: 'status', message: `Browser shortcut executed: ${op.name}.` });
  return baseResponse({ message: 'shortcut execution started', value });
}

async function listConnectedBrowsers(_id, request, _op) {
  await ensureContext();
  const value = [{
    deviceId: state.sessionId,
    name: state.sessionName || 'Playwright Chromium',
    platform: `${process.platform}/${backend}`,
    version: null,
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
      'file_upload',
      'gif_creator',
      'shortcuts',
    ],
    isLocal: true,
    selected: true,
    connected: true,
    tabs: tabList(),
  }];
  return baseResponse({ value, text: JSON.stringify(value, null, 2), tabId: request?.target?.tabId || state.selectedTabId });
}

async function selectBrowser(_id, request, op) {
  const deviceId = String(op.deviceId || '').trim();
  if (!deviceId) {
    throw new BrowserSidecarError('invalid_request', 'select_browser requires deviceId.');
  }
  if (deviceId !== state.sessionId) {
    throw new BrowserSidecarError(
      'not_found',
      `No connected browser has deviceId "${deviceId}". Call list_connected_browsers to see currently connected browsers.`,
    );
  }
  return baseResponse({
    tabId: request?.target?.tabId || state.selectedTabId,
    message: `Connected to browser "${state.sessionName || 'Playwright Chromium'}".`,
    value: {
      deviceId: state.sessionId,
      name: state.sessionName || 'Playwright Chromium',
      isLocal: true,
      selected: true,
    },
  });
}

async function switchBrowser(_id, request, op) {
  const waitMs = Number.isFinite(op.waitMs) ? Math.max(0, Math.min(op.waitMs, 120_000)) : 0;
  if (waitMs > 0) {
    await new Promise((resolve) => setTimeout(resolve, waitMs));
  }
  return baseResponse({
    tabId: request?.target?.tabId || state.selectedTabId,
    message: `Connected to browser "${state.sessionName || 'Playwright Chromium'}".`,
    value: {
      deviceId: state.sessionId,
      name: state.sessionName || 'Playwright Chromium',
      isLocal: true,
      selected: true,
      prompt: op.prompt || null,
    },
  });
}

async function historyEntries(_id, _request, op) {
  const queries = Array.isArray(op.queries) ? op.queries.map((item) => String(item).toLowerCase()) : [];
  const from = op.from ? Date.parse(op.from) : null;
  const to = op.to ? Date.parse(op.to) : null;
  let entries = [...state.history];
  if (Number.isFinite(from)) {
    entries = entries.filter((entry) => Date.parse(entry.dateVisited) >= from);
  }
  if (Number.isFinite(to)) {
    entries = entries.filter((entry) => Date.parse(entry.dateVisited) <= to);
  }
  if (queries.length > 0) {
    entries = entries.filter((entry) => {
      const haystack = `${entry.title || ''} ${entry.url || ''}`.toLowerCase();
      return queries.some((query) => haystack.includes(query));
    });
  }
  entries.sort((a, b) => Date.parse(b.dateVisited) - Date.parse(a.dateVisited));
  entries = takeLimit(entries, op.limit || 100);
  return baseResponse({ value: entries, text: JSON.stringify(entries, null, 2) });
}

async function tabsContent(_id, request, op) {
  if (!Array.isArray(op.urls) || op.urls.length === 0) {
    throw new BrowserSidecarError('invalid_request', 'tabs_content requires urls.');
  }
  await ensureContext();
  const previous = state.selectedTabId;
  const results = [];
  for (const url of op.urls) {
    const page = await state.context.newPage();
    try {
      const response = await page.goto(url, {
        waitUntil: mapWaitUntil(op.waitUntil),
        timeout: timeoutMs(request),
      });
      const title = await page.title().catch(() => null);
      const resolvedUrl = page.url?.() || url;
      const content = await contentForPage(page, op.contentType || op.content_type || 'text', op.maxChars || 50_000);
      recordHistory(resolvedUrl, title);
      results.push({
        url: resolvedUrl,
        requestedUrl: url,
        title,
        status: response?.status?.() ?? null,
        content,
      });
    } catch (error) {
      results.push({
        url,
        requestedUrl: url,
        title: null,
        status: null,
        content: null,
        error: error?.message || String(error),
      });
    } finally {
      await page.close().catch(() => {});
    }
  }
  state.selectedTabId = previous;
  return baseResponse({ value: results, text: JSON.stringify(results, null, 2), tabId: previous });
}

async function contentExport(id, request, op) {
  const { page, tabId } = await pageFor(request);
  await fsp.mkdir(artifactDir, { recursive: true });
  const format = (op.format || 'text').toLowerCase();
  const extension = format === 'html' ? 'html' : format === 'markdown' || format === 'md' ? 'md' : 'txt';
  const filePath = path.join(artifactDir, `content-${Date.now()}-${Math.random().toString(16).slice(2)}.${extension}`);
  const content = await contentForPage(page, format, op.maxChars || 200_000, op.selector || null);
  await fsp.writeFile(filePath, content || '');
  const stat = await fsp.stat(filePath);
  const artifact = {
    id: path.basename(filePath),
    kind: 'other',
    uri: pathToFileURL(filePath).href,
    path: filePath,
    mimeType: extension === 'html' ? 'text/html' : 'text/plain',
    sizeBytes: stat.size,
    metadata: { format, tabId, url: page.url?.() || null },
  };
  sendEvent(id, { type: 'artifact', artifact });
  return baseResponse({
    tabId,
    message: 'content exported',
    artifacts: [artifact],
    value: { path: filePath, artifact, content },
    text: filePath,
  });
}

async function downloadMedia(id, request, op) {
  const { page, tabId } = await pageFor(request);
  let download = null;
  let directUrl = op.url || null;
  if (!directUrl && op.selector) {
    directUrl = await page.locator(op.selector).first().evaluate((el) => {
      const target = el.closest?.('a[href]') || el;
      return target.href || target.src || target.currentSrc || target.getAttribute?.('href') || target.getAttribute?.('src') || null;
    }).catch(() => null);
  }
  if (!op.url && (op.selector || (Number.isFinite(op.x) && Number.isFinite(op.y)))) {
    const downloadPromise = page.waitForEvent?.('download', { timeout: timeoutMs(request) }).catch(() => null);
    if (op.selector) {
      await page.locator(op.selector).first().click({ timeout: timeoutMs(request) }).catch(() => {});
    } else {
      await page.mouse.click(op.x, op.y);
    }
    download = downloadPromise ? await downloadPromise : null;
  }
  let artifact;
  if (download) {
    await fsp.mkdir(artifactDir, { recursive: true });
    const fileName = downloadFileName(op.filename || download.suggestedFilename?.() || 'download.bin');
    const filePath = path.join(artifactDir, fileName);
    await download.saveAs(filePath);
    artifact = await artifactForDownloadedFile(filePath, { tabId, source: 'playwright-download' });
  } else {
    if (!directUrl) {
      throw new BrowserSidecarError('not_found', 'download requires url, selector with href/src, or coordinates that trigger a download.');
    }
    const resolved = new URL(directUrl, page.url?.() || 'about:blank').href;
    artifact = await downloadUrlToArtifact(resolved, op.filename, { tabId, source: 'direct-url' });
  }
  sendEvent(id, { type: 'download', artifact });
  return baseResponse({
    tabId,
    message: 'download completed',
    artifacts: [artifact],
    value: { artifact },
    text: artifact.path || artifact.uri || artifact.id,
  });
}

async function clipboardOperation(_id, request, op) {
  const { page, tabId } = await pageFor(request);
  const action = op.action || 'read_text';
  if (action === 'write_text' || action === 'write') {
    const text = op.text ?? clipboardTextFromItems(op.items) ?? '';
    state.clipboard.text = String(text);
    state.clipboard.items = op.items ? normalizeClipboardItems(op.items) : clipboardItemsForText(String(text));
    await page.evaluate((value) => navigator.clipboard?.writeText?.(value), String(text)).catch(() => {});
    return baseResponse({ tabId, message: 'clipboard written', value: { text: String(text) } });
  }
  const text = await page
    .evaluate(() => navigator.clipboard?.readText?.())
    .catch(() => state.clipboard.text);
  state.clipboard.text = text ?? state.clipboard.text;
  const items = state.clipboard.items.length > 0 ? state.clipboard.items : clipboardItemsForText(state.clipboard.text);
  if (action === 'read') {
    return baseResponse({ tabId, value: items, text: state.clipboard.text });
  }
  return baseResponse({ tabId, value: state.clipboard.text, text: state.clipboard.text });
}

async function dialogOperation(_id, request, op) {
  const { tabId } = await pageFor(request);
  const entry = state.dialogs.get(tabId) || null;
  const action = op.action || 'get';
  if (action === 'get') {
    return baseResponse({ tabId, value: entry ? dialogDescriptor(entry) : null });
  }
  if (!entry?.dialog) {
    throw new BrowserSidecarError('not_found', 'No active JavaScript dialog is available for this tab.');
  }
  if (action === 'accept') {
    await entry.dialog.accept(op.promptText ?? op.prompt_text ?? '').catch((error) => {
      throw new BrowserSidecarError('host_error', `failed to accept dialog: ${error.message}`);
    });
  } else if (action === 'dismiss') {
    await entry.dialog.dismiss().catch((error) => {
      throw new BrowserSidecarError('host_error', `failed to dismiss dialog: ${error.message}`);
    });
  } else {
    throw new BrowserSidecarError('invalid_request', `Unsupported dialog action: ${action}`);
  }
  state.dialogs.delete(tabId);
  return baseResponse({ tabId, message: `dialog ${action} completed`, value: dialogDescriptor(entry) });
}

async function markTab(_id, request, op) {
  const { page, tabId } = await pageFor(request);
  const status = op.status || 'handoff';
  state.tabMarks.set(tabId, {
    status,
    markedAt: new Date().toISOString(),
    url: page.url?.() || null,
    title: await page.title().catch(() => null),
  });
  return baseResponse({ tabId, message: `tab marked ${status}`, value: state.tabMarks.get(tabId) });
}

async function playwrightWaitForEvent(id, request, op) {
  const { page, tabId } = await pageFor(request);
  const event = String(op.event || '');
  const timeout = timeoutMs(request);
  if (event === 'download') {
    const download = await page.waitForEvent('download', { timeout });
    const suggestedFilename = await download.suggestedFilename();
    await fsp.mkdir(artifactDir, { recursive: true });
    const fileName = downloadFileName(suggestedFilename || 'download.bin');
    const downloadPath = path.join(artifactDir, fileName);
    await download.saveAs(downloadPath);
    const artifact = await artifactForDownloadedFile(downloadPath, { tabId, source: 'playwright-wait-event' });
    sendEvent(id, { type: 'download', artifact });
    return baseResponse({
      tabId,
      artifacts: artifact ? [artifact] : [],
      value: {
        event,
        path: downloadPath,
        suggestedFilename,
        url: download.url?.() || null,
        artifact,
      },
    });
  }
  if (event === 'filechooser') {
    const fileChooser = await page.waitForEvent('filechooser', { timeout });
    const chooserId = `filechooser-${state.nextFileChooserSeq++}`;
    state.fileChoosers.set(chooserId, fileChooser);
    return baseResponse({
      tabId,
      value: {
        event,
        chooserId,
        isMultiple: fileChooser.isMultiple(),
      },
    });
  }
  throw new BrowserSidecarError('invalid_request', `Unsupported Playwright event: ${event}`);
}

async function playwrightFileChooserSetFiles(_id, request, op) {
  const chooserId = op.chooserId || op.id;
  const fileChooser = state.fileChoosers.get(chooserId);
  if (!fileChooser) {
    throw new BrowserSidecarError('not_found', `Unknown or expired file chooser: ${chooserId}`);
  }
  const files = await validateUploadPaths(op.files || op.paths || []);
  await fileChooser.setFiles(files, { timeout: timeoutMs(request) });
  state.fileChoosers.delete(chooserId);
  return baseResponse({
    message: 'file chooser files set',
    value: {
      chooserId,
      files,
      count: files.length,
    },
  });
}

async function playwrightWaitForLoadState(_id, request, op) {
  const { page, tabId } = await pageFor(request);
  const stateName = mapWaitUntil(op.state || op.waitUntil || 'load');
  await page.waitForLoadState(stateName, { timeout: timeoutMs(request) });
  return baseResponse({
    tabId,
    url: page.url(),
    title: await page.title().catch(() => null),
    value: { state: stateName },
  });
}

async function playwrightWaitForUrl(_id, request, op) {
  const { page, tabId } = await pageFor(request);
  if (!op.url) {
    throw new BrowserSidecarError('invalid_request', 'waitForURL requires url.');
  }
  await page.waitForURL(op.url, {
    timeout: timeoutMs(request),
    waitUntil: mapWaitUntil(op.waitUntil),
  });
  return baseResponse({
    tabId,
    url: page.url(),
    title: await page.title().catch(() => null),
    value: { url: page.url() },
  });
}

async function playwrightDomSnapshot(id, request, op) {
  const { page } = await pageFor(request);
  const text = clip(
    await page.evaluate((visibleOnly) => {
      if (!visibleOnly) return document.documentElement?.outerHTML || '';
      const walker = document.createTreeWalker(document.body || document.documentElement, NodeFilter.SHOW_ELEMENT);
      const entries = [];
      while (walker.nextNode()) {
        const el = walker.currentNode;
        const rect = el.getBoundingClientRect();
        const style = getComputedStyle(el);
        if (rect.width <= 0 || rect.height <= 0 || style.visibility === 'hidden' || style.display === 'none') continue;
        const label = [el.tagName.toLowerCase(), el.id ? `#${el.id}` : '', el.className ? `.${String(el.className).trim().split(/\s+/).join('.')}` : ''].join('');
        const content = (el.innerText || el.textContent || '').trim().replace(/\s+/g, ' ').slice(0, 120);
        entries.push(`${label}${content ? ` ${content}` : ''}`);
      }
      return entries.join('\n');
    }, !!op.visibleOnly),
    op.maxChars || 100_000,
  );
  sendEvent(id, { type: 'dom_snapshot', snapshot: { text } });
  return baseResponse({ text, value: { text } });
}

async function playwrightElementInfo(_id, request, op) {
  const { page } = await pageFor(request);
  let value;
  if (op.selector) {
    value = await elementSnapshots(page, op.selector);
  } else {
    const x = Number(op.x ?? 0);
    const y = Number(op.y ?? 0);
    value = await page.evaluate(({ x, y }) => {
      const el = document.elementFromPoint(x, y);
      if (!el) return [];
      const rect = el.getBoundingClientRect();
      return [{
        selector: el.id ? `#${CSS.escape(el.id)}` : el.tagName.toLowerCase(),
        tagName: el.tagName.toLowerCase(),
        text: (el.innerText || el.textContent || '').trim(),
        role: el.getAttribute('role'),
        x: rect.x,
        y: rect.y,
        width: rect.width,
        height: rect.height,
        visible: rect.width > 0 && rect.height > 0,
      }];
    }, { x, y });
  }
  return baseResponse({ value, text: JSON.stringify(value, null, 2) });
}

async function playwrightElementScreenshot(id, request, op) {
  const { page } = await pageFor(request);
  const artifact = await screenshotArtifact(page, request, {
    selector: op.selector || undefined,
    format: op.format || 'png',
    clip: !op.selector && Number.isFinite(op.x) && Number.isFinite(op.y) ? zoomClip([op.x, op.y]) : undefined,
  });
  sendEvent(id, { type: 'screenshot_captured', artifact });
  return baseResponse({ artifacts: [artifact], value: { ...artifact, ...screenshotCaptures.get(artifact) } });
}

async function readPage(id, request, op) {
  const { page, tabId } = await pageFor(request);
  const elements = rememberRefs(tabId, await elementSnapshots(page, selectorForRef(tabId, op.refId)));
  const text = clip(await pageText(page), op.maxChars || 50_000);
  const filtered =
    op.filter === 'interactive'
      ? elements.filter((el) => el.interactive !== false)
      : elements;
  const value = {
    tabId,
    text,
    elements: filtered,
    tree: renderElementTree(filtered, op.depth || 15, op.maxChars || 50_000),
  };
  sendEvent(id, { type: 'dom_snapshot', snapshot: { text, elements: filtered } });
  return baseResponse({ value, text: value.tree });
}

async function findElements(id, request, op) {
  const { page, tabId } = await pageFor(request);
  const query = String(op.query || '').trim().toLowerCase();
  if (!query) {
    throw new BrowserSidecarError('invalid_request', 'find requires a query.');
  }
  const all = rememberRefs(tabId, await elementSnapshots(page, null));
  const scored = all
    .map((element) => ({ element, score: scoreElement(element, query) }))
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, op.limit || 20)
    .map((item) => item.element);
  const value = { query: op.query, matches: scored, count: scored.length };
  sendEvent(id, { type: 'status', message: `Browser find returned ${scored.length} match(es).` });
  return baseResponse({ value, text: renderElementTree(scored, 3, 20_000) });
}

async function formInput(id, request, op) {
  const { page, tabId } = await pageFor(request);
  const selector = selectorForRef(tabId, op.ref || op.reference);
  if (!selector) {
    throw new BrowserSidecarError('not_found', `Unknown browser element reference: ${op.ref || op.reference}`);
  }
  const locator = page.locator(selector).first();
  await setElementValue(locator, op.value, timeoutMs(request));
  sendEvent(id, { type: 'status', message: 'Browser form input completed.' });
  return baseResponse({ message: 'form input completed', value: op.value });
}

async function computer(id, request, op) {
  const { page, tabId } = await pageFor(request);
  const action = op.action;
  const point = op.coordinate || null;
  switch (action) {
    case 'left_click':
    case 'right_click':
    case 'double_click':
    case 'triple_click': {
      const [x, y] = requirePoint(point, action);
      await page.mouse.click(x, y, {
        button: action === 'right_click' ? 'right' : 'left',
        clickCount: action === 'double_click' ? 2 : action === 'triple_click' ? 3 : 1,
      });
      break;
    }
    case 'hover': {
      const [x, y] = requirePoint(point, action);
      await page.mouse.move(x, y);
      break;
    }
    case 'left_click_drag': {
      const [startX, startY] = requirePoint(op.startCoordinate, action);
      const [endX, endY] = requirePoint(point, action);
      await page.mouse.move(startX, startY);
      await page.mouse.down();
      await page.mouse.move(endX, endY);
      await page.mouse.up();
      break;
    }
    case 'type':
      await page.keyboard.type(String(op.text ?? ''), { delay: op.delayMs || 0 });
      break;
    case 'key':
      await page.keyboard.press(String(op.text || 'Enter'));
      break;
    case 'wait':
      await page.waitForTimeout(Math.max(0, Math.min(10_000, (op.duration || 0) * 1000)));
      break;
    case 'scroll': {
      const amount = Number.isFinite(op.amount) ? op.amount : 500;
      const direction = op.scrollDirection || 'down';
      const deltaX = direction === 'left' ? -amount : direction === 'right' ? amount : 0;
      const deltaY = direction === 'up' ? -amount : direction === 'down' ? amount : 0;
      if (page.mouse.wheel) {
        await page.mouse.wheel(deltaX, deltaY);
      } else {
        await page.evaluate(({ x, y }) => window.scrollBy(x, y), { x: deltaX, y: deltaY });
      }
      break;
    }
    case 'scroll_to': {
      const selector = selectorForRef(tabId, op.refId || op.ref);
      if (!selector) throw new BrowserSidecarError('not_found', 'scroll_to requires a valid refId.');
      await page.locator(selector).first().scrollIntoViewIfNeeded({ timeout: timeoutMs(request) });
      break;
    }
    case 'screenshot':
    case 'zoom': {
      const artifact = await screenshotArtifact(page, request, {
        fullPage: false,
        selector: op.refId ? selectorForRef(tabId, op.refId) : undefined,
        format: 'png',
        clip: action === 'zoom' && point ? zoomClip(point) : undefined,
        save: op.save === true,
      });
      if (artifact.path) sendEvent(id, { type: 'screenshot_captured', artifact });
      return baseResponse({ message: 'computer action completed', value: screenshotCaptures.get(artifact), artifacts: artifact.path ? [artifact] : [] });
    }
    default:
      throw new BrowserSidecarError('invalid_request', `Unsupported computer action: ${action}`);
  }
  sendEvent(id, { type: 'status', message: 'Browser computer action completed.' });
  return baseResponse({ message: 'computer action completed', value: { action } });
}

async function tabs(id, request, op) {
  await ensureContext();
  const action = op.action || 'list';
  if (action === 'new') {
    const { page, tabId } = await newTab(op.url);
    sendEvent(id, { type: 'tab_opened', sessionId: state.sessionId, tabId });
    return baseResponse({
      tabId,
      url: page.url(),
      title: await page.title().catch(() => null),
      value: tabDescriptor(tabId, page),
    });
  }
  if (action === 'selected') {
    const page = state.selectedTabId ? state.pages.get(state.selectedTabId) : null;
    return baseResponse({ value: page ? tabDescriptor(state.selectedTabId, page) : null });
  }
  if (action === 'get') {
    const tabId = request?.target?.tabId || state.selectedTabId;
    const page = tabId ? state.pages.get(tabId) : null;
    if (!page) throw new BrowserSidecarError('not_found', `Unknown browser tab: ${tabId}`);
    state.selectedTabId = tabId;
    return baseResponse({ tabId, value: tabDescriptor(tabId, page) });
  }
  if (action === 'close') {
    await closeTab(request?.target?.tabId || state.selectedTabId);
    return baseResponse({ message: 'tab closed', value: tabList() });
  }
  if (action === 'finalize') {
    const keep = state.selectedTabId;
    for (const tabId of [...state.pages.keys()]) {
      if (tabId !== keep) await closeTab(tabId);
    }
    return baseResponse({ message: 'tabs finalized', value: tabList() });
  }
  if (action === 'name_session') {
    state.sessionName = op.name || null;
    return baseResponse({ message: 'session named', value: { name: state.sessionName } });
  }
  return baseResponse({ value: tabList(), text: JSON.stringify(tabList(), null, 2) });
}

async function locatorAction(id, request, op) {
  const { page } = await pageFor(request);
  const locator = resolveLocator(page, op, { single: !multiLocatorAction(op.action) });
  const options = normalizeOptions(op.options || {}, request);
  let value = null;
  switch (op.action) {
    case 'click':
      await locator.click(options);
      break;
    case 'double_click':
      await locator.dblclick(options);
      break;
    case 'type':
      await locator.type(String(op.value ?? ''), options);
      break;
    case 'fill':
      await locator.fill(String(op.value ?? ''), options);
      break;
    case 'press':
      await locator.press(String(op.value ?? 'Enter'), options);
      break;
    case 'count':
      value = await resolveLocator(page, op, { single: false }).count();
      break;
    case 'text':
    case 'text_content':
      value = await locator.textContent(options).catch(() => locator.innerText(options));
      break;
    case 'inner_text':
      value = await locator.innerText(options);
      break;
    case 'get_attribute':
      value = await locator.getAttribute(String(op.value || ''), options);
      break;
    case 'is_enabled':
      value = await locator.isEnabled(options);
      break;
    case 'all_text_contents':
      value = await resolveLocator(page, op, { single: false }).allTextContents();
      break;
    case 'is_visible':
      value = await locator.isVisible(options);
      break;
    case 'wait_for':
      await locator.waitFor({ state: op.options?.state || 'visible', timeout: options.timeout });
      break;
    case 'screenshot': {
      const artifact = await screenshotArtifact(page, request, {
        selector: op.selector,
        format: op.options?.format || 'png',
      });
      sendEvent(id, { type: 'screenshot_captured', artifact });
      return baseResponse({ artifacts: [artifact], value: { ...artifact, ...screenshotCaptures.get(artifact) } });
    }
    case 'check':
      await locator.check(options);
      break;
    case 'uncheck':
      await locator.uncheck(options);
      break;
    case 'set_checked':
      await locator.setChecked(!!op.value, options);
      break;
    case 'select_option':
      value = await locator.selectOption(op.value, options);
      break;
    default:
      throw new BrowserSidecarError('invalid_request', `Unsupported locator action: ${op.action}`);
  }
  sendEvent(id, { type: 'status', message: `Browser locator ${op.action} completed.` });
  return baseResponse({ message: `locator ${op.action} completed`, value });
}

function resolveLocator(page, op, { single } = { single: true }) {
  if (!op.selector) {
    throw new BrowserSidecarError('invalid_request', 'locator requires selector.');
  }
  const scope = op.frameSelector ? page.frameLocator(op.frameSelector) : page;
  let locator = scope.locator(op.selector);
  const filter = op.filter && typeof op.filter === 'object' ? op.filter : null;
  if (filter) {
    const filterOptions = {};
    if (filter.hasText != null) filterOptions.hasText = String(filter.hasText);
    if (filter.hasNotText != null) filterOptions.hasNotText = String(filter.hasNotText);
    if (filter.has != null) filterOptions.has = scope.locator(String(filter.has));
    if (filter.hasNot != null) filterOptions.hasNot = scope.locator(String(filter.hasNot));
    if (Object.keys(filterOptions).length > 0) locator = locator.filter(filterOptions);
  }
  if (op.andSelector) {
    locator = locator.and(scope.locator(String(op.andSelector)));
  }
  if (op.orSelector) {
    locator = locator.or(scope.locator(String(op.orSelector)));
  }
  if (Number.isInteger(op.nthIndex)) {
    locator = locator.nth(op.nthIndex);
  } else if (single) {
    locator = locator.first();
  }
  return locator;
}

function multiLocatorAction(action) {
  return action === 'count' || action === 'all_text_contents';
}

async function pageTextOperation(_id, request, op) {
  const { page } = await pageFor(request);
  const text = clip(await pageText(page), op.maxChars || 50_000);
  return baseResponse({ text, value: { text } });
}

async function batch(id, request, op) {
  if (!Array.isArray(op.actions)) {
    throw new BrowserSidecarError('invalid_request', 'batch actions must be an array.');
  }
  const results = [];
  for (const action of op.actions) {
    results.push(await executeRequest(id, { ...request, operation: action }));
  }
  return baseResponse({ value: { results }, message: `batch completed ${results.length} action(s)` });
}

async function resize(_id, request, op) {
  const { page } = await pageFor(request, op.viewport);
  await page.setViewportSize(toViewportSize(op.viewport));
  return baseResponse({ message: 'viewport resized', value: op.viewport });
}

async function closeTarget(_id, _request, op) {
  const target = op.target || 'tab';
  if ((target === 'tab' || target === 'all') && state.selectedTabId) {
    await closeTab(state.selectedTabId);
  }
  if ((target === 'session' || target === 'all') && state.context) {
    await state.context.close().catch(() => {});
    state.context = null;
    state.pages.clear();
    state.selectedTabId = null;
  }
  if (target === 'all' && state.browser) {
    await state.browser.close().catch(() => {});
    state.browser = null;
    state.context = null;
    state.pages.clear();
    state.selectedTabId = null;
  }
  return baseResponse({ message: `closed ${target}` });
}

async function ensureContext(viewport) {
  if (!state.playwright) {
    state.playwright = await loadPlaywright();
  }
  if (!state.context) {
    if (backend === 'cdp') {
      const endpoint = process.env.KLAUS_BROWSER_CDP_ENDPOINT;
      if (!endpoint) {
        throw new BrowserSidecarError(
          'unavailable',
          'KLAUS_BROWSER_BACKEND=cdp requires KLAUS_BROWSER_CDP_ENDPOINT.',
        );
      }
      state.browser = await state.playwright.chromium.connectOverCDP(endpoint);
      state.context = state.browser.contexts()[0] || await state.browser.newContext();
    } else if (backend === 'persistent') {
      const userDataDir =
        process.env.KLAUS_BROWSER_USER_DATA_DIR ||
        path.join(process.cwd(), '.klaus', 'browser-profile');
      await fsp.mkdir(userDataDir, { recursive: true });
      state.context = await state.playwright.chromium.launchPersistentContext(userDataDir, {
        headless,
        viewport: viewport ? toViewportSize(viewport) : undefined,
        deviceScaleFactor: viewport?.deviceScaleFactor,
        isMobile: viewport?.isMobile,
        acceptDownloads: true,
        channel: process.env.KLAUS_BROWSER_CHANNEL || undefined,
        executablePath: process.env.KLAUS_BROWSER_EXECUTABLE_PATH || undefined,
      });
      state.browser = state.context.browser?.() || null;
    } else {
      state.browser = await state.playwright.chromium.launch({
        headless,
        channel: process.env.KLAUS_BROWSER_CHANNEL || undefined,
        executablePath: process.env.KLAUS_BROWSER_EXECUTABLE_PATH || undefined,
      });
      state.context = await state.browser.newContext({
        viewport: viewport ? toViewportSize(viewport) : undefined,
        deviceScaleFactor: viewport?.deviceScaleFactor,
        isMobile: viewport?.isMobile,
        acceptDownloads: true,
      });
    }
    for (const page of state.context.pages?.() || []) {
      const tabId = newTabId();
      registerPage(tabId, page);
      state.selectedTabId ||= tabId;
    }
  }
  return state.context;
}

async function pageFor(request, viewport) {
  await ensureContext(viewport);
  let tabId = request?.target?.tabId || state.selectedTabId;
  let page = tabId ? state.pages.get(tabId) : null;
  if (!page || page.isClosed()) {
    tabId = tabId && !state.pages.has(tabId) ? tabId : newTabId();
    page = await state.context.newPage();
    registerPage(tabId, page);
  }
  state.selectedTabId = tabId;
  if (viewport) {
    await page.setViewportSize(toViewportSize(viewport));
  }
  return { page, tabId };
}

function newTabId() {
  return `tab-${state.nextTabSeq++}`;
}

function registerPage(tabId, page) {
  state.pages.set(tabId, page);
  state.selectedTabId = tabId;
  instrumentPage(page, tabId);
  page.on?.('close', () => {
    state.pages.delete(tabId);
    state.refs.delete(tabId);
    if (state.selectedTabId === tabId) {
      state.selectedTabId = [...state.pages.keys()][0] || null;
    }
  });
}

async function newTab(url, viewport) {
  await ensureContext(viewport);
  const tabId = newTabId();
  const page = await state.context.newPage();
  registerPage(tabId, page);
  if (url) {
    await page.goto(url, { waitUntil: 'load' });
    recordHistory(page.url(), await page.title().catch(() => null));
  }
  return { page, tabId };
}

async function closeTab(tabId) {
  const page = state.pages.get(tabId);
  if (page && !page.isClosed()) {
    await page.close().catch(() => {});
  }
  state.pages.delete(tabId);
  state.refs.delete(tabId);
  if (state.selectedTabId === tabId) {
    state.selectedTabId = [...state.pages.keys()][0] || null;
  }
}

let verifiedPlaywrightLoaderV1 = null;

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

function instrumentPage(page, tabId) {
  if (state.pageInstrumented.has(page)) {
    return;
  }
  state.pageInstrumented.add(page);
  page.on('console', (message) => {
    const location = message.location();
    pushCapped(state.console, {
      level: normalizeConsoleLevel(message.type()),
      text: message.text(),
      timestampMs: Date.now(),
      url: location?.url || null,
      line: location?.lineNumber || null,
      column: location?.columnNumber || null,
      tabId,
    });
  });
  page.on('request', (request) => {
    const requestId = `request-${++state.networkSeq}`;
    state.requestIds.set(request, requestId);
    pushCapped(state.network, {
      requestId,
      url: request.url(),
      method: request.method(),
      resourceType: request.resourceType(),
      requestHeaders: safeHeaders(() => request.headers()),
      startedAtMs: Date.now(),
      tabId,
    });
  });
  page.on('response', (response) => {
    const request = response.request();
    const entry = networkEntryFor(request);
    if (entry) {
      entry.status = response.status();
      entry.responseHeaders = safeHeaders(() => response.headers());
      entry.endedAtMs = Date.now();
    }
  });
  page.on('requestfailed', (request) => {
    const entry = networkEntryFor(request);
    if (entry) {
      entry.error = request.failure()?.errorText || 'request failed';
      entry.endedAtMs = Date.now();
    }
  });
  page.on?.('dialog', (dialog) => {
    state.dialogs.set(tabId, {
      dialog,
      type: dialog.type?.() || 'alert',
      message: dialog.message?.() || '',
      defaultValue: dialog.defaultValue?.() || '',
      openedAt: new Date().toISOString(),
    });
  });
}

function networkEntryFor(request) {
  const requestId = state.requestIds.get(request);
  if (!requestId) {
    return null;
  }
  return state.network.find((entry) => entry.requestId === requestId) || null;
}

function safeHeaders(read) {
  try {
    return read();
  } catch (_error) {
    return null;
  }
}

async function pageText(page) {
  return clip(await page.locator('body').innerText({ timeout: 5_000 }).catch(() => ''));
}

async function contentForPage(page, contentType, maxChars, selector = null) {
  const normalized = String(contentType || 'text').toLowerCase();
  if (normalized === 'html') {
    if (selector) {
      const html = await page.locator(selector).first().evaluate((el) => el.outerHTML).catch(() => '');
      return clip(html, maxChars);
    }
    return clip(await page.content(), maxChars);
  }
  const text = selector
    ? await page.locator(selector).first().innerText({ timeout: 5_000 }).catch(() => '')
    : await pageText(page);
  if (normalized === 'markdown' || normalized === 'md') {
    return clip(textToMarkdown(text), maxChars);
  }
  return clip(text, maxChars);
}

function textToMarkdown(text) {
  return String(text || '')
    .split('\n')
    .map((line) => line.trimEnd())
    .join('\n')
    .trim();
}

function recordHistory(url, title) {
  if (!url || url === 'about:blank') return;
  state.history.unshift({
    url,
    title: title || null,
    dateVisited: new Date().toISOString(),
  });
  if (state.history.length > 500) {
    state.history.splice(500);
  }
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

function dialogDescriptor(entry) {
  return {
    type: entry.type,
    message: entry.message,
    defaultValue: entry.defaultValue,
    openedAt: entry.openedAt,
  };
}

async function elementSnapshots(page, rootSelector) {
  return page.evaluate((rootSelectorArg) => {
    const root = rootSelectorArg ? document.querySelector(rootSelectorArg) : document;
    if (!root) {
      return [];
    }
    const escapeCss = (value) => {
      if (globalThis.CSS?.escape) {
        return globalThis.CSS.escape(value);
      }
      return String(value).replace(/[^a-zA-Z0-9_-]/g, '\\$&');
    };
    const selectorFor = (el) => {
      if (el.id) {
        return `#${escapeCss(el.id)}`;
      }
      const name = el.getAttribute('name');
      if (name) {
        return `${el.tagName.toLowerCase()}[name="${name.replaceAll('"', '\\"')}"]`;
      }
      const aria = el.getAttribute('aria-label');
      if (aria) {
        return `${el.tagName.toLowerCase()}[aria-label="${aria.replaceAll('"', '\\"')}"]`;
      }
      return el.tagName.toLowerCase();
    };
    return Array.from(root.querySelectorAll('a,button,input,textarea,select,[role],[aria-label]'))
      .slice(0, 100)
      .map((el) => {
        const rect = el.getBoundingClientRect();
        const style = getComputedStyle(el);
        const visible =
          rect.width > 0 &&
          rect.height > 0 &&
          style.visibility !== 'hidden' &&
          style.display !== 'none';
        return {
          selector: selectorFor(el),
          role: el.getAttribute('role') || null,
          name: el.getAttribute('aria-label') || el.getAttribute('name') || null,
          text: (el.innerText || el.value || el.textContent || '').trim().slice(0, 500) || null,
          x: rect.x,
          y: rect.y,
          width: rect.width,
          height: rect.height,
          visible,
          disabled: Boolean(el.disabled || el.getAttribute('aria-disabled') === 'true'),
        };
      });
  }, rootSelector);
}

function rememberRefs(tabId, elements) {
  const refs = new Map();
  const enriched = (elements || []).map((element, index) => {
    const ref = element.ref || `ref_${index + 1}`;
    const selector = element.selector || element.primarySelector || null;
    if (selector) {
      refs.set(ref, selector);
    }
    return {
      ref,
      ...element,
      interactive: element.interactive ?? true,
    };
  });
  state.refs.set(tabId, refs);
  return enriched;
}

function selectorForRef(tabId, refId) {
  if (!refId) return null;
  if (refId.startsWith('#') || refId.startsWith('.') || refId.includes('[') || refId.includes(' ')) {
    return refId;
  }
  return state.refs.get(tabId)?.get(refId) || null;
}

function renderElementTree(elements, depth, maxChars) {
  const lines = (elements || []).slice(0, 200).map((element) => {
    const label = [
      element.ref ? `[${element.ref}]` : '',
      element.role || element.tagName || 'element',
      element.name ? `"${element.name}"` : '',
      element.text ? `text="${clip(String(element.text), 120)}"` : '',
      element.selector ? `selector=${element.selector}` : '',
    ]
      .filter(Boolean)
      .join(' ');
    return label;
  });
  return clip(lines.slice(0, depth ? Math.max(depth * 20, 20) : lines.length).join('\n'), maxChars);
}

function scoreElement(element, query) {
  const haystack = [
    element.ref,
    element.selector,
    element.role,
    element.name,
    element.text,
    element.tagName,
  ]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();
  if (!haystack) return 0;
  if (haystack === query) return 100;
  if (haystack.includes(query)) return 50 + Math.min(query.length, 30);
  return query
    .split(/\s+/)
    .filter((part) => part && haystack.includes(part))
    .reduce((sum) => sum + 5, 0);
}

async function setElementValue(locator, value, timeout) {
  const stringValue = value == null ? '' : String(value);
  const kind = await locator.evaluate((el) => {
    const tag = el.tagName.toLowerCase();
    return {
      tag,
      type: el.getAttribute('type') || '',
      contentEditable: el.isContentEditable,
    };
  });
  if (kind.tag === 'input' && ['checkbox', 'radio'].includes(kind.type)) {
    if (Boolean(value)) {
      await locator.check({ timeout });
    } else {
      await locator.uncheck({ timeout });
    }
    return;
  }
  if (kind.tag === 'select') {
    await locator.selectOption(stringValue, { timeout });
    return;
  }
  if (kind.contentEditable) {
    await locator.fill(stringValue, { timeout }).catch(async () => {
      await locator.click({ timeout });
      await locator.press(process.platform === 'darwin' ? 'Meta+A' : 'Control+A');
      await locator.type(stringValue, { timeout });
    });
    return;
  }
  await locator.fill(stringValue, { timeout });
}

function requirePoint(point, action) {
  if (!Array.isArray(point) || point.length < 2 || !Number.isFinite(point[0]) || !Number.isFinite(point[1])) {
    throw new BrowserSidecarError('invalid_request', `${action} requires coordinate [x, y].`);
  }
  return [point[0], point[1]];
}

function zoomClip(point) {
  const [x, y] = requirePoint(point, 'zoom');
  const size = 240;
  return {
    x: Math.max(0, x - size / 2),
    y: Math.max(0, y - size / 2),
    width: size,
    height: size,
  };
}

const screenshotCaptures = new WeakMap();

async function screenshotArtifact(page, request, op = {}) {
  const save = op.save !== false;
  if (save) await fsp.mkdir(artifactDir, { recursive: true });
  const format = op.format || 'png';
  const fileName = `screenshot-${Date.now()}-${Math.random().toString(16).slice(2)}.${format === 'jpeg' ? 'jpg' : 'png'}`;
  const filePath = path.join(artifactDir, fileName);
  const options = {
    ...(save ? { path: filePath } : {}),
    fullPage: !!op.fullPage,
    type: format,
    timeout: timeoutMs(request),
  };
  if (op.clip) {
    options.clip = op.clip;
  }
  if (format === 'jpeg' && Number.isInteger(op.quality)) {
    options.quality = op.quality;
  }
  const bytes = op.selector ? await page.locator(op.selector).first().screenshot(options) : await page.screenshot(options);
  const mimeType = format === 'jpeg' ? 'image/jpeg' : 'image/png';
  const artifact = {
    id: path.basename(filePath),
    kind: 'screenshot',
    ...(save ? { uri: pathToFileURL(filePath).href, path: filePath } : {}),
    mimeType,
    sizeBytes: bytes.length,
    metadata: { fullPage: !!op.fullPage, clip: op.clip || null },
  };
  // Keep image bytes outside the exact artifact codec and its lightweight events.
  screenshotCaptures.set(artifact, { dataUrl: `data:${mimeType};base64,${Buffer.from(bytes).toString('base64')}`, mimeType, saved: save, ...(save ? { path: filePath } : {}) });
  return artifact;
}

async function validateUploadPaths(paths) {
  if (!Array.isArray(paths) || paths.length === 0) {
    throw new BrowserSidecarError('invalid_request', 'file_upload requires at least one path.');
  }
  const maxBytes = Number.parseInt(process.env.KLAUS_BROWSER_MAX_UPLOAD_BYTES || String(10 * 1024 * 1024), 10);
  let total = 0;
  const resolved = [];
  for (const item of paths) {
    const filePath = path.resolve(String(item));
    const stat = await fsp.stat(filePath).catch((error) => {
      throw new BrowserSidecarError('not_found', `Upload file not found: ${filePath}`, { code: error?.code || null });
    });
    if (!stat.isFile()) {
      throw new BrowserSidecarError('invalid_request', `Upload path is not a file: ${filePath}`);
    }
    total += stat.size;
    if (Number.isFinite(maxBytes) && maxBytes > 0 && total > maxBytes) {
      throw new BrowserSidecarError(
        'permission_denied',
        `Upload files exceed the configured ${maxBytes} byte limit.`,
        { maxBytes, totalBytes: total },
      );
    }
    resolved.push(filePath);
  }
  return resolved;
}

async function encodeGif(framePaths, filePath, fps) {
  if (process.env.KLAUS_BROWSER_GIF_ENCODER !== 'none') {
    const encoded = await encodeGifWithFfmpeg(framePaths, filePath, fps).catch((error) => ({
      encoded: false,
      encoder: 'minimal-gif-fallback',
      error: error?.message || String(error),
    }));
    if (encoded.encoded) {
      return encoded;
    }
  }
  await fsp.writeFile(filePath, minimalGifBuffer());
  return { encoded: false, encoder: 'minimal-gif-fallback' };
}

async function encodeGifWithFfmpeg(framePaths, filePath, fps) {
  if (framePaths.length === 0) {
    throw new Error('no frames to encode');
  }
  const listPath = path.join(path.dirname(filePath), `gif-input-${Date.now()}-${Math.random().toString(16).slice(2)}.txt`);
  const normalizedFps = Math.max(1, fps);
  const duration = 1 / normalizedFps;
  const lines = [];
  for (const framePath of framePaths) {
    lines.push(`file '${framePath.replaceAll("'", "'\\''")}'`);
    lines.push(`duration ${duration}`);
  }
  lines.push(`file '${framePaths[framePaths.length - 1].replaceAll("'", "'\\''")}'`);
  await fsp.writeFile(listPath, `${lines.join('\n')}\n`);
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
      else reject(new Error(`ffmpeg failed with ${signal || code}: ${stderr.trim()}`));
    });
  });
}

function minimalGifBuffer() {
  return Buffer.from('R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==', 'base64');
}

function gifFileName(filename) {
  const raw = filename ? path.basename(String(filename)) : `recording-${Date.now()}.gif`;
  const cleaned = raw.replace(/[^a-zA-Z0-9._-]/g, '_') || `recording-${Date.now()}.gif`;
  return cleaned.toLowerCase().endsWith('.gif') ? cleaned : `${cleaned}.gif`;
}

function downloadFileName(filename) {
  const raw = filename ? path.basename(String(filename)) : `download-${Date.now()}.bin`;
  return raw.replace(/[^a-zA-Z0-9._-]/g, '_') || `download-${Date.now()}.bin`;
}

async function downloadUrlToArtifact(url, filename, metadata = {}) {
  await fsp.mkdir(artifactDir, { recursive: true });
  const parsed = new URL(url);
  const fromUrl = path.basename(decodeURIComponent(parsed.pathname || '')) || 'download.bin';
  const filePath = path.join(artifactDir, downloadFileName(filename || fromUrl));
  const response = await fetch(url);
  if (!response.ok) {
    throw new BrowserSidecarError('host_error', `download failed with HTTP ${response.status}: ${url}`);
  }
  const bytes = Buffer.from(await response.arrayBuffer());
  await fsp.writeFile(filePath, bytes);
  const artifact = await artifactForDownloadedFile(filePath, {
    ...metadata,
    url,
    status: response.status,
  });
  artifact.mimeType = response.headers.get('content-type') || artifact.mimeType;
  return artifact;
}

async function artifactForDownloadedFile(filePath, metadata = {}) {
  const stat = await fsp.stat(filePath);
  return {
    id: path.basename(filePath),
    kind: 'download',
    uri: pathToFileURL(filePath).href,
    path: filePath,
    mimeType: mimeTypeForPath(filePath),
    sizeBytes: stat.size,
    metadata,
  };
}

function mimeTypeForPath(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === '.html' || ext === '.htm') return 'text/html';
  if (ext === '.txt' || ext === '.md') return 'text/plain';
  if (ext === '.png') return 'image/png';
  if (ext === '.jpg' || ext === '.jpeg') return 'image/jpeg';
  if (ext === '.gif') return 'image/gif';
  if (ext === '.pdf') return 'application/pdf';
  return 'application/octet-stream';
}

function normalizeShortcuts(value) {
  const list = Array.isArray(value) ? value : value && typeof value === 'object' ? Object.values(value) : [];
  return list
    .filter((item) => item && typeof item === 'object')
    .map((item, index) => ({
      id: String(item.id || item.command || item.name || `shortcut-${index + 1}`),
      command: item.command || item.name || item.id || `shortcut-${index + 1}`,
      description: item.description || null,
      isWorkflow: item.isWorkflow ?? item.workflow ?? false,
      shortcut: item.shortcut || null,
    }));
}

function clampNumber(value, min, max) {
  const number = Number(value);
  if (!Number.isFinite(number)) return min;
  return Math.max(min, Math.min(max, number));
}

function tabDescriptor(tabId, page) {
  return {
    id: tabId,
    type: 'page',
    url: page.url?.() || null,
    selected: state.selectedTabId === tabId,
  };
}

function tabList() {
  return [...state.pages.entries()].map(([tabId, page]) => tabDescriptor(tabId, page));
}

function normalizeOptions(options, request) {
  const out = { ...options };
  if (out.timeoutMs && !out.timeout) {
    out.timeout = out.timeoutMs;
  }
  if (!out.timeout) {
    out.timeout = timeoutMs(request);
  }
  delete out.timeoutMs;
  return out;
}

function baseResponse(values) {
  return {
    sessionId: state.sessionId,
    tabId: state.selectedTabId,
    ...values,
  };
}

function mapWaitUntil(waitUntil) {
  switch (waitUntil) {
    case 'commit':
      return 'commit';
    case 'dom_content_loaded':
      return 'domcontentloaded';
    case 'network_idle':
      return 'networkidle';
    case 'load':
    case undefined:
    case null:
      return 'load';
    default:
      return 'load';
  }
}

function timeoutMs(request) {
  return request?.timeoutMs || 30_000;
}

function toViewportSize(viewport) {
  return { width: viewport.width, height: viewport.height };
}

function normalizeConsoleLevel(level) {
  switch (level) {
    case 'warning':
    case 'warn':
      return 'warning';
    case 'error':
      return 'error';
    case 'debug':
      return 'debug';
    case 'info':
      return 'info';
    default:
      return 'log';
  }
}

function filterBySince(entries, sinceMs) {
  if (!Number.isFinite(sinceMs)) {
    return [...entries];
  }
  const cutoff = Date.now() - sinceMs;
  return entries.filter((entry) => !entry.timestampMs || entry.timestampMs >= cutoff || !entry.startedAtMs || entry.startedAtMs >= cutoff);
}

function takeLimit(entries, limit) {
  if (!Number.isFinite(limit) || limit <= 0) {
    return entries;
  }
  return entries.slice(-limit);
}

function pushCapped(list, entry) {
  list.push(entry);
  if (list.length > 500) {
    list.splice(0, list.length - 500);
  }
}

function clip(text, max = 8_000) {
  if (!text || text.length <= max) {
    return text;
  }
  return `${text.slice(0, max)}\n[truncated]`;
}

function codeForOperation(operation) {
  if (!operation) {
    return 'host_error';
  }
  switch (operation.type) {
    case 'navigate':
      return 'navigation_failed';
    case 'eval':
      return 'evaluation_failed';
    case 'click':
    case 'type':
    case 'snapshot':
    case 'screenshot':
    case 'upload_image':
    case 'file_upload':
    case 'download':
    case 'shortcuts_execute':
    case 'select_browser':
    case 'playwright_wait_for_event':
    case 'playwright_file_chooser_set_files':
    case 'playwright_wait_for_url':
    case 'playwright_dom_snapshot':
    case 'playwright_element_info':
    case 'playwright_element_screenshot':
    case 'read_page':
    case 'find':
    case 'form_input':
    case 'locator':
      return 'not_found';
    case 'gif_creator':
    case 'shortcuts_list':
    case 'list_connected_browsers':
    case 'switch_browser':
    case 'playwright_wait_for_load_state':
      return 'unavailable';
    case 'tabs':
    case 'computer':
    case 'page_text':
    case 'batch':
    case 'resize':
    case 'close':
      return 'invalid_request';
    default:
      return 'host_error';
  }
}

function normalizeError(error, fallbackCode, fallbackMessage) {
  if (error instanceof BrowserSidecarError) {
    return {
      code: error.code,
      message: error.message,
      details: error.details || null,
    };
  }
  const code = /playwright|Cannot find package|Cannot find module/.test(error?.message || '')
    ? 'unavailable'
    : fallbackCode;
  return {
    code,
    message: fallbackMessage || error?.message || 'Browser sidecar error.',
    details: error?.stack ? { stack: error.stack } : null,
  };
}

function sendEvent(id, event) {
  write({ id, event });
}

function write(value) {
  const encoded = exactBrowserRuntimeJsonCodecV1.encode(value);
  const framed = new Uint8Array(encoded.length + 1);
  for (let index = 0; index < encoded.length; index += 1) framed[index] = encoded[index];
  framed[encoded.length] = 10;
  process.stdout.write(framed);
}

async function shutdown() {
  await state.extensionBridge?.close?.().catch(() => {});
  for (const page of state.pages.values()) {
    await page?.close?.().catch(() => {});
  }
  await state.context?.close?.().catch(() => {});
  await state.browser?.close?.().catch(() => {});
}

class BrowserSidecarError extends Error {
  constructor(code, message, details = null) {
    super(message);
    this.name = 'BrowserSidecarError';
    this.code = code;
    this.details = details;
  }
}

if (!fs.existsSync(artifactDir)) {
  await fsp.mkdir(artifactDir, { recursive: true }).catch(() => {});
}
