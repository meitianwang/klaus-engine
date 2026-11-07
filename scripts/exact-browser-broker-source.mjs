export function exactBrowserBrokerSourceV1() {
  return String.raw`const KLAUS_BROWSER_BROKER_SYNC_MAX_V1 = 16 * 1024 * 1024;
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
    if (this.creditOutstanding || this.destroyed) return;
    this.creditOutstanding = true;
    this.owner.broker.sendMux(this.owner.objectId, this.streamId, BrowserMuxKindV1.ReadCredit, {
      sequenceClass: 'credit', correlation: this.lastDataSequence, credit: 1, payload: Buffer.alloc(0),
    });
  }
  _write(chunk, _encoding, callback) {
    const sequence = this.owner.broker.sendMux(
      this.owner.objectId, this.streamId, BrowserMuxKindV1.Write,
      { sequenceClass: 'write', correlation: 0n, credit: 0, payload: Buffer.from(chunk) },
    );
    this.writeCallbacks.set(sequence, callback);
  }
  _final(callback) {
    this.finalCallback = callback;
    this.owner.broker.sendMux(this.owner.objectId, this.streamId, BrowserMuxKindV1.HalfClose, {
      sequenceClass: 'control', correlation: 0n, credit: 0, payload: Buffer.alloc(0),
    });
  }
  _destroy(error, callback) {
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
    return exactBrowserRuntimeJsonCodecV1.decode(response.subarray(20));
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
      const value = exactBrowserRuntimeJsonCodecV1.decode(payload);
      if (!exactOwnKeysV1(value, ['code', 'signal'])) throw browserBrokerFailureV1('Invalid browser exit frame.');
      proxy.exitSequence = sequence;
      proxy.events.emit('exit', value.code, value.signal);
    } else if (kind === BrowserMuxKindV1.Close && streamId === 0 && payload.length === 0) {
      proxy.events.emit('close', null, null);
    } else if (kind === BrowserMuxKindV1.CleanupComplete && streamId === 0) {
      const value = exactBrowserRuntimeJsonCodecV1.decode(payload);
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
`
}
