import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { createHash } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import { describe, it } from 'node:test'
import { fileURLToPath, pathToFileURL } from 'node:url'

import {
  assertExactBrowserCandidateState,
  CANDIDATE_LOCK,
  CANDIDATE_ROOT,
  CANDIDATE_TREE,
} from './exact-browser-candidate.mjs'

const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const TREE = join(REPO_ROOT, CANDIDATE_ROOT, CANDIDATE_TREE)
const CLIENT = join(TREE, 'scripts', 'browser-client.mjs')
const SIDECAR = join(TREE, 'runtime', 'playwright-sidecar.mjs')
const UTILITY_SHIMS = [
  'check-extension-installed',
  'check-native-host-manifest',
  'install-native-host-manifest',
  'chrome-is-running',
  'installed-browsers',
  'open-chrome-window',
]

function runNode(args, options = {}) {
  return spawnSync(process.execPath, args, {
    cwd: REPO_ROOT,
    encoding: 'utf8',
    ...options,
    env: { ...process.env, ...options.env },
  })
}

function runClientVm(scenario) {
  const script = `
    import vm from 'node:vm';
    import { readFileSync } from 'node:fs';
    import { types } from 'node:util';

    const context = vm.createContext({});
    const RealmUint8Array = vm.runInContext('Uint8Array', context);
    const source = readFileSync(${JSON.stringify(CLIENT)}, 'utf8');
    const brands = new WeakSet();
    let acquireFactory = null;
    const brand = (transport) => { brands.add(transport); return transport; };
    const required = () => Object.assign(
      new Error('Rust browser host transport is required'),
      { code: 'host_error' },
    );
    const synthetic = new vm.SyntheticModule([
      'acquireBrowserHostTransportV1',
      'assertBrowserHostTransportV1',
      'isBrowserRuntimeProxyV1',
      'browserHostDefaults',
      'browserDocumentationText',
    ], function initialize() {
      this.setExport('acquireBrowserHostTransportV1', () => {
        if (!acquireFactory) throw required();
        return acquireFactory();
      });
      this.setExport('assertBrowserHostTransportV1', (value) => {
        if (
          !brands.has(value) ||
          value.version !== 1 ||
          !Object.hasOwn(value, 'open') || typeof value.open !== 'function' ||
          !Object.hasOwn(value, 'send') || typeof value.send !== 'function' ||
          !Object.hasOwn(value, 'close') || typeof value.close !== 'function'
        ) throw required();
      });
      this.setExport('isBrowserRuntimeProxyV1', types.isProxy);
      this.setExport('browserHostDefaults', Object.freeze({ backend: 'extension' }));
      this.setExport(
        'browserDocumentationText',
        '# Klaus Bundled Browser Runtime\\n\\nLOCKED DOCUMENTATION',
      );
    }, { context });
    const client = new vm.SourceTextModule(source, { context });
    await client.link((specifier) => {
      if (specifier !== 'klaus:browser-host-transport') throw new Error('denied import ' + specifier);
      return synthetic;
    });
    await synthetic.evaluate();
    await client.evaluate();
    const realmBytes = (text) => new RealmUint8Array(Buffer.from(text, 'utf8'));
    const tick = () => new Promise((resolve) => setImmediate(resolve));
    ${scenario}
  `
  const result = runNode(
    ['--experimental-vm-modules', '--input-type=module', '-e', script],
    { env: { NODE_NO_WARNINGS: '1' } },
  )
  assert.equal(result.status, 0, result.stderr)
  assert.equal(result.stderr, '')
  return JSON.parse(result.stdout)
}

function inspectModule(path) {
  const script = `
    const module = await import(${JSON.stringify(pathToFileURL(path).href)});
    process.stdout.write(JSON.stringify({
      exports: Object.keys(module),
      same: module.default === module.migration,
      frozen: Object.isFrozen(module.migration),
      keys: Object.keys(module.migration),
      values: module.migration,
    }));
  `
  const result = runNode(['--input-type=module', '-e', script])
  assert.equal(result.status, 0, result.stderr)
  assert.equal(result.stderr, '')
  return JSON.parse(result.stdout)
}

function extractExactRuntimeJsonCodec(source) {
  const codec = source.match(
    /KLAUS_EXACT_BROWSER_RUNTIME_JSON_CODEC_V1_BEGIN ([0-9a-f]{64}) \*\/\n([\s\S]*?)\n\/\* KLAUS_EXACT_BROWSER_RUNTIME_JSON_CODEC_V1_END ([0-9a-f]{64})/,
  )
  assert.ok(codec)
  assert.equal(codec[1], codec[3])
  assert.equal(createHash('sha256').update(codec[2]).digest('hex'), codec[1])
  assert.doesNotMatch(codec[2], /JSON\.(?:parse|stringify)|TextEncoder|TextDecoder/)
  return codec
}

describe('exact browser migration candidate', () => {
  it('locks a complete deterministic 24-file candidate tree', () => {
    const state = assertExactBrowserCandidateState({ repoRoot: REPO_ROOT })
    assert.equal(state.lock.files.length, 24)
    assert.equal(state.lock.root, CANDIDATE_TREE)
    assert.match(state.lock.sourceTreeSha256, /^[0-9a-f]{64}$/)
    assert.match(state.lock.pathSetSha256, /^[0-9a-f]{64}$/)
    assert.match(state.lock.treeSha256, /^[0-9a-f]{64}$/)
    assert.equal(
      readFileSync(join(REPO_ROOT, CANDIDATE_ROOT, CANDIDATE_LOCK), 'utf8'),
      `${JSON.stringify(state.lock)}\n`,
    )
  })

  it('removes ambient Node authority from the browser client module', () => {
    const source = readFileSync(CLIENT, 'utf8')
    assert.match(source, /from 'klaus:browser-host-transport';/)
    assert.equal((source.match(/^import\b/gm) || []).length, 1)
    assert.doesNotMatch(source, /node:|\bprocess\b|defaultSidecarPath|connectNative/)
    assert.match(source, /KLAUS_EXACT_BROWSER_RUNTIME_JSON_CODEC_V1_BEGIN [0-9a-f]{64}/)
    assert.match(source, /KLAUS_EXACT_BROWSER_RUNTIME_JSON_CODEC_V1_END [0-9a-f]{64}/)
    const clientCodec = extractExactRuntimeJsonCodec(source)
    const sidecarCodec = extractExactRuntimeJsonCodec(readFileSync(SIDECAR, 'utf8'))
    assert.equal(sidecarCodec[1], clientCodec[1])
    assert.equal(sidecarCodec[2], clientCodec[2])
  })

  it('fails standalone sidecar execution closed before the application line boundary', () => {
    const executed = runNode([SIDECAR], {
      input: [
        '{"id":"request-1","request":{}}',
        '{"id":"request-2","request":{ }}',
        '{"id":"history-1","request":{"operation":{"type":"history","queries":[]},"target":{}}}',
        '',
      ].join('\n'),
    })
    assert.equal(executed.status, 70)
    assert.equal(executed.stdout, '')
    assert.equal(
      executed.stderr,
      process.platform === 'win32'
        ? 'Browser broker native pipe binding failed.\n'
        : 'Browser broker package readiness failed.\n',
    )
  })

  it('removes host Playwright resolution and exposes only the verified loader seam', () => {
    const source = readFileSync(SIDECAR, 'utf8')
    assert.match(source, /function installVerifiedPlaywrightLoaderV1\(loader\)/)
    assert.match(source, /Pinned Playwright 1\.61\.1 asset is missing or corrupt/)
    assert.doesNotMatch(
      source,
      /KLAUS_BROWSER_PLAYWRIGHT_MODULE|requireFromCwd|import\('playwright'\)|npm install playwright/,
    )
  })

  it('uses only the sealed four-lane broker and Rust-owned FFmpeg policy', () => {
    const source = readFileSync(SIDECAR, 'utf8')
    assert.doesNotMatch(source, /node:child_process|function runProcess\(|\bspawn\(/)
    assert.match(source, /KLAUS_BROWSER_BROKER_ENV_ARG_V1 = '--klaus-browser-broker-env-key='/)
    assert.match(source, /readExactFdV1\(6, 48\)/)
    assert.match(source, /writeAllFdV1\(3, bytes\)/)
    assert.match(source, /readExactFdV1\(4, length\)/)
    assert.match(source, /writeAllFdV1\(5, unixPackageReadyV1\(permit\)\)/)
    assert.match(source, /BrowserFfmpegEncode: 7/)
    assert.match(source, /BrowserFfmpegEncode\(\{ listPath, filePath, fps \}\)/)
    assert.doesNotMatch(source, /KLAUS_BROWSER_FFMPEG/)
    assert.match(source, /new URL\('\.\.\/\.\.\/playwright-runtime-v1\/'/)
    assert.match(source, /registerCloseHooks\(proxy, graceful, force\)/)
    assert.match(source, /sourceOptions: projectRuntimeJsonV1/)
    assert.match(source, /tempDirectories: projectRuntimeJsonV1/)
    assert.match(source, /plannerTrace: projectRuntimeJsonV1/)
  })

  it('matches ECMAScript number bytes and rejects noncanonical decode forms', () => {
    const script = `
      import vm from 'node:vm';
      import { readFileSync } from 'node:fs';
      import { types } from 'node:util';
      const source = readFileSync(${JSON.stringify(CLIENT)}, 'utf8');
      const match = source.match(
        /KLAUS_EXACT_BROWSER_RUNTIME_JSON_CODEC_V1_BEGIN [0-9a-f]{64} \\*\\/\\n([\\s\\S]*?)\\n\\/\\* KLAUS_EXACT_BROWSER_RUNTIME_JSON_CODEC_V1_END/,
      );
      if (!match) throw new Error('codec block missing');
      const context = vm.createContext({ isProxy: types.isProxy });
      const factory = vm.runInContext('(' + match[1] + ')', context);
      const codec = factory(context.isProxy);
      const bytes = Buffer.allocUnsafe(8);
      let state = 0x9e3779b97f4a7c15n;
      const mask = (1n << 64n) - 1n;
      const numbers = [0, -0, NaN, Infinity, -Infinity, 1e-7, 1e-6, 1e20, 1e21];
      for (let index = 0; index < 256; index += 1) {
        state ^= state << 13n;
        state ^= state >> 7n;
        state ^= state << 17n;
        state &= mask;
        bytes.writeBigUInt64BE(state);
        numbers.push(bytes.readDoubleBE());
      }
      for (const number of numbers) {
        const actual = Buffer.from(codec.encode(number)).toString('utf8');
        const expected = JSON.stringify(number);
        if (actual !== expected) {
          throw new Error('number drift ' + number + ': ' + actual + ' !== ' + expected);
        }
      }
      context.codec = codec;
      const objectBytes = Buffer.from(vm.runInContext(` + JSON.stringify(`(() => {
        const value = {};
        value.b = "\ud800";
        value["2"] = "two";
        value["1"] = "one";
        value.control = String.fromCharCode(8, 12, 10, 13, 9, 0);
        return codec.encode(value);
      })()`) + `, context)).toString('utf8');
      const RealmUint8Array = vm.runInContext('Uint8Array', context);
      const invalid = [
        ' null',
        '{"x":1,"x":1}',
        '-0',
        '1e0',
        '"\\\\u0061"',
        '﻿null',
      ];
      const errors = [];
      for (const text of invalid) {
        try { codec.decode(new RealmUint8Array(Buffer.from(text, 'utf8'))); }
        catch (error) { errors.push([error.code, error.message]); }
      }
      for (const malformed of [[0xc0, 0xaf], [0xed, 0xa0, 0x80], [0xf4, 0x90, 0x80, 0x80]]) {
        try { codec.decode(new RealmUint8Array(malformed)); }
        catch (error) { errors.push([error.code, error.message]); }
      }
      process.stdout.write(JSON.stringify({ count: numbers.length, objectBytes, errors }));
    `
    const executed = runNode(
      ['--input-type=module', '-e', script],
      { env: { NODE_NO_WARNINGS: '1' } },
    )
    assert.equal(executed.status, 0, executed.stderr)
    const result = JSON.parse(executed.stdout)
    assert.equal(result.count, 265)
    assert.equal(
      result.objectBytes,
      '{"1":"one","2":"two","b":"\\ud800","control":"\\b\\f\\n\\r\\t\\u0000"}',
    )
    assert.equal(result.errors.length, 9)
    assert.ok(result.errors.every(([code, message]) =>
      code === 'host_error' && message === 'Invalid canonical browser RuntimeJson bytes.'))
  })

  it('requires a branded transport before returning or mutating globals', () => {
    const result = runClientVm(`
      const forged = { version: 1, open() {}, send() {}, close() {} };
      let directError;
      try {
        client.namespace.createBrowserRegistry({ transport: forged });
      } catch (error) {
        directError = { code: error.code, message: error.message };
      }
      const globals = { sentinel: true };
      let setupError;
      try {
        await client.namespace.setupBrowserRuntime({ globals, options: { transport: forged } });
      } catch (error) {
        setupError = { code: error.code, message: error.message };
      }
      let forbiddenError;
      try {
        client.namespace.createBrowserRegistry({ transport: forged, env: undefined });
      } catch (error) {
        forbiddenError = { code: error.code, message: error.message };
      }
      let capabilityFieldError;
      const widened = brand({
        version: 1,
        open() {},
        send() {},
        close() {},
        cwd: undefined,
      });
      try {
        client.namespace.createBrowserRegistry({ transport: widened });
      } catch (error) {
        capabilityFieldError = { code: error.code, message: error.message };
      }
      let proxyTrapCount = 0;
      const proxyOptions = new Proxy({}, {
        ownKeys() { proxyTrapCount += 1; return []; },
        getOwnPropertyDescriptor() { proxyTrapCount += 1; return undefined; },
        get() { proxyTrapCount += 1; return undefined; },
      });
      let proxyOptionsError;
      try {
        client.namespace.createBrowserRegistry(proxyOptions);
      } catch (error) {
        proxyOptionsError = { code: error.code, message: error.message };
      }
      let proxyCapabilityTrapCount = 0;
      const proxyCapability = brand(new Proxy({
        version: 1,
        open() {},
        send() {},
        close() {},
      }, {
        get() { proxyCapabilityTrapCount += 1; throw new Error('trap'); },
        getOwnPropertyDescriptor() { proxyCapabilityTrapCount += 1; throw new Error('trap'); },
        ownKeys() { proxyCapabilityTrapCount += 1; throw new Error('trap'); },
      }));
      let proxyCapabilityError;
      try {
        client.namespace.createBrowserRegistry({ transport: proxyCapability });
      } catch (error) {
        proxyCapabilityError = { code: error.code, message: error.message };
      }

      const records = [];
      let callbacks;
      const capability = brand({
        version: 1,
        open(configuration, next) {
          records.push({ type: 'open', configuration, frozen: Object.isFrozen(configuration) });
          callbacks = next;
        },
        send(bytes, done) {
          const request = JSON.parse(Buffer.from(bytes).toString('utf8'));
          records.push({ type: 'send', request });
          setImmediate(() => {
            done();
            callbacks.message(realmBytes(
              '{"id":"' + request.id + '","ok":true,"response":{"value":[]}}',
            ));
          });
        },
        close() {},
      });
      const registry = client.namespace.createBrowserRegistry({
        transport: capability,
        backend: '',
        headless: false,
        artifactDir: '/artifacts',
        cdpEndpoint: 'ws://cdp',
        userDataDir: '/profile',
        channel: 'chrome',
        executablePath: '/chrome',
        browserType: 'client-only',
      });
      const browser = await registry.getDefault();
      await browser.tabs.list();
      await browser.user.history();
      await browser.tabs.content();
      const listed = await registry.list();
      process.stdout.write(JSON.stringify({
        directError,
        setupError,
        forbiddenError,
        capabilityFieldError,
        proxyOptionsError,
        proxyTrapCount,
        proxyCapabilityError,
        proxyCapabilityTrapCount,
        globals,
        records,
        listed,
      }));
    `)
    const required = {
      code: 'host_error',
      message: 'Rust browser host transport is required',
    }
    assert.deepEqual(result.directError, required)
    assert.deepEqual(result.setupError, required)
    assert.deepEqual(result.forbiddenError, required)
    assert.deepEqual(result.capabilityFieldError, required)
    assert.deepEqual(result.proxyOptionsError, required)
    assert.equal(result.proxyTrapCount, 0)
    assert.deepEqual(result.proxyCapabilityError, required)
    assert.equal(result.proxyCapabilityTrapCount, 0)
    assert.deepEqual(result.globals, { sentinel: true })
    assert.deepEqual(result.records[0], {
      type: 'open',
      configuration: {
        artifactDir: '/artifacts',
        headless: false,
        backend: 'extension',
        cdpEndpoint: 'ws://cdp',
        userDataDir: '/profile',
        channel: 'chrome',
        executablePath: '/chrome',
      },
      frozen: true,
    })
    assert.equal(result.records[1].request.id, 'browser-client-1')
    assert.deepEqual(result.records[2].request.request.operation, {
      type: 'history',
      queries: [],
    })
    assert.deepEqual(result.records[3].request.request.operation, {
      type: 'tabs_content',
      urls: [],
      contentType: 'text',
    })
    assert.equal(result.listed[0].type, 'client-only')
    assert.equal(result.listed[0].metadata.backend, 'extension')
  })

  it('uses canonical exact RuntimeJson bytes and ignores malformed host frames', () => {
    const result = runClientVm(`
      const rawRequests = [];
      let callbacks;
      const capability = brand({
        version: 1,
        open(_configuration, next) { callbacks = next; },
        send(bytes, done) {
          const raw = Buffer.from(bytes).toString('utf8');
          rawRequests.push(raw);
          const request = JSON.parse(raw);
          setImmediate(() => {
            done();
            callbacks.message(realmBytes('{ "id":"' + request.id + '","ok":true}'));
            callbacks.message(new RealmUint8Array([0xc0, 0xaf]));
            callbacks.message(realmBytes('﻿{"id":"' + request.id + '","ok":true}'));
            callbacks.message(realmBytes(
              '{"id":"' + request.id + '","id":"' + request.id + '","ok":true}',
            ));
            callbacks.message(realmBytes(
              '{"id":"' + request.id + '","ok":true,"response":{"value":1e0}}',
            ));
            callbacks.message(realmBytes('{"id":"unmatched","event":{"type":"ignored"}}'));
            callbacks.message(realmBytes('{"id":"' + request.id + '","event":{"type":"progress"}}'));
            callbacks.message(realmBytes(
              '{"id":"' + request.id + '","ok":true,"response":{"value":{"__proto__":{"polluted":true},"constructor":"inert"}}}',
            ));
          });
        },
        close() {},
      });
      const registry = client.namespace.createBrowserRegistry({ transport: capability });
      const browser = await registry.getDefault();
      const options = vm.runInContext(` + JSON.stringify(`(() => {
        const metadata = {};
        metadata.b = "\ud800";
        metadata["2"] = Infinity;
        metadata["1"] = NaN;
        metadata.z = -0;
        return {
          urls: [],
          contentType: "text",
          maxChars: 1,
          waitUntil: "load",
          metadata,
        };
      })()`) + `, context);
      const value = await browser.tabs.content(options);
      const realmObjectPrototype = vm.runInContext('Object.prototype', context);

      const proxyOptions = vm.runInContext(` + JSON.stringify(`({
        urls: [],
        contentType: "text",
        maxChars: 1,
        waitUntil: "load",
        metadata: new Proxy({}, {}),
      })`) + `, context);
      let proxyError;
      try {
        await browser.tabs.content(proxyOptions);
      } catch (error) {
        proxyError = { code: error.code, message: error.message };
      }
      const validOptions = vm.runInContext(` + JSON.stringify(`({
        urls: [],
        contentType: "text",
        maxChars: 1,
        waitUntil: "load",
        metadata: {},
      })`) + `, context);
      await browser.tabs.content(validOptions);

      process.stdout.write(JSON.stringify({
        rawRequests,
        proxyError,
        value: {
          protoExact: Object.getPrototypeOf(value) === realmObjectPrototype,
          hasProto: Object.hasOwn(value, '__proto__'),
          protoValue: value.__proto__,
          constructorValue: value.constructor,
          polluted: ({}).polluted || null,
        },
      }));
    `)
    assert.equal(result.rawRequests.length, 2)
    assert.match(
      result.rawRequests[0],
      /"metadata":\{"1":null,"2":null,"b":"\\ud800","z":0\}/,
    )
    assert.match(result.rawRequests[0], /"id":"browser-client-1"/)
    assert.match(result.rawRequests[1], /"id":"browser-client-3"/)
    assert.deepEqual(result.proxyError, {
      code: 'host_error',
      message: 'Browser request contains a value outside exact RuntimeJson.',
    })
    assert.deepEqual(result.value, {
      protoExact: true,
      hasProto: true,
      protoValue: { polluted: true },
      constructorValue: 'inert',
      polluted: null,
    })
  })

  it('rejects every unsupported RuntimeJson image before send', () => {
    const result = runClientVm(`
      let callbacks;
      const sends = [];
      const capability = brand({
        version: 1,
        open(_configuration, next) { callbacks = next; },
        send(bytes, done) {
          const request = JSON.parse(Buffer.from(bytes).toString('utf8'));
          sends.push(request.id);
          setImmediate(() => {
            done();
            callbacks.message(realmBytes(
              '{"id":"' + request.id + '","ok":true,"response":{"value":{"title":"ok"}}}',
            ));
          });
        },
        close() {},
      });
      const registry = client.namespace.createBrowserRegistry({ transport: capability });
      const browser = await registry.getDefault();
      const tab = await browser.tabs.getDefault();
      const expressions = [
        '({ metadata: { bad: undefined } })',
        '({ metadata: { bad: [, 1] } })',
        ` + JSON.stringify(`(() => {
          const metadata = {};
          Object.defineProperty(metadata, "bad", { enumerable: true, get() { return 1; } });
          return { metadata };
        })()`) + `,
        ` + JSON.stringify(`(() => {
          const metadata = {};
          metadata.self = metadata;
          return { metadata };
        })()`) + `,
        ` + JSON.stringify(`(() => {
          const metadata = {};
          metadata[Symbol("bad")] = 1;
          return { metadata };
        })()`) + `,
        '({ metadata: { bad: new Date(0) } })',
        ` + JSON.stringify(`(() => {
          const metadata = {};
          Object.defineProperty(metadata, "toJSON", { value() {}, configurable: true });
          return { metadata };
        })()`) + `,
      ];
      const errors = [];
      for (const expression of expressions) {
        const options = vm.runInContext(expression, context);
        try {
          await tab.title(options);
        } catch (error) {
          errors.push({ code: error.code, message: error.message });
        }
      }
      const valid = vm.runInContext('({ metadata: Object.create(null) })', context);
      const title = await tab.title(valid);
      process.stdout.write(JSON.stringify({ errors, sends, title }));
    `)
    assert.equal(result.errors.length, 7)
    assert.ok(result.errors.every(({ code, message }) =>
      code === 'host_error' &&
      message === 'Browser request contains a value outside exact RuntimeJson.'))
    assert.deepEqual(result.sends, ['browser-client-8'])
    assert.equal(result.title, 'ok')
  })

  it('generation-checks close, reopen, dispose, and late callbacks', () => {
    const result = runClientVm(`
      const callbackSets = [];
      const sends = [];
      let opens = 0;
      let closes = 0;
      const capability = brand({
        version: 1,
        open(_configuration, callbacks) {
          opens += 1;
          callbackSets.push(callbacks);
        },
        send(bytes, done) {
          const request = JSON.parse(Buffer.from(bytes).toString('utf8'));
          sends.push({ id: request.id, done });
        },
        close() { closes += 1; },
      });
      const registry = client.namespace.createBrowserRegistry({ transport: capability });
      const browser = await registry.getDefault();

      const first = browser.tabs.list();
      await tick();
      callbackSets[0].close(new Error('startup failed'));
      let firstError;
      try { await first; } catch (error) { firstError = error.message; }

      let secondSettled = false;
      const second = browser.tabs.list().then((value) => {
        secondSettled = true;
        return value;
      });
      sends[1].done();
      callbackSets[0].message(realmBytes(
        '{"id":"browser-client-2","ok":true,"response":{"value":[{"id":"old"}]}}',
      ));
      await tick();
      const lateOldIgnored = !secondSettled;
      callbackSets[1].message(realmBytes(
        '{"id":"browser-client-2","ok":true,"response":{"value":[{"id":"new"}]}}',
      ));
      const secondValue = await second;

      let disposedSettled = false;
      const disposed = browser.tabs.list().then(
        () => { disposedSettled = true; },
        () => { disposedSettled = true; },
      );
      await browser.dispose();
      sends[2].done();
      callbackSets[1].message(realmBytes(
        '{"id":"browser-client-3","ok":true,"response":{"value":[{"id":"late"}]}}',
      ));
      callbackSets[1].close(new Error('late close'));
      await tick();

      const fourth = browser.tabs.list();
      sends[3].done();
      callbackSets[2].message(realmBytes(
        '{"id":"browser-client-4","ok":true,"response":{"value":[{"id":"reopened"}]}}',
      ));
      const fourthValue = await fourth;
      void disposed;
      process.stdout.write(JSON.stringify({
        opens,
        closes,
        ids: sends.map(({ id }) => id),
        firstError,
        lateOldIgnored,
        disposedSettled,
        secondValue,
        fourthValue,
      }));
    `)
    assert.deepEqual(result, {
      opens: 3,
      closes: 1,
      ids: [
        'browser-client-1',
        'browser-client-2',
        'browser-client-3',
        'browser-client-4',
      ],
      firstError: 'Browser sidecar exited.',
      lateOldIgnored: true,
      disposedSettled: false,
      secondValue: [{ id: 'new', type: 'page', url: null, selected: false }],
      fourthValue: [{ id: 'reopened', type: 'page', url: null, selected: false }],
    })
  })

  it('makes all six utility modules inert exact Rust-CLI migration shims', () => {
    for (const subcommand of UTILITY_SHIMS) {
      const path = join(TREE, 'scripts', `${subcommand}.mjs`)
      const imported = inspectModule(path)
      assert.deepEqual(imported.exports, ['default', 'migration'])
      assert.equal(imported.same, true)
      assert.equal(imported.frozen, true)
      assert.deepEqual(imported.keys, [
        'version',
        'from',
        'command',
        'subcommand',
        'directExitCode',
      ])
      assert.deepEqual(imported.values, {
        version: 1,
        from: `scripts/${subcommand}.mjs`,
        command: 'klaus-browser',
        subcommand,
        directExitCode: 64,
      })
      const direct = runNode([path])
      assert.equal(direct.status, 64)
      assert.equal(direct.stdout, '')
      assert.equal(
        direct.stderr,
        `This browser utility moved to the Rust CLI: klaus-browser ${subcommand}.\n`,
      )
      assert.doesNotMatch(readFileSync(path, 'utf8'), /node:child_process/)
    }
  })

  it('fails the legacy JavaScript native host closed without bridge activity', () => {
    const path = join(TREE, 'runtime', 'native-host.mjs')
    const imported = inspectModule(path)
    assert.deepEqual(imported.exports, ['default', 'migration'])
    assert.deepEqual(imported.values, {
      version: 1,
      from: 'runtime/native-host.mjs',
      command: 'klaus-browser',
      subcommand: 'install-native-host-manifest',
      directExitCode: 64,
    })
    const direct = runNode([path])
    assert.equal(direct.status, 64)
    assert.equal(direct.stdout, '')
    assert.equal(
      direct.stderr,
      'Legacy JavaScript native host disabled; run klaus-browser install-native-host-manifest.\n',
    )
    assert.doesNotMatch(readFileSync(path, 'utf8'), /node:(?:net|child_process)/)
  })

  it('removes built-in native messaging selection and backoff', () => {
    const source = readFileSync(join(TREE, 'extension', 'service-worker.js'), 'utf8')
    assert.doesNotMatch(source, /connectNative|nativePort|NATIVE_HOST_NAME|nativeUnavailableUntil/)
    assert.match(source, /await connectWebSocket\(\)/)
    assert.match(source, /socket\.send\(JSON\.stringify\(message\)\)/)
  })

  it('ships only Rust-hosted browser setup and native utility guidance', () => {
    const playwright = readFileSync(join(TREE, 'docs', 'playwright.md'), 'utf8')
    const skill = readFileSync(join(TREE, 'skills', 'control-browser', 'SKILL.md'), 'utf8')
    const api = JSON.parse(readFileSync(join(TREE, 'docs', 'api.json'), 'utf8'))
    const plugin = JSON.parse(
      readFileSync(join(TREE, '.codex-plugin', 'plugin.json'), 'utf8'),
    )
    for (const source of [playwright, skill]) {
      assert.doesNotMatch(
        source,
        /npm install playwright|PlaywrightBrowserRuntime::default|node plugins\/klaus-bundled|native messaging host first|standalone Node loading/,
      )
    }
    assert.match(playwright, /klaus-browser check-extension-installed --json/)
    assert.match(skill, /context-bound transport capability/)
    assert.equal(api.nativeHost, 'klaus-browser-native-host')
    assert.equal(api.extension.builtInTransport, 'authenticated-websocket')
    assert.equal(api.runtimeTransport.owner, 'rust')
    assert.equal(api.runtimeTransport.standaloneNode, false)
    assert.deepEqual(
      api.extension.scripts.map(({ directExitCode }) => directExitCode),
      [64, 64, 64, 64, 64, 64],
    )
    assert.match(plugin.description, /Rust-owned pinned Playwright broker/)
  })

  it('exposes a deterministic repository check command', () => {
    const checked = runNode(['scripts/exact-browser-candidate.mjs', '--check'])
    assert.equal(checked.status, 0, checked.stderr)
    assert.match(checked.stdout, /24 files/)
  })
})
