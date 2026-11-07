import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { describe, it } from 'node:test'
import { fileURLToPath } from 'node:url'

import {
  analyzePlaywrightJavaScript,
  assertPlaywrightTerminalCandidateAccounting,
  canonicalAsciiJson,
  createPlaywrightProcessPrimitiveState,
} from './playwright-process-primitives-lock.mjs'

const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const LOCK = join(REPO_ROOT, 'playwright-process-primitives.lock.json')

function analyze(source, path = 'fixture.js') {
  const bytes = Buffer.from(source, 'utf8')
  return analyzePlaywrightJavaScript({
    packageName: 'playwright',
    path,
    bytes,
    fileSha256: createHash('sha256').update(bytes).digest('hex'),
  })
}

function u64be(value) {
  const bytes = Buffer.allocUnsafe(8)
  bytes.writeBigUInt64BE(BigInt(value))
  return bytes
}

function withSyntheticPackages(callback) {
  const root = mkdtempSync(join(tmpdir(), 'klaus-playwright-lock-'))
  try {
    for (const name of ['playwright', 'playwright-core']) {
      const packageRoot = join(root, name)
      mkdirSync(join(packageRoot, 'lib'), { recursive: true })
      writeFileSync(
        join(packageRoot, 'package.json'),
        `${JSON.stringify({ name, version: '1.61.1' })}\n`,
      )
      writeFileSync(
        join(packageRoot, 'lib', 'entry.js'),
        name === 'playwright'
          ? 'const cp = require("child_process"); cp.execFile("tool", []);\n'
          : 'process.kill(123, 0);\n',
      )
    }
    return callback(root)
  } finally {
    rmSync(root, { recursive: true, force: true })
  }
}

describe('playwright 1.61.1 process primitive lock', () => {
  it('keeps the checked lock canonical and binds all six approved Broker operations', () => {
    const bytes = readFileSync(LOCK)
    assert.equal(bytes.at(-1), 0x0a)
    assert.equal(bytes.subarray(0, -1).includes(0x0a), false)
    assert.equal(bytes.subarray(0, -1).includes(0x0d), false)
    const lock = JSON.parse(bytes)
    assert.equal(bytes.toString('ascii'), `${canonicalAsciiJson(lock)}\n`)
    assert.deepEqual(Object.keys(lock), [
      'version',
      'packages',
      'files',
      'callsites',
      'graphSha256',
    ])
    assert.equal(lock.version, 1)
    assert.deepEqual(lock.packages.map(({ name, version }) => [name, version]), [
      ['playwright', '1.61.1'],
      ['playwright-core', '1.61.1'],
    ])
    assert.equal(lock.files.length, 168)
    assert.equal(lock.callsites.length, 54)
    const brokerCounts = new Map()
    for (const callsite of lock.callsites) {
      assert.deepEqual(Object.keys(callsite), [
        'package',
        'path',
        'fileSha256',
        'startByte',
        'endByte',
        'primitive',
        'bindingTrace',
        'classification',
        'brokerOperation',
      ])
      assert.equal(callsite.bindingTrace.at(-1).kind, 'Call')
      if (callsite.classification === 'Broker') {
        brokerCounts.set(
          callsite.brokerOperation,
          (brokerCounts.get(callsite.brokerOperation) ?? 0) + 1,
        )
      } else {
        assert.equal(callsite.classification, 'DeniedPinnedPackagePrimitive')
        assert.equal(callsite.brokerOperation, null)
      }
    }
    assert.deepEqual(Object.fromEntries(brokerCounts), {
      PlaywrightLaunch: 1,
      PlaywrightTreeKill: 2,
      PlaywrightMacOsSwVers: 1,
      PlaywrightWindowsWinldd: 1,
      PlaywrightLinuxLdd: 1,
      PlaywrightLinuxLdconfig: 1,
    })
    const signalExitKill = lock.callsites.find(({ path, startByte, primitive }) =>
      path === 'lib/utilsBundle.js' &&
      startByte === 1599403 &&
      primitive === 'ProcessKill')
    assert.ok(signalExitKill)
    assert.ok(signalExitKill.bindingTrace.some(({ kind, name }) =>
      kind === 'Assignment' && name === 'process5'))
    const exportedSpawnAsyncRawCall = lock.callsites.find(({
      package: packageName,
      path,
      startByte,
      primitive,
    }) =>
      packageName === 'playwright-core' &&
      path === 'lib/coreBundle.js' &&
      startByte === 358888 &&
      primitive === 'ChildProcessSpawn')
    assert.ok(exportedSpawnAsyncRawCall)
    assert.equal(
      exportedSpawnAsyncRawCall.classification,
      'DeniedPinnedPackagePrimitive',
    )
    assert.equal(exportedSpawnAsyncRawCall.brokerOperation, null)
    const graph = {
      version: lock.version,
      packages: lock.packages,
      files: lock.files,
      callsites: lock.callsites,
    }
    const graphBytes = Buffer.from(canonicalAsciiJson(graph), 'ascii')
    const digest = createHash('sha256')
      .update(Buffer.from('KLAUS_PLAYWRIGHT_PROCESS_GRAPH_V1\0', 'ascii'))
      .update(u64be(graphBytes.length))
      .update(graphBytes)
      .digest('hex')
    assert.equal(lock.graphSha256, digest)
  })

  it('resolves direct, assigned, destructured, computed-literal, and process aliases', () => {
    const callsites = analyze(`
      const cp = require("node:child_process");
      const direct = cp["spawn"];
      direct("one");
      const { execFileSync: syncFile } = require("child_process");
      syncFile("two");
      const runtime = process;
      const terminate = runtime.kill;
      terminate(123, 0);
      globalThis.process["kill"](456, 0);
    `)
    assert.deepEqual(callsites.map(({ primitive }) => primitive), [
      'ChildProcessSpawn',
      'ChildProcessExecFileSync',
      'ProcessKill',
      'ProcessKill',
    ])
    assert.ok(callsites[0].bindingTrace.some(({ kind }) => kind === 'ComputedLiteral'))
    assert.ok(callsites[1].bindingTrace.some(({ kind }) => kind === 'Destructure'))
    assert.ok(callsites.every(({ classification }) => classification === 'DeniedPinnedPackagePrimitive'))
  })

  it('resolves dynamic import, module.require, and aliased require origins', () => {
    const dynamicImport = analyze(`
      const cp = await import("node:child_process");
      cp.spawn("dynamic");
    `, 'fixture.mjs')
    const moduleRequire = analyze(`
      const cp = module.require("child_process");
      cp.execFile("module-require", []);
    `)
    const directModuleRequire = analyze(
      'module.require("node:child_process").spawn("direct-module-require");',
    )
    const aliasedRequire = analyze(`
      const load = require;
      const cp = load("node:child_process");
      cp.spawnSync("aliased", []);
    `)
    const aliasedModuleRequire = analyze(`
      const load = module.require;
      load("child_process").fork("aliased-module-require");
    `)
    assert.deepEqual(dynamicImport.map(({ primitive }) => primitive), [
      'ChildProcessSpawn',
    ])
    assert.deepEqual(moduleRequire.map(({ primitive }) => primitive), [
      'ChildProcessExecFile',
    ])
    assert.deepEqual(directModuleRequire.map(({ primitive }) => primitive), [
      'ChildProcessSpawn',
    ])
    assert.deepEqual(aliasedRequire.map(({ primitive }) => primitive), [
      'ChildProcessSpawnSync',
    ])
    assert.deepEqual(aliasedModuleRequire.map(({ primitive }) => primitive), [
      'ChildProcessFork',
    ])
    assert.equal(dynamicImport[0].bindingTrace[0].kind, 'Import')
    assert.equal(moduleRequire[0].bindingTrace[0].kind, 'Require')
    assert.equal(directModuleRequire[0].bindingTrace[0].kind, 'Require')
    assert.equal(aliasedRequire[0].bindingTrace[0].kind, 'Require')
    assert.equal(aliasedModuleRequire[0].bindingTrace[0].kind, 'Require')
  })

  it('resolves extra-argument loaders and module aliases without silent omission', () => {
    const callsites = analyze(`
      require("child_process", 0).spawn("direct-extra");
      module.require("node:child_process", 0).execFile("module-extra", []);
      const load = require;
      load("child_process", 0).spawnSync("alias-extra", []);
      const moduleAlias = module;
      moduleAlias.require("node:child_process").fork("module-alias");
      const { require: destructuredLoad } = module;
      destructuredLoad("child_process").exec("module-destructure");
      global.require("child_process", 0).execSync("global-require");
      const globalLoad = globalThis.require;
      globalLoad("node:child_process").execFileSync("global-alias");
    `)
    assert.deepEqual(callsites.map(({ primitive }) => primitive), [
      'ChildProcessSpawn',
      'ChildProcessExecFile',
      'ChildProcessSpawnSync',
      'ChildProcessFork',
      'ChildProcessExec',
      'ChildProcessExecSync',
      'ChildProcessExecFileSync',
    ])
    assert.equal(callsites.instrumentation.terminalCalls, 7)
    assert.equal(callsites.instrumentation.mappedTerminalCalls, 7)
  })

  it('resolves constant child_process specifiers through every supported loader root', () => {
    const callsites = analyze(`
      const modulePrefix = "child_";
      const moduleName = modulePrefix + "process";
      const root = globalThis;
      root.require(moduleName).spawn("global-alias");
      const { require: rootRequire } = globalThis;
      rootRequire(moduleName).spawn("global-destructure");
      process.getBuiltinModule(moduleName).spawn("process-builtin");
      const runtime = globalThis.process;
      runtime.getBuiltinModule(moduleName).spawn("process-alias");
      require.main.require(moduleName).spawn("require-main");
    `)
    assert.deepEqual(callsites.map(({ primitive }) => primitive), [
      'ChildProcessSpawn',
      'ChildProcessSpawn',
      'ChildProcessSpawn',
      'ChildProcessSpawn',
      'ChildProcessSpawn',
    ])
    assert.equal(callsites.instrumentation.terminalCalls, 5)
    assert.equal(callsites.instrumentation.mappedTerminalCalls, 5)
  })

  it('fails closed on unproven specifiers through every supported loader root', () => {
    const escaped = [
      `
        const moduleName = runtimeModuleName();
        const root = globalThis;
        root.require(moduleName).spawn("global-alias");
      `,
      `
        const moduleName = runtimeModuleName();
        const { require: rootRequire } = globalThis;
        rootRequire(moduleName).spawn("global-destructure");
      `,
      `
        const moduleName = runtimeModuleName();
        const runtime = global.process;
        runtime.getBuiltinModule(moduleName).spawn("process-builtin");
      `,
      `
        const moduleName = runtimeModuleName();
        require.main.require(moduleName).spawn("require-main");
      `,
    ]
    for (const source of escaped) {
      assert.throws(
        () => analyze(source),
        /unknown module|unknown.*specifier|independent root capability escape/,
      )
    }
  })

  it('invalidates static specifiers on every unsupported binding write', () => {
    const mutated = [
      `
        let moduleName = "child_";
        moduleName += "process";
        require(moduleName).spawn("compound");
      `,
      `
        let moduleName = "child_process";
        moduleName ||= "fs";
        const root = globalThis;
        root.require(moduleName).spawn("logical-assignment");
      `,
      `
        let moduleName = "child_process";
        moduleName++;
        process.getBuiltinModule(moduleName).spawn("update");
      `,
      `
        let moduleName = "child_process";
        ({ moduleName } = runtimeConfig());
        require.main.require(moduleName).spawn("destructure");
      `,
      `
        let moduleName = "child_process";
        for (moduleName of runtimeNames()) break;
        require(moduleName).spawn("for-of");
      `,
      `
        let moduleName = "child_process";
        eval("moduleName = 'fs'");
        require(moduleName).spawn("direct-eval");
      `,
      `
        let moduleName = "child_process";
        (() => { moduleName = runtimeModuleName(); })();
        require(moduleName).spawn("closure-write");
      `,
    ]
    for (const source of mutated) {
      assert.throws(
        () => analyze(source),
        /unknown loader specifier/,
      )
    }
  })

  it('invalidates static specifiers for intrinsic eval identity aliases', () => {
    const escaped = [
      `
        let moduleName = "fs";
        const eval = globalThis.eval;
        eval("moduleName = 'child_process'");
        globalThis.require(moduleName).spawn("eval-alias");
      `,
      `
        let moduleName = "fs";
        const { eval, require: load } = globalThis;
        eval("moduleName = 'child_process'");
        load(moduleName).spawn("eval-destructure");
      `,
      `
        let moduleName = "fs";
        const intrinsicEval = globalThis.eval;
        const eval = intrinsicEval;
        eval("moduleName = 'child_process'");
        process.getBuiltinModule(moduleName).spawn("eval-chain");
      `,
      `
        let moduleName = "fs";
        let eval;
        eval = global.eval;
        eval("moduleName = 'child_process'");
        require.main.require(moduleName).spawn("eval-assignment");
      `,
      `
        let moduleName = "fs";
        function invoke(eval) {
          eval("moduleName = 'child_process'");
        }
        invoke(globalThis.eval);
        require(moduleName).spawn("eval-parameter");
      `,
      `
        let moduleName = "fs";
        function invoke(eval) {
          eval("moduleName = 'child_process'");
        }
        invoke(runtimeEvaluator());
        require(moduleName).spawn("unknown-eval-parameter");
      `,
    ]
    for (const source of escaped) {
      assert.throws(
        () => analyze(source),
        /unknown loader specifier/,
      )
    }
  })

  it('does not treat proven non-intrinsic eval shadows as direct eval', () => {
    const callsites = analyze(`
      const eval = () => undefined;
      eval("ignored");
      function invoke(eval) {
        eval("also ignored");
      }
      invoke(() => undefined);
      function wrapper() {}
      {
        let eval = globalThis.eval;
        eval = wrapper;
        eval("reassigned");
      }
      const moduleName = "child_process";
      require(moduleName).spawn("non-intrinsic");
    `)
    assert.deepEqual(callsites.map(({ primitive }) => primitive), [
      'ChildProcessSpawn',
    ])
    assert.equal(callsites.instrumentation.terminalCalls, 1)
    assert.equal(callsites.instrumentation.mappedTerminalCalls, 1)
  })

  it('fails closed when a constructor can return intrinsic eval', () => {
    assert.throws(
      () => analyze(`
        let moduleName = "fs";
        function EvalFactory() {
          return globalThis.eval;
        }
        const eval = new EvalFactory();
        eval("moduleName = 'child_process'");
        require(moduleName).spawn("constructor-eval");
      `),
      /unknown loader specifier/,
    )

    const ordinaryNew = analyze(`
      const value = new Map();
      value.set("eval", "ordinary");
      const moduleName = "child_process";
      require(moduleName).spawn("ordinary-new");
    `)
    assert.deepEqual(ordinaryNew.map(({ primitive }) => primitive), [
      'ChildProcessSpawn',
    ])
    assert.equal(ordinaryNew.instrumentation.terminalCalls, 1)
    assert.equal(ordinaryNew.instrumentation.mappedTerminalCalls, 1)
  })

  it('keeps dynamic non-control loader exceptions pinned to exact package paths', () => {
    const pinnedLoader = `
      function requireOrImport(file) {
        return require(file);
      }
    `
    assert.throws(
      () => analyze(pinnedLoader),
      /unknown loader specifier/,
    )
    assert.deepEqual(
      analyze(pinnedLoader, 'lib/common/index.js'),
      [],
    )
  })

  it('keeps loop lexical process bindings inside their for scopes', () => {
    for (const source of [
      `
        for (let process = { kill() {} }, done = false; !done; done = true) {
          process.kill(1);
        }
        process.kill(2);
      `,
      `
        for (let process of [{ kill() {} }]) process.kill(1);
        process.kill(2);
      `,
      `
        for (let process in { local: true }) {
          ({ kill() {} }).kill(process);
        }
        process.kill(2);
      `,
    ]) {
      const callsites = analyze(source)
      assert.deepEqual(callsites.map(({ primitive }) => primitive), ['ProcessKill'])
    }
  })

  it('keeps static-block lexical process bindings inside the static block', () => {
    const callsites = analyze(`
      class Fixture {
        static {
          let process = { kill() {} };
          process.kill(1);
        }
      }
      process.kill(2);
    `)
    assert.deepEqual(callsites.map(({ primitive }) => primitive), ['ProcessKill'])
  })

  it('fails closed when global process escapes through unsupported root flows', () => {
    const escaped = [
      `
        function terminate(runtime = process) { runtime.kill(1); }
        terminate();
      `,
      `
        const holder = { runtime: process };
        holder.runtime.kill(1);
      `,
      `
        function runtime() { return process; }
        runtime().kill(1);
      `,
      `
        const runtime = flag ? process : fallback;
        runtime.kill(1);
      `,
      `
        const runtime = process || fallback;
        runtime.kill(1);
      `,
      `
        const root = globalThis;
        root.process.kill(1);
      `,
      `
        const { process: runtime } = globalThis;
        runtime.kill(1);
      `,
      `
        const key = runtimeKey();
        globalThis[key];
      `,
    ]
    for (const source of escaped) {
      assert.throws(
        () => analyze(source),
        /independent root capability escape/,
      )
    }
  })

  it('allows only defensive empty-object process fallbacks with non-control consumers', () => {
    const callsites = analyze(`
      const runtime = process || {};
      const argv = runtime.argv || [];
      const env = runtime.env || {};
      const platform = runtime.platform;
      const cwd = typeof process === "object" && process &&
        typeof process.cwd === "function" ? process.cwd() : ".";
      const hasEnv = "env" in process;
      function processOk(candidate) {
        return candidate && typeof candidate === "object" &&
          typeof candidate.kill === "function" &&
          typeof candidate.pid === "number";
      }
      if (!processOk(process)) throw new Error("unsupported runtime");
      if (process) void cwd;
    `)
    assert.deepEqual(callsites, [])
  })

  it('allows computed global keys only when every assignment proves non-capability use', () => {
    const callsites = analyze(`
      let queueKey;
      if (hasSymbol) queueKey = Symbol.for("queue");
      else queueKey = "___queue";
      const queue = globalThis[queueKey] || [];
      function publish(context) {
        Object.defineProperty(context, queueKey, { value: queue });
      }
      publish(globalThis);
      const target = typeof globalThis !== "undefined"
        ? globalThis
        : fallbackGlobal || self;
      target.queue = queue;
      (function(unusedGlobal) { return 1; })(globalThis);
    `)
    assert.deepEqual(callsites, [])

    const pinnedJestSymbol = `
      const RuntimeSymbol = globalThis["jest-symbol-do-not-touch"] || globalThis.Symbol;
      const reader = globalThis[RuntimeSymbol.for("jest-native-read-file")];
    `
    assert.throws(
      () => analyze(pinnedJestSymbol),
      /independent root capability escape/,
    )
    assert.deepEqual(
      analyze(pinnedJestSymbol, 'lib/matchers/expect.js'),
      [],
    )

    const pinnedBabelEvaluator = `
      function _evaluate(callee, object) {
        const direct = global[callee.node.name];
        const context = global[object.node.name];
      }
    `
    assert.throws(
      () => analyze(pinnedBabelEvaluator),
      /independent root capability escape/,
    )
    assert.deepEqual(
      analyze(pinnedBabelEvaluator, 'lib/transform/babelBundle.js'),
      [],
    )
  })

  it('does not let a recognized exec hide an escaped global process root', () => {
    assert.throws(
      () => analyze(`
        require("child_process").exec("recognized");
        const holder = { runtime: process };
        holder.runtime.kill(1);
      `),
      /independent root capability escape/,
    )
  })

  it('does not let a recognized terminal hide a module-alias bypass', () => {
    const source = `
      require("child_process").spawn("recognized");
      const moduleAlias = module;
      moduleAlias.require("child_process").spawn("must-not-be-omitted");
    `
    const callsites = analyze(source)
    assert.equal(callsites.length, 2)
    assert.equal(callsites.instrumentation.terminalCalls, 2)
    assert.equal(callsites.instrumentation.mappedTerminalCalls, 2)

    const bytes = Buffer.from(source, 'utf8')
    assert.throws(
      () => assertPlaywrightTerminalCandidateAccounting({
        packageName: 'playwright',
        path: 'fixture.js',
        bytes,
        callsites: callsites.slice(0, 1),
      }),
      /independent root capability escape \(unlocked terminal ChildProcessSpawn\)/,
    )
    assert.deepEqual(
      assertPlaywrightTerminalCandidateAccounting({
        packageName: 'playwright',
        path: 'fixture.js',
        bytes,
        callsites,
      }).terminalCalls,
      2,
    )
  })

  it('fails closed on dynamic and cross-file child-process flows', () => {
    assert.throws(
      () => analyze('const cp = await import(runtimeModule); cp.spawn("tool");', 'fixture.mjs'),
      /dynamic import|unknown module specifier/,
    )
    assert.throws(
      () => analyze(`
        const load = require.bind(module);
        load("child_process").spawn("tool");
      `),
      /unresolved require loader flow/,
    )
    assert.throws(
      () => analyze(`
        const load = module.require.bind(module);
        load("child_process").spawn("tool");
      `),
      /unresolved module.require loader flow/,
    )
    assert.throws(
      () => analyze('consume(module);'),
      /unresolved module alias flow/,
    )
    assert.throws(
      () => analyze('const moduleAlias = module; consume(moduleAlias);'),
      /unresolved module alias flow/,
    )
    assert.throws(
      () => analyze('const { require: load, ...rest } = module;'),
      /unresolved module alias destructure/,
    )
    assert.throws(
      () => analyze('let moduleAlias = module; moduleAlias = consume;'),
      /module alias has divergent assignments/,
    )
    assert.throws(
      () => analyze('unknownLoader("child_process").spawn("tool");'),
      /unknown child_process loader/,
    )
    assert.throws(
      () => analyze('export { spawn } from "node:child_process";', 'fixture.mjs'),
      /cross-file child_process export/,
    )
    assert.throws(
      () => analyze('export * from "child_process";', 'fixture.mjs'),
      /cross-file child_process export/,
    )
    assert.throws(
      () => analyze(`
        import { spawn } from "node:child_process";
        export { spawn };
      `, 'fixture.mjs'),
      /cross-file primitive export/,
    )
    assert.throws(
      () => analyze('export { spawn } from "./bridge.mjs";', 'fixture.mjs'),
      /unknown cross-file re-export/,
    )
    assert.throws(
      () => analyze('export * from "./bridge.mjs";', 'fixture.mjs'),
      /unknown cross-file re-export/,
    )
  })

  it('propagates the pinned local spawnAsync wrapper and does not double-count its raw call', () => {
    const callsites = analyze(`
      const { spawn } = require("child_process");
      function spawnAsync(file, args) {
        const child = spawn(file, args);
        return Promise.resolve(child);
      }
      spawnAsync("first", []);
      spawnAsync("second", []);
    `)
    assert.equal(callsites.length, 2)
    assert.ok(callsites.every(({ primitive }) => primitive === 'ChildProcessSpawn'))
    assert.ok(callsites.every(({ bindingTrace }) =>
      bindingTrace.some(({ kind, name }) => kind === 'Return' && name === 'spawnAsync')))
  })

  it('locks the raw spawn when the spawnAsync wrapper value escapes', () => {
    const callsites = analyze(`
      const { spawn } = require("child_process");
      function spawnAsync(file, args) {
        return spawn(file, args);
      }
      const utils = { spawnAsync: () => spawnAsync };
      module.exports = utils;
    `)
    assert.equal(callsites.length, 1)
    assert.equal(callsites[0].primitive, 'ChildProcessSpawn')
    assert.equal(callsites[0].classification, 'DeniedPinnedPackagePrimitive')
    assert.equal(callsites[0].brokerOperation, null)
    assert.equal(callsites[0].bindingTrace.some(({ kind, name }) =>
      kind === 'Return' && name === 'spawnAsync'), false)
  })

  it('propagates a global process capability through a same-file parameter', () => {
    const callsites = analyze(`
      function processOk(runtime) {
        return typeof runtime.kill === "function";
      }
      function terminate(runtime) {
        runtime.kill(runtime.pid, 0);
      }
      const runtime = global.process;
      processOk(runtime);
      terminate(runtime);
    `)
    assert.equal(callsites.length, 1)
    assert.equal(callsites[0].primitive, 'ProcessKill')
    assert.ok(callsites[0].bindingTrace.some(({ kind, name }) =>
      kind === 'Parameter' && name === 'runtime'))
    assert.deepEqual(analyze(`
      const originalCwd = process.cwd;
      originalCwd.call(process);
      const runtime = global.process;
      const originalExit = runtime.reallyExit;
      originalExit.call(runtime, 0);
    `), [])
  })

  it('fails closed on nonliteral computed process properties', () => {
    assert.throws(
      () => analyze('const cp = require("child_process"); cp[method]("tool");'),
      /nonliteral computed process property/,
    )
    assert.throws(
      () => analyze('const runtime = process; runtime[method](123, 0);'),
      /nonliteral computed process property/,
    )
  })

  it('does not mistake a lexically shadowed process object for the Node global', () => {
    assert.deepEqual(
      analyze('function local(process) { process.kill(123); } local({ kill() {} });'),
      [],
    )
  })

  it('fails closed when a process namespace escapes through an unresolved alias flow', () => {
    assert.throws(
      () => analyze('const cp = require("child_process"); consume(cp);'),
      /unresolved primitive alias flow cp/,
    )
    assert.throws(
      () => analyze('const cp = require("child_process"); module.exports = cp.spawn;'),
      /unsupported process alias assignment|unresolved primitive property flow/,
    )
    assert.throws(
      () => analyze('const { spawn } = require("child_process"); module.exports = spawn;'),
      /unsupported process alias assignment|unresolved primitive alias flow spawn/,
    )
    assert.throws(
      () => analyze('consume(global.process);'),
      /unresolved process namespace flow/,
    )
    assert.throws(
      () => analyze('let originalCwd = process.cwd; originalCwd = consume; originalCwd.call(process);'),
      /unresolved process namespace flow/,
    )
  })

  it('builds deterministic package/file/callsite bytes independent of traversal order', () => {
    withSyntheticPackages((packagesRoot) => {
      const first = createPlaywrightProcessPrimitiveState({ packagesRoot })
      const second = createPlaywrightProcessPrimitiveState({ packagesRoot })
      assert.ok(first.lockBytes.equals(second.lockBytes))
      assert.deepEqual(first.lock.files.map(({ package: packageName, path }) => `${packageName}/${path}`), [
        'playwright-core/lib/entry.js',
        'playwright-core/package.json',
        'playwright/lib/entry.js',
        'playwright/package.json',
      ])
      assert.deepEqual(first.lock.callsites.map(({ primitive }) => primitive), [
        'ChildProcessExecFile',
        'ProcessKill',
      ])
      assert.deepEqual(first.instrumentation, {
        javascriptFiles: 2,
        terminalCalls: 2,
        mappedTerminalCalls: 2,
        mappingSha256: first.instrumentation.mappingSha256,
      })
      assert.match(first.instrumentation.mappingSha256, /^[0-9a-f]{64}$/)
      assert.deepEqual(first.instrumentation, second.instrumentation)
      assert.match(first.lock.graphSha256, /^[0-9a-f]{64}$/)
    })
  })

  it('uses only the frozen control escape form in canonical ASCII JSON', () => {
    assert.equal(canonicalAsciiJson('quote" slash\\ line\n tab\t'), '"quote\\" slash\\\\ line\\u000a tab\\u0009"')
    assert.throws(() => canonicalAsciiJson('not-ascii-\u00e9'), /ASCII only/)
    assert.throws(() => canonicalAsciiJson(-1), /unsigned safe integer/)
  })
})
