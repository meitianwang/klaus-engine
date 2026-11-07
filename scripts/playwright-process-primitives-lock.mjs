#!/usr/bin/env node

import { createHash } from 'node:crypto'
import {
  lstatSync,
  readFileSync,
  readdirSync,
  writeFileSync,
} from 'node:fs'
import { dirname, join, posix, relative, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import Module from 'node:module'
import { gunzipSync } from 'node:zlib'

const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..')
export const LOCK_PATH = 'playwright-process-primitives.lock.json'

const PACKAGE_SPECS = Object.freeze([
  Object.freeze({
    name: 'playwright',
    version: '1.61.1',
    integrity:
      'sha512-DWnY5o3YbLWK4GovuAVwpqL+1VwGNdUGrRr++8j8PtQQzvAVZUIMjKQ90fY689sEJZJBbZVw1rXaOKSTitkzPQ==',
    tarballSha256: '2f9c0a0dc4d68c5bd759f4730d7be1d4e9b45fb5ff937a1906069dd4e69252c3',
  }),
  Object.freeze({
    name: 'playwright-core',
    version: '1.61.1',
    integrity:
      'sha512-h7Qlt6m4REp25qvIdvbDtVmD4LqVXfpRxhORv9L0jzETM05p4fuPJ3dKyuSXQxDSbXnmS79HAgi9589lGSpLkg==',
    tarballSha256: '3740bba3a3e93b4c7bd7c0f6e3c0e5f8cdce498bef13511e4d534cc99a8ce198',
  }),
])

const PRIMITIVE_NAMES = Object.freeze(new Map([
  ['spawn', 'ChildProcessSpawn'],
  ['spawnSync', 'ChildProcessSpawnSync'],
  ['exec', 'ChildProcessExec'],
  ['execFile', 'ChildProcessExecFile'],
  ['execSync', 'ChildProcessExecSync'],
  ['execFileSync', 'ChildProcessExecFileSync'],
  ['fork', 'ChildProcessFork'],
  ['kill', 'ProcessKill'],
]))

const PRIMITIVE_ORDER = Object.freeze([
  'ChildProcessSpawn',
  'ChildProcessSpawnSync',
  'ChildProcessExec',
  'ChildProcessExecFile',
  'ChildProcessExecSync',
  'ChildProcessExecFileSync',
  'ChildProcessFork',
  'ProcessKill',
])

const TRACE_KIND_ORDER = Object.freeze([
  'Import',
  'Require',
  'Destructure',
  'Property',
  'ComputedLiteral',
  'Assignment',
  'Parameter',
  'Return',
  'Call',
])

const BROKER_OPERATIONS = Object.freeze(new Set([
  'PlaywrightLaunch',
  'PlaywrightTreeKill',
  'PlaywrightMacOsSwVers',
  'PlaywrightLinuxLdd',
  'PlaywrightLinuxLdconfig',
  'PlaywrightWindowsWinldd',
]))

let cachedAcorn

function loadPinnedNodeAcorn() {
  if (cachedAcorn) return cachedAcorn
  if (process.versions.node !== '20.19.4') {
    throw new Error(
      `playwright process lock requires Node 20.19.4, received ${process.versions.node}`,
    )
  }
  const source = process.binding('natives')['internal/deps/acorn/acorn/dist/acorn']
  if (typeof source !== 'string') {
    throw new Error('Node 20.19.4 embedded Acorn source is unavailable')
  }
  const embedded = new Module('klaus-pinned-node-acorn')
  embedded.filename = 'klaus-pinned-node-acorn.js'
  embedded.paths = []
  embedded._compile(source, embedded.filename)
  if (embedded.exports.version !== '8.14.0') {
    throw new Error(`unexpected embedded Acorn version ${embedded.exports.version}`)
  }
  cachedAcorn = embedded.exports
  return cachedAcorn
}

function sha256(bytes) {
  return createHash('sha256').update(bytes).digest('hex')
}

function u64be(value) {
  const bytes = Buffer.allocUnsafe(8)
  bytes.writeBigUInt64BE(BigInt(value))
  return bytes
}

function compareUtf8(left, right) {
  return Buffer.compare(Buffer.from(left, 'utf8'), Buffer.from(right, 'utf8'))
}

function assertAscii(value, label) {
  if (!/^[\x00-\x7f]*$/.test(value)) {
    throw new Error(`${label} must contain ASCII only`)
  }
}

function canonicalString(value) {
  assertAscii(value, 'canonical lock string')
  let output = '"'
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index)
    if (code === 0x22) output += '\\"'
    else if (code === 0x5c) output += '\\\\'
    else if (code < 0x20) output += `\\u00${code.toString(16).padStart(2, '0')}`
    else output += value[index]
  }
  return `${output}"`
}

export function canonicalAsciiJson(value) {
  if (value === null) return 'null'
  if (typeof value === 'boolean') return value ? 'true' : 'false'
  if (typeof value === 'number') {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new Error(`canonical lock integer is not an unsigned safe integer: ${value}`)
    }
    return String(value)
  }
  if (typeof value === 'string') return canonicalString(value)
  if (Array.isArray(value)) return `[${value.map(canonicalAsciiJson).join(',')}]`
  if (typeof value === 'object') {
    return `{${Object.entries(value)
      .map(([key, nested]) => `${canonicalString(key)}:${canonicalAsciiJson(nested)}`)
      .join(',')}}`
  }
  throw new Error(`unsupported canonical lock value: ${typeof value}`)
}

function validateRelativePath(path, label) {
  assertAscii(path, label)
  if (
    path.length === 0 ||
    path.includes('\\') ||
    path.startsWith('/') ||
    path.split('/').some((component) => component === '' || component === '.' || component === '..')
  ) {
    throw new Error(`${label} is not a canonical relative path: ${path}`)
  }
}

function collectPackageFiles(packageRoot) {
  const output = []
  const caseKeys = new Set()
  function visit(directory) {
    const entries = readdirSync(directory, { withFileTypes: true })
      .sort((left, right) => compareUtf8(left.name, right.name))
    for (const entry of entries) {
      const absolute = join(directory, entry.name)
      const metadata = lstatSync(absolute)
      const path = relative(packageRoot, absolute).split('\\').join('/')
      validateRelativePath(path, 'package file path')
      const caseKey = path.toLowerCase()
      if (caseKeys.has(caseKey)) throw new Error(`package path case alias: ${path}`)
      caseKeys.add(caseKey)
      if (metadata.isSymbolicLink()) throw new Error(`package contains symlink: ${path}`)
      if (metadata.isDirectory()) {
        visit(absolute)
      } else if (metadata.isFile()) {
        const bytes = readFileSync(absolute)
        output.push({ path, bytes, size: bytes.length, sha256: sha256(bytes) })
      } else {
        throw new Error(`package contains nonregular entry: ${path}`)
      }
    }
  }
  visit(packageRoot)
  return output.sort((left, right) => compareUtf8(left.path, right.path))
}

function parseTarOctal(field, label) {
  const text = field.toString('ascii').replace(/\0.*$/s, '').trim()
  if (!/^[0-7]+$/.test(text)) throw new Error(`invalid tar ${label}: ${JSON.stringify(text)}`)
  const value = Number.parseInt(text, 8)
  if (!Number.isSafeInteger(value) || value < 0) throw new Error(`unsafe tar ${label}`)
  return value
}

function tarText(field) {
  const end = field.indexOf(0)
  return field.subarray(0, end === -1 ? field.length : end).toString('utf8')
}

function collectTarballFiles(bytes, spec) {
  if (sha256(bytes) !== spec.tarballSha256) {
    throw new Error(`${spec.name} tarball SHA-256 drifted`)
  }
  const integrity = `sha512-${createHash('sha512').update(bytes).digest('base64')}`
  if (integrity !== spec.integrity) throw new Error(`${spec.name} tarball SRI drifted`)
  const tar = gunzipSync(bytes)
  const files = new Map()
  for (let offset = 0; offset + 512 <= tar.length;) {
    const header = tar.subarray(offset, offset + 512)
    if (header.every((byte) => byte === 0)) break
    const storedChecksum = parseTarOctal(header.subarray(148, 156), 'checksum')
    const checksumHeader = Buffer.from(header)
    checksumHeader.fill(0x20, 148, 156)
    const actualChecksum = checksumHeader.reduce((sum, byte) => sum + byte, 0)
    if (actualChecksum !== storedChecksum) throw new Error(`${spec.name} tar checksum drifted`)
    const name = tarText(header.subarray(0, 100))
    const prefix = tarText(header.subarray(345, 500))
    const archivePath = prefix ? `${prefix}/${name}` : name
    const size = parseTarOctal(header.subarray(124, 136), 'size')
    const type = header[156]
    const dataStart = offset + 512
    const dataEnd = dataStart + size
    if (dataEnd > tar.length) throw new Error(`${spec.name} tar entry is truncated: ${archivePath}`)
    if (type !== 0 && type !== 0x30) {
      throw new Error(`${spec.name} tar contains nonregular entry: ${archivePath}`)
    }
    if (!archivePath.startsWith('package/')) {
      throw new Error(`${spec.name} tar path is outside package/: ${archivePath}`)
    }
    const path = archivePath.slice('package/'.length)
    validateRelativePath(path, 'tarball package path')
    if (files.has(path)) throw new Error(`${spec.name} tar contains duplicate path: ${path}`)
    files.set(path, Buffer.from(tar.subarray(dataStart, dataEnd)))
    offset = dataStart + Math.ceil(size / 512) * 512
  }
  return files
}

function verifyExtractedPackage(packageFiles, tarballFiles, spec) {
  if (packageFiles.length !== tarballFiles.size) {
    throw new Error(`${spec.name} extracted file count differs from its pinned tarball`)
  }
  for (const file of packageFiles) {
    const tarballBytes = tarballFiles.get(file.path)
    if (!tarballBytes || !tarballBytes.equals(file.bytes)) {
      throw new Error(`${spec.name} extracted bytes differ from its pinned tarball: ${file.path}`)
    }
  }
}

function parseJavaScript(source, path) {
  const acorn = loadPinnedNodeAcorn()
  const options = {
    ecmaVersion: 'latest',
    allowHashBang: true,
    allowAwaitOutsideFunction: true,
    locations: false,
    sourceType: path.endsWith('.mjs') ? 'module' : 'script',
  }
  try {
    return acorn.parse(source, options)
  } catch (scriptError) {
    if (options.sourceType === 'module') throw scriptError
    try {
      return acorn.parse(source, { ...options, sourceType: 'module' })
    } catch {
      throw new Error(`${path}: JavaScript parse failure: ${scriptError.message}`)
    }
  }
}

function walkAst(root) {
  const nodes = []
  const parents = new Map()
  const stack = [{ node: root, parent: null }]
  while (stack.length > 0) {
    const { node, parent } = stack.pop()
    if (!node || typeof node.type !== 'string') continue
    nodes.push(node)
    if (parent) parents.set(node, parent)
    const children = []
    for (const [key, value] of Object.entries(node)) {
      if (key === 'start' || key === 'end' || key === 'loc') continue
      if (Array.isArray(value)) {
        for (const item of value) {
          if (item && typeof item.type === 'string') children.push(item)
        }
      } else if (value && typeof value.type === 'string') {
        children.push(value)
      }
    }
    for (let index = children.length - 1; index >= 0; index -= 1) {
      stack.push({ node: children[index], parent: node })
    }
  }
  return { nodes, parents }
}

function patternIdentifiers(pattern, output = []) {
  if (!pattern) return output
  if (pattern.type === 'Identifier') output.push(pattern)
  else if (pattern.type === 'RestElement') patternIdentifiers(pattern.argument, output)
  else if (pattern.type === 'AssignmentPattern') patternIdentifiers(pattern.left, output)
  else if (pattern.type === 'ArrayPattern') {
    for (const element of pattern.elements) patternIdentifiers(element, output)
  } else if (pattern.type === 'ObjectPattern') {
    for (const property of pattern.properties) {
      if (property.type === 'RestElement') patternIdentifiers(property.argument, output)
      else patternIdentifiers(property.value, output)
    }
  }
  return output
}

function buildLexicalBindings(ast, nodes, parents) {
  const scopeOwner = (node) =>
    node.type === 'Program' ||
    node.type === 'BlockStatement' ||
    node.type === 'CatchClause' ||
    node.type === 'ForStatement' ||
    node.type === 'ForInStatement' ||
    node.type === 'ForOfStatement' ||
    node.type === 'SwitchStatement' ||
    node.type === 'StaticBlock' ||
    node.type === 'FunctionDeclaration' ||
    node.type === 'FunctionExpression' ||
    node.type === 'ArrowFunctionExpression'
  const scopes = new Map()
  let nextScopeId = 0
  for (const node of nodes) {
    if (!scopeOwner(node)) continue
    let parent = parents.get(node)
    while (parent && !scopes.has(parent)) parent = parents.get(parent)
    scopes.set(node, {
      id: nextScopeId,
      owner: node,
      parent: parent ? scopes.get(parent) : null,
      declared: new Set(),
    })
    nextScopeId += 1
  }
  const programScope = scopes.get(ast)
  const declarationKeys = new Map()

  function containingScope(node, includeSelf = false) {
    let current = includeSelf ? node : parents.get(node)
    while (current && !scopes.has(current)) current = parents.get(current)
    return current ? scopes.get(current) : programScope
  }

  function functionScope(node) {
    let scope = containingScope(node)
    while (
      scope &&
      scope.owner.type !== 'Program' &&
      scope.owner.type !== 'FunctionDeclaration' &&
      scope.owner.type !== 'FunctionExpression' &&
      scope.owner.type !== 'ArrowFunctionExpression'
    ) scope = scope.parent
    return scope ?? programScope
  }

  function declare(identifier, scope) {
    scope.declared.add(identifier.name)
    const key = `${scope.id}:${identifier.name}`
    declarationKeys.set(identifier, key)
    return key
  }

  for (const node of nodes) {
    if (node.type === 'VariableDeclarator') {
      const declaration = parents.get(node)
      const scope = declaration?.kind === 'var'
        ? functionScope(node)
        : containingScope(node)
      for (const identifier of patternIdentifiers(node.id)) declare(identifier, scope)
    } else if (node.type === 'FunctionDeclaration' && node.id) {
      const ownScope = scopes.get(node)
      declare(node.id, ownScope.parent ?? programScope)
      for (const parameter of node.params) {
        for (const identifier of patternIdentifiers(parameter)) declare(identifier, ownScope)
      }
    } else if (
      (node.type === 'FunctionExpression' || node.type === 'ArrowFunctionExpression')
    ) {
      const ownScope = scopes.get(node)
      if (node.type === 'FunctionExpression' && node.id) declare(node.id, ownScope)
      for (const parameter of node.params) {
        for (const identifier of patternIdentifiers(parameter)) declare(identifier, ownScope)
      }
    } else if (node.type === 'ImportDeclaration') {
      for (const specifier of node.specifiers) declare(specifier.local, programScope)
    } else if (node.type === 'ClassDeclaration' && node.id) {
      declare(node.id, containingScope(node))
    } else if (node.type === 'CatchClause' && node.param) {
      const ownScope = scopes.get(node)
      for (const identifier of patternIdentifiers(node.param)) declare(identifier, ownScope)
    }
  }

  function resolveKey(name, node) {
    let scope = containingScope(node)
    while (scope) {
      if (scope.declared.has(name)) return `${scope.id}:${name}`
      scope = scope.parent
    }
    return null
  }

  return {
    isDeclaration(identifier) {
      return declarationKeys.has(identifier)
    },
    declarationKey(identifier) {
      return declarationKeys.get(identifier) ?? resolveKey(identifier.name, identifier)
    },
    referenceKey(identifier) {
      return resolveKey(identifier.name, identifier)
    },
  }
}

function byteOffsetMap(source) {
  const map = new Uint32Array(source.length + 1)
  let bytes = 0
  let index = 0
  while (index < source.length) {
    map[index] = bytes
    const code = source.charCodeAt(index)
    if (code >= 0xd800 && code <= 0xdbff && index + 1 < source.length) {
      const next = source.charCodeAt(index + 1)
      if (next >= 0xdc00 && next <= 0xdfff) {
        map[index + 1] = bytes
        bytes += 4
        index += 2
        continue
      }
    }
    bytes += Buffer.byteLength(source[index], 'utf8')
    index += 1
  }
  map[source.length] = bytes
  return map
}

function traceStep(kind, node, name, byteOffsets) {
  if (!TRACE_KIND_ORDER.includes(kind)) throw new Error(`unknown binding trace kind ${kind}`)
  if (name !== null) assertAscii(name, 'binding trace name')
  return {
    kind,
    startByte: byteOffsets[node.start],
    endByte: byteOffsets[node.end],
    name,
  }
}

function cloneValue(value, extraTrace = []) {
  return value && { ...value, trace: [...value.trace, ...extraTrace] }
}

function literalProperty(member) {
  if (!member.computed && member.property.type === 'Identifier') {
    return { name: member.property.name, kind: 'Property' }
  }
  if (
    member.computed &&
    member.property.type === 'Literal' &&
    typeof member.property.value === 'string'
  ) {
    return { name: member.property.value, kind: 'ComputedLiteral' }
  }
  return null
}

function unwrapTransparent(expression) {
  let current = expression
  for (;;) {
    if (current.type === 'AwaitExpression') {
      current = current.argument
      continue
    }
    if (current.type === 'ChainExpression') {
      current = current.expression
      continue
    }
    if (current.type === 'SequenceExpression' && current.expressions.length > 0) {
      current = current.expressions[current.expressions.length - 1]
      continue
    }
    if (
      current.type === 'CallExpression' &&
      current.callee.type === 'Identifier' &&
      ['__toESM', '__toCommonJS'].includes(current.callee.name) &&
      current.arguments.length >= 1
    ) {
      current = current.arguments[0]
      continue
    }
    return current
  }
}

function identifierName(pattern) {
  return pattern?.type === 'Identifier' ? pattern.name : null
}

function enclosingFunctionName(node, parents) {
  let current = node
  while ((current = parents.get(current))) {
    if (
      current.type === 'FunctionDeclaration' ||
      current.type === 'FunctionExpression' ||
      current.type === 'ArrowFunctionExpression'
    ) {
      if (current.id?.type === 'Identifier') return current.id.name
      const parent = parents.get(current)
      if (parent?.type === 'VariableDeclarator' && parent.id.type === 'Identifier') {
        return parent.id.name
      }
      return null
    }
  }
  return null
}

const PINNED_NON_CONTROL_DYNAMIC_LOADER_CALLS = Object.freeze(new Set([
  'playwright\0lib/common/index.js\0require((0, import_package3.libPath)("transform", "babelBundle"))',
  'playwright\0lib/common/index.js\0require(file)',
  'playwright\0lib/transform/esmLoader.js\0require((0, import_package2.libPath)("transform", "babelBundle"))',
  'playwright\0lib/transform/esmLoader.js\0require(file)',
  'playwright\0lib/transform/babelBundle.js\0require(require.resolve(name, { paths: [".", ctx.path] }))',
  'playwright\0lib/transform/babelBundle.js\0require(name)',
  'playwright\0lib/package.js\0require(import_path.default.join(packageRoot, "package.json"))',
  'playwright-core\0lib/package.js\0require(import_path.default.join(packageRoot, "package.json"))',
  'playwright-core\0lib/serverRegistry.js\0require(import_path.default.join(packageRoot, "package.json"))',
  'playwright-core\0lib/coreBundle.js\0require(import_path8.default.join(packageRoot, "package.json"))',
  'playwright-core\0lib/coreBundle.js\0require(import_path19.default.join(linkTarget, "browsers.json"))',
  'playwright-core\0lib/coreBundle.js\0require(import_path19.default.join(packageRoot, "browsers.json"))',
  'playwright-core\0lib/coreBundle.js\0require(initPage)',
  'playwright-core\0lib/coreBundle.js\0require(import_path43.default.join(packageRoot, "api.json"))',
  'playwright-core\0lib/coreBundle.js\0require(import_path44.default.join(browser.referenceDir, "package.json"))',
  'playwright-core\0lib/coreBundle.js\0require(libPath("tools", "cli-client", "help.json"))',
  'playwright-core\0lib/coreBundle.js\0require(descriptor.playwrightLib)',
  'playwright-core\0lib/tools/cli-client/program.js\0require((0, import_package.libPath)("tools", "cli-client", "help.json"))',
]))

const PINNED_NON_CONTROL_DYNAMIC_LOADER_FUNCTIONS = Object.freeze(new Set([
  'playwright\0lib/transform/babelBundle.js\0loadStat',
  'playwright\0lib/transform/babelBundle.js\0loadCountry',
  'playwright\0lib/transform/babelBundle.js\0loadFeature',
]))

function isPinnedNonControlDynamicLoaderCall({
  packageName,
  path,
  node,
  source,
  parents,
}) {
  if (PINNED_NON_CONTROL_DYNAMIC_LOADER_CALLS.has(
    `${packageName}\0${path}\0${source.slice(node.start, node.end)}`,
  )) return true
  const functionName = enclosingFunctionName(node, parents)
  return PINNED_NON_CONTROL_DYNAMIC_LOADER_FUNCTIONS.has(
    `${packageName}\0${path}\0${functionName ?? '<top-level>'}`,
  )
}

function classifyCallsite({ packageName, path, node, source, parents, wrapper }) {
  if (packageName !== 'playwright-core' || path !== 'lib/coreBundle.js') {
    return { classification: 'DeniedPinnedPackagePrimitive', brokerOperation: null }
  }
  const text = source.slice(node.start, node.end)
  const functionName = enclosingFunctionName(node, parents)
  if (
    !wrapper &&
    functionName === 'launchProcess' &&
    /^childProcess\.spawn\(options\.command, options\.args \|\| \[\], spawnOptions\)$/.test(text)
  ) {
    return { classification: 'Broker', brokerOperation: 'PlaywrightLaunch' }
  }
  if (
    !wrapper &&
    functionName === 'killProcess' &&
    (/^childProcess\.spawnSync\(`taskkill \/pid /.test(text) ||
      /^process\.kill\(-spawnedProcess\.pid, "SIGKILL"\)$/.test(text))
  ) {
    return { classification: 'Broker', brokerOperation: 'PlaywrightTreeKill' }
  }
  if (
    !wrapper &&
    functionName === 'determineUserAgent' &&
    /\.execSync\)\("sw_vers -productVersion",/.test(text)
  ) {
    return { classification: 'Broker', brokerOperation: 'PlaywrightMacOsSwVers' }
  }
  if (wrapper === 'spawnAsync') {
    if (functionName === 'missingFileDependencies' && /^spawnAsync\("ldd",/.test(text)) {
      return { classification: 'Broker', brokerOperation: 'PlaywrightLinuxLdd' }
    }
    if (functionName === 'missingDLOPENLibraries' && /^spawnAsync\("\/sbin\/ldconfig",/.test(text)) {
      return { classification: 'Broker', brokerOperation: 'PlaywrightLinuxLdconfig' }
    }
    if (functionName === 'missingFileDependenciesWindows' && /^spawnAsync\(executable,/.test(text)) {
      return { classification: 'Broker', brokerOperation: 'PlaywrightWindowsWinldd' }
    }
  }
  return { classification: 'DeniedPinnedPackagePrimitive', brokerOperation: null }
}

function validateCallsiteClassification(value) {
  if (value.classification === 'Broker') {
    if (!BROKER_OPERATIONS.has(value.brokerOperation)) {
      throw new Error(`invalid Broker operation ${value.brokerOperation}`)
    }
  } else if (
    value.classification !== 'DeniedPinnedPackagePrimitive' ||
    value.brokerOperation !== null
  ) {
    throw new Error('invalid pinned-package primitive classification')
  }
}

function isReferencePosition(node, parent) {
  if (!parent) return true
  if (parent.type === 'VariableDeclarator' && parent.id === node) return false
  if (
    (parent.type === 'FunctionDeclaration' ||
      parent.type === 'FunctionExpression' ||
      parent.type === 'ArrowFunctionExpression') &&
    (parent.id === node || parent.params.includes(node))
  ) return false
  if (parent.type === 'MemberExpression' && parent.property === node && !parent.computed) return false
  if (parent.type === 'Property' && parent.key === node && !parent.computed && !parent.shorthand) return false
  if (
    (parent.type === 'MethodDefinition' || parent.type === 'PropertyDefinition') &&
    parent.key === node &&
    !parent.computed
  ) return false
  if (parent.type === 'AssignmentExpression' && parent.left === node) return false
  if (parent.type === 'LabeledStatement' && parent.label === node) return false
  return true
}

function bindingEscapesDirectCalls(nodes, parents, lexical, key) {
  for (const node of nodes) {
    if (
      node.type !== 'Identifier' ||
      lexical.referenceKey(node) !== key ||
      lexical.isDeclaration(node)
    ) continue
    const parent = parents.get(node)
    if (!isReferencePosition(node, parent)) continue
    if (parent?.type === 'CallExpression' && parent.callee === node) continue
    return true
  }
  return false
}

function createStaticStringResolver(nodes, parents, lexical) {
  const assignmentsByKey = new Map()
  const unknownWriteKeys = new Set()
  const evalSourcesByKey = new Map()
  const localFunctions = new Map()
  const ambiguousFunctionKeys = new Set()
  const INTRINSIC_EVAL = Symbol('intrinsic eval')
  const NON_INTRINSIC_EVAL = Symbol('non-intrinsic eval')
  const UNKNOWN_EVAL = Symbol('unknown eval')
  const GLOBAL_OBJECT = Symbol('global object')
  const NON_GLOBAL_OBJECT = Symbol('non-global object')
  const UNKNOWN_GLOBAL_OBJECT = Symbol('unknown global object')

  function append(key, expression) {
    if (key === null || !expression) return
    const assignments = assignmentsByKey.get(key) ?? []
    assignments.push(expression)
    assignmentsByKey.set(key, assignments)
  }

  function appendEvalSource(key, value, node, linear = null) {
    if (key === null) return
    const sources = evalSourcesByKey.get(key) ?? []
    sources.push({ value, node, linear })
    evalSourcesByKey.set(key, sources)
  }

  function bindingKey(identifier) {
    return lexical.declarationKey(identifier) ?? lexical.referenceKey(identifier)
  }

  function registerLocalFunction(key, target) {
    if (key === null || ambiguousFunctionKeys.has(key)) return
    const previous = localFunctions.get(key)
    if (previous && previous !== target) {
      localFunctions.delete(key)
      ambiguousFunctionKeys.add(key)
    } else {
      localFunctions.set(key, target)
    }
  }

  function markUnknownPattern(pattern) {
    for (const identifier of patternIdentifiers(pattern)) {
      const key = bindingKey(identifier)
      if (key !== null) unknownWriteKeys.add(key)
    }
  }

  function patternPropertyName(property) {
    if (!property.computed && property.key.type === 'Identifier') return property.key.name
    if (property.key.type === 'Literal' && typeof property.key.value === 'string') {
      return property.key.value
    }
    return null
  }

  function recordEvalPattern(pattern, source, node) {
    const handled = new Set()
    if (pattern.type === 'ObjectPattern') {
      for (const property of pattern.properties) {
        if (
          property.type === 'Property' &&
          patternPropertyName(property) === 'eval' &&
          property.value.type === 'Identifier'
        ) {
          const key = bindingKey(property.value)
          appendEvalSource(
            key,
            source
              ? { kind: 'GlobalEvalProperty', object: source }
              : UNKNOWN_EVAL,
            node,
          )
          handled.add(property.value)
        }
      }
    }
    for (const identifier of patternIdentifiers(pattern)) {
      if (!handled.has(identifier)) {
        appendEvalSource(bindingKey(identifier), UNKNOWN_EVAL, node)
      }
    }
  }

  for (const node of nodes) {
    if (node.type === 'VariableDeclarator') {
      if (node.id.type === 'Identifier') {
        const key = lexical.declarationKey(node.id)
        if (node.init) append(key, node.init)
        appendEvalSource(key, node.init ?? NON_INTRINSIC_EVAL, node)
        if (node.init?.type === 'FunctionExpression' || node.init?.type === 'ArrowFunctionExpression') {
          registerLocalFunction(key, node.init)
        }
      } else {
        markUnknownPattern(node.id)
        recordEvalPattern(node.id, node.init, node)
      }
    } else if (node.type === 'AssignmentExpression') {
      if (node.left.type === 'Identifier') {
        const key = lexical.referenceKey(node.left)
        if (node.operator === '=') {
          append(key, node.right)
          appendEvalSource(key, node.right, node)
          if (localFunctions.has(key) && localFunctions.get(key) !== node.right) {
            localFunctions.delete(key)
            ambiguousFunctionKeys.add(key)
          }
        } else if (key !== null) {
          unknownWriteKeys.add(key)
          appendEvalSource(key, UNKNOWN_EVAL, node)
        }
      } else {
        markUnknownPattern(node.left)
        recordEvalPattern(node.left, node.right, node)
      }
    } else if (node.type === 'UpdateExpression') {
      markUnknownPattern(node.argument)
      for (const identifier of patternIdentifiers(node.argument)) {
        appendEvalSource(bindingKey(identifier), UNKNOWN_EVAL, node)
      }
    } else if (node.type === 'ForInStatement' || node.type === 'ForOfStatement') {
      if (node.left.type === 'VariableDeclaration') {
        for (const declaration of node.left.declarations) {
          markUnknownPattern(declaration.id)
          recordEvalPattern(declaration.id, null, node)
        }
      } else {
        markUnknownPattern(node.left)
        recordEvalPattern(node.left, null, node)
      }
    } else if (node.type === 'FunctionDeclaration' && node.id) {
      const key = lexical.declarationKey(node.id)
      appendEvalSource(key, NON_INTRINSIC_EVAL, node)
      registerLocalFunction(key, node)
    } else if (node.type === 'FunctionExpression' && node.id) {
      appendEvalSource(lexical.declarationKey(node.id), NON_INTRINSIC_EVAL, node)
    } else if (node.type === 'ClassDeclaration' && node.id) {
      appendEvalSource(lexical.declarationKey(node.id), NON_INTRINSIC_EVAL, node)
    } else if (node.type === 'ImportDeclaration') {
      for (const specifier of node.specifiers) {
        appendEvalSource(lexical.declarationKey(specifier.local), UNKNOWN_EVAL, specifier)
      }
    } else if (node.type === 'CatchClause' && node.param) {
      for (const identifier of patternIdentifiers(node.param)) {
        appendEvalSource(lexical.declarationKey(identifier), UNKNOWN_EVAL, node)
      }
    }
  }

  const directCallsByFunction = new Map()
  const escapedFunctionKeys = new Set()
  for (const node of nodes) {
    if (node.type !== 'Identifier' || lexical.isDeclaration(node)) continue
    const key = lexical.referenceKey(node)
    if (key === null || !localFunctions.has(key)) continue
    const parent = parents.get(node)
    if (!isReferencePosition(node, parent)) continue
    if (parent?.type === 'CallExpression' && parent.callee === node) {
      const calls = directCallsByFunction.get(key) ?? []
      calls.push(parent)
      directCallsByFunction.set(key, calls)
    } else {
      escapedFunctionKeys.add(key)
    }
  }

  for (const [ownerKey, target] of localFunctions) {
    const directCalls = directCallsByFunction.get(ownerKey) ?? []
    const escaped = escapedFunctionKeys.has(ownerKey)
    for (let index = 0; index < target.params.length; index += 1) {
      const parameter = target.params[index]
      if (parameter.type !== 'Identifier') {
        for (const identifier of patternIdentifiers(parameter)) {
          appendEvalSource(lexical.declarationKey(identifier), UNKNOWN_EVAL, parameter)
        }
        continue
      }
      const key = lexical.declarationKey(parameter)
      if (escaped || directCalls.length === 0) {
        appendEvalSource(key, UNKNOWN_EVAL, parameter)
        continue
      }
      for (const call of directCalls) {
        const argument = call.arguments[index]
        const value = !argument
          ? NON_INTRINSIC_EVAL
          : argument.type === 'SpreadElement'
            ? UNKNOWN_EVAL
            : argument
        appendEvalSource(
          key,
          value,
          call,
          target.body.type === 'BlockStatement'
            ? { container: target.body, position: target.body.start }
            : null,
        )
      }
    }
  }

  function containingStatement(node) {
    let current = node
    while (current) {
      const parent = parents.get(current)
      if (!parent) return null
      if (
        parent.type === 'Program' ||
        parent.type === 'BlockStatement' ||
        parent.type === 'StaticBlock'
      ) return { container: parent, statement: current }
      current = parent
    }
    return null
  }

  function linearLocation(source) {
    if (source.linear) return source.linear
    const placement = containingStatement(source.node)
    if (!placement) return null
    const { container, statement } = placement
    if (
      source.node.type === 'VariableDeclarator' &&
      statement.type === 'VariableDeclaration'
    ) return { container, position: source.node.start }
    if (
      source.node.type === 'AssignmentExpression' &&
      statement.type === 'ExpressionStatement' &&
      statement.expression === source.node
    ) return { container, position: source.node.start }
    if (
      (source.node.type === 'FunctionDeclaration' ||
        source.node.type === 'ClassDeclaration') &&
      statement === source.node
    ) {
      return {
        container,
        position: source.node.type === 'FunctionDeclaration'
          ? container.start
          : source.node.start,
      }
    }
    return null
  }

  function reachingSources(sources, atCall) {
    const callPlacement = containingStatement(atCall)
    if (!callPlacement) return sources
    const locations = sources.map(linearLocation)
    if (locations.some((location) => !location || location.container !== callPlacement.container)) {
      return sources
    }
    const beforeCall = sources.filter((source, index) => locations[index].position <= atCall.start)
    if (beforeCall.length === 0) return sources
    const latestPosition = Math.max(...beforeCall.map((source) => linearLocation(source).position))
    return beforeCall.filter((source) => linearLocation(source).position === latestPosition)
  }

  function mergeIdentity(values, unknown) {
    if (values.length === 0) return unknown
    const first = values[0]
    return values.every((value) => value === first) ? first : unknown
  }

  function unwrapIdentity(expression) {
    let current = expression
    for (;;) {
      if (current.type === 'AwaitExpression') {
        current = current.argument
      } else if (current.type === 'ChainExpression') {
        current = current.expression
      } else if (current.type === 'SequenceExpression' && current.expressions.length > 0) {
        current = current.expressions[current.expressions.length - 1]
      } else {
        return current
      }
    }
  }

  function resolveGlobalObject(expression, atCall, seen = new Set()) {
    if (expression === UNKNOWN_EVAL || expression === INTRINSIC_EVAL) {
      return UNKNOWN_GLOBAL_OBJECT
    }
    if (expression === NON_INTRINSIC_EVAL) return NON_GLOBAL_OBJECT
    if (expression?.kind === 'GlobalEvalProperty') return UNKNOWN_GLOBAL_OBJECT
    const node = unwrapIdentity(expression)
    if (node.type === 'Identifier') {
      const key = lexical.referenceKey(node)
      if (key === null) {
        if (node.name === 'global' || node.name === 'globalThis') return GLOBAL_OBJECT
        return UNKNOWN_GLOBAL_OBJECT
      }
      const seenKey = `global:${key}`
      if (seen.has(seenKey)) return UNKNOWN_GLOBAL_OBJECT
      const sources = evalSourcesByKey.get(key) ?? []
      const nextSeen = new Set(seen)
      nextSeen.add(seenKey)
      return mergeIdentity(
        reachingSources(sources, atCall).map((source) =>
          resolveGlobalObject(source.value, atCall, nextSeen)),
        UNKNOWN_GLOBAL_OBJECT,
      )
    }
    if (node.type === 'AssignmentExpression' && node.operator === '=') {
      return resolveGlobalObject(node.right, atCall, seen)
    }
    if (node.type === 'ConditionalExpression') {
      return mergeIdentity([
        resolveGlobalObject(node.consequent, atCall, seen),
        resolveGlobalObject(node.alternate, atCall, seen),
      ], UNKNOWN_GLOBAL_OBJECT)
    }
    if (node.type === 'LogicalExpression') {
      return mergeIdentity([
        resolveGlobalObject(node.left, atCall, seen),
        resolveGlobalObject(node.right, atCall, seen),
      ], UNKNOWN_GLOBAL_OBJECT)
    }
    if (
      node.type === 'Literal' ||
      node.type === 'TemplateLiteral' ||
      node.type === 'ObjectExpression' ||
      node.type === 'ArrayExpression' ||
      node.type === 'FunctionExpression' ||
      node.type === 'ArrowFunctionExpression' ||
      node.type === 'ClassExpression' ||
      node.type === 'NewExpression'
    ) return NON_GLOBAL_OBJECT
    return UNKNOWN_GLOBAL_OBJECT
  }

  function resolveEvalIdentity(expression, atCall, seen = new Set()) {
    if (
      expression === INTRINSIC_EVAL ||
      expression === NON_INTRINSIC_EVAL ||
      expression === UNKNOWN_EVAL
    ) return expression
    if (expression?.kind === 'GlobalEvalProperty') {
      return resolveGlobalObject(expression.object, atCall) === GLOBAL_OBJECT
        ? INTRINSIC_EVAL
        : UNKNOWN_EVAL
    }
    const node = unwrapIdentity(expression)
    if (node.type === 'Identifier') {
      const key = lexical.referenceKey(node)
      if (key === null) {
        if (node.name === 'eval') return INTRINSIC_EVAL
        if (node.name === 'undefined' || node.name === 'NaN' || node.name === 'Infinity') {
          return NON_INTRINSIC_EVAL
        }
        return UNKNOWN_EVAL
      }
      const seenKey = `eval:${key}`
      if (seen.has(seenKey)) return UNKNOWN_EVAL
      const sources = evalSourcesByKey.get(key) ?? []
      const nextSeen = new Set(seen)
      nextSeen.add(seenKey)
      return mergeIdentity(
        reachingSources(sources, atCall).map((source) =>
          resolveEvalIdentity(source.value, atCall, nextSeen)),
        UNKNOWN_EVAL,
      )
    }
    if (node.type === 'MemberExpression' && literalProperty(node)?.name === 'eval') {
      return resolveGlobalObject(node.object, atCall) === GLOBAL_OBJECT
        ? INTRINSIC_EVAL
        : UNKNOWN_EVAL
    }
    if (node.type === 'AssignmentExpression' && node.operator === '=') {
      return resolveEvalIdentity(node.right, atCall, seen)
    }
    if (node.type === 'ConditionalExpression') {
      return mergeIdentity([
        resolveEvalIdentity(node.consequent, atCall, seen),
        resolveEvalIdentity(node.alternate, atCall, seen),
      ], UNKNOWN_EVAL)
    }
    if (node.type === 'LogicalExpression') {
      return mergeIdentity([
        resolveEvalIdentity(node.left, atCall, seen),
        resolveEvalIdentity(node.right, atCall, seen),
      ], UNKNOWN_EVAL)
    }
    if (
      node.type === 'Literal' ||
      node.type === 'TemplateLiteral' ||
      node.type === 'ObjectExpression' ||
      node.type === 'ArrayExpression' ||
      node.type === 'FunctionExpression' ||
      node.type === 'ArrowFunctionExpression' ||
      node.type === 'ClassExpression' ||
      node.type === 'UnaryExpression' ||
      node.type === 'BinaryExpression'
    ) return NON_INTRINSIC_EVAL
    return UNKNOWN_EVAL
  }

  let hasPotentialDirectEval = false
  for (const node of nodes) {
    if (
      node.type !== 'CallExpression' ||
      node.optional === true ||
      node.callee.type !== 'Identifier' ||
      node.callee.name !== 'eval'
    ) continue
    if (resolveEvalIdentity(node.callee, node) !== NON_INTRINSIC_EVAL) {
      hasPotentialDirectEval = true
      break
    }
  }

  function resolve(expression, seen = new Set()) {
    if (!expression) return null
    const node = unwrapTransparent(expression)
    if (node.type === 'Literal' && typeof node.value === 'string') {
      return node.value
    }
    if (
      node.type === 'TemplateLiteral' &&
      node.expressions.length === 0 &&
      node.quasis.length === 1
    ) return node.quasis[0].value.cooked
    if (node.type === 'Identifier') {
      const key = lexical.referenceKey(node)
      if (
        key === null ||
        seen.has(key) ||
        hasPotentialDirectEval ||
        unknownWriteKeys.has(key)
      ) return null
      const assignments = assignmentsByKey.get(key) ?? []
      if (assignments.length === 0) return null
      const nextSeen = new Set(seen)
      nextSeen.add(key)
      const values = assignments.map((assignment) => resolve(assignment, nextSeen))
      const value = values[0]
      return value !== null && values.every((candidate) => candidate === value)
        ? value
        : null
    }
    if (node.type === 'BinaryExpression' && node.operator === '+') {
      const left = resolve(node.left, seen)
      const right = resolve(node.right, seen)
      return left === null || right === null ? null : left + right
    }
    if (node.type === 'ConditionalExpression') {
      const consequent = resolve(node.consequent, seen)
      const alternate = resolve(node.alternate, seen)
      return consequent !== null && consequent === alternate ? consequent : null
    }
    if (node.type === 'LogicalExpression') {
      const left = resolve(node.left, seen)
      const right = resolve(node.right, seen)
      return left !== null && left === right ? left : null
    }
    return null
  }

  return resolve
}

function discoverIndependentTerminalCandidates({
  packageName,
  path,
  source,
  nodes,
  parents,
  lexical,
  byteOffsets,
}) {
  const values = new Map()
  const localFunctions = new Map()
  const staticSpecifier = createStaticStringResolver(nodes, parents, lexical)

  function sameValue(left, right) {
    return left.kind === right.kind &&
      (left.kind !== 'Primitive' || left.primitive === right.primitive)
  }

  function bind(identifier, value) {
    if (!identifier || !value) return false
    const key = lexical.declarationKey(identifier) ?? lexical.referenceKey(identifier)
    if (key === null) {
      throw new Error(
        `${packageName}/${path}: independent terminal accounting found an undeclared capability alias`,
      )
    }
    const previous = values.get(key)
    if (previous && !sameValue(previous, value)) {
      throw new Error(
        `${packageName}/${path}: independent terminal accounting found a divergent capability alias`,
      )
    }
    if (previous) return false
    values.set(key, value)
    return true
  }

  function resolve(expression) {
    if (!expression) return null
    const node = unwrapTransparent(expression)
    if (node.type === 'Identifier') {
      const key = lexical.referenceKey(node)
      if (key === null) {
        if (node.name === 'process') return { kind: 'ProcessNamespace' }
        if (node.name === 'require') return { kind: 'RequireLoader' }
        if (node.name === 'module') return { kind: 'ModuleNamespace' }
        if (['global', 'globalThis'].includes(node.name)) return { kind: 'GlobalObject' }
      }
      return key === null ? null : values.get(key) ?? null
    }
    if (node.type === 'ImportExpression') {
      const specifier = staticSpecifier(node.source)
      if (['child_process', 'node:child_process'].includes(specifier)) {
        return { kind: 'ChildProcessNamespace' }
      }
      return specifier === null ? { kind: 'UnknownModuleNamespace' } : null
    }
    if (node.type === 'CallExpression') {
      const loader = resolve(node.callee)
      const firstArgument = node.arguments[0]
      const specifier = firstArgument?.type === 'SpreadElement'
        ? null
        : staticSpecifier(firstArgument)
      if (loader?.kind !== 'RequireLoader') {
        if (['child_process', 'node:child_process'].includes(specifier)) {
          throw new Error(
            `${packageName}/${path}: independent terminal accounting found an unknown child_process loader at byte ${byteOffsets[node.start]}`,
          )
        }
        return null
      }
      if (['child_process', 'node:child_process'].includes(specifier)) {
        return { kind: 'ChildProcessNamespace' }
      }
      if (specifier === null) {
        if (isPinnedNonControlDynamicLoaderCall({ packageName, path, node, source, parents })) {
          return null
        }
        throw new Error(
          `${packageName}/${path}: independent terminal accounting found an unknown loader specifier at byte ${byteOffsets[node.start]}`,
        )
      }
      return null
    }
    if (node.type !== 'MemberExpression') return null
    const property = literalProperty(node)
    if (
      property?.name === 'process' &&
      node.object.type === 'Identifier' &&
      ['global', 'globalThis'].includes(node.object.name) &&
      lexical.referenceKey(node.object) === null
    ) return { kind: 'ProcessNamespace' }
    if (
      property?.name === 'require' &&
      node.object.type === 'Identifier' &&
      ['global', 'globalThis'].includes(node.object.name) &&
      lexical.referenceKey(node.object) === null
    ) return { kind: 'RequireLoader' }
    const base = resolve(node.object)
    if (!base) return null
    if (!property) {
      if (['GlobalObject', 'UnknownModuleNamespace'].includes(base.kind)) return null
      throw new Error(
        `${packageName}/${path}: independent terminal accounting found a nonliteral capability property at byte ${byteOffsets[node.start]}`,
      )
    }
    if (base.kind === 'GlobalObject') {
      if (property.name === 'process') return { kind: 'ProcessNamespace' }
      if (property.name === 'require') return { kind: 'RequireLoader' }
      if (property.name === 'module') return { kind: 'ModuleNamespace' }
      return null
    }
    if (base.kind === 'ModuleNamespace' && property.name === 'require') {
      return { kind: 'RequireLoader' }
    }
    if (base.kind === 'ModuleNamespace') return null
    if (base.kind === 'RequireLoader') {
      if (property.name === 'main') return { kind: 'ModuleNamespace' }
      if (['cache', 'extensions', 'resolve'].includes(property.name)) return null
      throw new Error(
        `${packageName}/${path}: independent terminal accounting found an unknown require loader step at byte ${byteOffsets[node.start]}`,
      )
    }
    if (base.kind === 'ChildProcessNamespace' && property.name === 'default') {
      return base
    }
    if (base.kind === 'ChildProcessNamespace' && PRIMITIVE_NAMES.has(property.name)) {
      return {
        kind: 'Primitive',
        primitive: PRIMITIVE_NAMES.get(property.name),
        primitiveName: property.name,
      }
    }
    if (base.kind === 'ChildProcessNamespace') {
      throw new Error(
        `${packageName}/${path}: independent terminal accounting found an unknown child_process step at byte ${byteOffsets[node.start]}`,
      )
    }
    if (base.kind === 'ProcessNamespace' && property.name === 'kill') {
      return {
        kind: 'Primitive',
        primitive: 'ProcessKill',
        primitiveName: 'kill',
      }
    }
    if (base.kind === 'ProcessNamespace' && property.name === 'getBuiltinModule') {
      return { kind: 'RequireLoader' }
    }
    if (base.kind === 'ProcessNamespace' && property.name === 'mainModule') {
      return { kind: 'ModuleNamespace' }
    }
    if (base.kind === 'ProcessNamespace') return null
    if (
      base.kind === 'UnknownModuleNamespace' &&
      PRIMITIVE_NAMES.has(property.name)
    ) {
      throw new Error(
        `${packageName}/${path}: independent terminal accounting found an unknown module process primitive ${property.name} at byte ${byteOffsets[node.start]}`,
      )
    }
    return null
  }

  for (const node of nodes) {
    if (node.type === 'FunctionDeclaration' && node.id?.type === 'Identifier') {
      const key = lexical.declarationKey(node.id)
      if (key !== null) localFunctions.set(key, node)
    } else if (
      node.type === 'VariableDeclarator' &&
      node.id.type === 'Identifier' &&
      (node.init?.type === 'FunctionExpression' || node.init?.type === 'ArrowFunctionExpression')
    ) {
      const key = lexical.declarationKey(node.id)
      if (key !== null) localFunctions.set(key, node.init)
    }
    if (node.type !== 'ImportDeclaration') continue
    if (!['child_process', 'node:child_process'].includes(node.source.value)) continue
    for (const specifier of node.specifiers) {
      if (specifier.type === 'ImportSpecifier') {
        const imported = specifier.imported.name ?? specifier.imported.value
        if (!PRIMITIVE_NAMES.has(imported)) {
          throw new Error(
            `${packageName}/${path}: independent terminal accounting found an unknown child_process import`,
          )
        }
        bind(specifier.local, {
          kind: 'Primitive',
          primitive: PRIMITIVE_NAMES.get(imported),
          primitiveName: imported,
        })
      } else {
        bind(specifier.local, { kind: 'ChildProcessNamespace' })
      }
    }
  }

  let changed = true
  let passes = 0
  while (changed) {
    changed = false
    passes += 1
    if (passes > nodes.length + 1) {
      throw new Error(
        `${packageName}/${path}: independent terminal accounting did not converge`,
      )
    }
    for (const node of nodes) {
      if (node.type === 'VariableDeclarator') {
        const value = resolve(node.init)
        if (!value) continue
        if (node.id.type === 'Identifier') {
          changed = bind(node.id, value) || changed
          continue
        }
        if (node.id.type !== 'ObjectPattern') {
          throw new Error(
            `${packageName}/${path}: independent terminal accounting found an unknown capability pattern`,
          )
        }
        for (const property of node.id.properties) {
          if (
            property.type !== 'Property' ||
            property.computed ||
            property.kind !== 'init'
          ) {
            throw new Error(
              `${packageName}/${path}: independent terminal accounting found an unknown capability destructure`,
            )
          }
          const name = property.key.name ?? property.key.value
          const local = property.value.type === 'Identifier' ? property.value : null
          if (value.kind === 'ChildProcessNamespace') {
            if (!local || !PRIMITIVE_NAMES.has(name)) {
              throw new Error(
                `${packageName}/${path}: independent terminal accounting found an unknown child_process destructure`,
              )
            }
            changed = bind(local, {
              kind: 'Primitive',
              primitive: PRIMITIVE_NAMES.get(name),
              primitiveName: name,
            }) || changed
          } else if (value.kind === 'ProcessNamespace' && name === 'kill') {
            if (!local) throw new Error(`${packageName}/${path}: invalid process.kill destructure`)
            changed = bind(local, {
              kind: 'Primitive',
              primitive: 'ProcessKill',
              primitiveName: 'kill',
            }) || changed
          } else if (value.kind === 'ModuleNamespace' && name === 'require') {
            if (!local) throw new Error(`${packageName}/${path}: invalid module.require destructure`)
            changed = bind(local, { kind: 'RequireLoader' }) || changed
          } else if (value.kind === 'GlobalObject') {
            const globalKinds = new Map([
              ['process', 'ProcessNamespace'],
              ['require', 'RequireLoader'],
              ['module', 'ModuleNamespace'],
            ])
            const kind = globalKinds.get(name)
            if (kind) {
              if (!local) {
                throw new Error(
                  `${packageName}/${path}: invalid global capability destructure`,
                )
              }
              changed = bind(local, { kind }) || changed
            }
          } else if (
            value.kind === 'UnknownModuleNamespace' &&
            PRIMITIVE_NAMES.has(name)
          ) {
            throw new Error(
              `${packageName}/${path}: independent terminal accounting found an unknown module primitive destructure`,
            )
          }
        }
      } else if (
        node.type === 'AssignmentExpression' &&
        node.operator === '=' &&
        node.left.type === 'Identifier'
      ) {
        const value = resolve(node.right)
        if (value) changed = bind(node.left, value) || changed
      } else if (node.type === 'CallExpression') {
        const callee = unwrapTransparent(node.callee)
        if (callee.type !== 'Identifier') continue
        const key = lexical.referenceKey(callee)
        const target = key === null ? null : localFunctions.get(key)
        if (!target) continue
        for (let index = 0; index < node.arguments.length; index += 1) {
          const argument = node.arguments[index]
          if (argument.type === 'SpreadElement') continue
          const value = resolve(argument)
          if (!value) continue
          const parameter = target.params[index]
          if (!parameter || parameter.type !== 'Identifier') {
            throw new Error(
              `${packageName}/${path}: independent terminal accounting found an unknown parameter flow`,
            )
          }
          changed = bind(parameter, value) || changed
        }
      }
    }
  }

  const directCallsByFunction = new Map()
  for (const node of nodes) {
    if (node.type !== 'CallExpression') continue
    const value = resolve(node.callee)
    if (value?.kind !== 'Primitive') continue
    let owner = node
    while ((owner = parents.get(owner))) {
      if (
        owner.type === 'FunctionDeclaration' ||
        owner.type === 'FunctionExpression' ||
        owner.type === 'ArrowFunctionExpression'
      ) break
    }
    if (!owner) continue
    const calls = directCallsByFunction.get(owner) ?? []
    calls.push({ node, value })
    directCallsByFunction.set(owner, calls)
  }

  const wrappers = new Map()
  for (const node of nodes) {
    if (node.type !== 'FunctionDeclaration' || node.id?.name !== 'spawnAsync') continue
    const direct = directCallsByFunction.get(node) ?? []
    if (direct.length !== 1 || direct[0].value.primitive !== 'ChildProcessSpawn') {
      continue
    }
    const key = lexical.declarationKey(node.id)
    if (key !== null) {
      wrappers.set(key, {
        primitive: 'ChildProcessSpawn',
        rawCall: direct[0].node,
        escaped: bindingEscapesDirectCalls(nodes, parents, lexical, key),
      })
    }
  }

  const candidates = new Map()
  for (const node of nodes) {
    if (node.type !== 'CallExpression') continue
    let value = resolve(node.callee)
    let rawCall = null
    if (value?.kind !== 'Primitive') {
      const key = node.callee.type === 'Identifier'
        ? lexical.referenceKey(node.callee)
        : null
      const wrapper = key === null ? null : wrappers.get(key)
      if (!wrapper) continue
      value = {
        kind: 'Primitive',
        primitive: wrapper.primitive,
      }
      rawCall = wrapper.rawCall
    }
    const rawWrapper = [...wrappers.values()].find((wrapper) => wrapper.rawCall === node)
    if (rawWrapper && !rawWrapper.escaped) continue
    const key = `${byteOffsets[node.start]}:${byteOffsets[node.end]}`
    const previous = candidates.get(key)
    if (previous && previous.primitive !== value.primitive) {
      throw new Error(
        `${packageName}/${path}: independent terminal candidate has divergent primitives at ${key}`,
      )
    }
    candidates.set(key, {
      node,
      primitive: value.primitive,
      rawCall,
    })
  }
  return candidates
}

function assertIndependentRootCapabilityConsumption({
  packageName,
  path,
  source,
  nodes,
  parents,
  lexical,
  byteOffsets,
  callsites,
}) {
  const lockedCalls = new Map(
    callsites.map((callsite) => [
      `${callsite.startByte}:${callsite.endByte}`,
      callsite.primitive,
    ]),
  )
  const referencesByKey = new Map()
  const assignmentsByKey = new Map()
  const localFunctions = new Map()
  const staticSpecifier = createStaticStringResolver(nodes, parents, lexical)

  function append(map, key, value) {
    const entries = map.get(key) ?? []
    entries.push(value)
    map.set(key, entries)
  }

  for (const node of nodes) {
    if (node.type === 'Identifier') {
      const key = lexical.referenceKey(node)
      const parent = parents.get(node)
      const isPatternIdentifier =
        parent?.type === 'Property' && parents.get(parent)?.type === 'ObjectPattern'
      if (
        key !== null &&
        !lexical.isDeclaration(node) &&
        !isPatternIdentifier &&
        isReferencePosition(node, parent)
      ) append(referencesByKey, key, node)
    }
    if (
      node.type === 'VariableDeclarator' &&
      node.id.type === 'Identifier' &&
      node.init
    ) {
      const key = lexical.declarationKey(node.id)
      if (key !== null) append(assignmentsByKey, key, node.init)
      if (node.init.type === 'FunctionExpression' || node.init.type === 'ArrowFunctionExpression') {
        if (key !== null) localFunctions.set(key, node.init)
      }
    } else if (
      node.type === 'AssignmentExpression' &&
      node.operator === '=' &&
      node.left.type === 'Identifier'
    ) {
      const key = lexical.referenceKey(node.left)
      if (key !== null) append(assignmentsByKey, key, node.right)
    } else if (node.type === 'FunctionDeclaration' && node.id?.type === 'Identifier') {
      const key = lexical.declarationKey(node.id)
      if (key !== null) localFunctions.set(key, node)
    }
  }

  function fail(node, detail) {
    throw new Error(
      `${packageName}/${path}: independent root capability escape (${detail}) at byte ${byteOffsets[node.start]}`,
    )
  }

  function isPinnedNonControlDynamicImport(node) {
    return Boolean(
      packageName === 'playwright' &&
      path === 'lib/transform/babelBundle.js' &&
      enclosingFunctionName(node, parents) === 'import_' &&
      source.slice(node.start, node.end) === 'import(filepath)',
    )
  }

  function isPinnedNonControlRootConsumer(node, parent) {
    return Boolean(
      packageName === 'playwright-core' &&
      path === 'lib/coreBundle.js' &&
      parent?.type === 'CallExpression' &&
      parent.arguments[0] === node &&
      source.slice(parent.callee.start, parent.callee.end) ===
        'eventsHelper.addEventListener' &&
      parent.arguments[1]?.type === 'Literal' &&
      parent.arguments[1].value === 'exit',
    )
  }

  function isPinnedNonControlRequireRoot(node, parent) {
    return Boolean(
      packageName === 'playwright' &&
      path === 'lib/transform/babelBundle.js' &&
      parent?.type === 'CallExpression' &&
      parent.arguments.includes(node) &&
      source.slice(parent.callee.start, parent.callee.end) ===
        '0, _rewriteStackTrace.endHiddenCallStack',
    )
  }

  function isGlobalProcessExpression(expression) {
    const node = unwrapTransparent(expression)
    if (
      node.type === 'Identifier' &&
      node.name === 'process' &&
      lexical.referenceKey(node) === null
    ) return true
    if (node.type !== 'MemberExpression') return false
    const property = literalProperty(node)
    return Boolean(
      property?.name === 'process' &&
      node.object.type === 'Identifier' &&
      ['global', 'globalThis'].includes(node.object.name) &&
      lexical.referenceKey(node.object) === null,
    )
  }

  function isProcessNamespaceExpression(expression, seen = new Set()) {
    const node = unwrapTransparent(expression)
    if (isGlobalProcessExpression(node)) return true
    if (node.type !== 'Identifier') return false
    const key = lexical.referenceKey(node)
    if (key === null || seen.has(key)) return false
    const assignments = assignmentsByKey.get(key) ?? []
    if (assignments.length === 0) return false
    const nextSeen = new Set(seen)
    nextSeen.add(key)
    return assignments.every((assignment) =>
      isProcessNamespaceExpression(assignment, nextSeen))
  }

  function isProvenSymbolNamespace(expression, seen = new Set()) {
    const node = unwrapTransparent(expression)
    if (node.type === 'Identifier') {
      if (node.name === 'Symbol' && lexical.referenceKey(node) === null) return true
      const key = lexical.referenceKey(node)
      if (key === null || seen.has(key)) return false
      const assignments = assignmentsByKey.get(key) ?? []
      if (assignments.length === 0) return false
      const nextSeen = new Set(seen)
      nextSeen.add(key)
      return assignments.every((assignment) =>
        isProvenSymbolNamespace(assignment, nextSeen))
    }
    if (node.type === 'LogicalExpression' && node.operator === '||') {
      return isProvenSymbolNamespace(node.left, seen) &&
        isProvenSymbolNamespace(node.right, seen)
    }
    if (node.type !== 'MemberExpression') return false
    const property = literalProperty(node)
    if (
      !property ||
      node.object.type !== 'Identifier' ||
      !['global', 'globalThis'].includes(node.object.name) ||
      lexical.referenceKey(node.object) !== null
    ) return false
    if (property.name === 'Symbol') return true
    return Boolean(
      packageName === 'playwright' &&
      path === 'lib/matchers/expect.js' &&
      property.name === 'jest-symbol-do-not-touch',
    )
  }

  function isProvenNonCapabilityGlobalKey(expression, seen = new Set()) {
    const node = unwrapTransparent(expression)
    if (
      packageName === 'playwright' &&
      path === 'lib/transform/babelBundle.js' &&
      enclosingFunctionName(node, parents) === '_evaluate' &&
      ['callee.node.name', 'object.node.name'].includes(
        source.slice(node.start, node.end),
      )
    ) return true
    const specifier = staticSpecifier(node)
    if (specifier !== null) {
      return !['process', 'require', 'module'].includes(specifier)
    }
    if (node.type === 'Identifier') {
      const key = lexical.referenceKey(node)
      if (key === null || seen.has(key)) return false
      const assignments = assignmentsByKey.get(key) ?? []
      if (assignments.length === 0) return false
      const nextSeen = new Set(seen)
      nextSeen.add(key)
      return assignments.every((assignment) =>
        isProvenNonCapabilityGlobalKey(assignment, nextSeen))
    }
    if (node.type !== 'CallExpression') return false
    const callee = unwrapTransparent(node.callee)
    if (
      callee.type === 'Identifier' &&
      callee.name === 'Symbol' &&
      lexical.referenceKey(callee) === null
    ) return true
    return Boolean(
      callee.type === 'MemberExpression' &&
      literalProperty(callee)?.name === 'for' &&
      isProvenSymbolNamespace(callee.object),
    )
  }

  function isNarrowNonControlGlobalConsumer(node, parent, argumentIndex) {
    if (argumentIndex !== 0) return false
    const callee = unwrapTransparent(parent.callee)
    if (
      callee.type !== 'MemberExpression' ||
      literalProperty(callee)?.name !== 'defineProperty' ||
      callee.object.type !== 'Identifier' ||
      callee.object.name !== 'Object' ||
      lexical.referenceKey(callee.object) !== null
    ) return false
    const key = parent.arguments[1]
    return Boolean(
      key &&
      key.type !== 'SpreadElement' &&
      isProvenNonCapabilityGlobalKey(key),
    )
  }

  function isBooleanOnlyFlow(node) {
    let current = node
    let parent = parents.get(current)
    while (
      parent?.type === 'LogicalExpression' &&
      (parent.left === current || parent.right === current)
    ) {
      current = parent
      parent = parents.get(current)
    }
    if (!parent) return false
    if (parent.type === 'ConditionalExpression' && parent.test === current) return true
    if (parent.type === 'IfStatement' && parent.test === current) return true
    if (parent.type === 'WhileStatement' && parent.test === current) return true
    if (parent.type === 'DoWhileStatement' && parent.test === current) return true
    if (parent.type === 'ForStatement' && parent.test === current) return true
    if (
      parent.type === 'UnaryExpression' &&
      parent.operator === '!' &&
      parent.argument === current
    ) return true
    return Boolean(
      parent.type === 'BinaryExpression' &&
      ['===', '!==', '==', '!='].includes(parent.operator),
    )
  }

  function isBooleanPredicateReturnFlow(node) {
    let current = node
    let parent = parents.get(current)
    while (
      parent?.type === 'LogicalExpression' &&
      (parent.left === current || parent.right === current)
    ) {
      current = parent
      parent = parents.get(current)
    }
    if (parent?.type !== 'ReturnStatement' || parent.argument !== current) return false
    let owner = parent
    while ((owner = parents.get(owner))) {
      if (
        owner.type === 'FunctionDeclaration' ||
        owner.type === 'FunctionExpression' ||
        owner.type === 'ArrowFunctionExpression'
      ) break
    }
    if (!owner) return false
    let key = null
    if (owner.id?.type === 'Identifier') key = lexical.declarationKey(owner.id)
    if (key === null) {
      const declaration = parents.get(owner)
      if (declaration?.type === 'VariableDeclarator' && declaration.id.type === 'Identifier') {
        key = lexical.declarationKey(declaration.id)
      }
    }
    if (key === null) return false
    const references = referencesByKey.get(key) ?? []
    return references.length > 0 && references.every((reference) => {
      const call = parents.get(reference)
      return call?.type === 'CallExpression' &&
        call.callee === reference &&
        isBooleanOnlyFlow(call)
    })
  }

  function isSafeProcessMethodExpression(expression, seen = new Set()) {
    const node = unwrapTransparent(expression)
    if (node.type === 'Identifier') {
      const key = lexical.referenceKey(node)
      if (key === null || seen.has(key)) return false
      const assignments = assignmentsByKey.get(key) ?? []
      if (assignments.length === 0) return false
      const nextSeen = new Set(seen)
      nextSeen.add(key)
      return assignments.every((assignment) =>
        isSafeProcessMethodExpression(assignment, nextSeen))
    }
    if (node.type !== 'MemberExpression') return false
    const property = literalProperty(node)
    return Boolean(
      property &&
      property.name !== 'kill' &&
      isProcessNamespaceExpression(node.object),
    )
  }

  function isApprovedRawSpawnWrapper(callNode, primitive) {
    if (primitive !== 'ChildProcessSpawn') return false
    let owner = callNode
    while ((owner = parents.get(owner))) {
      if (
        owner.type === 'FunctionDeclaration' ||
        owner.type === 'FunctionExpression' ||
        owner.type === 'ArrowFunctionExpression'
      ) break
    }
    if (owner?.type !== 'FunctionDeclaration' || owner.id?.name !== 'spawnAsync') {
      return false
    }
    const key = lexical.declarationKey(owner.id)
    if (key === null) return false
    const references = referencesByKey.get(key) ?? []
    if (references.length === 0) return false
    return references.every((reference) => {
      const parent = parents.get(reference)
      if (parent?.type !== 'CallExpression' || parent.callee !== reference) return false
      return lockedCalls.get(`${byteOffsets[parent.start]}:${byteOffsets[parent.end]}`) ===
        'ChildProcessSpawn'
    })
  }

  const bindingStack = new Set()
  const completedNodes = new Set()

  function consumeBinding(identifier, kind) {
    const key = lexical.declarationKey(identifier) ?? lexical.referenceKey(identifier)
    if (key === null) fail(identifier, `unresolved ${kind} binding`)
    const stackKey = `${key}:${kind}`
    if (bindingStack.has(stackKey)) fail(identifier, `${kind} alias cycle`)
    const references = referencesByKey.get(key) ?? []
    if (references.length === 0) fail(identifier, `unconsumed ${kind} alias`)
    bindingStack.add(stackKey)
    try {
      for (const reference of references) consume(reference, kind)
    } finally {
      bindingStack.delete(stackKey)
    }
  }

  function consumePattern(pattern, kind, origin) {
    if (pattern.type !== 'ObjectPattern') fail(origin, `unsupported ${kind} pattern`)
    for (const property of pattern.properties) {
      if (
        property.type !== 'Property' ||
        property.computed ||
        property.kind !== 'init' ||
        property.value.type !== 'Identifier'
      ) fail(property, `unsupported ${kind} destructure`)
      const name = property.key.name ?? property.key.value
      let nestedKind = null
      if (kind === 'GlobalObject') {
        if (name === 'process') nestedKind = 'ProcessNamespace'
        else if (name === 'require') nestedKind = 'RequireLoader'
        else if (name === 'module') nestedKind = 'ModuleNamespace'
      } else if (kind === 'ProcessNamespace' && name === 'kill') {
        nestedKind = 'ProcessKill'
      } else if (kind === 'ModuleNamespace' && name === 'require') {
        nestedKind = 'RequireLoader'
      } else if (kind === 'ChildProcessNamespace') {
        if (name === 'default') nestedKind = 'ChildProcessNamespace'
        else if (PRIMITIVE_NAMES.has(name)) nestedKind = PRIMITIVE_NAMES.get(name)
        else fail(property, 'unknown child_process destructure')
      }
      if (nestedKind) consumeBinding(property.value, nestedKind)
    }
  }

  function consume(node, kind) {
    const completionKey = `${node.start}:${node.end}:${kind}`
    if (completedNodes.has(completionKey)) return
    completedNodes.add(completionKey)
    const parent = parents.get(node)
    if (!parent) fail(node, `unconsumed ${kind} root`)

    if (
      (parent.type === 'AwaitExpression' && parent.argument === node) ||
      (parent.type === 'ChainExpression' && parent.expression === node)
    ) {
      consume(parent, kind)
      return
    }
    if (parent.type === 'SequenceExpression' && parent.expressions.includes(node)) {
      if (parent.expressions.at(-1) === node) consume(parent, kind)
      return
    }
    if (isBooleanOnlyFlow(node) || isBooleanPredicateReturnFlow(node)) return
    if (
      parent.type === 'CallExpression' &&
      parent.arguments[0] === node &&
      parent.callee.type === 'Identifier' &&
      ['__toESM', '__toCommonJS'].includes(parent.callee.name)
    ) {
      consume(parent, kind)
      return
    }
    if (parent.type === 'VariableDeclarator' && parent.init === node) {
      if (parent.id.type === 'Identifier') consumeBinding(parent.id, kind)
      else consumePattern(parent.id, kind, node)
      return
    }
    if (parent.type === 'AssignmentExpression' && parent.right === node) {
      if (parent.operator !== '=' || parent.left.type !== 'Identifier') {
        fail(node, `unsupported ${kind} assignment`)
      }
      consumeBinding(parent.left, kind)
      return
    }
    if (parent.type === 'AssignmentPattern' && parent.right === node) {
      fail(node, `${kind} default-parameter flow`)
    }
    if (parent.type === 'Property' && parent.value === node) {
      fail(node, `${kind} object-property flow`)
    }
    if (
      (parent.type === 'ReturnStatement' && parent.argument === node) ||
      ((parent.type === 'ArrowFunctionExpression') && parent.body === node)
    ) fail(node, `${kind} return flow`)
    if (parent.type === 'LogicalExpression') {
      if (isBooleanOnlyFlow(node)) return
      if (kind === 'GlobalObject') {
        consume(parent, kind)
        return
      }
      if (
        kind === 'ProcessNamespace' &&
        parent.operator === '||' &&
        parent.left === node &&
        parent.right.type === 'ObjectExpression' &&
        parent.right.properties.length === 0
      ) {
        consume(parent, kind)
        return
      }
      fail(node, `${kind} conditional flow`)
    }
    if (parent.type === 'ConditionalExpression') {
      if (parent.test === node) return
      if (kind === 'GlobalObject') {
        consume(parent, kind)
        return
      }
      fail(node, `${kind} conditional flow`)
    }

    if (parent.type === 'MemberExpression' && parent.object === node) {
      const property = literalProperty(parent)
      if (!property) {
        if (
          kind === 'GlobalObject' &&
          parent.computed &&
          isProvenNonCapabilityGlobalKey(parent.property)
        ) return
        fail(parent, `nonliteral ${kind} property`)
      }
      if (kind === 'GlobalObject') {
        if (property.name === 'process') consume(parent, 'ProcessNamespace')
        else if (property.name === 'require') consume(parent, 'RequireLoader')
        else if (property.name === 'module') consume(parent, 'ModuleNamespace')
        return
      }
      if (kind === 'ProcessNamespace') {
        if (property.name === 'kill') consume(parent, 'ProcessKill')
        else if (property.name === 'mainModule') consume(parent, 'ModuleNamespace')
        else if (property.name === 'getBuiltinModule') consume(parent, 'RequireLoader')
        return
      }
      if (kind === 'ModuleNamespace') {
        if (property.name === 'require') consume(parent, 'RequireLoader')
        else if (![
          'exports',
          'filename',
          'id',
          'loaded',
          'path',
          'paths',
        ].includes(property.name)) fail(parent, `unknown module property ${property.name}`)
        return
      }
      if (kind === 'RequireLoader') {
        if (property.name === 'main') consume(parent, 'ModuleNamespace')
        else if (!['cache', 'extensions', 'resolve'].includes(property.name)) {
          fail(parent, `unknown require property ${property.name}`)
        }
        return
      }
      if (kind === 'ChildProcessNamespace') {
        if (property.name === 'default') consume(parent, kind)
        else if (PRIMITIVE_NAMES.has(property.name)) {
          consume(parent, PRIMITIVE_NAMES.get(property.name))
        } else fail(parent, `unknown child_process property ${property.name}`)
        return
      }
      fail(parent, `${kind} property escape`)
    }

    if (parent.type === 'CallExpression') {
      if (parent.callee === node) {
        if (kind === 'RequireLoader') {
          const firstArgument = parent.arguments[0]
          const specifier = firstArgument?.type === 'SpreadElement'
            ? null
            : staticSpecifier(firstArgument)
          if (['child_process', 'node:child_process'].includes(specifier)) {
            consume(parent, 'ChildProcessNamespace')
          }
          if (specifier === null) {
            if (isPinnedNonControlDynamicLoaderCall({
              packageName,
              path,
              node: parent,
              source,
              parents,
            })) {
              return
            }
            fail(parent, 'unknown loader specifier')
          }
          return
        }
        if (PRIMITIVE_ORDER.includes(kind)) {
          const locked = lockedCalls.get(
            `${byteOffsets[parent.start]}:${byteOffsets[parent.end]}`,
          )
          if (locked === kind || isApprovedRawSpawnWrapper(parent, kind)) return
          fail(parent, `unlocked terminal ${kind}`)
        }
        fail(parent, `${kind} call escape`)
      }
      const argumentIndex = parent.arguments.indexOf(node)
      if (argumentIndex !== -1) {
        const callee = unwrapTransparent(parent.callee)
        const functionKey = callee.type === 'Identifier'
          ? lexical.referenceKey(callee)
          : null
        const target = (
          callee.type === 'FunctionExpression' ||
          callee.type === 'ArrowFunctionExpression'
        )
          ? callee
          : functionKey === null
            ? null
            : localFunctions.get(functionKey)
        if (target) {
          const parameter = target.params[argumentIndex]
          if (parameter?.type !== 'Identifier') {
            fail(node, `unsupported ${kind} parameter flow`)
          }
          const parameterKey = lexical.declarationKey(parameter)
          if (
            parameterKey !== null &&
            (referencesByKey.get(parameterKey) ?? []).length === 0
          ) return
          consumeBinding(parameter, kind)
          return
        }
        const calleeProperty = callee.type === 'MemberExpression'
          ? literalProperty(callee)
          : null
        if (
          kind === 'ProcessNamespace' &&
          argumentIndex === 0 &&
          ['call', 'bind'].includes(calleeProperty?.name) &&
          isSafeProcessMethodExpression(callee.object)
        ) return
        if (kind === 'ProcessNamespace' && isPinnedNonControlRootConsumer(node, parent)) {
          return
        }
        if (kind === 'RequireLoader' && isPinnedNonControlRequireRoot(node, parent)) {
          return
        }
        if (
          kind === 'GlobalObject' &&
          isNarrowNonControlGlobalConsumer(node, parent, argumentIndex)
        ) return
        fail(node, `${kind} argument escape`)
      }
    }

    if (
      parent.type === 'UnaryExpression' &&
      parent.argument === node &&
      ['typeof', 'void'].includes(parent.operator)
    ) return
    if (
      parent.type === 'BinaryExpression' &&
      ['===', '!==', '==', '!='].includes(parent.operator)
    ) return
    if (
      parent.type === 'BinaryExpression' &&
      parent.operator === 'in' &&
      parent.right === node
    ) return
    fail(node, `unsupported ${kind} flow through ${parent.type}`)
  }

  const globalObjectTouchMemo = new Map()
  function globalObjectTouchesCapability(node, bindingSeen = new Set()) {
    const memoKey = `${node.start}:${node.end}`
    if (globalObjectTouchMemo.has(memoKey)) return globalObjectTouchMemo.get(memoKey)
    const parent = parents.get(node)
    if (!parent) return true
    let result = false
    if (parent.type === 'MemberExpression' && parent.object === node) {
      const property = literalProperty(parent)
      result = property
        ? ['process', 'require', 'module'].includes(property.name)
        : !(
            parent.computed &&
            isProvenNonCapabilityGlobalKey(parent.property)
          )
    } else if (parent.type === 'VariableDeclarator' && parent.init === node) {
      if (parent.id.type === 'ObjectPattern') {
        result = parent.id.properties.some((property) =>
          property.type !== 'Property' ||
          property.computed ||
          ['process', 'require', 'module'].includes(property.key.name ?? property.key.value))
      } else if (parent.id.type === 'Identifier') {
        const key = lexical.declarationKey(parent.id)
        if (key === null || bindingSeen.has(key)) result = true
        else {
          const nextSeen = new Set(bindingSeen)
          nextSeen.add(key)
          result = (referencesByKey.get(key) ?? []).some((reference) =>
            globalObjectTouchesCapability(reference, nextSeen))
        }
      } else result = true
    } else if (parent.type === 'AssignmentExpression' && parent.right === node) {
      if (parent.left.type !== 'Identifier') result = true
      else {
        const key = lexical.referenceKey(parent.left)
        if (key === null || bindingSeen.has(key)) result = true
        else {
          const nextSeen = new Set(bindingSeen)
          nextSeen.add(key)
          result = (referencesByKey.get(key) ?? []).some((reference) =>
            globalObjectTouchesCapability(reference, nextSeen))
        }
      }
    } else if (
      (parent.type === 'ConditionalExpression' || parent.type === 'LogicalExpression')
    ) result = isBooleanOnlyFlow(node)
      ? false
      : globalObjectTouchesCapability(parent, bindingSeen)
    else if (
      parent.type === 'BinaryExpression' &&
      parent.operator === 'in' &&
      parent.right === node
    ) result = false
    else if (
      parent.type === 'UnaryExpression' &&
      parent.operator === 'typeof'
    ) result = false
    else result = true
    globalObjectTouchMemo.set(memoKey, result)
    return result
  }

  for (const node of nodes) {
    if (
      node.type === 'Identifier' &&
      lexical.referenceKey(node) === null &&
      isReferencePosition(node, parents.get(node))
    ) {
      if (node.name === 'process') consume(node, 'ProcessNamespace')
      else if (node.name === 'module') consume(node, 'ModuleNamespace')
      else if (node.name === 'require') consume(node, 'RequireLoader')
      else if (
        ['global', 'globalThis'].includes(node.name) &&
        globalObjectTouchesCapability(node)
      ) consume(node, 'GlobalObject')
    } else if (node.type === 'ImportExpression') {
      const specifier = staticSpecifier(node.source)
      if (['child_process', 'node:child_process'].includes(specifier)) {
        consume(node, 'ChildProcessNamespace')
      } else if (specifier === null && !isPinnedNonControlDynamicImport(node)) {
        fail(node, 'unknown dynamic import root')
      }
    }
  }
}

// This is the executable full-instrumentation gate, not a frozen expected
// count. It runs a second capability analysis and requires every independently
// discovered terminal candidate to map one-to-one to a lock entry.
function createTerminalInstrumentationAccounting({
  packageName,
  path,
  bytes,
  nodes,
  parents,
  lexical,
  byteOffsets,
  callsites,
}) {
  const sourceSize = bytes.length
  const source = bytes.toString('utf8')
  assertIndependentRootCapabilityConsumption({
    packageName,
    path,
    source,
    nodes,
    parents,
    lexical,
    byteOffsets,
    callsites,
  })
  const terminalCandidates = discoverIndependentTerminalCandidates({
    packageName,
    path,
    source,
    nodes,
    parents,
    lexical,
    byteOffsets,
  })
  const nodesBySpan = new Map()
  for (const node of nodes) {
    const key = `${byteOffsets[node.start]}:${byteOffsets[node.end]}`
    const entries = nodesBySpan.get(key) ?? []
    entries.push(node)
    nodesBySpan.set(key, entries)
  }
  const callNodesBySpan = new Map()
  for (const node of nodes) {
    if (node.type !== 'CallExpression') continue
    const key = `${byteOffsets[node.start]}:${byteOffsets[node.end]}`
    const entries = callNodesBySpan.get(key) ?? []
    entries.push(node)
    callNodesBySpan.set(key, entries)
  }

  const expectedNodeKinds = Object.freeze({
    Import: new Set([
      'Identifier',
      'MemberExpression',
      'ImportDeclaration',
      'ImportExpression',
    ]),
    Require: new Set(['CallExpression']),
    Destructure: new Set(['Property', 'ImportSpecifier']),
    Property: new Set(['MemberExpression']),
    ComputedLiteral: new Set(['MemberExpression']),
    Assignment: new Set(['Identifier']),
    Parameter: new Set(['Identifier']),
    Return: new Set([
      'FunctionDeclaration',
      'FunctionExpression',
      'ArrowFunctionExpression',
    ]),
    Call: new Set(['CallExpression']),
  })
  const primitiveNames = new Map(
    [...PRIMITIVE_NAMES].map(([name, primitive]) => [primitive, name]),
  )
  const seenTerminalNodes = new Set()
  const seenMappings = new Set()
  const mappings = []
  for (const callsite of callsites) {
    if (
      callsite.startByte < 0 ||
      callsite.endByte <= callsite.startByte ||
      callsite.endByte > sourceSize
    ) {
      throw new Error(
        `${packageName}/${path}: terminal instrumentation span is outside source`,
      )
    }
    const spanKey = `${callsite.startByte}:${callsite.endByte}`
    const callNodes = callNodesBySpan.get(spanKey) ?? []
    const candidate = terminalCandidates.get(spanKey)
    if (callNodes.length !== 1 || !candidate || candidate.node !== callNodes[0]) {
      throw new Error(
        `${packageName}/${path}: terminal instrumentation has no unique AST call at ${spanKey}`,
      )
    }
    if (candidate.primitive !== callsite.primitive) {
      throw new Error(
        `${packageName}/${path}: terminal AST primitive differs from its lock entry at ${spanKey}`,
      )
    }
    if (seenTerminalNodes.has(callNodes[0])) {
      throw new Error(
        `${packageName}/${path}: terminal AST call maps to more than one lock entry at ${spanKey}`,
      )
    }
    seenTerminalNodes.add(callNodes[0])
    if (callsite.bindingTrace.length === 0) {
      throw new Error(`${packageName}/${path}: terminal binding trace is empty`)
    }
    const first = callsite.bindingTrace[0]
    if (
      !(
        first.kind === 'Import' &&
        ['process', 'child_process', 'node:child_process'].includes(first.name)
      ) &&
      !(
        first.kind === 'Require' &&
        ['child_process', 'node:child_process'].includes(first.name)
      )
    ) {
      throw new Error(
        `${packageName}/${path}: terminal trace does not start at a process capability`,
      )
    }
    for (const step of callsite.bindingTrace) {
      if (
        step.startByte < 0 ||
        step.endByte <= step.startByte ||
        step.endByte > sourceSize
      ) {
        throw new Error(`${packageName}/${path}: terminal trace span is outside source`)
      }
      const spanNodes = nodesBySpan.get(`${step.startByte}:${step.endByte}`) ?? []
      const accepted = expectedNodeKinds[step.kind]
      if (!accepted || !spanNodes.some(({ type }) => accepted.has(type))) {
        throw new Error(
          `${packageName}/${path}: terminal trace step ${step.kind} has no matching AST node`,
        )
      }
    }
    const last = callsite.bindingTrace.at(-1)
    if (
      last.kind !== 'Call' ||
      last.startByte !== callsite.startByte ||
      last.endByte !== callsite.endByte ||
      last.name !== primitiveNames.get(callsite.primitive)
    ) {
      throw new Error(
        `${packageName}/${path}: terminal trace does not end at its locked call`,
      )
    }
    const mapping = canonicalAsciiJson({
      package: packageName,
      path,
      fileSha256: callsite.fileSha256,
      startByte: callsite.startByte,
      endByte: callsite.endByte,
      primitive: callsite.primitive,
      bindingTrace: callsite.bindingTrace,
    })
    if (seenMappings.has(mapping)) {
      throw new Error(`${packageName}/${path}: duplicate terminal instrumentation mapping`)
    }
    seenMappings.add(mapping)
    mappings.push(mapping)
  }
  if (seenTerminalNodes.size !== terminalCandidates.size) {
    throw new Error(
      `${packageName}/${path}: independent terminal accounting found ${terminalCandidates.size} candidates but mapped ${seenTerminalNodes.size}`,
    )
  }
  return {
    terminalCalls: terminalCandidates.size,
    mappedTerminalCalls: seenMappings.size,
    mappings,
  }
}

export function assertPlaywrightTerminalCandidateAccounting({
  packageName,
  path,
  bytes,
  callsites,
}) {
  const source = bytes.toString('utf8')
  if (!Buffer.from(source, 'utf8').equals(bytes)) {
    throw new Error(`${packageName}/${path}: JavaScript source is not canonical UTF-8`)
  }
  const ast = parseJavaScript(source, `${packageName}/${path}`)
  const { nodes, parents } = walkAst(ast)
  const lexical = buildLexicalBindings(ast, nodes, parents)
  return createTerminalInstrumentationAccounting({
    packageName,
    path,
    bytes,
    nodes,
    parents,
    lexical,
    byteOffsets: byteOffsetMap(source),
    callsites,
  })
}

/**
 * Resolve every child-process primitive in one pinned package module. The
 * resolver is deliberately fail-closed: a tainted namespace or primitive may
 * only flow through the trace shapes represented by the frozen schema.
 */
export function analyzePlaywrightJavaScript({
  packageName,
  path,
  bytes,
  fileSha256,
  isKnownCrossFileSource = null,
}) {
  const source = bytes.toString('utf8')
  if (!Buffer.from(source, 'utf8').equals(bytes)) {
    throw new Error(`${packageName}/${path}: JavaScript source is not canonical UTF-8`)
  }
  const ast = parseJavaScript(source, `${packageName}/${path}`)
  const { nodes, parents } = walkAst(ast)
  const lexical = buildLexicalBindings(ast, nodes, parents)
  const byteOffsets = byteOffsetMap(source)
  const bindings = new Map()
  const primitiveCalls = new Map()
  const handledTaintedNodes = new Set()
  const localFunctions = new Map()
  const processThisAliasAssignments = new Map()
  const moduleNamespaceBindings = new Set()
  const requireLoaderBindings = new Set()
  const staticModuleSpecifier = createStaticStringResolver(nodes, parents, lexical)
  let safeProcessThisAliases = new Set()

  function recordProcessThisAliasAssignment(key, expression) {
    const assignments = processThisAliasAssignments.get(key) ?? []
    assignments.push(expression)
    processThisAliasAssignments.set(key, assignments)
  }

  for (const node of nodes) {
    if (node.type === 'FunctionDeclaration' && node.id?.type === 'Identifier') {
      const key = lexical.declarationKey(node.id)
      if (key !== null) localFunctions.set(key, node)
    } else if (
      node.type === 'VariableDeclarator' &&
      node.id.type === 'Identifier' &&
      (node.init?.type === 'FunctionExpression' || node.init?.type === 'ArrowFunctionExpression')
    ) {
      const key = lexical.declarationKey(node.id)
      if (key !== null) localFunctions.set(key, node.init)
    }
    if (node.type === 'VariableDeclarator' && node.id.type === 'Identifier' && node.init) {
      const key = lexical.declarationKey(node.id)
      if (key !== null) recordProcessThisAliasAssignment(key, node.init)
    } else if (node.type === 'AssignmentExpression' && node.left.type === 'Identifier') {
      const key = lexical.referenceKey(node.left)
      if (key !== null) recordProcessThisAliasAssignment(key, node.right)
    }
  }

  function everyAssignmentResolves(key, seen, predicate) {
    if (key === null || seen.has(key)) return false
    const assignments = processThisAliasAssignments.get(key) ?? []
    if (assignments.length === 0) return false
    const nextSeen = new Set(seen)
    nextSeen.add(key)
    return assignments.every((assignment) => predicate(assignment, nextSeen))
  }

  function resolveGlobalObject(expression, seen = new Set()) {
    if (!expression) return false
    const node = unwrapTransparent(expression)
    if (node.type === 'Identifier') {
      const key = lexical.referenceKey(node)
      if (key === null) return ['global', 'globalThis'].includes(node.name)
      return everyAssignmentResolves(key, seen, resolveGlobalObject)
    }
    if (
      node.type === 'ConditionalExpression' ||
      node.type === 'LogicalExpression'
    ) {
      const left = node.type === 'ConditionalExpression' ? node.consequent : node.left
      const right = node.type === 'ConditionalExpression' ? node.alternate : node.right
      return resolveGlobalObject(left, seen) && resolveGlobalObject(right, seen)
    }
    return false
  }

  function resolveProcessNamespace(expression, seen = new Set()) {
    if (!expression) return false
    const node = unwrapTransparent(expression)
    if (node.type === 'Identifier') {
      const key = lexical.referenceKey(node)
      if (key === null) return node.name === 'process'
      return everyAssignmentResolves(key, seen, resolveProcessNamespace)
    }
    if (node.type === 'MemberExpression') {
      return literalProperty(node)?.name === 'process' &&
        resolveGlobalObject(node.object, seen)
    }
    if (
      node.type === 'ConditionalExpression' ||
      node.type === 'LogicalExpression'
    ) {
      const left = node.type === 'ConditionalExpression' ? node.consequent : node.left
      const right = node.type === 'ConditionalExpression' ? node.alternate : node.right
      return resolveProcessNamespace(left, seen) && resolveProcessNamespace(right, seen)
    }
    return false
  }

  function resolveModuleNamespace(expression) {
    if (!expression) return false
    const node = unwrapTransparent(expression)
    if (
      node.type === 'Identifier' &&
      node.name === 'module' &&
      lexical.referenceKey(node) === null
    ) return true
    if (node.type === 'Identifier') {
      const key = lexical.referenceKey(node)
      return key !== null && moduleNamespaceBindings.has(key)
    }
    if (node.type !== 'MemberExpression') return false
    const property = literalProperty(node)
    if (property?.name === 'module' && resolveGlobalObject(node.object)) return true
    if (property?.name === 'main' && resolveRequireLoader(node.object)) return true
    return property?.name === 'mainModule' && resolveProcessNamespace(node.object)
  }

  function resolveRequireLoader(expression) {
    if (!expression) return false
    const node = unwrapTransparent(expression)
    if (
      node.type === 'Identifier' &&
      node.name === 'require' &&
      lexical.referenceKey(node) === null
    ) return true
    if (node.type === 'Identifier') {
      const key = lexical.referenceKey(node)
      return key !== null && requireLoaderBindings.has(key)
    }
    if (node.type !== 'MemberExpression') return false
    const property = literalProperty(node)
    return Boolean(
      (property?.name === 'require' &&
        (resolveModuleNamespace(node.object) || resolveGlobalObject(node.object))) ||
      (property?.name === 'getBuiltinModule' && resolveProcessNamespace(node.object)),
    )
  }

  let loaderGraphChanged = true
  let loaderGraphPasses = 0
  while (loaderGraphChanged) {
    loaderGraphChanged = false
    loaderGraphPasses += 1
    if (loaderGraphPasses > nodes.length + 1) {
      throw new Error(`${packageName}/${path}: module/require loader resolver did not converge`)
    }
    for (const node of nodes) {
      let identifier = null
      let expression = null
      if (node.type === 'VariableDeclarator' && node.id.type === 'Identifier') {
        identifier = node.id
        expression = node.init
      } else if (
        node.type === 'AssignmentExpression' &&
        node.operator === '=' &&
        node.left.type === 'Identifier'
      ) {
        identifier = node.left
        expression = node.right
      }
      if (identifier) {
        const key = lexical.declarationKey(identifier) ?? lexical.referenceKey(identifier)
        if (
          key !== null &&
          resolveModuleNamespace(expression) &&
          !moduleNamespaceBindings.has(key)
        ) {
          moduleNamespaceBindings.add(key)
          loaderGraphChanged = true
        }
        if (
          key !== null &&
          resolveRequireLoader(expression) &&
          !requireLoaderBindings.has(key)
        ) {
          requireLoaderBindings.add(key)
          loaderGraphChanged = true
        }
      }
      if (
        node.type === 'VariableDeclarator' &&
        node.id.type === 'ObjectPattern' &&
        resolveGlobalObject(node.init)
      ) {
        for (const property of node.id.properties) {
          if (
            property.type !== 'Property' ||
            property.computed ||
            property.kind !== 'init'
          ) {
            throw new Error(
              `${packageName}/${path}: unresolved global capability destructure at byte ${byteOffsets[property.start]}`,
            )
          }
          const name = property.key.name ?? property.key.value
          if (name !== 'require') continue
          const local = identifierName(property.value)
          if (!local) {
            throw new Error(
              `${packageName}/${path}: unresolved global.require destructure at byte ${byteOffsets[property.start]}`,
            )
          }
          const key = lexical.declarationKey(property.value)
          if (key !== null && !requireLoaderBindings.has(key)) {
            requireLoaderBindings.add(key)
            loaderGraphChanged = true
          }
        }
      }
      if (
        node.type === 'VariableDeclarator' &&
        node.id.type === 'ObjectPattern' &&
        resolveModuleNamespace(node.init)
      ) {
        for (const property of node.id.properties) {
          if (
            property.type !== 'Property' ||
            property.computed ||
            property.kind !== 'init'
          ) {
            throw new Error(
              `${packageName}/${path}: unresolved module alias destructure at byte ${byteOffsets[property.start]}`,
            )
          }
          const name = property.key.name ?? property.key.value
          if (name !== 'require') continue
          const local = identifierName(property.value)
          if (!local) {
            throw new Error(
              `${packageName}/${path}: unresolved module.require destructure at byte ${byteOffsets[property.start]}`,
            )
          }
          const key = lexical.declarationKey(property.value)
          if (key !== null && !requireLoaderBindings.has(key)) {
            requireLoaderBindings.add(key)
            loaderGraphChanged = true
          }
        }
      }
    }
  }

  for (const [key, assignments] of processThisAliasAssignments) {
    if (
      moduleNamespaceBindings.has(key) &&
      assignments.some((expression) => !resolveModuleNamespace(expression))
    ) {
      throw new Error(`${packageName}/${path}: module alias has divergent assignments`)
    }
    if (
      requireLoaderBindings.has(key) &&
      assignments.some((expression) => !resolveRequireLoader(expression))
    ) {
      throw new Error(`${packageName}/${path}: require loader alias has divergent assignments`)
    }
  }

  function rootModuleLoad(expression) {
    const node = unwrapTransparent(expression)
    if (
      node.type === 'CallExpression' &&
      resolveRequireLoader(node.callee)
    ) {
      const firstArgument = node.arguments[0]
      const specifier = firstArgument?.type === 'SpreadElement'
        ? null
        : staticModuleSpecifier(firstArgument)
      if (!['child_process', 'node:child_process'].includes(specifier)) {
        if (specifier === null) {
          if (isPinnedNonControlDynamicLoaderCall({ packageName, path, node, source, parents })) {
            return null
          }
          throw new Error(
            `${packageName}/${path}: unknown loader specifier at byte ${byteOffsets[node.start]}`,
          )
        }
        return null
      }
      return {
        kind: 'Namespace',
        trace: [traceStep('Require', node, specifier, byteOffsets)],
      }
    }
    if (node.type === 'ImportExpression') {
      const specifier = staticModuleSpecifier(node.source)
      if (!['child_process', 'node:child_process'].includes(specifier)) {
        return specifier === null
          ? {
              kind: 'UnknownModuleNamespace',
              trace: [traceStep('Import', node, null, byteOffsets)],
            }
          : null
      }
      return {
        kind: 'Namespace',
        trace: [traceStep('Import', node, specifier, byteOffsets)],
      }
    }
    return null
  }

  function resolveExpression(expression, resolving = new Set()) {
    if (!expression) return null
    const node = unwrapTransparent(expression)
    const loaded = rootModuleLoad(node)
    if (loaded) {
      handledTaintedNodes.add(node)
      return loaded
    }
    if (node.type === 'Identifier') {
      const key = lexical.referenceKey(node)
      if (node.name === 'process' && key === null) {
        return {
          kind: 'ProcessNamespace',
          trace: [traceStep('Import', node, 'process', byteOffsets)],
        }
      }
      if (resolving.has(node.name)) throw new Error(`${packageName}/${path}: cyclic primitive alias ${node.name}`)
      const bound = key === null ? null : bindings.get(key)
      if (!bound) return null
      return cloneValue(bound)
    }
    if (node.type === 'MemberExpression') {
      const property = literalProperty(node)
      if (
        property?.name === 'process' &&
        node.object.type === 'Identifier' &&
        ['global', 'globalThis'].includes(node.object.name) &&
        lexical.referenceKey(node.object) === null
      ) {
        return {
          kind: 'ProcessNamespace',
          trace: [traceStep('Import', node, 'process', byteOffsets)],
        }
      }
      const base = resolveExpression(node.object, resolving)
      if (!base) return null
      handledTaintedNodes.add(node)
      if (!property) {
        if (base.kind === 'UnknownModuleNamespace') return null
        throw new Error(`${packageName}/${path}: nonliteral computed process property at byte ${byteOffsets[node.start]}`)
      }
      const step = traceStep(property.kind, node, property.name, byteOffsets)
      if (base.kind === 'Namespace' && property.name === 'default') {
        return cloneValue(base, [step])
      }
      if (base.kind === 'Namespace' && PRIMITIVE_NAMES.has(property.name)) {
        return {
          kind: 'Primitive',
          primitive: PRIMITIVE_NAMES.get(property.name),
          primitiveName: property.name,
          trace: [...base.trace, step],
        }
      }
      if (base.kind === 'ProcessNamespace' && property.name === 'kill') {
        return {
          kind: 'Primitive',
          primitive: 'ProcessKill',
          primitiveName: 'kill',
          trace: [...base.trace, step],
        }
      }
      if (base.kind === 'ProcessNamespace') return null
      if (base.kind === 'UnknownModuleNamespace') {
        if (PRIMITIVE_NAMES.has(property.name)) {
          throw new Error(
            `${packageName}/${path}: unknown module specifier for process primitive ${property.name} at byte ${byteOffsets[node.start]}`,
          )
        }
        return null
      }
      throw new Error(
        `${packageName}/${path}: unresolved process namespace property ${property.name} at byte ${byteOffsets[node.start]}`,
      )
    }
    return null
  }

  function bindIdentifier(name, value, node, kind = 'Assignment') {
    if (!value) return false
    const key = lexical.declarationKey(node) ?? lexical.referenceKey(node)
    if (key === null) {
      throw new Error(`${packageName}/${path}: undeclared process alias ${name}`)
    }
    const next = cloneValue(value, [traceStep(kind, node, name, byteOffsets)])
    const previous = bindings.get(key)
    const serialized = JSON.stringify(next)
    if (previous && JSON.stringify(previous) !== serialized) {
      throw new Error(`${packageName}/${path}: primitive alias ${name} has divergent assignments`)
    }
    if (!previous) {
      bindings.set(key, next)
      return true
    }
    return false
  }

  function bindParameter(parameter, value) {
    if (parameter.type !== 'Identifier') {
      throw new Error(`${packageName}/${path}: unsupported primitive parameter pattern`)
    }
    const key = lexical.declarationKey(parameter)
    if (key === null) {
      throw new Error(`${packageName}/${path}: unresolved primitive parameter ${parameter.name}`)
    }
    const next = cloneValue(value, [traceStep('Parameter', parameter, parameter.name, byteOffsets)])
    const previous = bindings.get(key)
    if (previous) {
      const sameKind = previous.kind === next.kind
      const samePrimitive =
        previous.kind !== 'Primitive' ||
        (previous.primitive === next.primitive && previous.primitiveName === next.primitiveName)
      if (!sameKind || !samePrimitive) {
        throw new Error(`${packageName}/${path}: primitive parameter ${parameter.name} has divergent inputs`)
      }
      return false
    }
    bindings.set(key, next)
    return true
  }

  function isPinnedNonControlProcessConsumer(node, argumentIndex) {
    return Boolean(
      packageName === 'playwright-core' &&
      path === 'lib/coreBundle.js' &&
      argumentIndex === 0 &&
      source.slice(node.callee.start, node.callee.end) === 'eventsHelper.addEventListener' &&
      node.arguments[1]?.type === 'Literal' &&
      node.arguments[1].value === 'exit',
    )
  }

  function isPinnedNonControlRequireConsumer(identifier, parent) {
    return Boolean(
      packageName === 'playwright' &&
      path === 'lib/transform/babelBundle.js' &&
      parent?.type === 'CallExpression' &&
      parent.arguments.includes(identifier) &&
      source.slice(parent.callee.start, parent.callee.end) ===
        '0, _rewriteStackTrace.endHiddenCallStack',
    )
  }

  for (const node of nodes) {
    if (node.type === 'ExportAllDeclaration' || node.type === 'ExportNamedDeclaration') {
      if (!node.source) continue
      const specifier = staticModuleSpecifier(node.source)
      if (['child_process', 'node:child_process'].includes(specifier)) {
        throw new Error(
          `${packageName}/${path}: cross-file child_process export at byte ${byteOffsets[node.start]}`,
        )
      }
      if (!specifier || !isKnownCrossFileSource?.(specifier)) {
        throw new Error(
          `${packageName}/${path}: unknown cross-file re-export ${specifier ?? '<dynamic>'} at byte ${byteOffsets[node.start]}`,
        )
      }
      continue
    }
    if (node.type !== 'ImportDeclaration') continue
    if (!['child_process', 'node:child_process'].includes(node.source.value)) continue
    handledTaintedNodes.add(node)
    for (const specifier of node.specifiers) {
      const base = {
        kind: 'Namespace',
        trace: [traceStep('Import', node, node.source.value, byteOffsets)],
      }
      if (specifier.type === 'ImportSpecifier') {
        const imported = specifier.imported.name ?? specifier.imported.value
        if (!PRIMITIVE_NAMES.has(imported)) {
          throw new Error(`${packageName}/${path}: unsupported child_process import ${imported}`)
        }
        bindIdentifier(
          specifier.local.name,
          {
            kind: 'Primitive',
            primitive: PRIMITIVE_NAMES.get(imported),
            primitiveName: imported,
            trace: [...base.trace, traceStep('Destructure', specifier, imported, byteOffsets)],
          },
          specifier.local,
        )
      } else {
        bindIdentifier(specifier.local.name, base, specifier.local)
      }
    }
  }

  let changed = true
  let passes = 0
  while (changed) {
    changed = false
    passes += 1
    if (passes > nodes.length + 1) throw new Error(`${packageName}/${path}: alias resolver did not converge`)
    for (const node of nodes) {
      if (node.type === 'VariableDeclarator') {
        const value = resolveExpression(node.init)
        if (!value) continue
        handledTaintedNodes.add(node)
        if (node.id.type === 'Identifier') {
          changed = bindIdentifier(node.id.name, value, node.id) || changed
        } else if (node.id.type === 'ObjectPattern' && value.kind === 'Namespace') {
          for (const property of node.id.properties) {
            if (property.type !== 'Property' || property.computed || property.kind !== 'init') {
              throw new Error(`${packageName}/${path}: unsupported child_process destructuring`)
            }
            const name = property.key.name ?? property.key.value
            const local = identifierName(property.value)
            if (!local || !PRIMITIVE_NAMES.has(name)) {
              throw new Error(`${packageName}/${path}: unresolved child_process destructure ${name}`)
            }
            const primitive = {
              kind: 'Primitive',
              primitive: PRIMITIVE_NAMES.get(name),
              primitiveName: name,
              trace: [...value.trace, traceStep('Destructure', property, name, byteOffsets)],
            }
            changed = bindIdentifier(local, primitive, property.value) || changed
          }
        } else if (node.id.type === 'ObjectPattern' && value.kind === 'ProcessNamespace') {
          for (const property of node.id.properties) {
            if (property.type !== 'Property' || property.computed || property.kind !== 'init') {
              throw new Error(`${packageName}/${path}: unsupported process destructuring`)
            }
            const name = property.key.name ?? property.key.value
            if (name !== 'kill') continue
            const local = identifierName(property.value)
            if (!local) throw new Error(`${packageName}/${path}: unresolved process.kill destructure`)
            const primitive = {
              kind: 'Primitive',
              primitive: 'ProcessKill',
              primitiveName: 'kill',
              trace: [...value.trace, traceStep('Destructure', property, 'kill', byteOffsets)],
            }
            changed = bindIdentifier(local, primitive, property.value) || changed
          }
        } else if (node.id.type === 'ObjectPattern' && value.kind === 'UnknownModuleNamespace') {
          for (const property of node.id.properties) {
            if (property.type !== 'Property' || property.computed || property.kind !== 'init') {
              continue
            }
            const name = property.key.name ?? property.key.value
            if (PRIMITIVE_NAMES.has(name)) {
              throw new Error(
                `${packageName}/${path}: unknown module specifier for process primitive ${name} at byte ${byteOffsets[property.start]}`,
              )
            }
          }
        } else {
          throw new Error(`${packageName}/${path}: unsupported process alias pattern`)
        }
      } else if (node.type === 'AssignmentExpression') {
        const value = resolveExpression(node.right)
        if (!value) continue
        handledTaintedNodes.add(node)
        if (node.operator !== '=' || node.left.type !== 'Identifier') {
          throw new Error(`${packageName}/${path}: unsupported process alias assignment`)
        }
        changed = bindIdentifier(node.left.name, value, node.left) || changed
      } else if (node.type === 'CallExpression') {
        const callee = unwrapTransparent(node.callee)
        if (callee.type !== 'Identifier') continue
        if (['__toESM', '__toCommonJS'].includes(callee.name)) continue
        const key = lexical.referenceKey(callee)
        const target = key === null ? null : localFunctions.get(key)
        if (!target) continue
        for (let index = 0; index < node.arguments.length; index += 1) {
          const argument = node.arguments[index]
          if (argument.type === 'SpreadElement') {
            const spreadValue = resolveExpression(argument.argument)
            if (spreadValue) {
              throw new Error(`${packageName}/${path}: unsupported spread primitive argument`)
            }
            continue
          }
          const value = resolveExpression(argument)
          if (!value) continue
          const parameter = target.params[index]
          if (!parameter) {
            throw new Error(`${packageName}/${path}: primitive argument has no local parameter`)
          }
          handledTaintedNodes.add(node)
          changed = bindParameter(parameter, value) || changed
        }
      }
    }
  }

  function assertNotExportedCapability(expression, node) {
    const value = resolveExpression(expression)
    if (
      value &&
      ['Namespace', 'ProcessNamespace', 'Primitive', 'UnknownModuleNamespace'].includes(value.kind)
    ) {
      throw new Error(
        `${packageName}/${path}: cross-file primitive export at byte ${byteOffsets[node.start]}`,
      )
    }
  }

  for (const node of nodes) {
    if (node.type === 'ExportDefaultDeclaration') {
      assertNotExportedCapability(node.declaration, node)
      continue
    }
    if (node.type !== 'ExportNamedDeclaration' || node.source) continue
    if (node.declaration?.type === 'VariableDeclaration') {
      for (const declaration of node.declaration.declarations) {
        for (const identifier of patternIdentifiers(declaration.id)) {
          assertNotExportedCapability(identifier, node)
        }
      }
    }
    for (const specifier of node.specifiers) {
      assertNotExportedCapability(specifier.local, node)
    }
  }

  function isResolvedNonControlProcessMethod(expression) {
    const node = expression ? unwrapTransparent(expression) : null
    if (node?.type !== 'MemberExpression') return false
    const property = literalProperty(node)
    return Boolean(
      property &&
      property.name !== 'kill' &&
      resolveExpression(node.object)?.kind === 'ProcessNamespace',
    )
  }
  safeProcessThisAliases = new Set(
    [...processThisAliasAssignments]
      .filter(([, assignments]) =>
        assignments.length > 0 && assignments.every(isResolvedNonControlProcessMethod))
      .map(([key]) => key),
  )

  const wrappers = new Map()
  const directPrimitiveCallsByFunction = new Map()
  for (const node of nodes) {
    if (node.type !== 'CallExpression') continue
    const value = resolveExpression(node.callee)
    if (value?.kind !== 'Primitive') continue
    let owner = node
    while ((owner = parents.get(owner))) {
      if (
        owner.type === 'FunctionDeclaration' ||
        owner.type === 'FunctionExpression' ||
        owner.type === 'ArrowFunctionExpression'
      ) break
    }
    if (!owner) continue
    const calls = directPrimitiveCallsByFunction.get(owner) ?? []
    calls.push({ call: node, value })
    directPrimitiveCallsByFunction.set(owner, calls)
  }
  for (const node of nodes) {
    if (node.type !== 'FunctionDeclaration' || node.id?.type !== 'Identifier') continue
    const resolved = directPrimitiveCallsByFunction.get(node) ?? []
    if (node.id.name === 'spawnAsync' && resolved.length === 1) {
      const [{ call, value }] = resolved
      if (value.primitive !== 'ChildProcessSpawn') {
        throw new Error(`${packageName}/${path}: spawnAsync wrapper primitive drifted`)
      }
      const key = lexical.declarationKey(node.id)
      if (key === null) throw new Error(`${packageName}/${path}: unresolved spawnAsync declaration`)
      wrappers.set(key, {
        name: 'spawnAsync',
        primitive: value.primitive,
        primitiveName: value.primitiveName,
        trace: [
          ...value.trace,
          traceStep('Return', node, 'spawnAsync', byteOffsets),
        ],
        rawCall: call,
        escaped: bindingEscapesDirectCalls(nodes, parents, lexical, key),
      })
    }
  }

  for (const node of nodes) {
    if (node.type !== 'CallExpression') continue
    const direct = resolveExpression(node.callee)
    let value = direct?.kind === 'Primitive' ? direct : null
    let wrapper = null
    const wrapperKey = node.callee.type === 'Identifier' ? lexical.referenceKey(node.callee) : null
    if (!value && wrapperKey !== null && wrappers.has(wrapperKey)) {
      value = wrappers.get(wrapperKey)
      wrapper = value.name
    }
    if (!value) continue
    const rawWrapper = [...wrappers.values()].find((entry) => entry.rawCall === node)
    if (wrapper === null && rawWrapper && !rawWrapper.escaped) {
      handledTaintedNodes.add(node)
      continue
    }
    handledTaintedNodes.add(node)
    const call = traceStep('Call', node, value.primitiveName, byteOffsets)
    const classification = classifyCallsite({
      packageName,
      path,
      node,
      source,
      parents,
      wrapper,
    })
    validateCallsiteClassification(classification)
    const entry = {
      package: packageName,
      path,
      fileSha256,
      startByte: byteOffsets[node.start],
      endByte: byteOffsets[node.end],
      primitive: value.primitive,
      bindingTrace: [...value.trace, call],
      ...classification,
    }
    primitiveCalls.set(`${entry.startByte}:${entry.endByte}:${entry.primitive}:${canonicalAsciiJson(entry.bindingTrace)}`, entry)
  }

  for (const node of nodes) {
    if (node.type === 'CallExpression') {
      const required = rootModuleLoad(node)
      if (
        required?.kind === 'Namespace' &&
        !handledTaintedNodes.has(unwrapTransparent(node))
      ) {
        throw new Error(`${packageName}/${path}: unresolved child_process require at byte ${byteOffsets[node.start]}`)
      }
      if (!handledTaintedNodes.has(node)) {
        for (let index = 0; index < node.arguments.length; index += 1) {
          const argument = node.arguments[index]
          const expression = argument.type === 'SpreadElement' ? argument.argument : argument
          if (resolveExpression(expression)?.kind === 'ProcessNamespace') {
            const callee = unwrapTransparent(node.callee)
            const property = callee.type === 'MemberExpression' ? literalProperty(callee) : null
            const receiverKey =
              callee.type === 'MemberExpression' && callee.object.type === 'Identifier'
                ? lexical.referenceKey(callee.object)
                : null
            const safeThisBinding =
              index === 0 &&
              property?.name === 'call' &&
              receiverKey !== null &&
              safeProcessThisAliases.has(receiverKey)
            const safeDirectBind =
              index === 0 &&
              property?.name === 'bind' &&
              callee.type === 'MemberExpression' &&
              isResolvedNonControlProcessMethod(callee.object)
            if (
              safeThisBinding ||
              safeDirectBind ||
              isPinnedNonControlProcessConsumer(node, index)
            ) {
              handledTaintedNodes.add(node)
              continue
            }
            throw new Error(
              `${packageName}/${path}: unresolved process namespace flow at byte ${byteOffsets[expression.start]}`,
            )
          }
        }
      }
    }
    if (node.type === 'ImportExpression') {
      const imported = rootModuleLoad(node)
      if (imported?.kind === 'Namespace' && !handledTaintedNodes.has(node)) {
        throw new Error(
          `${packageName}/${path}: unresolved dynamic child_process import at byte ${byteOffsets[node.start]}`,
        )
      }
    }
    if (node.type === 'MemberExpression') {
      const value = resolveExpression(node)
      if (value?.kind === 'Primitive') {
        let flow = node
        let parent = parents.get(flow)
        while (
          parent &&
          ((parent.type === 'SequenceExpression' && parent.expressions.includes(flow)) ||
            (parent.type === 'ChainExpression' && parent.expression === flow))
        ) {
          flow = parent
          parent = parents.get(flow)
        }
        const allowed =
          (parent?.type === 'CallExpression' && parent.callee === flow) ||
          (parent?.type === 'VariableDeclarator' && parent.init === flow) ||
          (parent?.type === 'AssignmentExpression' && parent.right === flow) ||
          (parent?.type === 'UnaryExpression' && parent.operator === 'typeof' && parent.argument === flow)
        if (!allowed) {
          throw new Error(
            `${packageName}/${path}: unresolved primitive property flow at byte ${byteOffsets[node.start]}`,
          )
        }
      }

      const loaderProperty = literalProperty(node)
      const isRequireLoaderMember = Boolean(
        loaderProperty?.name === 'require' &&
        resolveRequireLoader(node),
      )
      if (isRequireLoaderMember) {
        const parent = parents.get(node)
        const assignedKey =
          parent?.type === 'VariableDeclarator' &&
          parent.init === node &&
          parent.id.type === 'Identifier'
            ? lexical.declarationKey(parent.id)
            : parent?.type === 'AssignmentExpression' &&
                parent.operator === '=' &&
                parent.right === node &&
                parent.left.type === 'Identifier'
              ? lexical.referenceKey(parent.left)
              : null
        const allowed =
          (parent?.type === 'CallExpression' && parent.callee === node) ||
          (assignedKey !== null && requireLoaderBindings.has(assignedKey))
        if (!allowed) {
          const label = resolveModuleNamespace(node.object)
            ? 'module.require'
            : 'global require'
          throw new Error(
            `${packageName}/${path}: unresolved ${label} loader flow at byte ${byteOffsets[node.start]}`,
          )
        }
      }
    }
    if (node.type === 'Identifier' && isReferencePosition(node, parents.get(node))) {
      const key = lexical.referenceKey(node)
      const isModuleNamespace =
        (node.name === 'module' && key === null) ||
        (key !== null && moduleNamespaceBindings.has(key))
      if (isModuleNamespace) {
        const parent = parents.get(node)
        const property = parent?.type === 'MemberExpression' && parent.object === node
          ? literalProperty(parent)
          : null
        const assignedKey =
          parent?.type === 'VariableDeclarator' &&
          parent.init === node &&
          parent.id.type === 'Identifier'
            ? lexical.declarationKey(parent.id)
            : parent?.type === 'AssignmentExpression' &&
                parent.operator === '=' &&
                parent.right === node &&
                parent.left.type === 'Identifier'
              ? lexical.referenceKey(parent.left)
              : null
        const destructured =
          parent?.type === 'VariableDeclarator' &&
          parent.init === node &&
          parent.id.type === 'ObjectPattern'
        const safeModuleProperties = [
          'children',
          'exports',
          'filename',
          'id',
          'loaded',
          'parent',
          'path',
          'paths',
          'require',
        ]
        const allowed =
          (property && safeModuleProperties.includes(property.name)) ||
          (parent?.type === 'UnaryExpression' && parent.operator === 'typeof') ||
          (assignedKey !== null && moduleNamespaceBindings.has(assignedKey)) ||
          destructured
        if (!allowed) {
          throw new Error(
            `${packageName}/${path}: unresolved module alias flow at byte ${byteOffsets[node.start]}`,
          )
        }
      }
    }
    if (
      node.type === 'Identifier' &&
      node.name === 'require' &&
      lexical.referenceKey(node) === null &&
      isReferencePosition(node, parents.get(node))
    ) {
      const parent = parents.get(node)
      const property = parent?.type === 'MemberExpression' && parent.object === node
        ? literalProperty(parent)
        : null
      const assignedKey =
        parent?.type === 'VariableDeclarator' &&
        parent.init === node &&
        parent.id.type === 'Identifier'
          ? lexical.declarationKey(parent.id)
          : parent?.type === 'AssignmentExpression' &&
              parent.operator === '=' &&
              parent.right === node &&
              parent.left.type === 'Identifier'
            ? lexical.referenceKey(parent.left)
            : null
      const allowed =
        (parent?.type === 'CallExpression' && parent.callee === node) ||
        (parent?.type === 'UnaryExpression' && parent.operator === 'typeof') ||
        (property && ['cache', 'extensions', 'main', 'resolve'].includes(property.name)) ||
        isPinnedNonControlRequireConsumer(node, parent) ||
        (assignedKey !== null && requireLoaderBindings.has(assignedKey))
      if (!allowed) {
        throw new Error(
          `${packageName}/${path}: unresolved require loader flow at byte ${byteOffsets[node.start]}`,
        )
      }
    }
    const key = node.type === 'Identifier' ? lexical.referenceKey(node) : null
    const parent = node.type === 'Identifier' ? parents.get(node) : null
    const isPatternIdentifier =
      parent?.type === 'Property' && parents.get(parent)?.type === 'ObjectPattern'
    if (
      node.type === 'Identifier' &&
      !lexical.isDeclaration(node) &&
      !isPatternIdentifier &&
      key !== null &&
      bindings.has(key) &&
      bindings.get(key)?.kind !== 'UnknownModuleNamespace' &&
      isReferencePosition(node, parent)
    ) {
      const bound = bindings.get(key)
      const allowed =
        handledTaintedNodes.has(parent) ||
        (parent?.type === 'MemberExpression' && handledTaintedNodes.has(parent)) ||
        (parent?.type === 'CallExpression' && handledTaintedNodes.has(parent)) ||
        (bound?.kind === 'ProcessNamespace' &&
          parent?.type === 'UnaryExpression' &&
          parent.operator === 'typeof') ||
        (bound?.kind === 'ProcessNamespace' &&
          parent?.type === 'LogicalExpression' &&
          parent.left === node)
      if (!allowed) {
        throw new Error(
          `${packageName}/${path}: unresolved primitive alias flow ${node.name} at byte ${byteOffsets[node.start]}`,
        )
      }
    }
  }

  const output = [...primitiveCalls.values()]
  const instrumentation = createTerminalInstrumentationAccounting({
    packageName,
    path,
    bytes,
    nodes,
    parents,
    lexical,
    byteOffsets,
    callsites: output,
  })
  Object.defineProperty(output, 'instrumentation', {
    value: instrumentation,
    enumerable: false,
    configurable: false,
    writable: false,
  })
  return output
}

function callsiteComparator(left, right) {
  const packageOrder = PACKAGE_SPECS.findIndex((entry) => entry.name === left.package) -
    PACKAGE_SPECS.findIndex((entry) => entry.name === right.package)
  if (packageOrder !== 0) return packageOrder
  const pathOrder = compareUtf8(left.path, right.path)
  if (pathOrder !== 0) return pathOrder
  if (left.startByte !== right.startByte) return left.startByte - right.startByte
  if (left.endByte !== right.endByte) return left.endByte - right.endByte
  const primitiveOrder = PRIMITIVE_ORDER.indexOf(left.primitive) - PRIMITIVE_ORDER.indexOf(right.primitive)
  if (primitiveOrder !== 0) return primitiveOrder
  return compareUtf8(canonicalAsciiJson(left.bindingTrace), canonicalAsciiJson(right.bindingTrace))
}

export function createPlaywrightProcessPrimitiveState({ packagesRoot, tarballsRoot = null }) {
  if (!packagesRoot) throw new Error('packagesRoot is required')
  const root = resolve(packagesRoot)
  const packages = PACKAGE_SPECS.map((spec) => ({ ...spec }))
  const packageFilesByName = new Map()
  const files = []
  const callsites = []
  for (const spec of PACKAGE_SPECS) {
    const packageRoot = join(root, spec.name)
    const packageJson = JSON.parse(readFileSync(join(packageRoot, 'package.json'), 'utf8'))
    if (packageJson.name !== spec.name || packageJson.version !== spec.version) {
      throw new Error(
        `${spec.name} package identity drifted: ${packageJson.name}@${packageJson.version}`,
      )
    }
    const packageFiles = collectPackageFiles(packageRoot)
    if (tarballsRoot) {
      const tarballPath = join(
        resolve(tarballsRoot),
        `${spec.name}-${spec.version}.tgz`,
      )
      const tarballFiles = collectTarballFiles(readFileSync(tarballPath), spec)
      verifyExtractedPackage(packageFiles, tarballFiles, spec)
    }
    packageFilesByName.set(spec.name, packageFiles)
    for (const file of packageFiles) {
      files.push({
        package: spec.name,
        path: file.path,
        size: file.size,
        sha256: file.sha256,
      })
    }
  }

  const packageFilePaths = new Map(
    [...packageFilesByName].map(([name, packageFiles]) => [
      name,
      new Set(packageFiles.map(({ path }) => path)),
    ]),
  )

  function isKnownCrossFileSource(packageName, importerPath, specifier) {
    let targetPackage = packageName
    let targetPath = null
    if (packageFilePaths.has(specifier)) {
      targetPackage = specifier
      targetPath = 'index.mjs'
    } else if (specifier.startsWith('./') || specifier.startsWith('../')) {
      targetPath = posix.normalize(posix.join(posix.dirname(importerPath), specifier))
      if (
        targetPath === '..' ||
        targetPath.startsWith('../') ||
        targetPath.startsWith('/')
      ) return false
    } else {
      for (const candidatePackage of PACKAGE_SPECS.map(({ name }) => name)) {
        if (!specifier.startsWith(`${candidatePackage}/`)) continue
        targetPackage = candidatePackage
        targetPath = specifier.slice(candidatePackage.length + 1)
        break
      }
    }
    if (!targetPath) return false
    const known = packageFilePaths.get(targetPackage)
    if (!known) return false
    const candidates = [
      targetPath,
      `${targetPath}.js`,
      `${targetPath}.mjs`,
      `${targetPath}.cjs`,
      posix.join(targetPath, 'index.js'),
      posix.join(targetPath, 'index.mjs'),
      posix.join(targetPath, 'index.cjs'),
    ]
    return candidates.some((candidate) => known.has(candidate))
  }

  let javascriptFiles = 0
  let terminalCalls = 0
  const instrumentationMappings = []
  for (const spec of PACKAGE_SPECS) {
    for (const file of packageFilesByName.get(spec.name)) {
      if (file.path.endsWith('.js') || file.path.endsWith('.mjs') || file.path.endsWith('.cjs')) {
        javascriptFiles += 1
        const analyzed = analyzePlaywrightJavaScript({
          packageName: spec.name,
          path: file.path,
          bytes: file.bytes,
          fileSha256: file.sha256,
          isKnownCrossFileSource: (source) =>
            isKnownCrossFileSource(spec.name, file.path, source),
        })
        callsites.push(...analyzed)
        terminalCalls += analyzed.instrumentation.terminalCalls
        instrumentationMappings.push(...analyzed.instrumentation.mappings)
      }
    }
  }
  files.sort((left, right) => compareUtf8(`${left.package}/${left.path}`, `${right.package}/${right.path}`))
  callsites.sort(callsiteComparator)
  const graph = { version: 1, packages, files, callsites }
  const graphBytes = Buffer.from(canonicalAsciiJson(graph), 'ascii')
  const graphSha256 = createHash('sha256')
    .update(Buffer.from('KLAUS_PLAYWRIGHT_PROCESS_GRAPH_V1\0', 'ascii'))
    .update(u64be(graphBytes.length))
    .update(graphBytes)
    .digest('hex')
  const lock = { ...graph, graphSha256 }
  instrumentationMappings.sort((left, right) => compareUtf8(left, right))
  const instrumentationBytes = Buffer.from(
    canonicalAsciiJson(instrumentationMappings),
    'ascii',
  )
  if (terminalCalls !== callsites.length) {
    throw new Error(
      `full terminal instrumentation mismatch: ${terminalCalls} terminals for ${callsites.length} lock entries`,
    )
  }
  const instrumentation = {
    javascriptFiles,
    terminalCalls,
    mappedTerminalCalls: instrumentationMappings.length,
    mappingSha256: createHash('sha256')
      .update(Buffer.from('KLAUS_PLAYWRIGHT_TERMINAL_INSTRUMENTATION_V1\0', 'ascii'))
      .update(u64be(instrumentationBytes.length))
      .update(instrumentationBytes)
      .digest('hex'),
  }
  if (instrumentation.mappedTerminalCalls !== terminalCalls) {
    throw new Error(
      `full terminal instrumentation is not bijective: ${instrumentation.mappedTerminalCalls} mappings for ${terminalCalls} terminals`,
    )
  }
  return {
    lock,
    graphBytes,
    lockBytes: Buffer.from(`${canonicalAsciiJson(lock)}\n`, 'ascii'),
    instrumentation,
  }
}

export function assertPlaywrightProcessPrimitiveLock({
  repoRoot = REPO_ROOT,
  packagesRoot,
  tarballsRoot = null,
  state = createPlaywrightProcessPrimitiveState({ packagesRoot, tarballsRoot }),
} = {}) {
  const actual = readFileSync(join(resolve(repoRoot), LOCK_PATH))
  if (!actual.equals(state.lockBytes)) throw new Error('playwright process primitive lock drifted')
  return state
}

function parseArguments(arguments_) {
  let mode = null
  let packagesRoot = process.env.KLAUS_PLAYWRIGHT_PACKAGES_ROOT || null
  let tarballsRoot = process.env.KLAUS_PLAYWRIGHT_TARBALLS_ROOT || null
  for (let index = 0; index < arguments_.length; index += 1) {
    const argument = arguments_[index]
    if (argument === '--check' || argument === '--write') {
      if (mode) throw new Error('playwright lock mode specified more than once')
      mode = argument
    } else if (argument === '--packages-root') {
      index += 1
      packagesRoot = arguments_[index]
      if (!packagesRoot) throw new Error('--packages-root requires a value')
    } else if (argument === '--tarballs-root') {
      index += 1
      tarballsRoot = arguments_[index]
      if (!tarballsRoot) throw new Error('--tarballs-root requires a value')
    } else {
      throw new Error(`unknown playwright lock argument: ${argument}`)
    }
  }
  if (!mode || !packagesRoot || !tarballsRoot) {
    throw new Error(
      'usage: node scripts/playwright-process-primitives-lock.mjs --check|--write --packages-root <extracted-root> --tarballs-root <tarball-root>',
    )
  }
  return { mode, packagesRoot, tarballsRoot }
}

function main() {
  const { mode, packagesRoot, tarballsRoot } = parseArguments(process.argv.slice(2))
  const state = createPlaywrightProcessPrimitiveState({ packagesRoot, tarballsRoot })
  if (mode === '--write') writeFileSync(join(REPO_ROOT, LOCK_PATH), state.lockBytes)
  else assertPlaywrightProcessPrimitiveLock({ packagesRoot, tarballsRoot, state })
  process.stdout.write(
    `playwright process primitive lock ${mode === '--write' ? 'written' : 'OK'}: ` +
      `${state.lock.files.length} files, ${state.lock.callsites.length} callsites, ` +
      `full instrumentation ${state.instrumentation.mappedTerminalCalls}/${state.instrumentation.terminalCalls} ` +
      `across ${state.instrumentation.javascriptFiles} JavaScript files, ` +
      `mapping ${state.instrumentation.mappingSha256}, graph ${state.lock.graphSha256}\n`,
  )
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try {
    main()
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.stack ?? error.message : String(error)}\n`)
    process.exitCode = 1
  }
}
