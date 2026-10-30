#!/usr/bin/env node

import { spawnSync } from 'node:child_process'
import { createHash } from 'node:crypto'
import {
  chmodSync,
  readFileSync,
  readdirSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs'
import { dirname, join, relative, resolve, sep } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

export const SOURCE_ROOT = 'plugins/klaus-bundled/browser'
export const MIRROR_ROOT = 'crates/sdk/assets/klaus-bundled/browser'
export const LOCK_FILE = 'bundled-browser-assets.lock.json'
export const EMBEDDED_LOCK_FILE = 'crates/sdk/assets/bundled-browser-assets.lock.json'
export const SOURCE_MANIFEST = 'ASSET-SOURCES.sha256'

const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const ALLOWED_MODES = new Set(['100644', '100755'])
const SHA256 = /^[0-9a-f]{64}$/

export function sha256(bytes) {
  return createHash('sha256').update(bytes).digest('hex')
}

function byteCompare(left, right) {
  return Buffer.compare(Buffer.from(left, 'utf8'), Buffer.from(right, 'utf8'))
}

function normalizedRelativePath(root, path) {
  return relative(root, path).split(sep).join('/')
}

function validateRelativePath(path) {
  if (!/^[\x21-\x7e]+$/.test(path) || path.includes('\\')) {
    throw new Error(`browser asset path must be printable ASCII with forward slashes: ${path}`)
  }
  const components = path.split('/')
  if (components.some((component) => component === '' || component === '.' || component === '..')) {
    throw new Error(`browser asset path has an invalid component: ${path}`)
  }
}

function collectRegularFiles(root, directory = root, output = []) {
  const entries = readdirSync(directory, { withFileTypes: true })
    .sort((left, right) => byteCompare(left.name, right.name))
  for (const entry of entries) {
    const path = join(directory, entry.name)
    const relativePath = normalizedRelativePath(root, path)
    if (entry.isSymbolicLink()) {
      throw new Error(`browser asset tree may not contain symlink: ${relativePath}`)
    }
    if (entry.isDirectory()) {
      collectRegularFiles(root, path, output)
    } else if (entry.isFile()) {
      validateRelativePath(relativePath)
      output.push(relativePath)
    } else {
      throw new Error(`browser asset tree contains a nonregular entry: ${relativePath}`)
    }
  }
  return output
}

function runGitIndex(repoRoot, sourceRoot) {
  const result = spawnSync(
    'git',
    ['ls-files', '--stage', '-z', '--', sourceRoot],
    { cwd: repoRoot, encoding: 'buffer', maxBuffer: 16 * 1024 * 1024 },
  )
  if (result.error) throw result.error
  if (result.status !== 0) {
    throw new Error(`git index query failed: ${result.stderr.toString('utf8').trim()}`)
  }

  const prefix = `${sourceRoot}/`
  const modes = new Map()
  for (const record of result.stdout.toString('utf8').split('\0')) {
    if (record === '') continue
    const tab = record.indexOf('\t')
    if (tab === -1) throw new Error(`malformed git index record: ${record}`)
    const [mode, , stage] = record.slice(0, tab).split(' ')
    const indexedPath = record.slice(tab + 1)
    if (stage !== '0') throw new Error(`browser asset has an unmerged index stage: ${indexedPath}`)
    if (!indexedPath.startsWith(prefix)) {
      throw new Error(`git returned an out-of-root browser asset: ${indexedPath}`)
    }
    const relativePath = indexedPath.slice(prefix.length)
    validateRelativePath(relativePath)
    if (!ALLOWED_MODES.has(mode)) {
      throw new Error(`browser asset has unsupported git mode ${mode}: ${relativePath}`)
    }
    if (modes.has(relativePath)) throw new Error(`browser asset is duplicated in index: ${relativePath}`)
    modes.set(relativePath, mode)
  }
  return modes
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

function samePathSet(left, right, label) {
  if (left.length !== right.length) {
    throw new Error(`${label} path count drift: expected ${left.length}, got ${right.length}`)
  }
  for (let index = 0; index < left.length; index += 1) {
    if (left[index] !== right[index]) {
      throw new Error(`${label} path drift at ${index}: expected ${left[index]}, got ${right[index]}`)
    }
  }
}

export function createBundledBrowserAssetState({
  repoRoot = REPO_ROOT,
  sourceRoot = SOURCE_ROOT,
  mirrorRoot = MIRROR_ROOT,
} = {}) {
  const repository = resolve(repoRoot)
  const sourceDirectory = resolve(repository, sourceRoot)
  const mirrorDirectory = resolve(repository, mirrorRoot)
  const sourcePaths = collectRegularFiles(sourceDirectory).sort(byteCompare)
  const mirrorPaths = collectRegularFiles(mirrorDirectory)
    .filter((path) => path !== SOURCE_MANIFEST)
    .sort(byteCompare)
  samePathSet(sourcePaths, mirrorPaths, 'browser asset mirror')

  const indexedModes = runGitIndex(repository, sourceRoot)
  const indexedPaths = [...indexedModes.keys()].sort(byteCompare)
  samePathSet(sourcePaths, indexedPaths, 'browser asset git index')
  const mirrorIndexedModes = runGitIndex(repository, mirrorRoot)
  mirrorIndexedModes.delete(SOURCE_MANIFEST)
  if (mirrorIndexedModes.size !== 0) {
    samePathSet(
      sourcePaths,
      [...mirrorIndexedModes.keys()].sort(byteCompare),
      'browser asset mirror git index',
    )
  }

  const folded = new Set()
  const files = sourcePaths.map((path) => {
    const foldedPath = path.toLowerCase()
    if (folded.has(foldedPath)) throw new Error(`browser asset path has a case alias: ${path}`)
    folded.add(foldedPath)

    const sourceBytes = readFileSync(join(sourceDirectory, path))
    const mirrorBytes = readFileSync(join(mirrorDirectory, path))
    if (!sourceBytes.equals(mirrorBytes)) {
      throw new Error(`browser asset mirror bytes drifted: ${path}`)
    }
    const mode = indexedModes.get(path)
    if (!ALLOWED_MODES.has(mode)) throw new Error(`browser asset mode missing: ${path}`)
    if (mirrorIndexedModes.size !== 0 && mirrorIndexedModes.get(path) !== mode) {
      throw new Error(`browser asset mirror git mode drifted: ${path}`)
    }
    if (mirrorIndexedModes.size === 0 && process.platform !== 'win32') {
      const expectedPermissions = mode === '100755' ? 0o755 : 0o644
      const actualPermissions = statSync(join(mirrorDirectory, path)).mode & 0o777
      if (actualPermissions !== expectedPermissions) {
        throw new Error(`browser asset mirror filesystem mode drifted: ${path}`)
      }
    }
    return {
      path,
      size: sourceBytes.length,
      mode,
      sha256: sha256(sourceBytes),
    }
  })

  const lock = {
    version: 1,
    sourceRoot,
    mirrorRoot,
    files,
    pathSetSha256: pathSetDigest(files),
    treeSha256: treeDigest(files),
  }
  const sourceManifest = files
    .map((file) => `${file.sha256}  ${file.path}\n`)
    .join('')
  return {
    lock,
    lockBytes: `${JSON.stringify(lock)}\n`,
    sourceManifest,
  }
}

export function assertBundledBrowserAssetState({
  repoRoot = REPO_ROOT,
  state = createBundledBrowserAssetState({ repoRoot }),
} = {}) {
  const repository = resolve(repoRoot)
  const checkedLock = readFileSync(join(repository, LOCK_FILE), 'utf8')
  if (checkedLock !== state.lockBytes) {
    throw new Error(`${LOCK_FILE} is not the canonical full-tree lock`)
  }
  const embeddedLock = readFileSync(join(repository, EMBEDDED_LOCK_FILE), 'utf8')
  if (embeddedLock !== state.lockBytes) {
    throw new Error(`${EMBEDDED_LOCK_FILE} is not the canonical embedded lock`)
  }
  const checkedManifest = readFileSync(
    join(repository, MIRROR_ROOT, SOURCE_MANIFEST),
    'utf8',
  )
  if (checkedManifest !== state.sourceManifest) {
    throw new Error(`${MIRROR_ROOT}/${SOURCE_MANIFEST} is not canonical`)
  }
  return state
}

function writeAtomic(path, bytes) {
  const temporary = `${path}.tmp-${process.pid}`
  try {
    writeFileSync(temporary, bytes)
    renameSync(temporary, path)
  } finally {
    rmSync(temporary, { force: true })
  }
}

function applyMirrorModes(repoRoot, files) {
  if (process.platform === 'win32') return
  for (const file of files) {
    const path = join(repoRoot, MIRROR_ROOT, file.path)
    const permissions = file.mode === '100755' ? 0o755 : 0o644
    chmodSync(path, permissions)
  }
}

function usage() {
  throw new Error('usage: node scripts/bundled-browser-assets-lock.mjs --check|--write')
}

function main() {
  const [mode, ...rest] = process.argv.slice(2)
  if (rest.length !== 0 || !['--check', '--write'].includes(mode)) usage()
  const state = createBundledBrowserAssetState()
  if (state.lock.files.length !== 24) {
    throw new Error(`frozen browser asset path count drift: ${state.lock.files.length}`)
  }
  if (mode === '--check') {
    assertBundledBrowserAssetState({ state })
    process.stdout.write(
      `bundled browser asset lock OK: ${state.lock.files.length} files, tree ${state.lock.treeSha256}\n`,
    )
    return
  }
  applyMirrorModes(REPO_ROOT, state.lock.files)
  writeAtomic(join(REPO_ROOT, LOCK_FILE), state.lockBytes)
  writeAtomic(join(REPO_ROOT, EMBEDDED_LOCK_FILE), state.lockBytes)
  writeAtomic(join(REPO_ROOT, MIRROR_ROOT, SOURCE_MANIFEST), state.sourceManifest)
  process.stdout.write(
    `wrote ${LOCK_FILE}, ${EMBEDDED_LOCK_FILE}, and ${MIRROR_ROOT}/${SOURCE_MANIFEST}\n`,
  )
}

if (process.argv[1]
    && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try {
    main()
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`)
    process.exitCode = 1
  }
}
