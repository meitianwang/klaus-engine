// Metadata-only maintenance for the exact browser runtime locks.
//
// The SDK embeds `crates/sdk/assets/exact-browser-runtime-assets-v1/
// exact-browser-runtime-assets.lock.json` (published) and keeps its companion
// candidate under `crates/sdk/tests/fixtures/...`. Their identities form one
// chain: the published lock's size and SHA-256 plus the candidate/published
// domain digests are recorded in `provider-assets.lock.json` and pinned as
// constants in `crates/sdk/src/browser_host/winldd_distribution.rs`, and the
// addon source closure digest is repeated in every Windows addon requirement.
//
// This script checks that chain and, with `--write`, reseals it after an
// intentional change to the lock content or to a source-closure file. It never
// downloads, builds, signs or approves a binary, and it refuses any lock that
// claims an issuer or a verified external artifact: a source checkout has none.
//
//   node scripts/browser-source-distribution.mjs --check
//   node scripts/browser-source-distribution.mjs --write

import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { readFileSync, writeFileSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { canonicalAsciiJson } from './playwright-process-primitives-lock.mjs'

export const RUNTIME_ROOT = 'crates/sdk/assets/exact-browser-runtime-assets-v1'
export const FIXTURE_ROOT = 'crates/sdk/tests/fixtures/exact-browser-runtime-assets-v1'
export const LOCK_FILE = 'exact-browser-runtime-assets.lock.json'
export const PROVIDER_LOCK = 'provider-assets.lock.json'
export const ADMISSION_SOURCE = 'crates/sdk/src/browser_host/winldd_distribution.rs'
export const RECEIPT_FILE = 'browser-runtime-assets.receipt.json'
export const EXTERNAL_INPUT_AVAILABILITY = 'required-distribution-input'
export const REQUIRED_BINDINGS = Object.freeze([
  'repository', 'ref', 'pipelineId', 'runId', 'jobId', 'manualApprovalId',
  'candidateSha256', 'receiptSha256',
])
export const SOURCE_CLOSURE_FILES = Object.freeze([
  'Cargo.lock', 'Cargo.toml', 'crates/browser-pipe-napi/Cargo.toml',
  'crates/browser-pipe-napi/src/lib.rs', 'rust-1.93-std-source.lock.json',
])
const CANDIDATE_DOMAIN = 'KLAUS_EXACT_BROWSER_RUNTIME_ASSETS_V1'
const PUBLISHED_DOMAIN = 'KLAUS_EXACT_BROWSER_RUNTIME_ASSETS_PUBLISHED_V1'
const SOURCE_CLOSURE_DOMAIN = 'KLAUS_BROWSER_PIPE_SOURCE_CLOSURE_V1'

export function sha256(bytes) {
  return createHash('sha256').update(bytes).digest('hex')
}

export function domainDigest(domain, value) {
  const bytes = Buffer.from(canonicalAsciiJson(value), 'ascii')
  const size = Buffer.alloc(8)
  size.writeBigUInt64BE(BigInt(bytes.length))
  return createHash('sha256').update(`${domain}\0`, 'ascii').update(size).update(bytes).digest('hex')
}

export function canonicalBytes(value) {
  return Buffer.from(`${canonicalAsciiJson(value)}\n`, 'ascii')
}

export function sourceClosure(root) {
  const files = SOURCE_CLOSURE_FILES.map((file) => {
    const bytes = readFileSync(path.join(root, file))
    return { path: file, size: bytes.length, sha256: sha256(bytes) }
  })
  return { files, sourceClosureSha256: domainDigest(SOURCE_CLOSURE_DOMAIN, files) }
}

/// Build the candidate (`published: false`) and published locks from content.
/// Key order follows the content so the canonical bytes stay stable.
export function sealLocks(content) {
  const { candidateSha256: _candidate, publishedSha256: _published, ...rest } = content
  const candidateContent = { ...rest, published: false }
  const candidate = {
    ...candidateContent,
    candidateSha256: domainDigest(CANDIDATE_DOMAIN, candidateContent),
  }
  const publishedContent = { ...candidateContent, published: true, candidateSha256: candidate.candidateSha256 }
  const published = {
    ...publishedContent,
    publishedSha256: domainDigest(PUBLISHED_DOMAIN, publishedContent),
  }
  return { candidate, published }
}

/// A source checkout records the shape of every external input but supplies
/// none of them and names no issuer.
export function validateUnconfiguredDistribution(runtime) {
  assert.equal(runtime.addonBuild.producer, null, 'a source checkout names no addon producer')
  const inputs = runtime.externalInputs
  assert.deepEqual(inputs.protectedIdentity, {
    verifier: null,
    repository: null,
    ref: null,
    jobId: null,
    requiredBindings: [...REQUIRED_BINDINGS],
    trustAnchorSource: 'platform-administrator-pinned',
    trustAnchorSha256: null,
    selfAssertedReceiptFieldsAreProof: false,
    availability: 'required-platform-administrator-input',
    artifact: null,
  }, 'an unconfigured identity cannot assert its own authority')
  assert.deepEqual(inputs.receipt, {
    requiredFile: RECEIPT_FILE,
    availability: EXTERNAL_INPUT_AVAILABILITY,
    artifact: null,
  }, 'no verified distribution receipt is available')
  assert.equal(inputs.browserArchives.length, 11, 'browser archive set drifted')
  for (const archive of inputs.browserArchives) {
    assert.equal(archive.availability, EXTERNAL_INPUT_AVAILABILITY, `${archive.requiredFile} availability drifted`)
    assert.equal(archive.artifact, null, 'recorded download metadata is not an admitted executable')
    assert.ok(archive.mirrorUrl === null || /^https:\/\//.test(archive.mirrorUrl), `${archive.requiredFile} mirror must be https or null`)
    assert.match(archive.sha256, /^[0-9a-f]{64}$/)
    assert.ok(Number.isSafeInteger(archive.size) && archive.size > 0)
  }
  assert.deepEqual(inputs.windowsAddons.map((entry) => entry.targetTriple), [
    'x86_64-pc-windows-msvc', 'aarch64-pc-windows-msvc', 'i686-pc-windows-msvc',
  ])
  for (const addon of inputs.windowsAddons) {
    assert.equal(addon.availability, EXTERNAL_INPUT_AVAILABILITY)
    assert.equal(addon.artifact, null, 'no verified Windows addon is available')
    assert.equal(addon.sourceClosureSha256, runtime.addonSourceClosure.sourceClosureSha256)
  }
}

function readAdmissionPins(root) {
  const source = readFileSync(path.join(root, ADMISSION_SOURCE), 'utf8')
  const pins = {}
  for (const name of ['PROVIDER_RUNTIME_LOCK_SHA256', 'PROVIDER_RUNTIME_CANDIDATE_SHA256', 'PROVIDER_RUNTIME_PUBLISHED_SHA256']) {
    pins[name] = source.match(new RegExp(`const ${name}: &str =\\s*"([a-f0-9]{64})";`))?.[1]
  }
  const size = source.match(/const PROVIDER_RUNTIME_LOCK_SIZE: usize = ([0-9_]+);/)?.[1]
  pins.PROVIDER_RUNTIME_LOCK_SIZE = size === undefined ? undefined : Number(size.replaceAll('_', ''))
  return { source, pins }
}

/// Check every link of the chain against the committed bytes.
export function validateSourceMetadata(root) {
  const publishedBytes = readFileSync(path.join(root, RUNTIME_ROOT, LOCK_FILE))
  const runtime = JSON.parse(publishedBytes)
  validateUnconfiguredDistribution(runtime)
  assert.deepEqual(runtime.addonSourceClosure, sourceClosure(root), 'addon source closure drifted')
  const { candidate, published } = sealLocks(runtime)
  assert.deepEqual(runtime, published, 'published lock content digest drifted')
  assert.deepEqual(publishedBytes, canonicalBytes(published), 'published lock must be canonical ASCII JSON plus LF')
  assert.deepEqual(
    readFileSync(path.join(root, FIXTURE_ROOT, LOCK_FILE)),
    canonicalBytes(candidate),
    'candidate lock differs from the published lock content',
  )
  const provider = JSON.parse(readFileSync(path.join(root, PROVIDER_LOCK), 'utf8')).browser.runtimeLock
  assert.equal(provider.length, publishedBytes.length, 'provider runtime lock length drifted')
  assert.equal(provider.sha256, sha256(publishedBytes), 'provider runtime lock SHA-256 drifted')
  assert.equal(provider.candidateSha256, runtime.candidateSha256, 'provider candidate digest drifted')
  assert.equal(provider.publishedSha256, runtime.publishedSha256, 'provider published digest drifted')
  assert.equal(provider.addonSourceClosureSha256, runtime.addonSourceClosure.sourceClosureSha256, 'provider source closure drifted')
  const { pins } = readAdmissionPins(root)
  assert.equal(pins.PROVIDER_RUNTIME_LOCK_SIZE, publishedBytes.length, 'PROVIDER_RUNTIME_LOCK_SIZE drifted')
  assert.equal(pins.PROVIDER_RUNTIME_LOCK_SHA256, sha256(publishedBytes), 'PROVIDER_RUNTIME_LOCK_SHA256 drifted')
  assert.equal(pins.PROVIDER_RUNTIME_CANDIDATE_SHA256, runtime.candidateSha256, 'PROVIDER_RUNTIME_CANDIDATE_SHA256 drifted')
  assert.equal(pins.PROVIDER_RUNTIME_PUBLISHED_SHA256, runtime.publishedSha256, 'PROVIDER_RUNTIME_PUBLISHED_SHA256 drifted')
  return runtime
}

function replaceExactlyOnce(text, pattern, replacement, label) {
  const matches = text.match(new RegExp(pattern.source, `${pattern.flags.replace('g', '')}g`))
  assert.equal(matches?.length, 1, `${label} must occur exactly once`)
  return text.replace(pattern, replacement)
}

/// Reseal the chain from the published lock's content. Content edits (for
/// example a field rename) are made in the published lock; this recomputes the
/// source closure, both digests, both lock files, the provider lock record and
/// the Rust admission pins.
export function writeSourceMetadata(root) {
  const current = JSON.parse(readFileSync(path.join(root, RUNTIME_ROOT, LOCK_FILE), 'utf8'))
  const closure = sourceClosure(root)
  const content = {
    ...current,
    addonSourceClosure: closure,
    externalInputs: {
      ...current.externalInputs,
      windowsAddons: current.externalInputs.windowsAddons.map((addon) => ({
        ...addon,
        sourceClosureSha256: closure.sourceClosureSha256,
      })),
    },
  }
  const { candidate, published } = sealLocks(content)
  validateUnconfiguredDistribution(published)
  const publishedBytes = canonicalBytes(published)
  writeFileSync(path.join(root, RUNTIME_ROOT, LOCK_FILE), publishedBytes)
  writeFileSync(path.join(root, FIXTURE_ROOT, LOCK_FILE), canonicalBytes(candidate))

  const providerPath = path.join(root, PROVIDER_LOCK)
  let provider = readFileSync(providerPath, 'utf8')
  const record = (field, value, quoted = true) => {
    const rendered = quoted ? `"${value}"` : String(value)
    provider = replaceExactlyOnce(
      provider,
      new RegExp(`("runtimeLock": \\{[\\s\\S]*?"${field}": )(?:"[0-9a-f]{64}"|[0-9]+)`),
      `$1${rendered}`,
      `provider runtimeLock.${field}`,
    )
  }
  record('length', publishedBytes.length, false)
  record('sha256', sha256(publishedBytes))
  record('candidateSha256', published.candidateSha256)
  record('publishedSha256', published.publishedSha256)
  record('addonSourceClosureSha256', closure.sourceClosureSha256)
  writeFileSync(providerPath, provider)

  const admissionPath = path.join(root, ADMISSION_SOURCE)
  let { source } = readAdmissionPins(root)
  const pin = (name, value) => {
    source = replaceExactlyOnce(
      source,
      new RegExp(`(const ${name}: &str =\\s*")[a-f0-9]{64}(";)`),
      `$1${value}$2`,
      name,
    )
  }
  pin('PROVIDER_RUNTIME_LOCK_SHA256', sha256(publishedBytes))
  pin('PROVIDER_RUNTIME_CANDIDATE_SHA256', published.candidateSha256)
  pin('PROVIDER_RUNTIME_PUBLISHED_SHA256', published.publishedSha256)
  const grouped = publishedBytes.length.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')
  source = replaceExactlyOnce(
    source,
    /(const PROVIDER_RUNTIME_LOCK_SIZE: usize = )[0-9_]+(;)/,
    `$1${grouped}$2`,
    'PROVIDER_RUNTIME_LOCK_SIZE',
  )
  writeFileSync(admissionPath, source)
  return validateSourceMetadata(root)
}

const isMain = process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1])
if (isMain) {
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
  const mode = process.argv[2]
  try {
    if (mode === '--check') validateSourceMetadata(root)
    else if (mode === '--write') writeSourceMetadata(root)
    else throw new Error('usage: node scripts/browser-source-distribution.mjs --check|--write')
    process.stdout.write(`browser source distribution metadata ${mode === '--write' ? 'resealed' : 'verified'}\n`)
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.stack ?? error.message : String(error)}\n`)
    process.exitCode = 1
  }
}
