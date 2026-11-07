import assert from 'node:assert/strict'
import { test } from 'node:test'
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import {
  ADMISSION_SOURCE, FIXTURE_ROOT, LOCK_FILE, PROVIDER_LOCK, RUNTIME_ROOT, SOURCE_CLOSURE_FILES,
  canonicalBytes, sealLocks, validateSourceMetadata, validateUnconfiguredDistribution, writeSourceMetadata,
} from './browser-source-distribution.mjs'
import { validateLockShape } from './check-distribution-packages.mjs'

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const runtime = JSON.parse(readFileSync(path.join(root, RUNTIME_ROOT, LOCK_FILE)))

test('committed runtime locks, provider record and Rust admission pins form one sealed chain', () => {
  validateSourceMetadata(root)
  validateLockShape(JSON.parse(readFileSync(path.join(root, PROVIDER_LOCK))))
})

test('an unconfigured source checkout never turns self-asserted fields into distribution proof', () => {
  for (const change of [
    (lock) => { lock.addonBuild.producer = 'claimed-producer' },
    (lock) => { lock.externalInputs.protectedIdentity.verifier = 'untrusted.mjs' },
    (lock) => { lock.externalInputs.protectedIdentity.repository = 'claimed/repository' },
    (lock) => { lock.externalInputs.protectedIdentity.ref = 'claimed-ref' },
    (lock) => { lock.externalInputs.protectedIdentity.jobId = 'claimed-job' },
    (lock) => { lock.externalInputs.protectedIdentity.trustAnchorSha256 = 'aa'.repeat(32) },
    (lock) => { lock.externalInputs.protectedIdentity.availability = 'verified' },
    (lock) => { lock.externalInputs.protectedIdentity.selfAssertedReceiptFieldsAreProof = true },
    (lock) => { lock.externalInputs.protectedIdentity.requiredBindings.pop() },
    (lock) => { lock.externalInputs.protectedIdentity.artifact = { verified: true } },
    (lock) => { lock.externalInputs.receipt.artifact = { verified: true } },
    (lock) => { lock.externalInputs.receipt.availability = 'verified' },
    (lock) => { lock.externalInputs.browserArchives[0].artifact = { absolutePath: '/tmp/existing' } },
    (lock) => { lock.externalInputs.browserArchives[0].availability = 'verified' },
    (lock) => { lock.externalInputs.browserArchives[0].mirrorUrl = 'http://insecure.example' },
    (lock) => { lock.externalInputs.windowsAddons[0].artifact = { absolutePath: '/tmp/existing' } },
    (lock) => { lock.externalInputs.windowsAddons[0].availability = 'verified' },
    (lock) => { lock.externalInputs.windowsAddons[0].sourceClosureSha256 = '00'.repeat(32) },
    (lock) => { delete lock.externalInputs.protectedIdentity },
  ]) {
    const altered = structuredClone(runtime)
    change(altered)
    assert.throws(() => validateUnconfiguredDistribution(altered))
  }
})

test('native-host source requirements refuse an injected producer, evidence or availability', () => {
  const base = JSON.parse(readFileSync(path.join(root, PROVIDER_LOCK)))
  for (const [field, value] of [
    ['producer', 'claimed-producer'], ['availability', 'verified'], ['artifact', { verified: true }],
  ]) {
    const altered = structuredClone(base)
    altered.browser.nativeHostDistribution[field] = value
    assert.throws(() => validateLockShape(altered))
  }
})

test('sealing is deterministic and reproduces the committed candidate and published bytes', () => {
  const { candidate, published } = sealLocks(runtime)
  assert.deepEqual(canonicalBytes(published), readFileSync(path.join(root, RUNTIME_ROOT, LOCK_FILE)))
  assert.deepEqual(canonicalBytes(candidate), readFileSync(path.join(root, FIXTURE_ROOT, LOCK_FILE)))
  assert.equal(candidate.published, false)
  assert.equal(published.published, true)
  assert.equal(published.candidateSha256, candidate.candidateSha256)
})

function scratchRoot() {
  const temporary = mkdtempSync(path.join(tmpdir(), 'klaus-source-distribution-'))
  for (const file of [
    ...SOURCE_CLOSURE_FILES,
    `${RUNTIME_ROOT}/${LOCK_FILE}`,
    `${FIXTURE_ROOT}/${LOCK_FILE}`,
    PROVIDER_LOCK,
    ADMISSION_SOURCE,
  ]) {
    mkdirSync(path.dirname(path.join(temporary, file)), { recursive: true })
    copyFileSync(path.join(root, file), path.join(temporary, file))
  }
  return temporary
}

test('every link of the chain fails independently when tampered', () => {
  const temporary = scratchRoot()
  try {
    validateSourceMetadata(temporary)
    for (const relative of [
      `${FIXTURE_ROOT}/${LOCK_FILE}`,
      SOURCE_CLOSURE_FILES[0],
    ]) {
      const file = path.join(temporary, relative)
      const original = readFileSync(file)
      writeFileSync(file, Buffer.concat([original, Buffer.from('tamper')]))
      assert.throws(() => validateSourceMetadata(temporary), relative)
      writeFileSync(file, original)
    }
    const admission = path.join(temporary, ADMISSION_SOURCE)
    const source = readFileSync(admission, 'utf8')
    writeFileSync(admission, source.replace(runtime.candidateSha256, '00'.repeat(32)))
    assert.throws(() => validateSourceMetadata(temporary), /PROVIDER_RUNTIME_CANDIDATE_SHA256/)
    writeFileSync(admission, source)
    const provider = path.join(temporary, PROVIDER_LOCK)
    const record = readFileSync(provider, 'utf8')
    writeFileSync(provider, record.replace(runtime.publishedSha256, '00'.repeat(32)))
    assert.throws(() => validateSourceMetadata(temporary), /published digest/)
    writeFileSync(provider, record)
    validateSourceMetadata(temporary)
  } finally {
    rmSync(temporary, { recursive: true, force: true })
  }
})

test('resealing after an intentional source-closure change updates every pin consistently', () => {
  const temporary = scratchRoot()
  try {
    const closureFile = path.join(temporary, 'crates/browser-pipe-napi/src/lib.rs')
    writeFileSync(closureFile, Buffer.concat([readFileSync(closureFile), Buffer.from('\n// change\n')]))
    assert.throws(() => validateSourceMetadata(temporary), /source closure/)
    const resealed = writeSourceMetadata(temporary)
    assert.notEqual(resealed.addonSourceClosure.sourceClosureSha256, runtime.addonSourceClosure.sourceClosureSha256)
    assert.notEqual(resealed.publishedSha256, runtime.publishedSha256)
    validateSourceMetadata(temporary)
  } finally {
    rmSync(temporary, { recursive: true, force: true })
  }
})
