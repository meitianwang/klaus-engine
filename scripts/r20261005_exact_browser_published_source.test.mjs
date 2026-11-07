// Defect: a valid published browser asset update reran legacy migrations because
// input-format detection depended on equality with an older candidate fixture.
// Work item: 2026-10-05 browser tab mentions. The locked RuntimeTransport V1
// contract identifies published input; fixture bytes remain strictly checked.
import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { test } from 'node:test'
import { fileURLToPath } from 'node:url'
import { createBundledBrowserAssetState, MIRROR_ROOT } from './bundled-browser-assets-lock.mjs'
import {
  CANDIDATE_LOCK,
  CANDIDATE_ROOT,
  createExactBrowserCandidateState,
  SOURCE_LOCK,
  SOURCE_ROOT,
} from './exact-browser-candidate.mjs'

const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..')

function publishedFixture() {
  const root = mkdtempSync(join(tmpdir(), 'klaus-published-browser-regression-'))
  const lock = JSON.parse(readFileSync(join(REPO_ROOT, SOURCE_LOCK), 'utf8'))
  for (const file of lock.files) {
    const bytes = readFileSync(join(REPO_ROOT, SOURCE_ROOT, file.path))
    for (const target of [SOURCE_ROOT, MIRROR_ROOT]) {
      const path = join(root, target, file.path)
      mkdirSync(dirname(path), { recursive: true })
      writeFileSync(path, bytes)
      chmodSync(path, file.mode === '100755' ? 0o755 : 0o644)
    }
  }
  mkdirSync(join(root, CANDIDATE_ROOT), { recursive: true })
  writeFileSync(
    join(root, CANDIDATE_ROOT, CANDIDATE_LOCK),
    readFileSync(join(REPO_ROOT, CANDIDATE_ROOT, CANDIDATE_LOCK)),
  )
  for (const args of [['init', '-q'], ['add', SOURCE_ROOT]]) {
    const result = spawnSync('git', args, { cwd: root, encoding: 'utf8' })
    assert.equal(result.status, 0, result.stderr)
  }
  return root
}

function updateApi(root, change) {
  const api = JSON.parse(readFileSync(join(root, SOURCE_ROOT, 'docs/api.json'), 'utf8'))
  change(api)
  const bytes = `${JSON.stringify(api, null, 2)}\n`
  for (const target of [SOURCE_ROOT, MIRROR_ROOT]) {
    writeFileSync(join(root, target, 'docs/api.json'), bytes)
  }
  const state = createBundledBrowserAssetState({ repoRoot: root })
  writeFileSync(join(root, SOURCE_LOCK), state.lockBytes)
  return state
}

test('published browser updates preserve all locked source bytes and require fixture refresh', () => {
  const root = publishedFixture()
  try {
    const source = updateApi(root, (api) => {
      api.browser['tabs.get'] += ' Updated published API documentation.'
    })
    const oldFixture = JSON.parse(readFileSync(join(root, CANDIDATE_ROOT, CANDIDATE_LOCK), 'utf8'))
    assert.notEqual(source.lock.treeSha256, oldFixture.treeSha256)
    const candidate = createExactBrowserCandidateState({ repoRoot: root })
    assert.equal(candidate.lock.treeSha256, source.lock.treeSha256)
    assert.equal(candidate.lock.sourceTreeSha256, oldFixture.sourceTreeSha256)
    for (const entry of candidate.entries) {
      assert.deepEqual(entry.bytes, readFileSync(join(root, SOURCE_ROOT, entry.path)), entry.path)
    }
    const api = JSON.parse(candidate.entries.find((entry) => entry.path === 'docs/api.json').bytes)
    assert.equal(typeof api.extension.scripts[0], 'object')
    assert.equal(api.extension.scripts[0].directExitCode, 64)
  } finally {
    rmSync(root, { recursive: true, force: true })
  }
})

test('unsupported published transport contracts are refused instead of remigrated', () => {
  const root = publishedFixture()
  try {
    updateApi(root, (api) => { api.runtimeTransport.owner = 'node' })
    assert.throws(
      () => createExactBrowserCandidateState({ repoRoot: root }),
      /runtime transport contract is unsupported/,
    )
  } finally {
    rmSync(root, { recursive: true, force: true })
  }
})
