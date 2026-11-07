import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import {
  chmodSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  symlinkSync,
  unlinkSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { describe, it } from 'node:test'
import { fileURLToPath } from 'node:url'

import {
  assertBundledBrowserAssetState,
  createBundledBrowserAssetState,
  EMBEDDED_LOCK_FILE,
  LOCK_FILE,
  MIRROR_ROOT,
  SOURCE_MANIFEST,
} from './bundled-browser-assets-lock.mjs'

const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..')

function git(root, ...args) {
  const result = spawnSync('git', args, {
    cwd: root,
    encoding: 'utf8',
    env: { ...process.env, LC_ALL: 'C' },
  })
  assert.equal(result.status, 0, result.stderr)
}

function fixture() {
  const root = mkdtempSync(join(tmpdir(), 'klaus-browser-assets-'))
  mkdirSync(join(root, 'source', 'bin'), { recursive: true })
  mkdirSync(join(root, 'mirror', 'bin'), { recursive: true })
  writeFileSync(join(root, 'source', 'a.txt'), 'alpha\n')
  writeFileSync(join(root, 'mirror', 'a.txt'), 'alpha\n')
  writeFileSync(join(root, 'source', 'bin', 'run'), '#!/bin/sh\nexit 0\n')
  writeFileSync(join(root, 'mirror', 'bin', 'run'), '#!/bin/sh\nexit 0\n')
  chmodSync(join(root, 'source', 'bin', 'run'), 0o755)
  chmodSync(join(root, 'mirror', 'bin', 'run'), 0o755)
  git(root, 'init', '-q')
  git(root, 'add', 'source')
  git(root, 'update-index', '--chmod=+x', 'source/bin/run')
  return root
}

function createFixtureState(root) {
  return createBundledBrowserAssetState({
    repoRoot: root,
    sourceRoot: 'source',
    mirrorRoot: 'mirror',
  })
}

describe('bundled browser full-tree asset lock', () => {
  it('round-trips the checked 24-file canonical lock and source manifest', () => {
    const state = assertBundledBrowserAssetState({ repoRoot: REPO_ROOT })
    assert.equal(state.lock.files.length, 24)
    assert.match(state.lock.pathSetSha256, /^[0-9a-f]{64}$/)
    assert.match(state.lock.treeSha256, /^[0-9a-f]{64}$/)
    assert.equal(
      readFileSync(join(REPO_ROOT, LOCK_FILE), 'utf8'),
      `${JSON.stringify(state.lock)}\n`,
    )
    assert.equal(
      readFileSync(join(REPO_ROOT, EMBEDDED_LOCK_FILE), 'utf8'),
      `${JSON.stringify(state.lock)}\n`,
    )
    const manifest = readFileSync(
      join(REPO_ROOT, MIRROR_ROOT, SOURCE_MANIFEST),
      'utf8',
    )
    assert.equal(manifest, state.sourceManifest)
    assert.equal(manifest.split('\n').filter(Boolean).length, 24)
  })

  it('uses Git index modes and deterministic binary aggregate hashes', () => {
    const root = fixture()
    try {
      const first = createFixtureState(root)
      const second = createFixtureState(root)
      assert.deepEqual(first, second)
      assert.deepEqual(
        first.lock.files.map(({ path, mode }) => ({ path, mode })),
        [
          { path: 'a.txt', mode: '100644' },
          { path: 'bin/run', mode: '100755' },
        ],
      )
      assert.notEqual(first.lock.pathSetSha256, first.lock.treeSha256)
    } finally {
      rmSync(root, { recursive: true, force: true })
    }
  })

  it('fails closed for mirror byte, mode, path-set, and index drift', () => {
    const root = fixture()
    try {
      writeFileSync(join(root, 'mirror', 'a.txt'), 'changed\n')
      assert.throws(() => createFixtureState(root), /mirror bytes drifted: a\.txt/)

      writeFileSync(join(root, 'mirror', 'a.txt'), 'alpha\n')
      if (process.platform !== 'win32') {
        chmodSync(join(root, 'mirror', 'a.txt'), 0o600)
        assert.throws(
          () => createFixtureState(root),
          /mirror filesystem mode drifted: a\.txt/,
        )
        chmodSync(join(root, 'mirror', 'a.txt'), 0o644)
      }
      writeFileSync(join(root, 'mirror', 'extra.txt'), 'extra\n')
      assert.throws(() => createFixtureState(root), /mirror path count drift/)

      unlinkSync(join(root, 'mirror', 'extra.txt'))
      writeFileSync(join(root, 'source', 'untracked.txt'), 'untracked\n')
      writeFileSync(join(root, 'mirror', 'untracked.txt'), 'untracked\n')
      assert.throws(() => createFixtureState(root), /git index path count drift/)
    } finally {
      rmSync(root, { recursive: true, force: true })
    }
  })

  it('rejects symlinks before hashing', { skip: process.platform === 'win32' }, () => {
    const root = fixture()
    try {
      symlinkSync('a.txt', join(root, 'source', 'alias.txt'))
      symlinkSync('a.txt', join(root, 'mirror', 'alias.txt'))
      assert.throws(() => createFixtureState(root), /may not contain symlink: alias\.txt/)
    } finally {
      rmSync(root, { recursive: true, force: true })
    }
  })

  it('exposes a deterministic repository check command', () => {
    const checked = spawnSync(
      process.execPath,
      ['scripts/bundled-browser-assets-lock.mjs', '--check'],
      { cwd: REPO_ROOT, encoding: 'utf8' },
    )
    assert.equal(checked.status, 0, checked.stderr)
    assert.match(checked.stdout, /24 files/)
  })
})
