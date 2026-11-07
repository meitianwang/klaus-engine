import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import {
  DEFAULT_KEEP_DAYS,
  PROTECTED_ROOT_FILES,
  checkBudget,
  classify,
  formatBytes,
  parseArguments,
  parseSize,
  planPrune,
  scanTarget,
  summarize,
} from './target-budget.mjs'

const DAY_MS = 24 * 60 * 60 * 1000
const NOW = Date.UTC(2026, 6, 30)

/** A fake target tree: paths map to `{ size, mtimeMs }`, directories are implied. */
function fakeFs(files) {
  const readdir = (dir, _options) => {
    dir = dir.replaceAll('\\', '/')
    const prefix = dir === '.' ? '' : `${dir}/`
    const names = new Map()
    for (const path of Object.keys(files)) {
      if (!path.startsWith(prefix)) continue
      const rest = path.slice(prefix.length)
      if (rest.length === 0) continue
      const [name, ...deeper] = rest.split('/')
      names.set(name, deeper.length > 0)
    }
    if (names.size === 0 && !Object.keys(files).some((path) => path.startsWith(prefix))) {
      const error = new Error(`ENOENT: ${dir}`)
      error.code = 'ENOENT'
      throw error
    }
    return [...names].map(([name, isDir]) => ({
      name,
      isDirectory: () => isDir,
    }))
  }
  const stat = (path) => {
    path = path.replaceAll('\\', '/')
    const entry = files[path]
    if (!entry) throw new Error(`no such fake file: ${path}`)
    return { size: entry.size, mtimeMs: entry.mtimeMs }
  }
  return { readdir, stat }
}

describe('parseSize and formatBytes', () => {
  it('reads the sizes a human would type', () => {
    assert.equal(parseSize('1024'), 1024)
    assert.equal(parseSize('40G'), 40 * 1024 ** 3)
    assert.equal(parseSize('500MiB'), 500 * 1024 ** 2)
    assert.equal(parseSize(' 1.5G '), Math.round(1.5 * 1024 ** 3))
  })

  it('refuses sizes it cannot represent exactly', () => {
    assert.throws(() => parseSize('lots'), /unparseable size/u)
    assert.throws(() => parseSize('40Q'), /unparseable size/u)
  })

  it('renders bytes at the largest unit that stays under four digits', () => {
    assert.equal(formatBytes(512), '512 B')
    assert.equal(formatBytes(2048), '2.0 KiB')
    assert.equal(formatBytes(250 * 1024 ** 3), '250.0 GiB')
  })
})

describe('classify', () => {
  it('treats unexpected root files as scratch and Cargo files as protected', () => {
    for (const name of PROTECTED_ROOT_FILES) assert.equal(classify(name), 'protected')
    assert.equal(classify('wire_dump.log'), 'scratch')
    assert.equal(classify('fix_domain_manifest.py'), 'scratch')
  })

  it('separates the per-profile buckets that prune treats differently', () => {
    assert.equal(classify('debug/incremental'), 'incremental')
    assert.equal(classify('debug/deps/libcore-abc.rlib'), 'deps')
    assert.equal(classify('debug/build/foo-abc'), 'build')
    assert.equal(classify('debug/examples'), 'examples')
    assert.equal(classify('debug/klaus-agent-server'), 'output')
    assert.equal(classify('x86_64-pc-windows-msvc/debug'), 'output')
  })
})

describe('planPrune', () => {
  const units = [
    { path: 'CACHEDIR.TAG', category: 'protected', bytes: 43, files: 1, mtimeMs: NOW },
    { path: 'wire_dump.log', category: 'scratch', bytes: 5_000_000, files: 1, mtimeMs: NOW },
    {
      path: 'debug/incremental/core-live',
      category: 'incremental',
      bytes: 74_000,
      files: 900,
      mtimeMs: NOW,
    },
    {
      path: 'debug/incremental/core-abandoned',
      category: 'incremental',
      bytes: 61_000,
      files: 800,
      mtimeMs: NOW - 30 * DAY_MS,
    },
    { path: 'debug/deps/fresh.rlib', category: 'deps', bytes: 500, files: 1, mtimeMs: NOW },
    {
      path: 'debug/deps/stale.rlib',
      category: 'deps',
      bytes: 700,
      files: 1,
      mtimeMs: NOW - 30 * DAY_MS,
    },
    { path: 'debug/klaus-agent-server', category: 'output', bytes: 900, files: 1, mtimeMs: NOW },
  ]

  it('drops root scratch regardless of age', () => {
    const { removals } = planPrune({ units, now: NOW })
    const byPath = new Map(removals.map((removal) => [removal.path, removal.reason]))
    assert.equal(byPath.get('wire_dump.log'), 'regenerable')
  })

  // Wiping the live cache costs about 30 seconds on every later edit, so the
  // per-crate caches age like any other artifact instead of going as a block.
  it('keeps the incremental cache a build is still using and drops abandoned ones', () => {
    const { removals } = planPrune({ units, now: NOW })
    const paths = removals.map((removal) => removal.path)
    assert.ok(!paths.includes('debug/incremental/core-live'))
    assert.ok(paths.includes('debug/incremental/core-abandoned'))
  })

  it('keeps fresh artifacts and Cargo-owned files, drops only stale ones', () => {
    const { removals, bytes } = planPrune({ units, now: NOW })
    const paths = removals.map((removal) => removal.path)
    assert.ok(paths.includes('debug/deps/stale.rlib'))
    assert.ok(!paths.includes('debug/deps/fresh.rlib'))
    assert.ok(!paths.includes('CACHEDIR.TAG'), 'Cargo files are never pruned')
    assert.ok(!paths.includes('debug/klaus-agent-server'), 'built outputs are never pruned')
    assert.equal(bytes, 5_000_000 + 61_000 + 700)
  })

  it('honours a wider keep window', () => {
    const { removals } = planPrune({ units, keepDays: 90, now: NOW })
    assert.ok(!removals.some((removal) => removal.path === 'debug/deps/stale.rlib'))
  })

  it('prunes every artifact when the keep window is zero', () => {
    const { removals } = planPrune({ units, keepDays: 0, now: NOW })
    assert.ok(removals.some((removal) => removal.path === 'debug/deps/fresh.rlib'))
  })

  it('prunes an artifact whose age lands exactly on the window', () => {
    const onTheBoundary = [
      {
        path: 'debug/deps/exactly-7d.rlib',
        category: 'deps',
        bytes: 1,
        files: 1,
        mtimeMs: NOW - 7 * DAY_MS,
      },
    ]
    const { removals } = planPrune({ units: onTheBoundary, keepDays: 7, now: NOW })
    assert.equal(removals.length, 1, 'age >= window prunes, so the boundary is not ambiguous')
  })
})

describe('checkBudget', () => {
  it('passes at exactly the budget', () => {
    assert.deepEqual(checkBudget({ totalBytes: 100, budgetBytes: 100 }), [])
  })

  it('reports the overage when over budget', () => {
    const [violation] = checkBudget({ totalBytes: 3 * 1024 ** 3, budgetBytes: 1024 ** 3 })
    assert.match(violation, /3\.0 GiB.*1\.0 GiB budget.*by 2\.0 GiB/u)
  })
})

describe('summarize', () => {
  it('totals bytes and files per category, largest first', () => {
    const summary = summarize([
      { category: 'deps', bytes: 10, files: 2, path: 'a', mtimeMs: NOW },
      { category: 'deps', bytes: 5, files: 1, path: 'b', mtimeMs: NOW },
      { category: 'incremental', bytes: 100, files: 9, path: 'c', mtimeMs: NOW },
    ])
    assert.equal(summary.totalBytes, 115)
    assert.equal(summary.totalFiles, 12)
    assert.deepEqual(summary.groups, [
      { category: 'incremental', bytes: 100, files: 9 },
      { category: 'deps', bytes: 15, files: 3 },
    ])
  })
})

describe('scanTarget', () => {
  it('keeps incremental caches per crate and deps per file', () => {
    const { readdir, stat } = fakeFs({
      'CACHEDIR.TAG': { size: 43, mtimeMs: NOW },
      'stray.log': { size: 7, mtimeMs: NOW },
      'debug/incremental/core-1/part.bin': { size: 300, mtimeMs: NOW },
      'debug/incremental/core-2/part.bin': { size: 400, mtimeMs: NOW - 9 * DAY_MS },
      'debug/deps/libcore-abc.rlib': { size: 50, mtimeMs: NOW - 9 * DAY_MS },
      'debug/deps/libcore-def.rlib': { size: 60, mtimeMs: NOW },
      'debug/klaus-agent-server': { size: 11, mtimeMs: NOW },
    })

    const units = scanTarget('.', { readdir, stat })
    const byPath = new Map(units.map((unit) => [unit.path, unit]))

    // One unit per crate cache, each carrying its own mtime, so a stale cache can
    // be dropped without taking the one in use with it.
    assert.equal(byPath.get('debug/incremental/core-1').bytes, 300)
    assert.equal(byPath.get('debug/incremental/core-1').category, 'incremental')
    assert.equal(byPath.get('debug/incremental/core-2').bytes, 400)
    assert.equal(byPath.get('debug/incremental/core-2').mtimeMs, NOW - 9 * DAY_MS)
    assert.equal(byPath.get('debug/deps/libcore-abc.rlib').bytes, 50)
    assert.equal(byPath.get('debug/deps/libcore-def.rlib').bytes, 60)
    assert.equal(byPath.get('CACHEDIR.TAG').category, 'protected')
    assert.equal(byPath.get('stray.log').category, 'scratch')
    assert.equal(byPath.get('debug/klaus-agent-server').category, 'output')
    assert.equal(summarize(units).totalBytes, 43 + 7 + 700 + 50 + 60 + 11)
  })

  it('feeds a plan that spares the fresh artifact and the Cargo file', () => {
    const { readdir, stat } = fakeFs({
      'CACHEDIR.TAG': { size: 43, mtimeMs: NOW },
      'debug/deps/old.rlib': { size: 50, mtimeMs: NOW - 30 * DAY_MS },
      'debug/deps/new.rlib': { size: 60, mtimeMs: NOW },
    })
    const { removals } = planPrune({ units: scanTarget('.', { readdir, stat }), now: NOW })
    assert.deepEqual(
      removals.map((removal) => removal.path),
      ['debug/deps/old.rlib'],
    )
  })
})

describe('parseArguments', () => {
  it('requires exactly one mode', () => {
    assert.throws(() => parseArguments([]), /usage:/u)
    assert.throws(() => parseArguments(['--report', '--check']), /exactly one/u)
    assert.equal(parseArguments(['--report']).mode, 'report')
  })

  it('defaults the keep window and budget', () => {
    const options = parseArguments(['--prune'])
    assert.equal(options.keepDays, DEFAULT_KEEP_DAYS)
    assert.equal(options.budget, '40G')
  })

  it('reads overrides and rejects nonsense', () => {
    assert.equal(parseArguments(['--prune', '--keep-days', '3']).keepDays, 3)
    assert.equal(parseArguments(['--check', '--budget', '80G']).budget, '80G')
    assert.throws(() => parseArguments(['--prune', '--keep-days', '-1']), /non-negative/u)
    assert.throws(() => parseArguments(['--sweep']), /unknown argument/u)
  })
})
