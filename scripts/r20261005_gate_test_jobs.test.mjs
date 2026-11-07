// Defect: a saturated host made tiny tests take seconds under the fixed -j4 pool.
// Work item: 2026-10-05 validation. Concurrency is bounded independently of the
// complete workspace/all-features test scope and existing timeout/retry policy.
import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { test } from 'node:test'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { nextestGateArguments, parseGateOptions } from './gate-options.mjs'

const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..')

test('gate job options default to four and reject malformed, duplicate, and unbounded values', () => {
  assert.deepEqual(parseGateOptions([]), { keepGoing: false, testJobs: 4 })
  assert.deepEqual(parseGateOptions(['--test-jobs', '1', '--keep-going']), { keepGoing: true, testJobs: 1 })
  assert.deepEqual(parseGateOptions(['--test-jobs=2']), { keepGoing: false, testJobs: 2 })
  for (const args of [
    ['--test-jobs'], ['--test-jobs', '0'], ['--test-jobs', '5'],
    ['--test-jobs', '-1'], ['--test-jobs', '1.5'], ['--test-jobs', '01'],
    ['--test-jobs='], ['--test-jobs', '--keep-going'],
    ['--test-jobs', '1', '--test-jobs=2'], ['--unknown'],
  ]) assert.throws(() => parseGateOptions(args), /test-jobs|unknown gate argument/)
  for (const value of [0, 5, -1, 1.5, NaN, '1']) {
    assert.throws(() => nextestGateArguments(value), /integer from 1 through 4/)
  }
})

test('actual gate spawns the complete suite with the requested test pool and retains default scope', () => {
  const root = mkdtempSync(join(tmpdir(), 'klaus-gate-jobs-regression-'))
  try {
    const capture = join(root, 'commands.jsonl')
    const recorder = join(root, 'record.mjs')
    writeFileSync(recorder, `
import assert from 'node:assert/strict';
import childProcess from 'node:child_process';
import { appendFileSync } from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
childProcess.spawnSync = (program, args) => {
  assert.ok(['cargo', 'node'].includes(program), 'Unexpected gate command');
  appendFileSync(process.env.KLAUS_GATE_ARGUMENTS_LOG, JSON.stringify({ program, args }) + '\\n');
  return { status: 0 };
};
syncBuiltinESMExports();
`)
    for (const args of [[], ['--test-jobs', '1']]) {
      writeFileSync(capture, '')
      const result = spawnSync(process.execPath, ['--import', pathToFileURL(recorder).href, 'scripts/gate.mjs', ...args], {
        cwd: REPO_ROOT,
        encoding: 'utf8',
        env: { ...process.env, KLAUS_GATE_ARGUMENTS_LOG: capture },
      })
      assert.equal(result.status, 0, result.stderr)
      const commands = readFileSync(capture, 'utf8').trim().split('\n').map((line) => JSON.parse(line))
      assert.equal(commands.length, 7)
      const nextest = commands.find((command) => command.program === 'cargo' && command.args[0] === 'nextest')
      assert.deepEqual(nextest.args, [
        'nextest', 'run', '--locked', '--workspace', '--all-features',
        args.length === 0 ? '-j4' : '-j1', '--no-fail-fast',
      ])
      assert.ok(commands.some((command) => command.program === 'cargo'
        && command.args.includes('clippy') && command.args.includes('--all-targets')
        && command.args.includes('--all-features')))
      assert.ok(commands.some((command) => command.program === 'node'
        && command.args[0] === '--test' && command.args[1] === 'scripts/'))
    }
    const rejected = spawnSync(process.execPath, ['scripts/gate.mjs', '--test-jobs', '0'], {
      cwd: REPO_ROOT, encoding: 'utf8',
    })
    assert.equal(rejected.status, 1)
    assert.equal(rejected.stdout, '')
    assert.match(rejected.stderr, /requires an integer from 1 through 4/)
  } finally {
    rmSync(root, { recursive: true, force: true })
  }
})
