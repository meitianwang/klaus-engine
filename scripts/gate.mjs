// Full local gate for changes whose impact requires workspace-wide validation.
//
// This exists because the checks used to live in a pipeline template that is
// gone: the Rust commands, the script tests and the asset/budget checks each had
// to be remembered separately, so in practice only the Rust ones ever ran and
// `target-budget` never did (build output had reached 91.9 GiB before this file
// existed). One entry point, one exit code.
//
//   node scripts/gate.mjs            run every step, stop at the first failure
//   node scripts/gate.mjs --keep-going  run all steps, then report
//   node scripts/gate.mjs --test-jobs 1  serialize the complete suite on busy hosts
//
// Deliberately NOT here: `check-distribution-packages.mjs` without
// `--skip-package` (crates.io packaging, which nothing in this repository does),
// `playwright-process-primitives-lock.mjs` and `exact-browser-candidate.mjs`
// (both need vendoring inputs a plain checkout does not have; their invariants
// are covered by their own tests under `node --test`), and
// `generate-node20-path-case-data.mjs` (a fixture generator, not a check).

import { spawnSync } from 'node:child_process'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { createGateChildProfile } from './gate-environment.mjs'
import { readNextestFailures } from './gate-junit.mjs'
import { nextestGateArguments, parseGateOptions } from './gate-options.mjs'

const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..')

// Compile and test concurrency is capped at 4. On machines running an
// endpoint-security (EDR) agent every file event is inspected, and full
// core-count parallelism produces events faster than the agent can drain them:
// the agent then pegs a core and starves the very processes it scans, and tests
// die on the 60s budget even though they finish in milliseconds on an idle
// machine. At -j4 the event rate stays under the agent's drain rate, so the
// suite usually finishes FASTER than at full parallelism, not slower.
const CAPPED_BUILD_ENV = { CARGO_BUILD_JOBS: '4' }

// The script tests pin Node 20: the case-data lock regenerates Unicode tables
// byte-for-byte from the running Node's ICU, and Node 22+ also no longer
// accepts a bare directory after --test, so on a newer major the suite fails
// with a misleading MODULE_NOT_FOUND before a single test runs. Fail fast with
// the actual reason instead.
if (!process.version.startsWith('v20.')) {
  console.error(
    `gate: requires Node 20 (script tests pin Node 20.19.4 semantics); running ${process.version}`,
  )
  process.exit(1)
}

let options
try {
  options = parseGateOptions(process.argv.slice(2))
} catch (error) {
  console.error(`gate: ${error.message}`)
  process.exit(1)
}

// Ordered so the cheap correctness checks report in seconds; the build-output
// budget runs last because it is housekeeping and must not hide a real failure.
const STEPS = Object.freeze([
  {
    name: 'fmt',
    command: 'cargo',
    args: ['fmt', '--all', '--', '--check'],
  },
  {
    name: 'bundled browser asset lock',
    command: 'node',
    args: ['scripts/bundled-browser-assets-lock.mjs', '--check'],
  },
  {
    name: 'distribution assets',
    command: 'node',
    args: ['scripts/check-distribution-packages.mjs', '--skip-package'],
  },
  {
    name: 'script tests',
    command: 'node',
    // A glob would be shell-expanded; node resolves the pattern itself here.
    args: ['--test', 'scripts/'],
  },
  {
    name: 'clippy',
    command: 'cargo',
    args: [
      'clippy',
      '--locked',
      '--workspace',
      '--all-targets',
      '--all-features',
      '--',
      '-D',
      'warnings',
    ],
    env: CAPPED_BUILD_ENV,
  },
  {
    // --no-fail-fast + the serial rerun below: a test killed by the 60s budget
    // on a saturated machine is indistinguishable from a hang in the aggregate
    // run. The rerun re-runs just the failed tests with -j1; with no
    // competition for CPU the starvation is gone, so a test that stays red
    // there is a real defect and one that passes was starved.
    name: 'tests',
    command: 'cargo',
    args: nextestGateArguments(options.testJobs),
    env: CAPPED_BUILD_ENV,
    rerunFailuresSerially: true,
  },
  {
    // Last, and above the script's own 40G default: this catches runaway build
    // output (it was written after `target/` reached 248 GiB) and a single gate
    // run adds a couple of GiB, so a threshold near the steady state would fail
    // right after the gate itself passed and would hide the results above it.
    name: 'target budget',
    command: 'node',
    args: ['scripts/target-budget.mjs', '--check', '--budget', '60G'],
  },
])

function runStep(step) {
  process.stdout.write(`\n== ${step.name} ==\n`)
  const started = Date.now()
  const profile = createGateChildProfile(step.env)
  let result
  try {
    result = spawnSync(step.command, step.args, {
      cwd: REPO_ROOT,
      stdio: 'inherit',
      env: profile.env,
    })
  } finally {
    profile.dispose()
  }
  const seconds = ((Date.now() - started) / 1000).toFixed(1)
  if (result.error) {
    return { name: step.name, ok: false, seconds, started, detail: result.error.message }
  }
  const ok = result.status === 0
  return {
    name: step.name,
    ok,
    seconds,
    started,
    detail: ok ? '' : `exit ${result.status ?? `signal ${result.signal}`}`,
  }
}

const keepGoing = options.keepGoing
const results = []
for (const step of STEPS) {
  let result = runStep(step)
  if (!result.ok && step.rerunFailuresSerially) {
    const failed = readNextestFailures(REPO_ROOT, { notBefore: result.started })
    if (failed && failed.length > 0) {
      process.stdout.write(
        `\n== tests: serial rerun of ${failed.length} failed test(s) (starvation check) ==\n`,
      )
      const rerun = runStep({
        name: 'tests (serial rerun)',
        command: 'cargo',
        args: [
          'nextest',
          'run',
          '--locked',
          '--workspace',
          '--all-features',
          '-j1',
          '-E',
          failed.map((name) => `test(${name})`).join(' or '),
        ],
        env: CAPPED_BUILD_ENV,
      })
      const totalSeconds = (parseFloat(result.seconds) + parseFloat(rerun.seconds)).toFixed(1)
      result = rerun.ok
        ? {
            ...result,
            ok: true,
            seconds: totalSeconds,
            detail: `${result.detail}; ${failed.length} failed test(s) passed on serial rerun`,
          }
        : { ...result, seconds: totalSeconds, detail: `${result.detail}; serial rerun also failed` }
    }
  }
  results.push(result)
  if (!result.ok && !keepGoing) break
}

process.stdout.write('\n== gate summary ==\n')
for (const { name, ok, seconds, detail } of results) {
  process.stdout.write(
    `${ok ? 'ok  ' : 'FAIL'}  ${seconds.padStart(6)}s  ${name}${detail ? `  (${detail})` : ''}\n`,
  )
}
const skipped = STEPS.length - results.length
if (skipped > 0) {
  process.stdout.write(`${skipped} step(s) not run after the failure above\n`)
}
process.exitCode = results.every((result) => result.ok) ? 0 : 1
