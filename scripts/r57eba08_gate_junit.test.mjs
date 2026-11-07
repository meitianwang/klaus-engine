// Defect: a shared Cargo build target hid the current workspace's JUnit failure.
// Root cause: the gate derived nextest's report path from CARGO_TARGET_DIR.
// Fixing commit: 57eba08c2069cfd20aca3db1a0a1e93eccf05dee.

import assert from 'node:assert/strict'
import { mkdirSync, mkdtempSync, rmSync, utimesSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, it } from 'node:test'

import { parseNextestFailures, readNextestFailures } from './gate-junit.mjs'

const STARTED = Date.UTC(2026, 8, 20, 17, 0, 0)

function report(body, { tests = 1, failures = 1, errors = 0, started = STARTED } = {}) {
  return `<testsuites tests="${tests}" failures="${failures}" errors="${errors}" timestamp="${new Date(started).toISOString()}"><testsuite>${body}</testsuite></testsuites>`
}

function writeReport(root, xml, modified = STARTED + 1000) {
  const directory = join(root, 'target/nextest/default')
  mkdirSync(directory, { recursive: true })
  const path = join(directory, 'junit.xml')
  writeFileSync(path, xml)
  utimesSync(path, modified / 1000, modified / 1000)
}

describe('nextest failure report gate regression', () => {
  it('reads the current workspace report when a shared Cargo target has a stale green report', () => {
    const fixture = mkdtempSync(join(tmpdir(), 'klaus-gate-junit-'))
    const previousTarget = process.env.CARGO_TARGET_DIR
    try {
      const workspace = join(fixture, 'workspace')
      const shared = join(fixture, 'shared')
      writeReport(workspace, report('<testcase name="module::failed"><failure/></testcase>'))
      writeReport(shared, report('<testcase name="module::old_pass"/>', {
        failures: 0, started: STARTED - 10_000,
      }), STARTED - 9000)
      process.env.CARGO_TARGET_DIR = join(shared, 'target')
      assert.deepEqual(readNextestFailures(workspace, { notBefore: STARTED }), ['module::failed'])
    } finally {
      if (previousTarget === undefined) delete process.env.CARGO_TARGET_DIR
      else process.env.CARGO_TARGET_DIR = previousTarget
      rmSync(fixture, { recursive: true, force: true })
    }
  })

  it('does not pin a later failure onto a successful self-closing testcase', () => {
    assert.deepEqual(parseNextestFailures(report(
      '<testcase name="module::passed"/><testcase name="module::failed"><failure message="timeout">detail</failure></testcase>',
      { tests: 2 },
    )), ['module::failed'])
  })

  it('recognizes paired and self-closing error tags without reading captured output as markup', () => {
    assert.deepEqual(parseNextestFailures(report(
      '<testcase name="module::first"><error/></testcase>' +
      '<testcase name="module::second"><error message="aborted">detail</error></testcase>' +
      '<testcase name="module::passed"><system-out><![CDATA[<error/>]]></system-out></testcase>',
      { tests: 3, failures: 0, errors: 2 },
    )), ['module::first', 'module::second'])
  })

  it('rejects a prior run even if its report file was copied with a fresh mtime', () => {
    const fixture = mkdtempSync(join(tmpdir(), 'klaus-gate-junit-'))
    try {
      writeReport(fixture, report('<testcase name="module::failed"><failure/></testcase>', {
        started: STARTED - 1000,
      }))
      assert.equal(readNextestFailures(fixture, { notBefore: STARTED }), null)
      writeReport(fixture, report('<testcase name="module::failed"><failure/></testcase>'), STARTED - 1)
      assert.equal(readNextestFailures(fixture, { notBefore: STARTED }), null)
    } finally {
      rmSync(fixture, { recursive: true, force: true })
    }
  })

  it('fails closed for missing, malformed, incomplete or inconsistent reports', () => {
    const fixture = mkdtempSync(join(tmpdir(), 'klaus-gate-junit-'))
    try {
      assert.equal(readNextestFailures(fixture, { notBefore: STARTED }), null)
      for (const xml of [
        'not xml',
        report('<testcase name="module::failed"><failure/></testcase>').replace('</testsuites>', ''),
        report('<testcase name="module::passed"/>'),
        report('<testcase name="module::failed"><failure/></testcase>', { tests: 2 }),
        report('<testcase><failure/></testcase>'),
        report('<testcase name="unsafe(name)"><failure/></testcase>'),
      ]) {
        writeReport(fixture, xml)
        assert.equal(readNextestFailures(fixture, { notBefore: STARTED }), null, xml)
      }
    } finally {
      rmSync(fixture, { recursive: true, force: true })
    }
  })

  it('preserves a complete zero-failure report and deduplicates same-named failed cases', () => {
    assert.deepEqual(parseNextestFailures(report('<testcase name="module::passed"/>', { failures: 0 })), [])
    assert.deepEqual(parseNextestFailures(report(
      '<testcase name="module::failed"><failure/></testcase><testcase name="module::failed"><error/></testcase>',
      { tests: 2, failures: 1, errors: 1 },
    )), ['module::failed'])
  })
})
