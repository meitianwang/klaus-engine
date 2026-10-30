import { readFileSync, statSync } from 'node:fs'
import { resolve } from 'node:path'

function attribute(attributes, name) {
  return new RegExp(`\\b${name}="([^"]*)"`, 'u').exec(attributes)?.[1]
}

/** Read the complete nextest report, refusing partial or inconsistent reports. */
export function parseNextestFailures(xml, { notBefore = 0 } = {}) {
  // Captured test output is data, not report markup.
  const report = xml.replace(/<!\[CDATA\[[\s\S]*?\]\]>/gu, '')
  const root = /<testsuites\b([^>]*)>/u.exec(report)
  if (!root || !/<\/testsuites>\s*$/u.test(report)) return null
  const count = (name) => {
    const value = attribute(root[1], name)
    if (!/^\d+$/u.test(value ?? '')) return null
    const number = Number(value)
    return Number.isSafeInteger(number) ? number : null
  }
  const tests = count('tests')
  const failures = count('failures')
  const errors = count('errors')
  const started = Date.parse(attribute(root[1], 'timestamp') ?? '')
  if (
    tests === null || failures === null || errors === null ||
    !Number.isFinite(started) || started < notBefore
  ) return null

  const names = []
  let seen = 0
  // A successful self-closing testcase must not consume the next testcase's
  // body and inherit its failure. Both paired and self-closing error tags count.
  const testcase = /<testcase\b([^>]*?)(?:\/>|>([\s\S]*?)<\/testcase>)/gu
  for (const [, attributes, body] of report.matchAll(testcase)) {
    seen += 1
    if (body == null || !/<(?:failure|error)(?:\s|\/?>)/u.test(body)) continue
    const name = attribute(attributes, 'name')
    // Rust test names contain no XML entities. An unexpected carrier is not a
    // safe filter expression, so leave the original failed step failed.
    if (!name || /[&<>"'()]/u.test(name)) return null
    names.push(name)
  }
  if (seen !== tests || names.length !== failures + errors) return null
  return [...new Set(names)]
}

/**
 * Nextest's report store is relative to the workspace root, even when Cargo
 * compilation artifacts are redirected with CARGO_TARGET_DIR. Never use a
 * different checkout's stale report to decide whether a failed gate recovered.
 */
export function readNextestFailures(repoRoot, { notBefore }) {
  const path = resolve(repoRoot, 'target/nextest/default/junit.xml')
  try {
    const before = statSync(path)
    if (!before.isFile() || before.mtimeMs < notBefore) return null
    const xml = readFileSync(path, 'utf8')
    const after = statSync(path)
    if (
      before.ino !== after.ino || before.size !== after.size ||
      before.mtimeMs !== after.mtimeMs
    ) return null
    return parseNextestFailures(xml, { notBefore })
  } catch {
    return null
  }
}
