import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { readFileSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import { describe, it } from 'node:test'
import { fileURLToPath } from 'node:url'

const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const GENERATOR = join(REPO_ROOT, 'scripts/generate-node20-path-case-data.mjs')
const CHECKED = join(
  REPO_ROOT,
  'crates/core/src/runtime_process/node20_path_case_data.rs',
)

describe('Node 20.19.4 case-data lock', () => {
  it('regenerates the checked lower, upper, cased, and case-ignorable tables byte-for-byte', () => {
    const result = spawnSync(process.execPath, [GENERATOR], {
      cwd: REPO_ROOT,
      encoding: null,
      maxBuffer: 4 * 1024 * 1024,
    })
    assert.equal(result.status, 0, result.stderr?.toString())
    assert.deepEqual(result.stdout, readFileSync(CHECKED))
  })

  it('fails closed when the executing Node/V8/ICU/Unicode/libuv identity drifts', () => {
    const source = readFileSync(GENERATOR, 'utf8')
    assert.match(source, /node: "20\.19\.4"/)
    assert.match(source, /v8: "11\.3\.244\.8-node\.29"/)
    assert.match(source, /uv: "1\.46\.0"/)
    assert.match(source, /unicode: "16\.0"/)
    assert.match(source, /icu: "77\.1"/)
    assert.match(source, /PATH_SOURCE_SHA256 = "[0-9a-f]{64}"/)
    assert.match(source, /process\.binding\("natives"\)\.path/)
  })
})
