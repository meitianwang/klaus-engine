// The gate must run its steps with a clean environment, not the developer's profile.
//
// Defect: the running app injects KLAUS_CONFIG_DIR=<real ~/.klaus> into every shell it
// spawns, and this machine's profile enables `worktree = true`. A gate started from inside
// the app inherited it, so `worktree_shortcut_surfaces_the_enter_worktree_tool_and_
// config_overrides` (SDK) went red depending only on who ran the gate.

import assert from 'node:assert/strict'
import { describe, it } from 'node:test'
import { existsSync, mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'

import { LEAKED_CONFIG_VARS, gateChildEnvironment, createGateChildProfile } from './gate-environment.mjs'

describe('gate child environment', () => {
  it('removes every profile variable the app injects', () => {
    const base = {
      PATH: '/usr/bin',
      KLAUS_CONFIG_DIR: '/Users/dev/.klaus',
      KLAUS_PLUGIN_HOME: '/Users/dev/.klaus/plugins',
      KLAUS_SESSION_DIR: '/Users/dev/.klaus/sessions',
    }
    const env = gateChildEnvironment({}, base)
    for (const key of LEAKED_CONFIG_VARS) assert.equal(key in env, false, `${key} is stripped`)
    assert.equal(env.PATH, '/usr/bin', 'unrelated variables survive')
  })

  it('strips after merging overrides, so a caller cannot reintroduce them', () => {
    // test-native.mjs spreads process.env into its own env; a strip that ran before the
    // merge would let the leaked values back in.
    const base = { KLAUS_CONFIG_DIR: '/Users/dev/.klaus' }
    const env = gateChildEnvironment({ ...base, CARGO_BUILD_JOBS: '4' }, base)
    assert.equal('KLAUS_CONFIG_DIR' in env, false)
    assert.equal(env.CARGO_BUILD_JOBS, '4', 'step-specific variables are kept')
  })

  it('leaves a clean environment untouched', () => {
    const env = gateChildEnvironment({}, { PATH: '/usr/bin', HOME: '/Users/dev' })
    assert.deepEqual(env, { PATH: '/usr/bin', HOME: '/Users/dev' })
  })

  it('owns all three profile paths and removes the complete child profile', () => {
    const inherited = {
      HOME: '/Users/dev',
      KLAUS_CONFIG_DIR: '/Users/dev/.klaus',
      KLAUS_PLUGIN_HOME: '/Users/dev/.klaus/plugins',
      KLAUS_SESSION_DIR: '/Users/dev/.klaus/sessions',
    }
    const first = createGateChildProfile({ ...inherited, CARGO_BUILD_JOBS: '4' }, inherited)
    const second = createGateChildProfile({}, inherited)
    const root = first.env.KLAUS_CONFIG_DIR
    try {
      assert.notEqual(root, inherited.KLAUS_CONFIG_DIR)
      assert.notEqual(root, second.env.KLAUS_CONFIG_DIR)
      assert.equal(first.env.KLAUS_PLUGIN_HOME, join(root, 'plugins'))
      assert.equal(first.env.KLAUS_SESSION_DIR, join(root, 'sessions'))
      assert.equal(first.env.HOME, inherited.HOME)
      assert.equal(first.env.CARGO_BUILD_JOBS, '4')
      mkdirSync(first.env.KLAUS_SESSION_DIR, { recursive: true })
      writeFileSync(join(first.env.KLAUS_SESSION_DIR, 'fixture.jsonl'), '{}\n')
    } finally {
      first.dispose()
      second.dispose()
    }
    assert.equal(existsSync(root), false)
    assert.equal(existsSync(second.env.KLAUS_CONFIG_DIR), false)
  })
})
