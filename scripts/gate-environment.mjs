// Environment for a gate child process: inherited, minus the profile variables the
// running Klaus app injects into every shell it spawns.
//
// `KLAUS_CONFIG_DIR` points at the developer's REAL profile, and this machine's
// enables `worktree = true`. A gate run started from inside the app therefore
// inherited it, and the SDK test
// `worktree_shortcut_surfaces_the_enter_worktree_tool_and_config_overrides` went red
// depending only on who ran the gate. The SDK test isolates its own config home now,
// but a gate must not depend on any one suite remembering that, so cleanliness is
// established here for every step.
//
// The strip happens AFTER the caller's overrides are merged: a caller that
// re-spreads `process.env` into its own env (as the desktop native gate does) must
// not be able to reintroduce the variables.

import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

/** Profile variables that must not leak into a gate child. */
export const LEAKED_CONFIG_VARS = [
  'KLAUS_CONFIG_DIR',
  'KLAUS_PLUGIN_HOME',
  'KLAUS_SESSION_DIR',
]

/** Inherited environment with [`LEAKED_CONFIG_VARS`] removed, after merging `overrides`. */
export function gateChildEnvironment(overrides = {}, base = process.env) {
  const env = { ...base, ...overrides }
  for (const key of LEAKED_CONFIG_VARS) delete env[key]
  return env
}

/** Stripping inherited paths must not send a child back to the default profile. */
export function createGateChildProfile(overrides = {}, base = process.env) {
  const root = mkdtempSync(join(tmpdir(), 'klaus-gate-profile-'))
  return {
    env: {
      ...gateChildEnvironment(overrides, base),
      KLAUS_CONFIG_DIR: root,
      KLAUS_PLUGIN_HOME: join(root, 'plugins'),
      KLAUS_SESSION_DIR: join(root, 'sessions'),
    },
    dispose() {
      rmSync(root, { recursive: true, force: true })
    },
  }
}
