import { createRequire, syncBuiltinESMExports } from 'node:module'

const dispatcherSymbol = Symbol.for('klaus.browser.playwright.process.v1')
const closeHooksSymbol = Symbol.for('klaus.browser.playwright.close-hooks.v1')
const childProcessNames = Object.freeze([
  'spawn',
  'spawnSync',
  'exec',
  'execFile',
  'execSync',
  'execFileSync',
  'fork',
])
const brokerNames = Object.freeze([
  'PlaywrightLaunch',
  'PlaywrightTreeKill',
  'PlaywrightMacOsSwVers',
  'PlaywrightLinuxLdd',
  'PlaywrightLinuxLdconfig',
  'PlaywrightWindowsWinldd',
  'BrowserFfmpegEncode',
])
const require = createRequire(import.meta.url)
const childProcess = require('node:child_process')
let installationState = 'Uninstalled'

function deniedPinnedPackagePrimitive() {
  throw new Error('Denied pinned Playwright package process primitive.')
}

export function installPinnedPlaywrightProcessPatchV1(operations, registerCloseHooks) {
  if (installationState !== 'Uninstalled') {
    throw new Error('Pinned Playwright process patch already installed.')
  }
  installationState = 'Installing'
  if (
    operations === null ||
    typeof operations !== 'object' ||
    Object.getPrototypeOf(operations) !== null ||
    Object.keys(operations).join(',') !== brokerNames.join(',')
  ) {
    throw new Error('Pinned Playwright process broker capability is invalid.')
  }
  if (typeof registerCloseHooks !== 'function') {
    throw new Error('Pinned Playwright close hook capability is invalid.')
  }
  const dispatcher = Object.create(null)
  for (const name of brokerNames) {
    const operation = operations[name]
    if (typeof operation !== 'function') {
      throw new Error('Pinned Playwright process broker capability is invalid.')
    }
    Object.defineProperty(dispatcher, name, {
      value: (...args) => Reflect.apply(operation, undefined, args),
      enumerable: true,
      configurable: false,
      writable: false,
    })
  }
  Object.defineProperty(dispatcher, 'DeniedPinnedPackagePrimitive', {
    value: deniedPinnedPackagePrimitive,
    enumerable: true,
    configurable: false,
    writable: false,
  })
  Object.freeze(dispatcher)
  for (const name of childProcessNames) {
    Object.defineProperty(childProcess, name, {
      value: deniedPinnedPackagePrimitive,
      enumerable: true,
      configurable: false,
      writable: false,
    })
  }
  Object.defineProperty(process, 'kill', {
    value: deniedPinnedPackagePrimitive,
    enumerable: true,
    configurable: false,
    writable: false,
  })
  syncBuiltinESMExports()
  Object.defineProperty(globalThis, dispatcherSymbol, {
    value: dispatcher,
    enumerable: false,
    configurable: false,
    writable: false,
  })
  Object.defineProperty(globalThis, closeHooksSymbol, {
    value: (proxy, graceful, force) => Reflect.apply(
      registerCloseHooks,
      undefined,
      [proxy, graceful, force],
    ),
    enumerable: false,
    configurable: false,
    writable: false,
  })
  installationState = 'Installed'
}
