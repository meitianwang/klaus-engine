#!/usr/bin/env node

import { createHash } from 'node:crypto'
import { spawnSync } from 'node:child_process'
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { gunzipSync } from 'node:zlib'
import { validateSourceMetadata } from './browser-source-distribution.mjs'

export const EXPECTED_RIPGREP_TARGETS = Object.freeze([
  'darwin-arm64',
  'darwin-x64',
  'linux-arm',
  'linux-arm64',
  'linux-ia32',
  'linux-ppc64',
  'linux-riscv64',
  'linux-s390x',
  'linux-x64',
  'win32-arm64',
  'win32-ia32',
  'win32-x64',
])

export const EXPECTED_RUST_TARGET_CFG = Object.freeze({
  'darwin-arm64': 'all(target_os = "macos", target_arch = "aarch64")',
  'darwin-x64': 'all(target_os = "macos", target_arch = "x86_64")',
  'linux-arm': 'all(target_os = "linux", target_arch = "arm")',
  'linux-arm64': 'all(target_os = "linux", target_arch = "aarch64")',
  'linux-ia32': 'all(target_os = "linux", target_arch = "x86")',
  'linux-ppc64': 'all(target_os = "linux", target_arch = "powerpc64", target_endian = "little")',
  'linux-riscv64': 'all(target_os = "linux", target_arch = "riscv64")',
  'linux-s390x': 'all(target_os = "linux", target_arch = "s390x")',
  'linux-x64': 'all(target_os = "linux", target_arch = "x86_64")',
  'win32-arm64': 'all(target_os = "windows", target_arch = "aarch64")',
  'win32-ia32': 'all(target_os = "windows", target_arch = "x86")',
  'win32-x64': 'all(target_os = "windows", target_arch = "x86_64")',
})

export const EXPECTED_EXECUTABLE_IDENTITIES = Object.freeze({
  'darwin-arm64': Object.freeze({ format: 'Mach-O', machine: 0x0100000c, class: 64, endian: 'little' }),
  'darwin-x64': Object.freeze({ format: 'Mach-O', machine: 0x01000007, class: 64, endian: 'little' }),
  'linux-arm': Object.freeze({ format: 'ELF', machine: 40, class: 32, endian: 'little' }),
  'linux-arm64': Object.freeze({ format: 'ELF', machine: 183, class: 64, endian: 'little' }),
  'linux-ia32': Object.freeze({ format: 'ELF', machine: 3, class: 32, endian: 'little' }),
  'linux-ppc64': Object.freeze({ format: 'ELF', machine: 21, class: 64, endian: 'little' }),
  'linux-riscv64': Object.freeze({ format: 'ELF', machine: 243, class: 64, endian: 'little' }),
  'linux-s390x': Object.freeze({ format: 'ELF', machine: 22, class: 64, endian: 'big' }),
  'linux-x64': Object.freeze({ format: 'ELF', machine: 62, class: 64, endian: 'little' }),
  'win32-arm64': Object.freeze({ format: 'PE', machine: 0xaa64, class: 64, endian: 'little' }),
  'win32-ia32': Object.freeze({ format: 'PE', machine: 0x014c, class: 32, endian: 'little' }),
  'win32-x64': Object.freeze({ format: 'PE', machine: 0x8664, class: 64, endian: 'little' }),
})

// The engine is unversioned: every crate in it sits at a fixed 0.0.0 and no
// release ever moves that (the reasoning is in the workspace manifest). The only
// reason these internal requirements carry a version at all is that
// `cargo package` rejects a path dependency without one, so the value is pinned
// here rather than read from the manifest: pinning it means giving the engine a
// real version number fails this check instead of silently starting a third
// release cadence. The two products that do have versions — the desktop bundle
// and the CLI crate — are outside the distribution set and unaffected.
//
// Everything below derives from this one string, including the `<name>-<version>`
// directory `cargo package` puts at the root of each archive. Spelling the version
// again at those sites is how lowering the engine to 0.0.0 left four places still
// expecting 0.1.0, and the archive-root one failed as "unsafe archive path".
export const ENGINE_VERSION = '0.0.0'
export const ENGINE_VERSION_REQ = `=${ENGINE_VERSION}`

export const DISTRIBUTION_PACKAGES = Object.freeze([
  'klaus-agent-icu-data',
  ...EXPECTED_RIPGREP_TARGETS.map((target) => `klaus-agent-ripgrep-${target}`),
  'klaus-agent-core',
  'klaus-agent-protocol',
  'klaus-agent-sdk',
  'klaus-agent-server',
  // The Windows execution sandbox. Present because `klaus-agent-core` depends on it
  // by path, and cargo refuses to publish a crate whose path dependency is not
  // published — not because anyone is expected to depend on it directly.
  'klaus-sandbox-win',
])

export const DISTRIBUTION_PACKAGE_PATHS = Object.freeze(Object.fromEntries([
  ['klaus-agent-icu-data', 'crates/icu-data'],
  ...EXPECTED_RIPGREP_TARGETS.map((target) => [
    `klaus-agent-ripgrep-${target}`,
    `crates/ripgrep-${target}`,
  ]),
  ['klaus-agent-core', 'crates/core'],
  ['klaus-agent-protocol', 'crates/protocol'],
  ['klaus-agent-sdk', 'crates/sdk'],
  ['klaus-agent-server', 'crates/server'],
  ['klaus-sandbox-win', 'crates/sandbox-win'],
]))

const SHA256 = /^[0-9a-f]{64}$/
const CRATES_IO_SOURCE = 'registry+https://github.com/rust-lang/crates.io-index'
const ICU_SOURCE_SHA256 = '0913674ff673c585f8bc08370916b6a6ccc30ffb6408a5c1bc3edbf5a687fd96'
const FIXED_ICU_ASSET_IDENTITIES = Object.freeze({
  raw: Object.freeze([31895376, '310d9b2cb42947fad7f388b09d2ae574e2adeb60ca76abf280438908f20b2d7b']),
  xz: Object.freeze([7633376, 'fa1f6481428576503e4e0a132f491a0f7e63a17d9a28dcbd81369318e3d5b76c']),
})
const FIXED_RIPGREP_ASSET_IDENTITIES = Object.freeze(Object.fromEntries([
  ['darwin-arm64', {
    npmIntegrity: 'sha512-r3ktHSvbFycQNF6sl7sNDPocpsI7J+mEzh1IaZFkY0spm3k2Z9t8hPAeOK7+p0l6p6/swkQC14XWX01low+94Q==',
    raw: [4528512, '6ef40346bf31fcce79d9614c7745c198542925a0c7d4911e1ffe794c53392ac1'],
    xz: [1285164, 'ed6dfbf1e723614cb588496a398caa31afc438d6969d79b1fba3db28609fa6ad'],
  }],
  ['darwin-x64', {
    npmIntegrity: 'sha512-25b4gWbL138dGuQU244ebCKKc0q05ULBMoFSz9oAEUHNeqK/lOJViDS7DRvbDazzAzSEdan391Znks/R5mkaTQ==',
    raw: [4742676, 'f999495980a5e6f1e7d26461ef5768b4013a62df610ed7d77a8b2de247a5b228'],
    xz: [1454004, '2d2947e2eac9af3b261e50bb94dfaecec973aa990b3014ea2f6747d1ac6e2fed'],
  }],
  ['linux-arm', {
    npmIntegrity: 'sha512-GDAvufNDHu8zqLEmXstalQF0Wh6wQvdsBi/Vg3Yi3CK4a8XoFXqqXVEHEZ9xQz3t0NfoSEc9JbvK9DDS6FxyxQ==',
    raw: [4115312, '0d8353810445d0606cc3c714601fd6ed0465b03746107ce2bb173499ffc34a67'],
    xz: [1138992, 'd9d52628013a19afc833f59732fc4a33238a3984d46004bb9bbca6c0b42b3967'],
  }],
  ['linux-arm64', {
    npmIntegrity: 'sha512-lQ/5zTG++U0E3IhVgS4EPTTn/U4okncaRMM5GOFfOYZywS4nuD31GhkHbNYlDk5CuDC68+hYJ0/eQeyCKJDA+g==',
    raw: [4771240, 'e152ea689d6e8420357e592f0d8253b96476c164118ca3e6e13074fa1705ddda'],
    xz: [1416008, '91e3bc2ed6e225e4345ca89668896b5a0675bd6c64ed9560fdd6f4f96056a598'],
  }],
  ['linux-ia32', {
    npmIntegrity: 'sha512-YWLkSUtFd4Jh5EepIhA9RJSfv3uMAVMo+2rBIGHPBnvgLrZciIs2cDKei1/p6Wc/aCzUoHyMAg2R6tw4ZCBKGg==',
    raw: [4512608, '47763deda9f135d94176258090162bc2b2faece1bb8b806c5d02be4e9ac43f18'],
    xz: [1457884, '37c4c54b236e6d1e46adef3a32c8eef056a9af191a32cd0a78228f308ce52631'],
  }],
  ['linux-ppc64', {
    npmIntegrity: 'sha512-quXVY8fwQ8O/lvU1yrSqSl3jlUzysRSb+AfUfCL/tRtphxsKlFvPAejryZ6vg4Bgvn8XL74xb4qMCDmWgYrT5w==',
    raw: [5967656, '921b000ad16eda974d9dffc1780771c917b540429b7afc4f72d1742d742abbf2'],
    xz: [1402556, '0bb277ed61df04b5703e2da1d208c6969f6edba5b3261526c729250453a54614'],
  }],
  ['linux-riscv64', {
    npmIntegrity: 'sha512-f5kBQBrWfQt8Q7OhSORuNDei5dkYagBj3y4jImSUXGMy8B/Ke7SltSRcUtjPv166FAFfHCAmWuZp3+cWnX2/Vw==',
    raw: [3684352, 'c0a0ed0c04224325ea9b9929ef8a7daf5fa2a3a6713d6f466ac0df820c079808'],
    xz: [1252272, '22b32f8e859a2257bc4fa4edb442a1b682a6014bf74a32e1105ebf96360ac087'],
  }],
  ['linux-s390x', {
    npmIntegrity: 'sha512-rTOcJFGGcl2c07RUOWUo4U1ndnemKhY6A9hnMB18uk7jSgJc0d/QLBGWMWpumdtoJtpizn/wIv5mXIisJukusQ==',
    raw: [4836808, '3932fc118f402eb72cc180d6238ed4d805a375c513613e3ce61320433cc6d760'],
    xz: [1156048, '440a0d9462db39df01feae09b394aae6023955e37f26d56dbd830815c0d92eb4'],
  }],
  ['linux-x64', {
    npmIntegrity: 'sha512-mQ3bVrUpnD2vs7QT0vX90Lt0cnUq467uFtEktIdsJJmW296RoSULRGqWgzG1AKxyBpNDD6l4ZO4qKf6SgyC23Q==',
    raw: [5728032, '193906679498de4d939345b937fa24e0e69a03c244bd70c859f5e41232713f21'],
    xz: [1742860, 'aa32a30e41ed2d72178f129b473748cb055d55d5dd700269e4d80dfe5aca3393'],
  }],
  ['win32-arm64', {
    npmIntegrity: 'sha512-vfTIjq1OHnzUjxZcHVQAMbnggp8dpGf+0QKFOZHwWPqFwXxQC8eCWM+5NUdoJ6yrElCeMzoUTXoK/LdZaniB+Q==',
    raw: [4883968, '0015ba7dfb2d8b62c39332d64e50c6e85f75230125e10c50a950ed14995c79bf'],
    xz: [1292600, 'cfad138e7e5c46f4ed44edb132b962a533051482f99251c049046890da92c173'],
  }],
  ['win32-ia32', {
    npmIntegrity: 'sha512-//rfAE+BOw5AC2EMmepmiE36jUuevtQYNQqqlw1s3m9FlRxjxEut97RkRPHAu9BG4mSojatZx+kXZXNdyI9caQ==',
    raw: [4761600, '72ca0c53d04d8c13cd245af0aedcd1e12dd20aa24439cb0d1fc5951547f3cceb'],
    xz: [1435860, '67eb31c1f231b6d64d73dfb1692c1eeab44e0d186508f1a5c93f46fa0bf668b9'],
  }],
  ['win32-x64', {
    npmIntegrity: 'sha512-KNPvtElldqILHdnAetujPaowkNbpqJy3ssIGGN6F6Kve9Qi+nNLI2DN01O83JjCEVQbCzl8Ov3QZ9Eov3BR8Dg==',
    raw: [5429760, 'f9dde63498b3193f098355dbec97af99dc4f6b8fa0df5ed04114a03012c042cb'],
    xz: [1491984, '5a04aad8d31242ca81d3792ddc1d25203fd88cca49a9a89e8e66a251061ad37e'],
  }],
].map(([target, identity]) => [target, Object.freeze(identity)])))
const FIXED_LICENSE_IDENTITIES = Object.freeze({
  microsoft: Object.freeze({
    file: 'LICENSE-MICROSOFT',
    length: 1113,
    sha256: 'ccf06b70760761ae1437395117bcf911501e3dc85385d84c7eba20d5dfab55dd',
  }),
  ripgrepMit: Object.freeze({
    file: 'LICENSE-RIPGREP-MIT',
    length: 1081,
    sha256: '0f96a83840e146e43c0ec96a22ec1f392e0680e6c1226e6f3ba87e0740af850f',
  }),
  icuNotice: Object.freeze({
    file: 'ICU-LICENSE.txt',
    length: 26487,
    sha256: '451167c55c0fa447cc2d5632714f5e3c567fe4f1e1badefab2c1333852198aca',
  }),
  icuWrapperMit: Object.freeze({
    file: 'LICENSE-MIT',
    length: 1081,
    sha256: '0948bee27e4516b4ac815367adde524de3fda562dd33fbe6250db2085ea4563b',
  }),
})

function invariant(condition, message) {
  if (!condition) throw new Error(message)
}

function positiveInteger(value, label) {
  invariant(Number.isSafeInteger(value) && value > 0, `${label} must be a positive integer`)
}

function exactKeys(value, expected, label) {
  invariant(value && typeof value === 'object' && !Array.isArray(value), `${label} must be an object`)
  const actual = Object.keys(value).sort()
  const wanted = [...expected].sort()
  invariant(JSON.stringify(actual) === JSON.stringify(wanted), `${label} fields must be exact`)
}

function identityShape(value, label, expectedFile) {
  exactKeys(value, ['file', 'length', 'sha256'], label)
  invariant(value && typeof value === 'object', `${label} must be an object`)
  invariant(value.file === expectedFile, `${label}.file must be the canonical path ${expectedFile}`)
  positiveInteger(value.length, `${label}.length`)
  invariant(SHA256.test(value.sha256), `${label}.sha256 must be lowercase SHA-256`)
}

function fixedIdentityShape(value, label, expected) {
  identityShape(value, label, expected.file)
  invariant(value.length === expected.length, `${label}.length must be ${expected.length}`)
  invariant(value.sha256 === expected.sha256, `${label}.sha256 must bind the fixed license bytes`)
}

function fixedAssetIdentityShape(value, label, expected) {
  invariant(
    value.length === expected[0] && value.sha256 === expected[1],
    `${label} must bind the fixed reviewed byte identity`,
  )
}

function canonicalSha512Sri(value) {
  if (typeof value !== 'string') return false
  const match = /^sha512-([A-Za-z0-9+/]+={0,2})$/.exec(value)
  if (!match) return false
  const digest = Buffer.from(match[1], 'base64')
  return digest.length === 64 && digest.toString('base64') === match[1]
}

export function validateLockShape(lock) {
  exactKeys(lock, ['schemaVersion', 'icu', 'ripgrep', 'licenses', 'browser', 'assets'], 'provider asset lock')
  invariant(lock?.schemaVersion === 1, 'provider asset lock schemaVersion must be 1')
  exactKeys(lock.icu, ['release', 'sourceAsset', 'sourceSha256', 'raw', 'xz'], 'icu')
  invariant(lock.icu.release === '77.1', 'provider asset lock must bind ICU 77.1')
  invariant(
    lock.icu.sourceAsset === 'icu4c-77_1-data-bin-l.zip',
    'provider asset lock must bind the ICU 77.1 source asset',
  )
  invariant(
    lock.icu.sourceSha256 === ICU_SOURCE_SHA256,
    'provider asset lock must bind the ICU 77.1 source SHA-256',
  )
  identityShape(lock.icu.raw, 'icu.raw', 'icudt77l.dat')
  identityShape(lock.icu.xz, 'icu.xz', 'crates/icu-data/data/icudt77l.dat.xz')
  fixedAssetIdentityShape(lock.icu.raw, 'icu.raw', FIXED_ICU_ASSET_IDENTITIES.raw)
  fixedAssetIdentityShape(lock.icu.xz, 'icu.xz', FIXED_ICU_ASSET_IDENTITIES.xz)

  exactKeys(
    lock.ripgrep,
    ['wrapper', 'wrapperVersion', 'binaryVersion', 'binaryRevision', 'compression'],
    'ripgrep',
  )
  invariant(lock.ripgrep.wrapper === '@vscode/ripgrep', 'ripgrep.wrapper must bind @vscode/ripgrep')
  invariant(lock.ripgrep.wrapperVersion === '1.18.0', 'ripgrep.wrapperVersion must bind 1.18.0')
  invariant(lock.ripgrep.binaryVersion === '15.0.0', 'ripgrep.binaryVersion must bind 15.0.0')
  invariant(lock.ripgrep.binaryRevision === '3a612f88b8', 'ripgrep.binaryRevision must bind 3a612f88b8')
  exactKeys(lock.ripgrep.compression, ['format', 'toolVersion', 'arguments'], 'ripgrep.compression')
  invariant(lock.ripgrep.compression.format === 'xz', 'ripgrep compression format must be xz')
  invariant(lock.ripgrep.compression.toolVersion === '5.8.2', 'ripgrep compression toolVersion must bind 5.8.2')
  invariant(
    JSON.stringify(lock.ripgrep.compression.arguments) === JSON.stringify(['-9e']),
    'ripgrep compression arguments must be exactly -9e',
  )

  exactKeys(lock.licenses, Object.keys(FIXED_LICENSE_IDENTITIES), 'licenses')
  for (const [name, identity] of Object.entries(FIXED_LICENSE_IDENTITIES)) {
    fixedIdentityShape(lock.licenses[name], `licenses.${name}`, identity)
  }

  exactKeys(lock.browser, ['bundledLock', 'runtimeLock', 'nativeHostDistribution'], 'browser')
  exactKeys(
    lock.browser.bundledLock,
    ['file', 'length', 'sha256', 'files', 'pathSetSha256', 'treeSha256'],
    'browser.bundledLock',
  )
  invariant(lock.browser.bundledLock.file === 'bundled-browser-assets.lock.json', 'browser bundled lock file drifted')
  positiveInteger(lock.browser.bundledLock.length, 'browser.bundledLock.length')
  invariant(SHA256.test(lock.browser.bundledLock.sha256), 'browser bundled lock SHA-256 is invalid')
  invariant(lock.browser.bundledLock.files === 24, 'browser bundled lock must bind 24 files')
  invariant(SHA256.test(lock.browser.bundledLock.pathSetSha256), 'browser path-set digest is invalid')
  invariant(SHA256.test(lock.browser.bundledLock.treeSha256), 'browser tree digest is invalid')
  exactKeys(
    lock.browser.runtimeLock,
    [
      'file', 'length', 'sha256', 'candidateSha256', 'publishedSha256',
      'sourceGraphSha256', 'processPatchSha256', 'patchedGraphSha256',
      'addonSourceClosureSha256',
    ],
    'browser.runtimeLock',
  )
  invariant(
    lock.browser.runtimeLock.file === 'crates/sdk/assets/exact-browser-runtime-assets-v1/exact-browser-runtime-assets.lock.json',
    'browser runtime lock file drifted',
  )
  positiveInteger(lock.browser.runtimeLock.length, 'browser.runtimeLock.length')
  invariant(SHA256.test(lock.browser.runtimeLock.sha256), 'browser runtime lock SHA-256 is invalid')
  for (const field of [
    'candidateSha256', 'publishedSha256', 'sourceGraphSha256',
    'processPatchSha256', 'patchedGraphSha256', 'addonSourceClosureSha256',
  ]) invariant(SHA256.test(lock.browser.runtimeLock[field]), `browser.runtimeLock.${field} is invalid`)

  const nativeHost = lock.browser.nativeHostDistribution
  exactKeys(
    nativeHost,
    [
      'manifestFile', 'version', 'producer', 'manifestTrustAnchorBuildEnv',
      'windowsSignerTrustAnchorBuildEnv', 'requiredEntryFields', 'targets',
      'availability', 'artifact',
    ],
    'browser.nativeHostDistribution',
  )
  invariant(
    nativeHost.manifestFile === 'klaus-browser-native-host.distribution.json' &&
      nativeHost.version === 1 &&
      nativeHost.producer === null,
    'native-host distribution manifest identity drifted',
  )
  invariant(
    nativeHost.manifestTrustAnchorBuildEnv === 'KLAUS_BROWSER_NATIVE_HOST_MANIFEST_SHA256' &&
      nativeHost.windowsSignerTrustAnchorBuildEnv === 'KLAUS_BROWSER_NATIVE_HOST_SIGNER_SHA256',
    'native-host distribution trust-anchor contract drifted',
  )
  invariant(
    JSON.stringify(nativeHost.requiredEntryFields) === JSON.stringify([
      'target', 'absolutePath', 'size', 'sha256', 'mode',
      'windowsSignerCertificateSha256',
    ]),
    'native-host distribution entry fields drifted',
  )
  invariant(
    nativeHost.availability === 'required-distribution-input' && nativeHost.artifact === null,
    'native-host distribution must remain fail-closed without a verified producer and artifact',
  )
  invariant(
    JSON.stringify(Object.keys(nativeHost.targets)) === JSON.stringify(EXPECTED_RIPGREP_TARGETS),
    'native-host distribution target set drifted',
  )
  for (const [target, entry] of Object.entries(nativeHost.targets)) {
    exactKeys(
      entry,
      ['binary', 'absolutePath', 'size', 'sha256', 'mode', 'windowsSignerCertificateSha256'],
      `browser.nativeHostDistribution.targets.${target}`,
    )
    const windows = target.startsWith('win32-')
    invariant(
      entry.binary === (windows ? 'klaus-browser-native-host.exe' : 'klaus-browser-native-host'),
      `${target} native-host binary name drifted`,
    )
    invariant(entry.mode === (windows ? '100644' : '100755'), `${target} native-host mode drifted`)
    invariant(
      entry.absolutePath === null && entry.size === null && entry.sha256 === null &&
        entry.windowsSignerCertificateSha256 === null,
      `${target} native-host external identity must remain explicitly unavailable`,
    )
  }

  const targets = Object.keys(lock?.assets ?? {}).sort()
  invariant(
    JSON.stringify(targets) === JSON.stringify([...EXPECTED_RIPGREP_TARGETS].sort()),
    'provider asset lock must contain exactly the fixed twelve targets',
  )
  for (const target of EXPECTED_RIPGREP_TARGETS) {
    const asset = lock.assets[target]
    const fixedIdentity = FIXED_RIPGREP_ASSET_IDENTITIES[target]
    invariant(fixedIdentity, `${target} must have a fixed reviewed asset identity`)
    exactKeys(asset, ['crate', 'npm', 'raw', 'xz'], target)
    invariant(
      asset.crate === `klaus-agent-ripgrep-${target}`,
      `${target}.crate must match its fixed package name`,
    )
    exactKeys(asset.npm, ['package', 'version', 'integrity', 'tarball'], `${target}.npm`)
    invariant(
      asset?.npm?.package === `@vscode/ripgrep-${target}` && asset.npm.version === '1.18.0',
      `${target}.npm must bind @vscode/ripgrep-${target}@1.18.0`,
    )
    invariant(canonicalSha512Sri(asset.npm.integrity), `${target}.npm.integrity must be a canonical SHA-512 SRI`)
    invariant(
      asset.npm.integrity === fixedIdentity.npmIntegrity,
      `${target}.npm.integrity must bind the fixed reviewed SRI`,
    )
    invariant(
      asset.npm.tarball === `https://registry.npmjs.org/@vscode/ripgrep-${target}/-/ripgrep-${target}-1.18.0.tgz`,
      `${target}.npm.tarball must be the canonical npm registry tarball`,
    )
    identityShape(asset.raw, `${target}.raw`, target.startsWith('win32-') ? 'rg.exe' : 'rg')
    fixedAssetIdentityShape(asset.raw, `${target}.raw`, fixedIdentity.raw)
    identityShape(
      asset.xz,
      `${target}.xz`,
      `crates/ripgrep-${target}/data/rg.xz`,
    )
    fixedAssetIdentityShape(asset.xz, `${target}.xz`, fixedIdentity.xz)
  }
  return lock
}

function verifyBrowserAssetLocks(repoRoot, browser) {
  const bundledBytes = readFileSync(path.join(repoRoot, browser.bundledLock.file))
  verifyIdentity(bundledBytes, browser.bundledLock, 'bundled browser lock')
  const bundled = JSON.parse(bundledBytes)
  invariant(bundled.files.length === browser.bundledLock.files, 'bundled browser file count drifted')
  invariant(bundled.pathSetSha256 === browser.bundledLock.pathSetSha256, 'bundled browser path set drifted')
  invariant(bundled.treeSha256 === browser.bundledLock.treeSha256, 'bundled browser tree drifted')

  const runtimeBytes = readFileSync(path.join(repoRoot, browser.runtimeLock.file))
  verifyIdentity(runtimeBytes, browser.runtimeLock, 'published browser runtime lock')
  const runtime = JSON.parse(runtimeBytes)
  invariant(runtime.published === true, 'browser runtime lock is not published')
  for (const [field, actual] of [
    ['candidateSha256', runtime.candidateSha256],
    ['publishedSha256', runtime.publishedSha256],
    ['sourceGraphSha256', runtime.sourceGraph.sha256],
    ['processPatchSha256', runtime.processPatch.sha256],
    ['patchedGraphSha256', runtime.patchedGraph.sha256],
    ['addonSourceClosureSha256', runtime.addonSourceClosure.sourceClosureSha256],
  ]) invariant(actual === browser.runtimeLock[field], `browser runtime ${field} drifted`)
  invariant(runtime.packages.length === 2, 'browser runtime must bind both Playwright packages')
  invariant(runtime.externalInputs.browserArchives.length === 11, 'browser runtime archive set drifted')
  invariant(runtime.externalInputs.windowsAddons.length === 3, 'browser runtime addon set drifted')
  invariant(runtime.externalInputs.protectedIdentity.trustAnchorSha256 === null, 'unverified trust anchor must remain fail-closed')
  // The whole digest chain: both lock files, the source closure, the provider
  // lock record and the Rust admission pins.
  validateSourceMetadata(repoRoot)
  return { bundled, runtime }
}

export function parseArguments(argv) {
  const result = { maxBytes: 10_000_000, package: true }
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index]
    if (argument === '--skip-package') {
      result.package = false
      continue
    }
    if (argument === '--max-bytes') {
      const raw = argv[index + 1]
      const value = Number(raw)
      invariant(Number.isSafeInteger(value) && value > 0, '--max-bytes must be a positive integer')
      result.maxBytes = value
      index += 1
      continue
    }
    throw new Error(`unknown argument: ${argument}`)
  }
  return result
}

function sameStringSet(actual, expected) {
  return JSON.stringify([...actual].sort()) === JSON.stringify([...expected].sort())
}

function isPublishablePackage(pkg) {
  return pkg.publish === null || (Array.isArray(pkg.publish) && pkg.publish.length > 0)
}

function canonicalFsPath(value) {
  return existsSync(value) ? realpathSync(value) : path.resolve(value)
}

function sourceWorkspacePackages(metadata) {
  const workspaceMembers = new Set(metadata.workspace_members)
  return metadata.packages.filter((pkg) => workspaceMembers.has(pkg.id))
}

function assertSourceDependencyClosure(packages) {
  const packageByRoot = new Map()
  for (const pkg of packages) {
    invariant(typeof pkg.manifest_path === 'string', `${pkg.name} workspace package is missing manifest_path`)
    const packageRoot = canonicalFsPath(path.dirname(pkg.manifest_path))
    invariant(!packageByRoot.has(packageRoot), `workspace package roots must be unique: ${packageRoot}`)
    packageByRoot.set(packageRoot, pkg)
  }

  for (const owner of packages) {
    for (const dependency of owner.dependencies ?? []) {
      if (dependency.path !== null && dependency.path !== undefined) {
        const target = packageByRoot.get(canonicalFsPath(dependency.path))
        // A version-less path dev-dependency is stripped by `cargo package`
        // and never enters the published graph; the test-support feature
        // self-references use exactly this shape.
        const strippedDevOnly = dependency.kind === 'dev' && dependency.req === '*'
        invariant(
          target
            && target.name === dependency.name
            && DISTRIBUTION_PACKAGES.includes(target.name)
            && isPublishablePackage(target)
            && (dependency.req === ENGINE_VERSION_REQ || strippedDevOnly)
            && dependency.source === null,
          `${owner.name} path dependency ${dependency.name} must be an exact publishable distribution workspace member`,
        )
        continue
      }
      invariant(
        !DISTRIBUTION_PACKAGES.includes(dependency.name),
        `${owner.name} path dependency ${dependency.name} must resolve through the reviewed workspace member`,
      )
      invariant(
        dependency.source === CRATES_IO_SOURCE,
        `${owner.name} registry dependency ${dependency.name} must use the canonical crates.io source without a path`,
      )
    }
  }
}

export function assertPublishableWorkspaceSet(metadata) {
  invariant(metadata && Array.isArray(metadata.packages), 'cargo metadata packages must be an array')
  invariant(Array.isArray(metadata.workspace_members), 'cargo metadata workspace_members must be an array')
  const packages = sourceWorkspacePackages(metadata)
  const publishable = packages
    .filter(isPublishablePackage)
    .map((pkg) => pkg.name)
  invariant(
    publishable.length === new Set(publishable).size && sameStringSet(publishable, DISTRIBUTION_PACKAGES),
    `publishable workspace packages must be exactly: ${DISTRIBUTION_PACKAGES.join(', ')}`,
  )
  assertSourceDependencyClosure(packages)
}

export function assertNormalizedDistributionMetadata(metadata, workspaceRoot) {
  invariant(metadata && Array.isArray(metadata.packages), 'normalized cargo metadata packages must be an array')
  invariant(Array.isArray(metadata.workspace_members), 'normalized cargo metadata workspace_members must be an array')
  const workspaceMembers = new Set(metadata.workspace_members)
  const packages = metadata.packages.filter((pkg) => workspaceMembers.has(pkg.id))
  invariant(
    packages.length === new Set(packages.map((pkg) => pkg.name)).size
      && sameStringSet(packages.map((pkg) => pkg.name), DISTRIBUTION_PACKAGES),
    'normalized workspace packages must be exactly the distribution package set',
  )

  const resolvedRoot = existsSync(workspaceRoot) ? realpathSync(workspaceRoot) : path.resolve(workspaceRoot)
  for (const packageName of DISTRIBUTION_PACKAGES) {
    const pkg = packages.find(({ name }) => name === packageName)
    invariant(pkg, `normalized metadata is missing ${packageName}`)
    invariant(
      pkg.version === ENGINE_VERSION,
      `${packageName} normalized version must be ${ENGINE_VERSION}`,
    )
    // One crate keeps a non-MIT expression: `klaus-agent-icu-data` embeds Unicode
    // data whose terms travel with it. Everything else in the distribution set is
    // this project's own code under the workspace license.
    const expectedLicense = {
      'klaus-agent-icu-data': 'MIT AND Unicode-3.0',
    }[packageName] ?? 'MIT'
    invariant(pkg.license === expectedLicense, `${packageName} normalized license must be ${expectedLicense}`)
    invariant(pkg.rust_version === '1.93', `${packageName} normalized rust-version must be 1.93`)
    const expectedManifest = path.join(resolvedRoot, DISTRIBUTION_PACKAGE_PATHS[packageName], 'Cargo.toml')
    invariant(
      path.resolve(pkg.manifest_path) === expectedManifest,
      `${packageName} metadata must come from its normalized packaged Cargo.toml`,
    )
    for (const dependency of pkg.dependencies ?? []) {
      invariant(
        dependency.path === null || dependency.path === undefined,
        `${packageName} normalized dependency ${dependency.name} must not retain a path`,
      )
      invariant(
        dependency.source === CRATES_IO_SOURCE,
        `${packageName} normalized dependency ${dependency.name} must use the canonical crates.io registry source`,
      )
      if (DISTRIBUTION_PACKAGES.includes(dependency.name)) {
        invariant(
          dependency.req === ENGINE_VERSION_REQ,
          `${packageName} local dependency ${dependency.name} must require exactly ${ENGINE_VERSION_REQ}`,
        )
      }
    }
  }
}

export function buildVerificationWorkspaceManifest() {
  const members = DISTRIBUTION_PACKAGES
    .map((packageName) => `  "${DISTRIBUTION_PACKAGE_PATHS[packageName]}",`)
    .join('\n')
  const patches = DISTRIBUTION_PACKAGES
    .map((packageName) => `${packageName} = { path = "${DISTRIBUTION_PACKAGE_PATHS[packageName]}" }`)
    .join('\n')
  return `[workspace]\nresolver = "2"\nmembers = [\n${members}\n]\n\n[patch.crates-io]\n${patches}\n`
}

export function buildCargoCheckArgs() {
  // The extracted crates ship production targets only; test targets read
  // repository-level seal fixtures (std source lock, sibling crate sources)
  // that `cargo package` correctly leaves out of the archives.
  //
  // `--offline` rather than `--locked`: the distribution set is a subset of the
  // workspace, so this workspace's dependency closure is legitimately smaller than
  // the repository's, and `--locked` refuses to prune the entries it does not need.
  // The repository lock is still seeded into the workspace first, so every version
  // that survives pruning is the one the repository pinned; `--offline` cannot reach
  // the network to find another, and [`assertNoResolvedVersionDrift`] fails the gate
  // if anything moved anyway (it caught a cached `anyhow` patch release doing exactly
  // that when this ran without the seeded lock).
  return ['check', '--workspace', '--lib', '--bins', '--all-features', '--offline']
}

/// Every version the extracted workspace resolved must be one the repository resolved.
///
/// This is the drift check `--locked` used to stand in for. Extra packages in the
/// repository lock are fine (the CLI's dependencies, for one, are reachable from no
/// distribution crate); a *different version* of a shared package is not, because it
/// would mean the crate that ships is not the crate that was reviewed.
export function assertNoResolvedVersionDrift(repoLockText, workspaceLockText) {
  const repoVersions = lockPackageVersions(repoLockText)
  for (const [name, versions] of lockPackageVersions(workspaceLockText)) {
    const reviewed = repoVersions.get(name)
    invariant(reviewed, `extracted workspace resolved ${name}, which the repository lock does not contain`)
    for (const version of versions) {
      invariant(
        reviewed.has(version),
        `extracted workspace resolved ${name} ${version}, but the repository resolved ${[...reviewed].join(', ')}`,
      )
    }
  }
}

/// `name -> set of versions` from a Cargo.lock, by scanning its `[[package]]` blocks.
function lockPackageVersions(lockText) {
  const versions = new Map()
  let name = null
  for (const rawLine of lockText.split('\n')) {
    const line = rawLine.trim()
    if (line === '[[package]]') {
      name = null
      continue
    }
    const nameMatch = /^name = "(.+)"$/.exec(line)
    if (nameMatch) {
      name = nameMatch[1]
      continue
    }
    const versionMatch = /^version = "(.+)"$/.exec(line)
    if (versionMatch && name !== null) {
      if (!versions.has(name)) versions.set(name, new Set())
      versions.get(name).add(versionMatch[1])
      name = null
    }
  }
  return versions
}

function sha256(data) {
  return createHash('sha256').update(data).digest('hex')
}

function verifyIdentity(data, identity, label) {
  invariant(data.length === identity.length, `${label} length ${data.length} != ${identity.length}`)
  const digest = sha256(data)
  invariant(digest === identity.sha256, `${label} SHA-256 ${digest} != ${identity.sha256}`)
}

function uniqueNamedFile(files, fileName, label) {
  const matches = [...files.entries()].filter(([name]) => name === fileName || name.endsWith(`/${fileName}`))
  invariant(matches.length === 1, `${label} must contain exactly one ${fileName}`)
  return matches[0][1]
}

export function verifyPackageLicenseFiles(packageName, files, licenses) {
  invariant(files instanceof Map, `${packageName} license files must be a Map`)
  const required = packageName === 'klaus-agent-icu-data'
    ? [licenses.icuNotice, licenses.icuWrapperMit]
    : packageName.startsWith('klaus-agent-ripgrep-')
      ? [licenses.microsoft, licenses.ripgrepMit]
      : []
  for (const identity of required) {
    verifyIdentity(
      uniqueNamedFile(files, identity.file, `${packageName} license set`),
      identity,
      `${packageName} license ${identity.file}`,
    )
  }
}

function decompressXz(file, maxBytes) {
  const result = spawnSync('xz', ['--decompress', '--stdout', file], {
    encoding: null,
    maxBuffer: maxBytes + 1,
  })
  if (result.error) throw new Error(`xz failed for ${file}: ${result.error.message}`)
  invariant(result.status === 0, `xz failed for ${file}: ${String(result.stderr)}`)
  invariant(result.stdout.length <= maxBytes, `${file} exceeds bounded decompression limit ${maxBytes}`)
  return result.stdout
}

function assertCrateConstants(repoRoot, target, asset) {
  const source = readFileSync(path.join(repoRoot, `crates/ripgrep-${target}/src/lib.rs`), 'utf8')
  for (const [name, value] of [
    ['TARGET_ID', target],
    ['FILE_NAME', asset.raw.file],
    ['NPM_PACKAGE', asset.npm.package],
    ['NPM_VERSION', asset.npm.version],
    ['NPM_INTEGRITY', asset.npm.integrity],
    ['RG_XZ_SHA256', asset.xz.sha256],
    ['RG_RAW_SHA256', asset.raw.sha256],
  ]) {
    invariant(
      source.includes(`pub const ${name}: &str = "${value}";`),
      `${target} crate constant ${name} does not match provider-assets.lock.json`,
    )
  }
  for (const [name, value] of [
    ['RG_XZ_LEN', asset.xz.length],
    ['RG_RAW_LEN', asset.raw.length],
  ]) {
    invariant(
      source.includes(`pub const ${name}: usize = ${value};`),
      `${target} crate constant ${name} does not match provider-assets.lock.json`,
    )
  }
}

function normalizeWhitespace(value) {
  return value
    .replace(/\s+/g, ' ')
    .replace(/\( /g, '(')
    .replace(/ \)/g, ')')
    .trim()
}

function assertTargetWiring(repoRoot, target) {
  const crate = `klaus-agent-ripgrep-${target}`
  const rustCrate = crate.replaceAll('-', '_')
  const cfg = EXPECTED_RUST_TARGET_CFG[target]
  invariant(typeof cfg === 'string', `${target} must have an exact Rust cfg mapping`)

  const rootManifest = readFileSync(path.join(repoRoot, 'Cargo.toml'), 'utf8')
  invariant(rootManifest.includes(`"crates/ripgrep-${target}"`), `${target} must be a workspace member`)
  invariant(
    rootManifest.includes(`${crate} = { version = "${ENGINE_VERSION_REQ}", path = "crates/ripgrep-${target}" }`),
    `${target} must be declared with the engine's fixed unversioned requirement`,
  )

  const coreManifest = readFileSync(path.join(repoRoot, 'crates/core/Cargo.toml'), 'utf8')
  invariant(
    coreManifest.includes(`[target.'cfg(${cfg})'.dependencies]\n${crate} = { workspace = true }`),
    `${target} must be selected by its exact core target cfg`,
  )

  const bundledSource = normalizeWhitespace(
    readFileSync(path.join(repoRoot, 'crates/core/src/fs/bundled_ripgrep.rs'), 'utf8'),
  )
  const selection = normalizeWhitespace(
    `#[cfg(${cfg})] fn selected_asset() -> Option<EmbeddedAsset> { Some(asset_from_crate!(${rustCrate})) }`,
  )
  invariant(bundledSource.includes(selection), `${target} must map its cfg to its own embedded crate`)
}

function assertResolverFailsClosed(repoRoot) {
  const source = readFileSync(path.join(repoRoot, 'crates/core/src/fs/ripgrep.rs'), 'utf8')
  const start = source.indexOf('pub fn resolve_ripgrep_command()')
  const end = source.indexOf('/// Test seam:', start)
  invariant(start !== -1 && end !== -1, 'ripgrep resolver source boundary is missing')
  const resolver = source.slice(start, end)
  invariant(
    !resolver.includes('OsString::from("rg")'),
    'ripgrep resolver must not silently fall back to a system rg',
  )
  invariant(
    resolver.includes('@vscode/ripgrep@1.18.0 has no bundled executable'),
    'ripgrep resolver must fail closed for unsupported wrapper targets',
  )
}

function parseMachOIdentity(raw) {
  invariant(raw.length >= 8, 'Mach-O payload is truncated')
  const magic = raw.subarray(0, 4).toString('hex')
  const variants = {
    cefaedfe: { class: 32, endian: 'little' },
    cffaedfe: { class: 64, endian: 'little' },
    feedface: { class: 32, endian: 'big' },
    feedfacf: { class: 64, endian: 'big' },
  }
  const variant = variants[magic]
  invariant(variant, 'payload is not a supported thin Mach-O executable')
  const machine = variant.endian === 'little' ? raw.readUInt32LE(4) : raw.readUInt32BE(4)
  return { format: 'Mach-O', machine, ...variant }
}

function parseElfIdentity(raw) {
  invariant(raw.length >= 20, 'ELF payload is truncated')
  invariant(raw.subarray(0, 4).equals(Buffer.from([0x7f, 0x45, 0x4c, 0x46])), 'payload is not ELF')
  const classBits = raw[4] === 1 ? 32 : raw[4] === 2 ? 64 : null
  const endian = raw[5] === 1 ? 'little' : raw[5] === 2 ? 'big' : null
  invariant(classBits !== null, 'ELF class must be 32-bit or 64-bit')
  invariant(endian !== null, 'ELF endian marker must be little or big')
  const machine = endian === 'little' ? raw.readUInt16LE(18) : raw.readUInt16BE(18)
  return { format: 'ELF', machine, class: classBits, endian }
}

function parsePeIdentity(raw) {
  invariant(raw.length >= 0x40 && raw.subarray(0, 2).toString('ascii') === 'MZ', 'payload is not PE')
  const peOffset = raw.readUInt32LE(0x3c)
  invariant(peOffset <= raw.length - 26, 'PE header is truncated or out of bounds')
  invariant(raw.subarray(peOffset, peOffset + 4).equals(Buffer.from('PE\0\0', 'binary')), 'PE signature is missing')
  const machine = raw.readUInt16LE(peOffset + 4)
  const optionalMagic = raw.readUInt16LE(peOffset + 24)
  const classBits = optionalMagic === 0x10b ? 32 : optionalMagic === 0x20b ? 64 : null
  invariant(classBits !== null, 'PE optional header class must be PE32 or PE32+')
  return { format: 'PE', machine, class: classBits, endian: 'little' }
}

function executableIdentity(raw) {
  if (raw.subarray(0, 4).equals(Buffer.from([0x7f, 0x45, 0x4c, 0x46]))) return parseElfIdentity(raw)
  if (raw.subarray(0, 2).toString('ascii') === 'MZ') return parsePeIdentity(raw)
  return parseMachOIdentity(raw)
}

export function verifyExecutableIdentity(raw, target) {
  const expected = EXPECTED_EXECUTABLE_IDENTITIES[target]
  invariant(expected, `${target} has no fixed executable identity`)
  const actual = executableIdentity(raw)
  for (const field of ['format', 'machine', 'class', 'endian']) {
    invariant(actual[field] === expected[field], `${target} ${field} ${String(actual[field])} != ${String(expected[field])}`)
  }
  return actual
}

export function verifyPinnedAssets(repoRoot) {
  const lockPath = path.join(repoRoot, 'provider-assets.lock.json')
  const lock = validateLockShape(JSON.parse(readFileSync(lockPath, 'utf8')))

  const icuXzPath = path.join(repoRoot, lock.icu.xz.file)
  const icuXz = readFileSync(icuXzPath)
  verifyIdentity(icuXz, lock.icu.xz, 'ICU XZ')
  verifyIdentity(decompressXz(icuXzPath, lock.icu.raw.length), lock.icu.raw, 'ICU raw')
  verifyPackageLicenseFiles(
    'klaus-agent-icu-data',
    new Map([
      [lock.licenses.icuNotice.file, readFileSync(path.join(repoRoot, 'crates/icu-data', lock.licenses.icuNotice.file))],
      [lock.licenses.icuWrapperMit.file, readFileSync(path.join(repoRoot, 'crates/icu-data', lock.licenses.icuWrapperMit.file))],
    ]),
    lock.licenses,
  )

  for (const target of EXPECTED_RIPGREP_TARGETS) {
    const asset = lock.assets[target]
    const xzPath = path.join(repoRoot, asset.xz.file)
    const compressed = readFileSync(xzPath)
    verifyIdentity(compressed, asset.xz, `${target} XZ`)
    const raw = decompressXz(xzPath, asset.raw.length)
    verifyIdentity(raw, asset.raw, `${target} raw`)
    verifyExecutableIdentity(raw, target)
    const crateRoot = path.join(repoRoot, `crates/ripgrep-${target}`)
    verifyPackageLicenseFiles(
      `klaus-agent-ripgrep-${target}`,
      new Map([
        [lock.licenses.microsoft.file, readFileSync(path.join(crateRoot, lock.licenses.microsoft.file))],
        [lock.licenses.ripgrepMit.file, readFileSync(path.join(crateRoot, lock.licenses.ripgrepMit.file))],
      ]),
      lock.licenses,
    )
    assertCrateConstants(repoRoot, target, asset)
    assertTargetWiring(repoRoot, target)
  }
  assertResolverFailsClosed(repoRoot)
  verifyBrowserAssetLocks(repoRoot, lock.browser)
  return lock
}

export function buildCargoMetadataArgs() {
  return ['metadata', '--locked', '--format-version', '1', '--no-deps']
}

function cargoMetadata(repoRoot) {
  const result = spawnSync('cargo', buildCargoMetadataArgs(), {
    cwd: repoRoot,
    encoding: 'utf8',
    maxBuffer: 16 * 1024 * 1024,
  })
  if (result.error) throw result.error
  invariant(result.status === 0, `cargo metadata failed:\n${result.stderr}`)
  return JSON.parse(result.stdout)
}

function tarString(header, offset, length, label) {
  const field = header.subarray(offset, offset + length)
  const terminator = field.indexOf(0)
  const bytes = terminator === -1 ? field : field.subarray(0, terminator)
  const value = bytes.toString('utf8')
  invariant(Buffer.from(value, 'utf8').equals(bytes), `${label} must be valid UTF-8`)
  return value
}

function tarOctal(header, offset, length, label) {
  const value = tarString(header, offset, length, label).trim()
  invariant(/^[0-7]+$/.test(value), `${label} must be canonical octal`)
  const parsed = Number.parseInt(value, 8)
  invariant(Number.isSafeInteger(parsed) && parsed >= 0, `${label} is out of range`)
  return parsed
}

function validateArchivePath(name, expectedRoot) {
  invariant(name.length > 0 && !name.includes('\\'), `unsafe archive path: ${name}`)
  invariant(!path.posix.isAbsolute(name), `unsafe archive path: ${name}`)
  invariant(path.posix.normalize(name) === name, `unsafe archive path: ${name}`)
  const segments = name.split('/')
  invariant(
    segments.length >= 2
      && segments[0] === expectedRoot
      && segments.every((segment) => segment.length > 0 && segment !== '.' && segment !== '..'),
    `unsafe archive path: ${name}`,
  )
}

export function parseCrateArchive(archiveBytes, packageName) {
  const expectedRoot = `${packageName}-${ENGINE_VERSION}`
  let tar
  try {
    tar = gunzipSync(archiveBytes, { maxOutputLength: 256 * 1024 * 1024 })
  } catch (error) {
    throw new Error(`cannot decompress ${packageName} crate archive: ${error instanceof Error ? error.message : String(error)}`)
  }
  invariant(tar.length % 512 === 0, `${packageName} tar length is not block aligned`)

  const files = new Map()
  let offset = 0
  let terminated = false
  let pendingLongName = null
  while (offset < tar.length) {
    const header = tar.subarray(offset, offset + 512)
    invariant(header.length === 512, `${packageName} tar header is truncated`)
    if (header.every((byte) => byte === 0)) {
      invariant(pendingLongName === null, `${packageName} tar ends with a dangling GNU long-name record`)
      invariant(tar.subarray(offset).every((byte) => byte === 0), `${packageName} tar has data after its terminator`)
      terminated = true
      break
    }

    const storedChecksum = tarOctal(header, 148, 8, `${packageName} tar checksum`)
    let computedChecksum = 0
    for (let index = 0; index < header.length; index += 1) {
      computedChecksum += index >= 148 && index < 156 ? 0x20 : header[index]
    }
    invariant(storedChecksum === computedChecksum, `${packageName} tar checksum mismatch`)

    const name = tarString(header, 0, 100, `${packageName} tar name`)
    const prefix = tarString(header, 345, 155, `${packageName} tar prefix`)
    const type = header[156] === 0 ? '0' : String.fromCharCode(header[156])
    const size = tarOctal(header, 124, 12, `${packageName} tar entry size`)
    const dataStart = offset + 512
    const dataEnd = dataStart + size
    invariant(dataEnd <= tar.length, `${packageName} archive entry is truncated`)

    // GNU long-name record: its data carries the NUL-terminated path of the
    // immediately following regular entry (cargo emits these for paths that
    // exceed the 100-byte ustar name field).
    if (type === 'L') {
      invariant(pendingLongName === null, `${packageName} archive chains GNU long-name records`)
      invariant(name === '././@LongLink' && !prefix, `${packageName} archive has a malformed GNU long-name record`)
      const raw = tar.subarray(dataStart, dataEnd)
      const nulIndex = raw.indexOf(0)
      pendingLongName = raw.subarray(0, nulIndex === -1 ? raw.length : nulIndex).toString('utf8')
      invariant(pendingLongName.length > 100, `${packageName} archive uses a GNU long-name record for a short path`)
      offset = dataStart + Math.ceil(size / 512) * 512
      continue
    }

    const entryPath = pendingLongName ?? (prefix ? `${prefix}/${name}` : name)
    if (pendingLongName !== null) {
      invariant(
        pendingLongName.startsWith(name.replace(/\0+$/u, '')) || name.length === 100,
        `${packageName} archive long-name record does not match its entry`,
      )
      pendingLongName = null
    }
    validateArchivePath(entryPath, expectedRoot)
    invariant(type === '0', `${packageName} archive contains unsupported tar entry type ${type}`)
    invariant(!files.has(entryPath), `${packageName} archive contains duplicate entry ${entryPath}`)

    files.set(entryPath, Buffer.from(tar.subarray(dataStart, dataEnd)))
    offset = dataStart + Math.ceil(size / 512) * 512
  }
  invariant(terminated, `${packageName} tar is missing its zero terminator`)
  invariant(files.has(`${expectedRoot}/Cargo.toml`), `${packageName} archive is missing normalized Cargo.toml`)
  return { files, contents: [...files.keys()] }
}

function materializeCrateArchive(parsed, workspaceRoot, packageName) {
  const archiveRoot = `${packageName}-${ENGINE_VERSION}/`
  const packageRoot = path.join(workspaceRoot, DISTRIBUTION_PACKAGE_PATHS[packageName])
  for (const [entryPath, data] of parsed.files) {
    invariant(entryPath.startsWith(archiveRoot), `${packageName} archive entry has the wrong root`)
    const relativePath = entryPath.slice(archiveRoot.length)
    const destination = path.join(packageRoot, ...relativePath.split('/'))
    invariant(
      destination.startsWith(`${path.resolve(workspaceRoot)}${path.sep}`),
      `archive destination escapes temporary workspace: ${entryPath}`,
    )
    mkdirSync(path.dirname(destination), { recursive: true })
    writeFileSync(destination, data, { flag: 'wx', mode: 0o644 })
  }
}

function verifyArchiveIsolation(packageName, contents) {
  const blobs = contents.filter((entry) => /\/data\/.*\.(?:xz|gz)$/.test(entry))
  if (packageName === 'klaus-agent-icu-data') {
    invariant(blobs.length === 1 && blobs[0].endsWith('/data/icudt77l.dat.xz'), 'ICU package must contain exactly its one XZ asset')
    invariant(contents.some((entry) => entry.endsWith('/ICU-LICENSE.txt')), 'ICU package must contain its redistribution notice')
  } else if (packageName.startsWith('klaus-agent-ripgrep-')) {
    invariant(blobs.length === 1 && blobs[0].endsWith('/data/rg.xz'), `${packageName} must contain exactly one rg.xz`)
    invariant(contents.some((entry) => entry.endsWith('/LICENSE-MICROSOFT')), `${packageName} must contain the Microsoft notice`)
    invariant(contents.some((entry) => entry.endsWith('/LICENSE-RIPGREP-MIT')), `${packageName} must contain the ripgrep MIT notice`)
  } else {
    invariant(blobs.length === 0, `${packageName} must not duplicate ICU or ripgrep blobs`)
    invariant(!contents.some((entry) => /icudt77|\/rg\.xz$/.test(entry)), `${packageName} contains a split asset`)
  }
}

function verifyBrowserArchive(packageName, parsed, repoRoot, browserState) {
  const marker = '/assets/exact-browser-runtime-assets-v1/'
  const bundledMarker = '/assets/klaus-bundled/browser/'
  if (packageName !== 'klaus-agent-sdk') {
    invariant(
      !parsed.contents.some((entry) => entry.includes(marker) || entry.includes(bundledMarker)),
      `${packageName} must not duplicate SDK browser assets`,
    )
    return
  }
  const root = `${packageName}-${ENGINE_VERSION}/`
  const expected = [
    ...browserState.bundled.files.map(({ path: assetPath }) => [
      `assets/klaus-bundled/browser/${assetPath}`,
      path.join(repoRoot, 'crates/sdk/assets/klaus-bundled/browser', ...assetPath.split('/')),
    ]),
    [
      'assets/klaus-bundled/browser/ASSET-SOURCES.sha256',
      path.join(repoRoot, 'crates/sdk/assets/klaus-bundled/browser/ASSET-SOURCES.sha256'),
    ],
    [
      'assets/bundled-browser-assets.lock.json',
      path.join(repoRoot, 'crates/sdk/assets/bundled-browser-assets.lock.json'),
    ],
  ]
  const runtime = browserState.runtime
  const runtimePaths = [
    'exact-browser-runtime-assets.lock.json',
    runtime.sourceGraph.file,
    runtime.processPatch.file,
    runtime.processPatch.preludeFile,
    runtime.patchedGraph.file,
    ...runtime.packages.map(({ fixturePath }) => fixturePath),
  ]
  for (const assetPath of runtimePaths) {
    expected.push([
      `assets/exact-browser-runtime-assets-v1/${assetPath}`,
      path.join(repoRoot, 'crates/sdk/assets/exact-browser-runtime-assets-v1', ...assetPath.split('/')),
    ])
  }
  for (const [archivePath, sourcePath] of expected) {
    const bytes = parsed.files.get(`${root}${archivePath}`)
    invariant(bytes, `SDK archive is missing ${archivePath}`)
    invariant(bytes.equals(readFileSync(sourcePath)), `SDK archive browser bytes drifted: ${archivePath}`)
  }
}

export function buildCargoPackageArgs(packageNames) {
  return [
    'package',
    '--locked',
    ...packageNames.flatMap((packageName) => ['-p', packageName]),
    '--allow-dirty',
    '--no-verify',
  ]
}

function checkPackagedWorkspace(repoRoot, workspaceRoot) {
  // Seeded, not enforced: cargo inherits every pinned version from here and is still
  // allowed to drop the entries this smaller workspace does not reach.
  copyFileSync(path.join(repoRoot, 'Cargo.lock'), path.join(workspaceRoot, 'Cargo.lock'))
  writeFileSync(path.join(workspaceRoot, 'Cargo.toml'), buildVerificationWorkspaceManifest(), { flag: 'wx' })
  const metadata = cargoMetadata(workspaceRoot)
  assertNormalizedDistributionMetadata(metadata, workspaceRoot)

  const result = spawnSync('cargo', buildCargoCheckArgs(), {
    cwd: workspaceRoot,
    encoding: 'utf8',
    env: {
      ...process.env,
      CARGO_TARGET_DIR: path.join(repoRoot, 'target'),
    },
    maxBuffer: 32 * 1024 * 1024,
  })
  if (result.error) throw result.error
  invariant(
    result.status === 0,
    `cargo check failed for the extracted distribution workspace:\n${result.stdout}\n${result.stderr}`,
  )

  // The lock cargo just wrote for the extracted crates, against the one the
  // repository already reviewed.
  assertNoResolvedVersionDrift(
    readFileSync(path.join(repoRoot, 'Cargo.lock'), 'utf8'),
    readFileSync(path.join(workspaceRoot, 'Cargo.lock'), 'utf8'),
  )
}

export function packageAndVerify(repoRoot, maxBytes) {
  const metadata = cargoMetadata(repoRoot)
  assertPublishableWorkspaceSet(metadata)
  const lock = validateLockShape(
    JSON.parse(readFileSync(path.join(repoRoot, 'provider-assets.lock.json'), 'utf8')),
  )
  const browserState = verifyBrowserAssetLocks(repoRoot, lock.browser)
  const versions = new Map(metadata.packages.map((pkg) => [pkg.name, pkg.version]))
  const archives = new Map()
  for (const packageName of DISTRIBUTION_PACKAGES) {
    const version = versions.get(packageName)
    invariant(version, `cargo metadata is missing ${packageName}`)
    const archive = path.join(repoRoot, 'target', 'package', `${packageName}-${version}.crate`)
    if (existsSync(archive)) rmSync(archive)
    archives.set(packageName, archive)
  }

  // Cargo must prepare the complete path-linked publish graph as one package
  // set. Packaging any downstream crate alone makes Cargo resolve unpublished
  // dependencies from the registry and cannot validate a fresh release.
  const result = spawnSync('cargo', buildCargoPackageArgs(DISTRIBUTION_PACKAGES), {
    cwd: repoRoot,
    encoding: 'utf8',
    maxBuffer: 32 * 1024 * 1024,
  })
  if (result.error) throw result.error
  invariant(
    result.status === 0,
    `cargo package failed for the distribution set:\n${result.stdout}\n${result.stderr}`,
  )

  const workspaceRoot = mkdtempSync(path.join(tmpdir(), 'klaus-agent-distribution-'))
  try {
    const output = []
    for (const packageName of DISTRIBUTION_PACKAGES) {
      const archive = archives.get(packageName)
      invariant(existsSync(archive), `cargo package did not create ${archive}`)
      const bytes = statSync(archive).size
      invariant(bytes < maxBytes, `${packageName} package is ${bytes} bytes; limit is < ${maxBytes}`)
      const parsed = parseCrateArchive(readFileSync(archive), packageName)
      verifyArchiveIsolation(packageName, parsed.contents)
      verifyBrowserArchive(packageName, parsed, repoRoot, browserState)
      verifyPackageLicenseFiles(packageName, parsed.files, lock.licenses)
      materializeCrateArchive(parsed, workspaceRoot, packageName)
      output.push({
        package: packageName,
        archive: path.relative(repoRoot, archive),
        bytes,
        files: parsed.contents.length,
      })
    }
    checkPackagedWorkspace(repoRoot, workspaceRoot)
    return output
  } finally {
    rmSync(workspaceRoot, { recursive: true, force: true })
  }
}

export function run(repoRoot, options) {
  verifyPinnedAssets(repoRoot)
  return {
    schemaVersion: 1,
    assets: 'verified',
    packages: options.package ? packageAndVerify(repoRoot, options.maxBytes) : [],
    maxBytes: options.maxBytes,
  }
}

const isMain = process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1])
if (isMain) {
  try {
    const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
    const result = run(repoRoot, parseArguments(process.argv.slice(2)))
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`)
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.stack ?? error.message : String(error)}\n`)
    process.exitCode = 1
  }
}
