import assert from 'node:assert/strict'
import { mkdtempSync, readFileSync, realpathSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import path from 'node:path'
import test from 'node:test'
import { gzipSync } from 'node:zlib'

import {
  ENGINE_VERSION,
  ENGINE_VERSION_REQ,
  assertNoResolvedVersionDrift,
  assertNormalizedDistributionMetadata,
  assertPublishableWorkspaceSet,
  buildCargoMetadataArgs,
  buildCargoCheckArgs,
  buildCargoPackageArgs,
  buildVerificationWorkspaceManifest,
  DISTRIBUTION_PACKAGE_PATHS,
  DISTRIBUTION_PACKAGES,
  EXPECTED_EXECUTABLE_IDENTITIES,
  EXPECTED_RIPGREP_TARGETS,
  EXPECTED_RUST_TARGET_CFG,
  parseArguments,
  parseCrateArchive,
  validateLockShape,
  verifyExecutableIdentity,
  verifyPackageLicenseFiles,
} from './check-distribution-packages.mjs'

const CRATES_IO_SOURCE = 'registry+https://github.com/rust-lang/crates.io-index'

function metadataFixture(
  root = path.resolve('/tmp/klaus-agent-distribution'),
  { normalized = false } = {},
) {
  const packages = DISTRIBUTION_PACKAGES.map((name) => {
    const id = `${name} ${ENGINE_VERSION} (path+file://${root}/${name}-${ENGINE_VERSION})`
    return {
      id,
      name,
      version: ENGINE_VERSION,
      publish: null,
      manifest_path: path.join(root, DISTRIBUTION_PACKAGE_PATHS[name], 'Cargo.toml'),
      license: {
        'klaus-agent-icu-data': 'MIT AND Unicode-3.0',
      }[name] ?? 'MIT',
      rust_version: '1.93',
      dependencies: [],
    }
  })
  packages.find(({ name }) => name === 'klaus-agent-core').dependencies.push({
    name: 'klaus-agent-icu-data',
    req: ENGINE_VERSION_REQ,
    path: normalized ? null : path.join(root, DISTRIBUTION_PACKAGE_PATHS['klaus-agent-icu-data']),
    source: normalized ? CRATES_IO_SOURCE : null,
  }, {
    name: 'serde',
    req: '^1',
    path: null,
    source: CRATES_IO_SOURCE,
  })
  return {
    packages,
    workspace_members: packages.map(({ id }) => id),
  }
}

function tarOctal(header, offset, length, value) {
  const encoded = `${value.toString(8).padStart(length - 1, '0')}\0`
  header.write(encoded, offset, length, 'ascii')
}

function crateArchive(entries) {
  const blocks = []
  for (const entry of entries) {
    const data = Buffer.from(entry.data ?? '')
    const header = Buffer.alloc(512)
    header.write(entry.name, 0, 100, 'utf8')
    tarOctal(header, 100, 8, 0o644)
    tarOctal(header, 108, 8, 0)
    tarOctal(header, 116, 8, 0)
    tarOctal(header, 124, 12, data.length)
    tarOctal(header, 136, 12, 0)
    header.fill(0x20, 148, 156)
    header.write(entry.type ?? '0', 156, 1, 'ascii')
    header.write('ustar  ', 257, 7, 'ascii')
    let checksum = 0
    for (const byte of header) checksum += byte
    header.write(`${checksum.toString(8).padStart(6, '0')}\0 `, 148, 8, 'ascii')
    blocks.push(header, data, Buffer.alloc((512 - (data.length % 512)) % 512))
  }
  blocks.push(Buffer.alloc(1024))
  return gzipSync(Buffer.concat(blocks))
}

function executableFixture(target) {
  const identity = EXPECTED_EXECUTABLE_IDENTITIES[target]
  if (identity.format === 'Mach-O') {
    const binary = Buffer.alloc(32)
    Buffer.from([0xcf, 0xfa, 0xed, 0xfe]).copy(binary)
    binary.writeUInt32LE(identity.machine, 4)
    return binary
  }
  if (identity.format === 'ELF') {
    const binary = Buffer.alloc(64)
    Buffer.from([0x7f, 0x45, 0x4c, 0x46]).copy(binary)
    binary[4] = identity.class === 64 ? 2 : 1
    binary[5] = identity.endian === 'little' ? 1 : 2
    if (identity.endian === 'little') binary.writeUInt16LE(identity.machine, 18)
    else binary.writeUInt16BE(identity.machine, 18)
    return binary
  }
  const binary = Buffer.alloc(128)
  binary.write('MZ', 0, 2, 'ascii')
  binary.writeUInt32LE(64, 0x3c)
  binary.write('PE\0\0', 64, 4, 'binary')
  binary.writeUInt16LE(identity.machine, 68)
  binary.writeUInt16LE(identity.class === 64 ? 0x20b : 0x10b, 88)
  return binary
}

test('packages the complete publish graph in one Cargo invocation', () => {
  assert.deepEqual(buildCargoPackageArgs(DISTRIBUTION_PACKAGES), [
    'package',
    '--locked',
    ...DISTRIBUTION_PACKAGES.flatMap((packageName) => ['-p', packageName]),
    '--allow-dirty',
    '--no-verify',
  ])
})

test('all distribution Cargo resolution is locked to the checked-in lockfile', () => {
  assert.deepEqual(buildCargoMetadataArgs(), [
    'metadata',
    '--locked',
    '--format-version',
    '1',
    '--no-deps',
  ])
})

test('distribution package set covers every publishable crate in one dependency-ordered gate', () => {
  assert.deepEqual(EXPECTED_RIPGREP_TARGETS, [
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
  assert.deepEqual(DISTRIBUTION_PACKAGES, [
    'klaus-agent-icu-data',
    ...EXPECTED_RIPGREP_TARGETS.map((target) => `klaus-agent-ripgrep-${target}`),
    'klaus-agent-core',
    'klaus-agent-protocol',
    'klaus-agent-sdk',
    'klaus-agent-server',
    'klaus-sandbox-win',
  ])
  assert.deepEqual(Object.keys(EXPECTED_RUST_TARGET_CFG), EXPECTED_RIPGREP_TARGETS)
  assert.equal(
    EXPECTED_RUST_TARGET_CFG['linux-ppc64'],
    'all(target_os = "linux", target_arch = "powerpc64", target_endian = "little")',
  )
})

test('publishable workspace members are exactly the distribution package set', () => {
  const metadata = metadataFixture()
  assert.doesNotThrow(() => assertPublishableWorkspaceSet(metadata))

  const expanded = structuredClone(metadata)
  expanded.packages.push({
    id: `unreviewed ${ENGINE_VERSION} (path+file:///tmp/unreviewed)`,
    name: 'unreviewed',
    version: ENGINE_VERSION,
    publish: null,
  })
  expanded.workspace_members.push(expanded.packages.at(-1).id)
  assert.throws(() => assertPublishableWorkspaceSet(expanded), /publishable workspace packages/)

  const missing = structuredClone(metadata)
  missing.packages.find(({ name }) => name === 'klaus-agent-sdk').publish = []
  assert.throws(() => assertPublishableWorkspaceSet(missing), /publishable workspace packages/)
})

test('source path dependencies are exact publishable distribution workspace members', () => {
  const root = path.resolve('/tmp/klaus-agent-source')
  const source = metadataFixture(root)
  assert.doesNotThrow(() => assertPublishableWorkspaceSet(source))

  const helper = {
    id: `helper ${ENGINE_VERSION} (path+file://${root}/crates/helper)`,
    name: 'helper',
    version: ENGINE_VERSION,
    publish: [],
    manifest_path: path.join(root, 'crates/helper/Cargo.toml'),
    dependencies: [],
  }
  const mutations = [
    (metadata) => {
      metadata.packages.push(helper)
      metadata.workspace_members.push(helper.id)
      metadata.packages.find(({ name }) => name === 'klaus-agent-core').dependencies.push({
        name: 'helper', req: ENGINE_VERSION_REQ, path: path.join(root, 'crates/helper'), source: null,
      })
    },
    (metadata) => {
      metadata.packages.find(({ name }) => name === 'klaus-agent-core').dependencies[0].path = path.join(root, 'outside')
    },
    (metadata) => {
      metadata.packages.find(({ name }) => name === 'klaus-agent-core').dependencies[0].req = '^0.1'
    },
    (metadata) => {
      metadata.packages.find(({ name }) => name === 'klaus-agent-core').dependencies[0].source = CRATES_IO_SOURCE
    },
  ]
  for (const mutate of mutations) {
    const metadata = structuredClone(source)
    mutate(metadata)
    assert.throws(() => assertPublishableWorkspaceSet(metadata), /path dependency/)
  }
})

test('normalized package manifests preserve release identity and registry-only local dependencies', () => {
  const root = path.resolve('/tmp/klaus-agent-distribution')
  const source = metadataFixture(root, { normalized: true })
  assert.doesNotThrow(() => assertNormalizedDistributionMetadata(source, root))

  const mutations = [
    (metadata) => { metadata.packages[0].version = '0.1.1' },
    (metadata) => { metadata.packages[0].license = 'Apache-2.0' },
    (metadata) => { metadata.packages[0].rust_version = '1.92' },
    (metadata) => { metadata.packages[0].manifest_path = path.join(root, 'Cargo.toml.orig') },
    (metadata) => { metadata.packages.find(({ name }) => name === 'klaus-agent-core').dependencies[0].req = '^0.1' },
    (metadata) => { metadata.packages.find(({ name }) => name === 'klaus-agent-core').dependencies[0].path = '/tmp/source/crates/icu-data' },
    (metadata) => { metadata.packages.find(({ name }) => name === 'klaus-agent-core').dependencies[0].source = null },
    (metadata) => { metadata.packages.find(({ name }) => name === 'klaus-agent-core').dependencies[1].source = 'git+https://example.invalid/serde' },
    (metadata) => {
      metadata.packages.find(({ name }) => name === 'klaus-agent-core').dependencies.push({
        name: 'helper', req: ENGINE_VERSION_REQ, path: '/tmp/helper', source: null,
      })
    },
  ]
  for (const mutate of mutations) {
    const metadata = structuredClone(source)
    mutate(metadata)
    assert.throws(() => assertNormalizedDistributionMetadata(metadata, root))
  }
})

test('normalized manifest roots accept Cargo canonicalizing a temporary-directory symlink', () => {
  const root = mkdtempSync(path.join(tmpdir(), 'klaus-agent-metadata-root-'))
  try {
    const metadata = metadataFixture(realpathSync(root), { normalized: true })
    assert.doesNotThrow(() => assertNormalizedDistributionMetadata(metadata, root))
  } finally {
    rmSync(root, { recursive: true, force: true })
  }
})

test('the extracted workspace may resolve fewer packages but never a different version', () => {
  const lock = (entries) => entries
    .map(([name, version]) => `[[package]]\nname = "${name}"\nversion = "${version}"\n`)
    .join('\n')

  const repo = lock([['serde', '1.0.1'], ['clap', '4.5.0'], ['thiserror', '2.0.18']])
  // A subset is the normal case: the distribution set does not reach the CLI's deps.
  assert.doesNotThrow(() => assertNoResolvedVersionDrift(repo, lock([['serde', '1.0.1']])))
  // Two versions of one package in the repository lock both count as reviewed.
  const twoVersions = lock([['thiserror', '1.0.69'], ['thiserror', '2.0.18']])
  assert.doesNotThrow(() => assertNoResolvedVersionDrift(twoVersions, lock([['thiserror', '1.0.69']])))

  // Drift is the failure this replaces `--locked` for.
  assert.throws(
    () => assertNoResolvedVersionDrift(repo, lock([['serde', '1.0.2']])),
    /resolved serde 1\.0\.2, but the repository resolved 1\.0\.1/,
  )
  assert.throws(
    () => assertNoResolvedVersionDrift(repo, lock([['smuggled', '0.1.0']])),
    /resolved smuggled, which the repository lock does not contain/,
  )
})

test('temporary registry patch workspace checks every packaged target and feature offline', () => {
  assert.deepEqual(buildCargoCheckArgs(), [
    'check',
    '--workspace',
    '--lib',
    '--bins',
    '--all-features',
    '--offline',
  ])
  const manifest = buildVerificationWorkspaceManifest()
  for (const packageName of DISTRIBUTION_PACKAGES) {
    const packagePath = DISTRIBUTION_PACKAGE_PATHS[packageName]
    assert.match(manifest, new RegExp(`"${packagePath}"`))
    assert.match(manifest, new RegExp(`^${packageName} = \\{ path = "${packagePath}" \\}$`, 'm'))
  }
})

test('crate archive parser rejects path traversal, links, and duplicate entries before extraction', () => {
  const packageName = 'klaus-agent-core'
  const safe = `${packageName}-${ENGINE_VERSION}/Cargo.toml`
  const parsed = parseCrateArchive(crateArchive([{ name: safe, data: '[package]\n' }]), packageName)
  assert.equal(parsed.files.get(safe).toString('utf8'), '[package]\n')

  for (const entries of [
    [{ name: '../escape', data: 'x' }],
    [{ name: `${packageName}-${ENGINE_VERSION}/../../escape`, data: 'x' }],
    [{ name: `/tmp/${packageName}-${ENGINE_VERSION}/Cargo.toml`, data: 'x' }],
    [{ name: `${packageName}-${ENGINE_VERSION}\\Cargo.toml`, data: 'x' }],
    [{ name: safe, type: '2' }],
    [{ name: safe, type: '1' }],
    [{ name: safe }, { name: safe }],
  ]) {
    assert.throws(() => parseCrateArchive(crateArchive(entries), packageName))
  }
})

test('all twelve ripgrep payloads bind format, CPU, class, and endian', () => {
  assert.deepEqual(Object.keys(EXPECTED_EXECUTABLE_IDENTITIES), EXPECTED_RIPGREP_TARGETS)
  for (const target of EXPECTED_RIPGREP_TARGETS) {
    assert.doesNotThrow(() => verifyExecutableIdentity(executableFixture(target), target))
  }

  for (const [target, mutate] of [
    ['darwin-x64', (binary) => binary.writeUInt32LE(0x0100000c, 4)],
    ['linux-x64', (binary) => binary.writeUInt16LE(3, 18)],
    ['linux-x64', (binary) => { binary[4] = 1 }],
    ['linux-x64', (binary) => { binary[5] = 2 }],
    ['win32-x64', (binary) => binary.writeUInt16LE(0x14c, 68)],
    ['win32-x64', (binary) => binary.writeUInt16LE(0x10b, 88)],
  ]) {
    const binary = executableFixture(target)
    mutate(binary)
    assert.throws(() => verifyExecutableIdentity(binary, target))
  }
})

test('source and packaged redistribution licenses match fixed byte identities', () => {
  const lock = JSON.parse(readFileSync('provider-assets.lock.json', 'utf8'))
  const ripgrep = new Map([
    ['LICENSE-MICROSOFT', readFileSync('crates/ripgrep-linux-x64/LICENSE-MICROSOFT')],
    ['LICENSE-RIPGREP-MIT', readFileSync('crates/ripgrep-linux-x64/LICENSE-RIPGREP-MIT')],
  ])
  const icu = new Map([
    ['ICU-LICENSE.txt', readFileSync('crates/icu-data/ICU-LICENSE.txt')],
    ['LICENSE-MIT', readFileSync('crates/icu-data/LICENSE-MIT')],
  ])
  assert.doesNotThrow(() => verifyPackageLicenseFiles('klaus-agent-ripgrep-linux-x64', ripgrep, lock.licenses))
  assert.doesNotThrow(() => verifyPackageLicenseFiles('klaus-agent-icu-data', icu, lock.licenses))

  const mutated = new Map(ripgrep)
  mutated.set('LICENSE-MICROSOFT', Buffer.concat([mutated.get('LICENSE-MICROSOFT'), Buffer.from('\n')]))
  assert.throws(
    () => verifyPackageLicenseFiles('klaus-agent-ripgrep-linux-x64', mutated, lock.licenses),
    /license.*length/i,
  )
})

test('every publishable workspace dependency carries an exact registry version and local path', () => {
  const manifest = readFileSync('Cargo.toml', 'utf8')
  // The requirement comes from the module under test rather than being spelled out
  // here: a literal in this file is a second place the engine's fixed version has to
  // be updated, and the last time it was raised this test was the thing that lagged.
  const escapedReq = ENGINE_VERSION_REQ.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  for (const [name, path] of [
    ['klaus-agent-core', 'crates/core'],
    ['klaus-agent-protocol', 'crates/protocol'],
    ['klaus-agent-sdk', 'crates/sdk'],
    ['klaus-agent-server', 'crates/server'],
  ]) {
    assert.match(
      manifest,
      new RegExp(`^${name} = \\{ version = "${escapedReq}", path = "${path}" \\}$`, 'm'),
    )
  }
})

test('lock validation rejects silently expanded or incomplete target sets', () => {
  const base = JSON.parse(readFileSync('provider-assets.lock.json', 'utf8'))
  assert.doesNotThrow(() => validateLockShape(base))
  delete base.assets['linux-x64']
  assert.throws(() => validateLockShape(base), /exactly the fixed twelve targets/)
})

test('native-host distribution identity remains external and fail-closed', () => {
  const lock = JSON.parse(readFileSync('provider-assets.lock.json'))
  assert.doesNotThrow(() => validateLockShape(lock))

  const injectedPath = structuredClone(lock)
  injectedPath.browser.nativeHostDistribution.targets['linux-x64'].absolutePath =
    '/tmp/klaus-browser-native-host'
  assert.throws(
    () => validateLockShape(injectedPath),
    /native-host external identity must remain explicitly unavailable/,
  )

  const relaxedMode = structuredClone(lock)
  relaxedMode.browser.nativeHostDistribution.targets['linux-x64'].mode = '100755-or-owner-exec'
  assert.throws(() => validateLockShape(relaxedMode), /native-host mode drifted/)
})

test('lock validation binds every source, compression, and canonical asset path', () => {
  const source = JSON.parse(readFileSync('provider-assets.lock.json', 'utf8'))
  const mutations = [
    (lock) => { lock.extra = true },
    (lock) => { lock.icu.sourceAsset = 'other.zip' },
    (lock) => { lock.icu.sourceSha256 = '0'.repeat(64) },
    (lock) => { lock.icu.raw.file = 'data/icudt77l.dat' },
    (lock) => { lock.icu.xz.file = 'crates/icu-data/data/../data/icudt77l.dat.xz' },
    (lock) => { lock.ripgrep.wrapper = '@other/ripgrep' },
    (lock) => { lock.ripgrep.wrapperVersion = '1.18.1' },
    (lock) => { lock.ripgrep.binaryVersion = '15.0.1' },
    (lock) => { lock.ripgrep.binaryRevision = 'fffffffffff' },
    (lock) => { lock.ripgrep.compression.format = 'gzip' },
    (lock) => { lock.ripgrep.compression.toolVersion = '5.8.1' },
    (lock) => { lock.ripgrep.compression.arguments = ['-9'] },
    (lock) => { lock.assets['linux-x64'].raw.file = 'bin/rg' },
    (lock) => { lock.assets['linux-x64'].xz.file = 'crates/ripgrep-linux-x64/data/../data/rg.xz' },
    (lock) => { lock.assets['linux-x64'].npm.extra = true },
  ]

  for (const mutate of mutations) {
    const lock = structuredClone(source)
    mutate(lock)
    assert.throws(() => validateLockShape(lock))
  }
})

test('lock validation rejects non-canonical or non-SHA-512 npm integrity values', () => {
  const source = JSON.parse(readFileSync('provider-assets.lock.json', 'utf8'))
  const mutations = [
    'sha512-not-base64!',
    `sha512-${Buffer.alloc(63).toString('base64')}`,
    `sha512-${Buffer.alloc(64).toString('base64').replace(/==$/, '')}`,
  ]

  for (const integrity of mutations) {
    const lock = structuredClone(source)
    lock.assets['linux-x64'].npm.integrity = integrity
    assert.throws(() => validateLockShape(lock), /canonical SHA-512 SRI/)
  }
})

test('lock validation binds each target SRI and raw/XZ byte identity to fixed reviewed values', () => {
  const source = JSON.parse(readFileSync('provider-assets.lock.json', 'utf8'))
  const wrongSri = `sha512-${Buffer.alloc(64).toString('base64')}`
  for (const target of EXPECTED_RIPGREP_TARGETS) {
    for (const mutate of [
      (asset) => { asset.npm.integrity = wrongSri },
      (asset) => { asset.raw.length += 1 },
      (asset) => { asset.raw.sha256 = '0'.repeat(64) },
      (asset) => { asset.xz.length += 1 },
      (asset) => { asset.xz.sha256 = '0'.repeat(64) },
    ]) {
      const lock = structuredClone(source)
      mutate(lock.assets[target])
      assert.throws(() => validateLockShape(lock), new RegExp(`${target}.*fixed`))
    }
  }
})

test('lock validation binds ICU raw and XZ byte identities to fixed reviewed values', () => {
  const source = JSON.parse(readFileSync('provider-assets.lock.json', 'utf8'))
  for (const mutate of [
    (icu) => { icu.raw.length += 1 },
    (icu) => { icu.raw.sha256 = '0'.repeat(64) },
    (icu) => { icu.xz.length += 1 },
    (icu) => { icu.xz.sha256 = '0'.repeat(64) },
  ]) {
    const lock = structuredClone(source)
    mutate(lock.icu)
    assert.throws(() => validateLockShape(lock), /icu\.(?:raw|xz).*fixed/)
  }
})

test('lock validation binds the exact redistribution license identities', () => {
  const source = JSON.parse(readFileSync('provider-assets.lock.json', 'utf8'))
  assert.doesNotThrow(() => validateLockShape(source))
  assert.deepEqual(source.licenses, {
    microsoft: {
      file: 'LICENSE-MICROSOFT',
      length: 1113,
      sha256: 'ccf06b70760761ae1437395117bcf911501e3dc85385d84c7eba20d5dfab55dd',
    },
    ripgrepMit: {
      file: 'LICENSE-RIPGREP-MIT',
      length: 1081,
      sha256: '0f96a83840e146e43c0ec96a22ec1f392e0680e6c1226e6f3ba87e0740af850f',
    },
    icuNotice: {
      file: 'ICU-LICENSE.txt',
      length: 26487,
      sha256: '451167c55c0fa447cc2d5632714f5e3c567fe4f1e1badefab2c1333852198aca',
    },
    icuWrapperMit: {
      file: 'LICENSE-MIT',
      length: 1081,
      sha256: '0948bee27e4516b4ac815367adde524de3fda562dd33fbe6250db2085ea4563b',
    },
  })
  for (const mutate of [
    (lock) => { lock.licenses.microsoft.length += 1 },
    (lock) => { lock.licenses.ripgrepMit.sha256 = '0'.repeat(64) },
    (lock) => { lock.licenses.icuNotice.file = 'NOTICE.txt' },
    (lock) => { lock.licenses.icuWrapperMit.extra = true },
  ]) {
    const lock = structuredClone(source)
    mutate(lock)
    assert.throws(() => validateLockShape(lock))
  }
})

test('CLI defaults to the crates.io decimal package limit and rejects invalid limits', () => {
  assert.deepEqual(parseArguments([]), { maxBytes: 10_000_000, package: true })
  assert.deepEqual(parseArguments(['--max-bytes', '42', '--skip-package']), {
    maxBytes: 42,
    package: false,
  })
  assert.throws(() => parseArguments(['--max-bytes', '0']), /positive integer/)
  assert.throws(() => parseArguments(['--wat']), /unknown argument/)
})
