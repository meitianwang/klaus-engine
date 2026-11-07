# Provider behavior runtime asset distribution

## Contract

The provider-behavior Eval depends on two runtime assets that cannot remain
embedded in `klaus-agent-core`:

- full little-endian ICU 77.1 data, required for Node 20-compatible default
  `Intl.Collator` behavior;
- the pinned `@vscode/ripgrep@1.18.0` executable set.

The uncompressed identities, npm SRI values, compressed identities, and source
paths are pinned in
`provider-assets.lock.json`. `scripts/check-distribution-packages.mjs` verifies
the lock, bounded XZ decompression, raw SHA-256, binary format, crate constants,
CPU/class/endianness, exact redistribution-notice bytes, package contents, and
the decimal crates.io package limit.

Browser runtime assets and the binary admission boundary for a source checkout
are described in `source-only-browser-distribution.md`.

## Package split

`klaus-agent-icu-data` owns the one ICU archive and its complete Unicode/ICU
redistribution notice. It exposes only:

```rust
pub fn common_data_77() -> Result<&'static [u8], String>;
```

The result is cached for the process lifetime, bounded to the pinned raw size,
checked by SHA-256, and stored at 16-byte alignment before V8 initialization.
The XZ package is 7,633,376 bytes; the prior gzip was 12,356,239 bytes and
could not fit in a registry package by itself.

The public `@vscode/ripgrep@1.18.0` wrapper manifest declares twelve optional
platform packages. Runtime fidelity follows the wrapper's platform selection
contract: a consumer install on Windows x64, Windows ARM64, Linux
ARM/ARM64, or Linux ppc64 must not silently change to a system executable.
Every declared wrapper target is therefore pinned to one Rust asset crate:

| Wrapper target | Rust cfg | Asset crate |
|---|---|---|
| `darwin-arm64` | `macos/aarch64` | `klaus-agent-ripgrep-darwin-arm64` |
| `darwin-x64` | `macos/x86_64` | `klaus-agent-ripgrep-darwin-x64` |
| `linux-arm` | `linux/arm` | `klaus-agent-ripgrep-linux-arm` |
| `linux-arm64` | `linux/aarch64` | `klaus-agent-ripgrep-linux-arm64` |
| `linux-ia32` | `linux/x86` | `klaus-agent-ripgrep-linux-ia32` |
| `linux-ppc64` | `linux/powerpc64 little-endian` | `klaus-agent-ripgrep-linux-ppc64` |
| `linux-riscv64` | `linux/riscv64` | `klaus-agent-ripgrep-linux-riscv64` |
| `linux-s390x` | `linux/s390x` | `klaus-agent-ripgrep-linux-s390x` |
| `linux-x64` | `linux/x86_64` | `klaus-agent-ripgrep-linux-x64` |
| `win32-arm64` | `windows/aarch64` | `klaus-agent-ripgrep-win32-arm64` |
| `win32-ia32` | `windows/x86` | `klaus-agent-ripgrep-win32-ia32` |
| `win32-x64` | `windows/x86_64` | `klaus-agent-ripgrep-win32-x64` |

The npm tarball URL and registry SHA-512 SRI for every package are recorded in
the asset lock alongside the raw and compressed identities. Changing the
wrapper version or target set requires an explicit lock update and review.

Each target crate contains only its XZ executable, constants, README, and
Microsoft/ripgrep license notices. Core selects only the current target crate,
verifies both compressed and raw identities, then places the executable in a
deterministic version/hash directory under the user executable cache. Writes
use a same-directory temporary file and atomic rename. On Unix, every cache
directory and the executable must be owned by the effective user, leaf
symlinks are rejected, and directories/files use mode `0700`; the executable
is rechecked against its pinned length and SHA-256 before use. Linux accepts a
non-empty absolute `XDG_CACHE_HOME` without first requiring HOME/passwd, and
falls back to an absolute non-empty HOME only when XDG is absent.
`KLAUS_RIPGREP_CACHE_DIR` provides an explicit executable location for hosts
whose normal cache mount is `noexec`; it and `XDG_CACHE_HOME` must be absolute.

Windows has a narrower, explicit boundary. The implicit cache is accepted only
under a non-empty absolute `LOCALAPPDATA`. An absolute
`KLAUS_RIPGREP_CACHE_DIR` is an operator-selected trusted-directory override.
The current standard-library implementation rejects leaf symlinks reported by
`symlink_metadata` and still verifies length/SHA-256, but it does **not** claim
to validate owner SID, DACLs, or every ancestor reparse point. Windows hosts
must therefore preserve the normal per-user `LOCALAPPDATA` ACLs or choose a
trusted override; resistance to a hostile same-host principal requires a
future handle-based WinAPI owner/DACL/reparse implementation.

Competing materializers tolerate another process removing a corrupt final,
retry final verification a bounded number of times, and cache only successful
paths. A transient race is therefore not memoized as a process-lifetime
failure.

Resolution order is:

1. non-empty `KLAUS_RIPGREP_PATH`, using ECMAScript trim semantics;
2. the verified `@vscode/ripgrep@1.18.0` asset for the current wrapper target.

Asset corruption and unsupported targets fail closed. The engine never falls
back to a system `rg`; hosts outside the wrapper's twelve-target contract must
set `KLAUS_RIPGREP_PATH` explicitly.

## Platform boundary

Twelve ripgrep asset packages do not imply twelve fully runnable Rust engine
targets. The current `rusty_v8` distribution supports only a subset of those
targets, the ppc64 executable is specifically powerpc64le, and the
little-endian ICU archive is intentionally rejected on big-endian s390x.
Adding complete engine support for the remaining targets requires separately
reviewed V8 artifacts and, for s390x, a separately packaged big-endian ICU
archive. CI and release notes must not claim broader engine support based only
on the ripgrep asset table.

## License and package gates

- ICU crate: `MIT AND Unicode-3.0`, with `LICENSE-MIT` and the complete
  `ICU-LICENSE.txt` notice.
- ripgrep target crates: MIT-distributed wrapper/binary, with both the
  Microsoft vscode-ripgrep notice and Andrew Gallant's ripgrep MIT notice.
- core must contain no ICU or ripgrep blob.
- every generated `.crate` must be strictly smaller than 10,000,000 bytes.

Run the immutable asset check without packaging:

```bash
node scripts/check-distribution-packages.mjs --skip-package
```

Run the complete package gate:

```bash
node scripts/check-distribution-packages.mjs --max-bytes 10000000
```

The gate never downloads executable assets during a consumer build. Cargo
dependency resolution is locked to the committed `Cargo.lock`, and any
source-tree drift before or after the gate is rejected. The one invocation
packages the thirteen asset crates plus
core, eval, protocol, SDK, and server, then safely parses and extracts every
archive. The gate checks the exact publishable-workspace set, archive size,
split-asset isolation, license payload identities, normalized package metadata,
and registry-only `=0.1.0` local dependency requirements. Finally it constructs
a temporary workspace from the eighteen extracted archives and runs
`cargo check --workspace --all-targets --all-features --locked`, so repository-
external `include_str!` paths and missing packaged sources fail before release.
The SDK therefore carries its bundled browser runtime/extension sources under
`crates/sdk/assets/` with an exact SHA-256/source-sync contract; its published
crate no longer depends on the repository-level `plugins/` directory.

## Release order

1. Confirm ownership of every package name in the target registry.
2. Publish `klaus-agent-icu-data` and the twelve ripgrep target crates.
3. Wait until the registry index resolves all thirteen exact versions.
4. Publish `klaus-agent-core`.
5. Publish eval/protocol/sdk after the core version resolves.
6. Publish server last.

Every publishable local dependency carries both an exact version and a path;
the path is used in the workspace and the exact version is used by registry
consumers.

## MSRV truthfulness

The workspace declares `rust-version = "1.93"`, matching the Rust 1.93.0
toolchain pinned by the release pipeline. This is the validated release floor
for the current `v8 147.4.0` dependency set. Any lower MSRV claim requires a
separately scoped dependency/backport change and a complete CI run on that
lower toolchain; release metadata must not advertise an untested compiler.
