# Browser assets in a source-only distribution

This tree contains the browser runtime's implementation, interfaces, build
recipes and the locked identities of its public dependencies. It does not
contain an approved binary issuer, a verified distribution receipt, or signed
Windows browser bridge artifacts. Every place that would need one of those
records the requirement explicitly and admits nothing until it is supplied by
an independently reviewed distribution step.

## What is pinned

- The twelve npm ripgrep distributions and the ICU data keep their versions, npm
  URLs, SHA-512 integrity values, raw and compressed SHA-256 identities and
  redistribution notices (see `provider-asset-distribution.md`).
- The SDK embeds the exact browser runtime lock
  `crates/sdk/assets/exact-browser-runtime-assets-v1/exact-browser-runtime-assets.lock.json`
  (`published: true`) and keeps its candidate
  `crates/sdk/tests/fixtures/exact-browser-runtime-assets-v1/exact-browser-runtime-assets.lock.json`
  (`published: false`). Both are canonical ASCII JSON plus a final LF, and carry
  domain-separated candidate and published digests.
- The lock binds the Playwright packages, the process patch, the source and
  patched graphs, the public browser archive identities (download URL, optional
  public mirror, size, SHA-256), and the Windows addon source closure.

`published: true` means the metadata is part of the SDK asset tree. It does not
certify any external input. A recorded download URL does not mean an executable
has been downloaded or admitted.

## The digest chain

The published lock's length and SHA-256 and its candidate/published digests are
recorded in `provider-assets.lock.json` (`browser.runtimeLock`) and pinned as
constants in `crates/sdk/src/browser_host/winldd_distribution.rs`. The addon
source-closure digest (over `Cargo.lock`, `Cargo.toml`,
`crates/browser-pipe-napi/{Cargo.toml,src/lib.rs}` and
`rust-1.93-std-source.lock.json`) is repeated in each Windows addon requirement.

`scripts/browser-source-distribution.mjs` maintains this chain and nothing else:

```sh
node scripts/browser-source-distribution.mjs --check   # verify every link
node scripts/browser-source-distribution.mjs --write   # reseal after an intentional change
```

`--write` recomputes the source closure, both digests, both lock files, the
provider lock record and the Rust pins together. It never downloads, builds,
signs or approves a binary, and it refuses a lock that names an issuer or claims
a verified artifact. `scripts/check-distribution-packages.mjs` runs the same
check as part of the distribution asset gate.

## Intentionally unavailable operations

- Admitting the Windows `PrintDeps.exe` helper through the broker's distribution
  path.
- Installing a native-messaging host from a distribution manifest. That needs a
  configured producer and manifest pins on every platform, plus a Windows signer
  pin on Windows.

`crates/sdk/src/browser_host/distribution_source.rs` holds the issuer
configuration: the workload identity and the native-host producer. Both are
`None` in a source distribution, so those admission paths reject. Nothing at
runtime — environment variables, files beside the executable, or self-reported
receipt fields — can supply them. A future signed binary distribution needs an
independently reviewed issuer and verifier, verified approval evidence, final
signed artifact identities and fixed trust anchors.

Other platforms and features are unaffected. Building from source is not a
signed binary release.

## Tests

Unit tests exercise successful admission with explicit synthetic identities so
the checks themselves stay covered; they do not validate Windows Authenticode or
prove that a Windows release exists. Focused checks from the engine root:

```sh
node --test scripts/browser-source-distribution.test.mjs scripts/check-distribution-packages.test.mjs
node scripts/check-distribution-packages.mjs --skip-package
cargo nextest run --locked --all-features -p klaus-agent-sdk --lib -E 'test(browser_host::)'
```
