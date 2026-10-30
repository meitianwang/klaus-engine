# klaus-agent ripgrep asset

This package contains one pinned, target-specific ripgrep executable used by
`klaus-agent-core`. The executable comes from the matching
`@vscode/ripgrep-linux-arm@1.18.0` npm package and is compressed with XZ.

The npm package is only the provenance of the bytes: the XZ payload is vendored
into this crate as `data/rg.xz` and embedded into the Rust binary at compile
time via `include_bytes!`, so nothing depends on npm and nothing is downloaded
at build or run time.

The core crate selects the asset only for the matching Rust target, performs a
bounded decompression, verifies the raw length and SHA-256, and materializes it
in a private process directory. Consumers should not use this crate directly.

`provider-assets.lock.json` in the klaus-engine source repository records
the npm SRI, tarball, raw identity, compressed identity, and exact TS reference
commit. Microsoft and ripgrep redistribution terms are included with this
package.
