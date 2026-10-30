# klaus-agent-icu-data

Pinned full ICU 77.1 common data for klaus-agent's embedded V8 runtime. This
crate exists separately so the core crate remains below registry package-size
limits while preserving Node 20-compatible `Intl` behavior.

The crate embeds the little-endian `icudt77l.dat` archive compressed with XZ.
At runtime it validates the compressed SHA-256, enforces a bounded
decompression size, validates the raw length and SHA-256, and returns a
process-lifetime 16-byte-aligned slice suitable for
`v8::icu::set_common_data_77`.

- ICU release: 77.1
- Source asset: `icu4c-77_1-data-bin-l.zip`
- Source ZIP SHA-256: `0913674ff673c585f8bc08370916b6a6ccc30ffb6408a5c1bc3edbf5a687fd96`
- Raw length: `31,895,376` bytes
- Raw SHA-256: `310d9b2cb42947fad7f388b09d2ae574e2adeb60ca76abf280438908f20b2d7b`
- XZ length: `7,633,376` bytes
- XZ SHA-256: `fa1f6481428576503e4e0a132f491a0f7e63a17d9a28dcbd81369318e3d5b76c`
- Compression: XZ Utils 5.8.2, `xz -9e`

The `l` suffix denotes little-endian data. This crate deliberately fails to
compile for big-endian targets rather than passing incompatible bytes to V8.
Redistribution terms are included in `ICU-LICENSE.txt`.
