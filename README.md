# Klaus Engine

The Rust agent engine behind [Klaus](https://klaus-ai.cn). It runs model turns, tools, sub-agents, planning and goals, permissions and sandboxing, and keeps the session record. The [Klaus desktop client](https://github.com/meitianwang/klaus-open-source) mounts this repository as a submodule and drives it as a child process.

## Layout

- `crates/core` — sessions, turns, providers, tools, context management, sandbox, storage.
- `crates/protocol` — the internal operation and event types.
- `crates/server` — the app-server JSON-RPC facade (`session/*`, `engine/*`), the one surface every client uses, plus the stdio, HTTP and WebSocket transports.
- `crates/cli` — the `klaus` command-line client, itself a client of the facade.
- `crates/sdk` — browser runtime host and embedding helpers.
- `crates/sandbox-*`, `crates/ripgrep-*`, `crates/icu-data` — platform helpers and pinned runtime assets, each with its own license file.
- `plugins/` — plugins bundled with the engine.
- `docs/` — design notes; `docs/host-integration-seams.md` describes how clients integrate.

## Build

Rust is pinned in `rust-toolchain.toml`.

```sh
cargo build --release -p klaus-agent-server --bin klaus-agent-stdio   # the engine child the desktop client runs
cargo build --release -p klaus-agent-cli                             # the `klaus` CLI
cargo nextest run --all-features -p klaus-agent-core                 # tests (cargo-nextest)
cargo clippy --all-targets --all-features -- -D warnings
```

`node scripts/gate.mjs` runs the full verification gate.

Binary admission for the Windows browser helper and native host stays closed in source builds: no distribution issuer is configured, so those artifacts are refused rather than trusted. See `docs/source-only-browser-distribution.md`.

## Contributing

Every capability a client needs is a named facade method first; there is no generic op passthrough. `AGENTS.md` records the engineering rules for this repository.

## License

MIT for this project's own code (see [LICENSE](LICENSE)). Vendored assets and adapted prompt material keep their own licenses; [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md) lists them and where their license files are kept.
