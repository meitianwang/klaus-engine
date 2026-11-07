# Provider behavior baseline runbook

This runbook is the operating contract for the provider-behavior baseline. Run
every command from the Rust repository root.

The pinned baseline snapshot is:

```text
baselineRevision 437d9d5fe669360becf7c1e08d4464af2c174ab0
contract         crates/core/tests/fixtures/provider_behavior/v1/baseline.contract.jsonl
```

The engine's own scripted runs are the only producer. See
`docs/provider-behavior-contract.md` for the full contract.

## What checks it

`crates/core/tests/provider_behavior_trace.rs`, behind the non-default `testing`
feature. It replays the scripted suites through the live engine and validates
every resulting trace against the checked-in contract: request shape, tool
catalog, event ordering, parallel declarations, and the 103 pinned rows.

There is deliberately no gate asserting that a fresh run equals the pinned rows
byte for byte, and re-recording the snapshot from this engine does not create one
either. The snapshot's ingress validation checks that each row is internally
consistent with the manifest's scripted stream and the current typed contracts.
An intentional behavior change therefore refreshes the affected rows, their
canonical shared form, and every pinned hash together; updating only the rows is
an incomplete refresh and is rejected.

The tool catalog has its own golden
(`default_tool_list_and_model_visible_specs_match_the_golden_baseline`, fixture
`crates/core/src/tools/builtins/fixtures/tool_schema_trace.json`, re-record with
`UPDATE_GOLDEN=1`). The system prompt has none on purpose: it is assembled from
constants this repository owns, and the app that consumes the engine surfaces
behavior problems directly, so a second place to re-record every deliberate
prompt edit costs more than it catches.

## Test execution

The gate runs through nextest (`.config/nextest.toml`), not `cargo test`: every
test from every binary runs out of one pool instead of one binary at a time, and
each test has a wall-clock budget of 30 seconds with a kill at 60. Tests that
legitimately need longer are registered as overrides in that file, each with the
measurement that justifies it.

```bash
cargo nextest run --locked --workspace --all-features
```

Per-test durations land in `<target>/nextest/default/junit.xml`. That file is the
only record of what is slow; use it to narrow the overrides rather than widening
them. A full green run takes about 170 seconds on a quiet 12 core machine, and
several times that while something else is competing for CPU (on a managed
machine that is typically endpoint-security software; see the AGENTS.md Test
Gate section).

Sharding is available and verified, for when the gate moves to a machine pool:
`cargo nextest archive --locked --workspace --all-features --archive-file <file>`
builds once, and `cargo nextest run --archive-file <file> --workspace-remap .
--partition hash:4/4` runs one quarter of the suite without rebuilding anything.

## Refreshing the baseline

The baseline snapshot is owned by this repository. To refresh it after an
intentional engine behavior change:

1. Update the affected contract rows (or regenerate them from scripted runs).
2. Put the snapshot back into canonical shared form:
   `UPDATE_GOLDEN=1 cargo test -p klaus-agent-core --features testing --lib
   baseline_contract_snapshot_stays_canonically_shared`. Line one is the shared
   value header and each request references its `system` and `tools` values from
   it, so a hand-edited row is only complete after this step.
3. Recompute the pinned hashes: `SOURCE_CONTRACT_SHA256` and
   `LIVE_PRELUDE_BOUNDARIES_SHA256` in
   `crates/core/src/testing/provider_behavior.rs`, and every
   `sourceContractSha256` consumer, including `rust-scripted-adaptations.json`,
   `live-prelude-boundaries.json`, and the descriptor/request projection fixtures
   under `crates/core/tests/fixtures/prompt_policy/`. Refresh their whole-fixture
   pins together. If scripted inputs or assertions changed, also refresh the
   manifest byte pins and the affected task hashes in both sibling registries.
4. Record and reconstruct catalogs with an explicit no-plugin catalog thunk
   (`plugin_catalog: Some(Arc::new(|| None))`). An ambient
   `KLAUS_PLUGIN_CATALOG_DIR` must never contribute tools to these fixtures.
5. Validate ingress from a cold canonical-contract cache. The derived cache lives
   beside the test executable under `klaus-test-cache/`; remove only the entries
   for the refreshed source hash before the focused nextest run. A warm cache
   does not establish that raw source catalogs and causal seeds still validate.
6. Land the fixture and pin updates in one reviewed change with the behavior
   change that motivated them.

Task calls use `fork_turns` on every provider surface. Omission and `all` inherit
full history; `none` starts a fresh child; positive integer strings preserve the
full conversation for compatibility. The historical `config.fork` switch, an
explicit role, and the inherited fork marker do not select or truncate history.
The scripted matcher binds full forks by their exact latest directive envelope,
since they retain the parent's cache key. The source-row refresh retains the
historical descriptor and directive ingress fingerprints; the checked migrations
still authenticate those predecessors before adopting current schemas and fork
boilerplate. Reconstruct each inherited prefix from its owning Task occurrence,
close every unresolved call with a distinct placeholder UUID, and preserve the
signed UUIDs when replacing only a pinned directive body.
