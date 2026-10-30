# Provider behavior contract

This document fixes the source and validation contract for the provider-behavior
baseline.

## Checked-in baseline

The authoritative reference is the checked-in baseline snapshot owned by this
repository:

- contract rows: `crates/core/tests/fixtures/provider_behavior/v1/baseline.contract.jsonl`
- manifest: `crates/core/tests/fixtures/provider_behavior/v1/manifest.json`
- scripted adaptations: `crates/core/tests/fixtures/provider_behavior/v1/rust-scripted-adaptations.json`
- live prelude boundaries: `crates/core/tests/fixtures/provider_behavior/v1/live-prelude-boundaries.json`

The snapshot identity is pinned:

- `baselineRevision` is the fixed 40-hex snapshot id recorded in the header row,
  in every contract row, and in the manifest.
- The contract bytes are pinned by `SOURCE_CONTRACT_SHA256` in
  `crates/core/src/testing/provider_behavior.rs`. Any edit to the snapshot must
  update that pin together with the sibling fixtures that embed
  `sourceContractSha256`.

## Snapshot layout

Line one of `baseline.contract.jsonl` is a header, and the 103 contract rows
follow it:

```json
{"schemaVersion":1,"baselineRevision":"<40-hex>","blobs":{"<sha256>":<value>}}
```

Each recorded request carries `{"$blob":"<sha256>"}` in place of its `system`
prompt and its rendered `tools` catalog, and every reader restores those values
from the header before decoding, canonicalizing or comparing anything. The two
fields are shared because they repeat by construction: 277 recorded requests
hold only 19 distinct system prompts and 32 distinct tool catalogs, which made
97.8 percent of the pre-header 10.9 MB snapshot byte-identical copies and left
one tool-description edit rewriting all 103 rows. Sharing changes no row
semantics, so the row schema version is unchanged, and a table key must equal
the SHA-256 of its value's canonical encoding.

The canonical form is pinned by the core unit test
`baseline_contract_snapshot_stays_canonically_shared`; re-record it with
`UPDATE_GOLDEN=1` when the snapshot is refreshed.

There is no external reference producer: the engine's own scripted runs are
validated against this snapshot, and updating the snapshot is an ordinary
reviewed change to this repository.

## Suite registry

The scripted registry contains:

- `provider-smoke` (11 tasks)
- `provider-core` (26 tasks)
- `provider-full` (34 tasks; 102 task/profile rows across `anthropic`,
  `openai`, and `generic`)
- `provider-qwen-boundary` (1 row)

All 103 task/profile keys are mandatory: the contract tests require the full set
and fail closed on a missing key.

## What the tests validate

`crates/core/tests/provider_behavior_trace.rs` replays `provider-full` and
`provider-qwen-boundary` through the live engine behind the `testing` feature,
and validates every resulting trace against the checked-in contract: request and
response envelopes, tool calls and results, engine events, history, side
effects, lifecycle ordering, and completion state.

Validation treats these as non-contract, so a trace may differ there:

- recursive JSON object-key ordering;
- exact temporary workspace paths and their memory slug;
- declared non-contract `durationMs` fields;
- exact host-derived `Platform`, `Shell`, and `OS Version` lines;
- a proven contiguous interval of parallel terminal events;
- ToolResult UUIDs owned by that proven interval.

Each of those must be declared per row in `normalization`, and a row whose
declarations do not match its content is rejected.

Tasks whose Rust surface intentionally differs from the pinned row are declared
in `rust-scripted-adaptations.json` and checked against that declaration instead
(currently `code-mode`, whose `exec`/`wait` pair replaces the recorded
`exec_wait`).

The current-plan tool transition is an explicit raw-source migration before
typed request decoding. `CURRENT_PLAN_SOURCE_CATALOGS` pins the complete SHA-256
and insertion position of all 12 historical catalogs exposing task tools. The
full predecessor definitions of `TaskCreate` and `TaskList` are checked, and
`CURRENT_PLAN_TARGET_SPECS` independently pins the complete runtime definitions
of `TaskCreate`, `TaskList`, and `TaskWrite`. Only the two descriptions are
replaced and `TaskWrite` is inserted at the pinned position. Unrecognized source
descriptions, schemas, extra fields, tool membership, or order fail closed; the
normal per-tool and trusted-catalog checks still run afterward. This migration
does not edit or re-sign the checked-in snapshot.

`TaskWrite` has separate exact target hashes for generic rendering and for
Anthropic/OpenAI rendering, which removes the schema's `default` keyword. The
request's provider profile selects its one permitted target; the two shapes
are not interchangeable.

The same transition changes the `follow-through` system fragment from task
appends to explicit plan replacement. Its complete old and new raw definitions
(including cache fields) each have a fixed SHA-256. Migration requires exactly
one exact predecessor and an independently verified runtime target, then replaces
only that fragment before decoding. All other system metadata comparisons remain
unchanged; altered guidance, duplicate fragments, and unknown live fragments are
still rejected. The signed source snapshot remains unchanged.

There is deliberately no gate asserting that a fresh run equals a pinned row
byte for byte. `baselineRevision` records the revision a row was captured at;
such a gate would measure agreement with that capture rather than correctness.
