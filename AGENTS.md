## Client Integration Surface

The engine has ONE outward surface: the app-server JSON-RPC facade in
`crates/server/src/app_server/`. Every client goes through it, including the CLI
that ships in this repo. A client is anything that drives a session it did not
itself create the engine for.

The raw Op/Event protocol (`OpKind`, `ProtocolEvent`, `ProtocolClient`) is the
facade's INTERNAL wiring, not an integration surface. The prohibition is on the
SUBMITTING side specifically: no client may build an op and push it in. Reading
the inner protocol is not the same permission — the facade's replies and events
are protocol-crate types, so a client decodes `ProtocolEvent` and the result
models as a matter of course, and the CLI depends on `klaus-agent-protocol` for
exactly that.

So: naming `OpKind` or holding a `ProtocolClient` is the line. Three kinds of
code may cross it:

1. The facade itself, which is what translates a named method into an op.
2. The transports that carry the inner protocol (`serve`, `connect`, the stdio
   and WebSocket shims).
3. Nothing else. In particular the serving modes (`klaus proto`, `klaus serve`,
   `klaus app-server`) are NOT an exemption: they hand over a byte stream and
   step out, so the ops on it belong to the caller, and `crates/cli/src` names
   neither type anywhere.

Why: two clients on two surfaces means every capability has to be added twice and
drifts once. The 4 MCP request ops proved the failure mode — they existed in the
protocol with a working handler, no client could reach them because the facade
never named them, and a dispatcher misroute that panicked the connection sat
there undetected until the CLI became the first caller.

Adding a capability therefore means adding a named facade method FIRST, and the
order is not a style preference. A row in `crates/server/src/app_server/methods.rs`
is what binds the method name to its params and result types; until it exists
there is nothing for a client to call and nothing for the schema bundle to
publish. Four things then follow from that one row, three of them automatically:

- `ALL_METHODS` gains it, so the published schema bundle describes it.
- `app_server::tests::every_declared_method_is_dispatched` fails until
  `service.rs` has an arm for it, because a declared method with no arm compiles
  on both sides and then returns `METHOD_NOT_FOUND`.
- `crates/cli/tests/facade_coverage.rs` fails until the CLI's table says how
  `klaus` reaches it, or states in writing why it deliberately does not.
- Only the client call itself is hand-written.

Never let a client assemble an op payload of its own.

The generic `session/op` / `engine/op` passthrough was retired by promoting each
op to a named method. Do not resurrect it in any form — not behind a debug flag,
not "temporarily", not "internal callers only". A passthrough is what makes the
facade's method table stop being the contract.

This is enforced, not remembered:

```bash
# The declared table is the CLI's only door, and it reaches all of it.
cargo nextest run -p klaus-agent-cli facade_coverage
# Every declared method is published and dispatched.
cargo nextest run -p klaus-agent-server app_server::
```

## Test Gate

Choose validation based on the changed behavior and its impact. Do not require
the full gate for every change:

- For a localized change with clear boundaries, run the defect regression tests,
  tests for affected callers or contracts, and relevant format, type, build,
  lint, or asset checks. Documentation-only changes need a diff review.
- Run the full gate when a change affects widely shared runtime behavior,
  cross-crate contracts, workspace dependencies or build configuration, or when
  targeted checks cannot establish the impact. Expand validation when failures
  or unresolved concerns justify it.
- Record why the chosen checks cover the change, together with their commands
  and original output. Once sufficient checks pass, proceed to completion; do
  not broaden or repeat testing without a concrete reason.

The full gate has one entry point:

```bash
node scripts/gate.mjs
```

It runs fmt, the bundled browser asset lock, the distribution asset check, the
build-output budget, the script tests, clippy with `-D warnings`, and the test
suite, cheapest first, and prints one summary. `--keep-going` runs every step
instead of stopping at the first failure. That file also records which scripts are
deliberately outside the gate and why. Do not re-add a check by leaving it to
memory: the previous arrangement had every check listed only in a pipeline
template, and after that template went away `target-budget` stopped running until
build output reached 91.9 GiB.

Tests run through nextest, not `cargo test`:

```bash
cargo nextest run --locked --workspace --all-features
```

Every test from every binary runs out of one pool instead of one binary at a
time, and `.config/nextest.toml` gives each test a 30 second SLOW mark and a 60
second kill. A test that needs longer must be registered there as an override
with the measurement that justifies it; that file is where "this test is slow"
becomes a reviewed decision instead of something nobody notices. Per-test
durations land in `<target>/nextest/default/junit.xml`. A green run is about
170 seconds for 4874 tests on a quiet 12 core machine; if it takes many minutes,
check what else is competing for CPU before suspecting the suite. On a managed
machine, endpoint-security (EDR) software is the usual suspect: its file-event
capture process alone can pin a core and slow the suite several times over. Such
agents are often protected against `kill` even as root and restart on reboot; if
your environment allows it, pause them through their own service controls, and
otherwise keep the build and test concurrency capped (the gate already caps it
at 4 for this reason).

Integration tests are aggregate targets, not one target per file:
`crates/core/tests/all.rs`, `crates/core/tests/all_testing.rs` (needs the
`testing` feature) and each other crate's `tests/all.rs` mount their files with
`#[path]`. `autotests = false` keeps a new file from silently becoming its own
target: add it to the matching aggregate instead.

## Defect Regression Tests

Every defect fix must land together with a pinned regression test:

1. Location: one file per defect under `crates/core/tests/regressions/`, mounted from `crates/core/tests/regressions.rs` via `#[path]`, which `crates/core/tests/all.rs` mounts in turn, so the suite stays part of one integration-test target.
2. File name: `r<id>_<short-slug>.rs`, where `<id>` is the tracker work-item id when one exists, else the short hash of the fixing commit.
3. The test exercises the PUBLIC engine surface that exposed the defect and must fail on the pre-fix engine.
4. The file header records what broke, the one-line root cause, and the fixing commit / work item, so a future failure traces straight back to its origin.

## Goal Workflow

When this project is in Goal mode and `docs/goal.md` exists:

1. Before doing active work, read this section, `docs/goal.md`, and `docs/goal-progress.md`.
2. Follow only the objective, scope, acceptance criteria, validation plan, and stop conditions in `docs/goal.md`.
3. Before ending each active work turn, update `docs/goal-progress.md` with completed work, validation evidence, risks/blockers, and the next step.
4. Keep `docs/goal.md` and `docs/goal-progress.md` during all active work turns.
5. If a Stop hook asks for a `<!-- codex-goal-turn: ... -->` marker, add the exact marker to `docs/goal-progress.md` before stopping again.
6. If work drifts from `docs/goal.md`, stop expanding scope and realign before continuing.
7. Only in the final completion turn, after all acceptance criteria are verified and final evidence is captured in the final response, delete `docs/goal.md` and `docs/goal-progress.md`.
8. Mark the Goal complete only after the workflow files are cleaned up.
