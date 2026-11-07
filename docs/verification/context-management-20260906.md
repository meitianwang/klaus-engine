# Context management verification

The original validation below used engine baseline `a7c85086b33b8ebfec2bb10f7e00b3310d394c50` and parent baseline `03042cdf7e361a23a9a1c67c9ae58a3b55f9d084`. This context-management change does not edit desktop source. Any subsequent integration validation is recorded in the parent repository's engine-pointer commit.

## Full engine gate

Run from the engine root with Node 20. No test-profile overrides were set for this authoritative run. The gate uses the repository's unchanged timeout and retry configuration.

```sh
PATH=$HOME/.nvm/versions/node/v20.19.4/bin:$PATH node scripts/gate.mjs --keep-going
```

Original output (process exit code: `0`):

```text
     Summary [4651.114s] 5322 tests run: 5322 passed (46 slow, 14 flaky), 6 skipped
== gate summary ==
ok       3.0s  fmt
ok       0.6s  bundled browser asset lock
ok       1.7s  distribution assets
ok      14.2s  script tests
ok      21.1s  clippy
ok    4715.2s  tests
ok       1.7s  target budget
```

Fourteen tests needed the existing automatic retry after an initial timeout or failure; all passed on retry. No timeout, retry, assertion, or gate rule was relaxed. The full local log is `/tmp/klaus-context-engine-default-gate.log`.

## Focused regressions

Run from the engine root. Test-only optimization reduces executable startup overhead on this machine; debug assertions and overflow checks remain enabled. No repository build settings were changed.

```sh
CARGO_PROFILE_TEST_OPT_LEVEL=1 CARGO_PROFILE_TEST_DEBUG=0 CARGO_PROFILE_TEST_DEBUG_ASSERTIONS=true CARGO_PROFILE_TEST_OVERFLOW_CHECKS=true CARGO_BUILD_JOBS=4 cargo nextest run --locked --workspace --all-features -j1 --no-fail-fast --retries 0 -E 'test(r20260905_) or test(r20260906_)'
```

Original output:

```text
     Summary [  46.009s] 20 tests run: 20 passed, 5308 skipped
```

## Desktop workspace

Run from the parent repository root:

```sh
cargo test --manifest-path apps/tauri/Cargo.toml --workspace -j4
```

Original nonempty test summaries:

```text
test result: ok. 10 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 1258 filtered out; finished in 0.85s
test result: ok. 1157 passed; 0 failed; 102 ignored; 0 measured; 0 filtered out; finished in 42.16s
```

The clean worktree needed bundled resource prerequisites. The speech helper was built, and existing local engine resource files were staged for these host tests and then removed. This is host test coverage, not validation of a newly packaged desktop application.

## Desktop command routing

```sh
npm run desktop:audit:command-map
```

Original output:

```text
{
  "invokeTotal": 372,
  "invokeRouted": 372,
  "pushTotal": 36,
  "pushRouted": 36,
  "shellOrphans": [],
  "shellPushOrphans": [],
  "uiPushOrphans": []
}
rows written to command-map.json: 408
```

## Change scope

Run from the parent repository root:

```sh
git status --short
git diff --name-only -- apps/tauri
```

Original output (the desktop diff command produced no output):

```text
 m engine/klaus-agent-rust
```

## PostCompact trigger completion and prototype retirement

The public Session regression reproduced the remaining source-matching defect
on engine baseline `f30a709e8671029d3f87ae90233bdf1b0abb7108` before production
changes. It exercises manual compaction, proactive compaction, overflow recovery,
and unsuccessful compaction with manual, auto, and wildcard hook matchers.

```sh
CARGO_BUILD_JOBS=4 cargo nextest run --locked --workspace --all-features -j4 --no-fail-fast --retries 0 -E 'test(r20260906_post_compact_hooks_match_trigger)'
```

Original output:

```text
     Summary [   0.157s] 4 tests run: 1 passed, 3 failed, 5340 skipped
```

All three successful-compaction cases selected `["auto", "manual", "wildcard"]`
instead of only the matching source plus the wildcard. The unsuccessful case
already correctly emitted no PostCompact hook.

The final implementation also incorporates the subsequently published engine
baseline `47ecdca83225b41c940edba73fc5bec6d65a6703`. Its existing message identity
and phase changes are preserved. An intermediate regression run on the older
baseline was deliberately interrupted to test the merged source instead; that
interrupted run is not a passing verification result. Only the new regression
fixture needed the added optional TextBlock fields.

The merged source passed formatting and the workspace lint gate:

```sh
cargo fmt --all -- --check
CARGO_BUILD_JOBS=4 cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
```

Formatting exited 0 with no output. Clippy exited 0 with no warnings; its original
completion line was:

```text
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 44.94s
```

The final merged-source regression command covers compaction, hook selection,
hook codecs, public Session behavior, and the existing compaction facade cases:

```sh
CARGO_BUILD_JOBS=4 cargo nextest run --locked --workspace --all-features -j4 --no-fail-fast --retries 0 -E 'test(compact) or test(hooks::bus::tests) or test(runtime_hooks_unit_codecs)'
```

Original output (exit 0):

```text
     Summary [  11.100s] 132 tests run: 132 passed, 5220 skipped
```

This is a scoped follow-up gate; the full-suite and desktop results above belong
to the earlier context-management implementation. No desktop source is changed
by the PostCompact follow-up.
