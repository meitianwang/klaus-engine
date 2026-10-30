# Context compaction recovery

This document describes how the normal context-management and overflow-recovery
paths decide when to compact, what they keep, and how they recover when the
context refills too quickly.

## Failure evidence

Recent local session records contain this `turn_lifecycle` failure:

```
History compaction is thrashing: the context refilled to the limit within 3 turns of compacting, 3 times in a row. A single file or tool result is likely too large for the context window — read large files in smaller ranges, or start a fresh session.
```

One failing transcript retained 475 messages, with 35 messages since its last
user prompt. Its recent compaction records had empty summaries and retained all
message slots: they were tool-result clears, not full summaries. The final usage
was 194381 input + 47232 cached input + 782 output = 242395 tokens. The earlier
full-request estimate fix is already in the baseline; this change addresses the
remaining independent defects.

## Preserved contracts and deliberate changes

- Keep the existing trigger: effective context minus an output reservation of
  at most 20000 tokens, minus a 13000-token compaction buffer. Keep authoritative
  provider usage as the anchor and include system/tool prefix estimates when no
  anchor exists. Do not subtract snip savings twice after invalidating the anchor.
- Group at the start of a model response, preserving tool calls and their results.
  A single human request can contain many compactable API rounds. Retained tail
  size cannot override its budget by walking back to the human request.
- Try a summary with the actual current request's model, system, tools, thinking,
  and cache parameters. Append the summary instruction and execute no tool calls.
  If it fails, use an independent request on the same model with thinking disabled,
  no tools, media references instead of binary payloads, and at most 20000 output
  tokens (also bounded by the model's resolved output limit).
- On summary overflow, retry smaller complete groups at most three times.
  Reactive recovery first retains the newest complete group and summarizes the
  older prefix; further overflow moves recent groups out of the summary input,
  keeping them verbatim. A reported token gap cannot exhaust every candidate in
  one step. Exhausted reactive retries keep the original history instead of
  entering the proactive path that drops oldest groups.
- Commit a replacement only after a complete text summary. Empty output,
  interrupted streams, provider errors, tool calls, and cancellation retain the
  original history. Invalidate old usage before persistence so resuming a compacted
  session does not restore the pre-compaction token count.
- Count only full summaries toward the rapid-refill breaker. Keep that breaker;
  successful microcompaction is housekeeping, not a full-summary generation.
  Three failed proactive attempts suppress further auto attempts while leaving
  one reactive attempt available per consecutive provider-overflow episode.
- Freeze the prepared request and cancellation signal per operation. Manual
  compaction starts fresh and uses the currently selected model and its resolved
  limits, including model changes made between turns. Speculative summaries
  require an unchanged source prefix and request configuration and must still
  reduce the current history after adding the new tail.
- Keep file restoration, file ledgers, hooks, protected context, running agents,
  surfaced tools, memory, plans, and task continuation. Bound file restoration by
  the remaining request budget and preserve its position before the retained tail.
- Tool output is recoverable from disk when reduced for the model. Preserve
  structured UI output and persisted replacement decisions. Keep Read and media
  exclusions and use the existing 200000-character aggregate budget. Single-result limits
  default to 50000 characters, with smaller tool limits still honored.

No frontend, desktop interaction, settings, or app-server method shape is changed.
Optional experimental reference-engine paths are not enabled by this change.

## Regression evidence

Before production edits, run from the engine root:

```
CARGO_BUILD_JOBS=4 cargo nextest run --locked -p klaus-agent-core --test all -E 'test(r20260906_compaction_preserves_progress)'
```

Original result:

```
Summary [0.087s] 4 tests run: 0 passed, 4 failed, 167 skipped
```

The cases reproduce an unbounded single-user tool loop, failed-summary history
replacement, a 128000-token summary output reservation, and orphan tool results
in a summary-overflow retry. Additional public Session regressions cover micro
versus full tracking, reactive recovery after auto failures, snip accounting,
manual model changes, lossless parallel tool output storage, and structured
provider overflow errors. Qoder bridge tests pin the summary output budget and
explicit thinking override through the existing capability field. Tests use synthetic messages and deterministic providers;
no user's transcript or provider credentials are included.

The final gate commands and their original results are recorded in
[the verification record](verification/context-management-20260906.md).

## Legacy prototype retirement

The uncommitted prototype in `fix+compaction-recovery-0.3.11` was audited before
retiring that worktree. Its completed-task tail reduction, failed-summary history
preservation, complete tool-call grouping, selected-model compaction, and usage
anchor invalidation are covered by the current implementation and public
regressions. The remaining decisions are:

- Complete `PostCompact` source metadata and matching for manual, proactive,
  and overflow recovery. Reuse the existing `trigger` field and apply the same
  source matcher as `PreCompact`; a failed compaction emits no completion hook.
- Keep independent proactive and reactive recovery. Routing both through one
  failure breaker would reintroduce blocked overflow recovery. Their retained
  histories need not be identical.
- Keep budgeted file restoration. A tool-call ledger path does not prove that
  the retained tail contains a successful, complete, current copy of that file,
  so it must not by itself exclude the file from restoration.
- Keep the dedicated standalone summary system instruction and strict EndTurn
  validation. Repeating the instruction as a final user message is not a missing
  capability; accepting an interrupted response as a summary would lose history.

No legacy compaction implementation needs to remain as a second development
baseline. Desktop interaction and UI code are outside these changes.
