# Goal controls

Clients use the app-server named facade for goal state and mutations. The engine
owns goal execution, token accounting, time accounting, and automatic admission.

- `session/goal` reads `{sessionId?, threadId?}` without creating an inference
  session. At least one identity is required; both must identify the same thread.
- `session/setGoal` accepts `objective?`, `status?`, `tokenBudget?`, and the
  conditional-write fields below. Unloaded saved threads support pause only;
  creating, editing, resuming, or changing a budget requires loading the thread.
- `session/clearGoal` deletes the current state. An unconditional human clear
  also records revocation when no goal row exists.

Every response contains `goalRevision`, the persisted thread revision. A current
snapshot has `threadGoal`; a successful clear has `goalCleared:true` when the
latest snapshot is empty. Goal change notifications carry the same revision,
including a missing `threadGoal` after clear. Clients ignore older revisions.
A mutation response is published before its corresponding notification.

`tokenBudget` has three distinct forms: omission preserves the existing budget,
`null` adopts the current configured maximum (or removes the ceiling when no
maximum exists), and a positive safe integer sets it. Objective limits count
Unicode code points. The maximum inline objective is 4000 code points.

`session/updateConfig` accepts `maximumGoalTokenBudget`: omission leaves the
maximum unchanged, `null` removes it, and a positive safe integer sets it.
Changing this setting preserves an existing goal budget until an explicit
`tokenBudget:null` adopts the new maximum. Its acknowledgement follows the
applied configuration. Native storage retains full signed integers; JSON goal
results reject values outside the exact safe-integer range without rounding.

## Conditional writes and retries

`expectedGoalId` on set is optional: omission preserves compatibility with an
unconditional mutation, `null` asserts that no goal exists, and a string asserts
the current generation. Clear accepts an omitted guard or a generation string.
Edits retain the generation, so clients also send `expectedGoalRevision` when
editing a previously observed snapshot. It is a nonnegative safe integer.
The store checks generation and revision in the same transaction as the write.

A definitive conditional-write rejection returns JSON-RPC `error.data` with
`{"code":"GOAL_CONFLICT","feature":"goal"}`. The internal protocol carries the
same value in `errorDetail`. A client refreshes the snapshot before confirming a
new intent. Other errors do not imply that the mutation was rejected.

An explicit composer action sets `submit:true`, `origin:"user"`, a nonempty
`clientRequestId`, and `objective`. The engine records one genuine human
objective message and admits work through the ordinary foreground queue.
`displayText` can provide the original human presentation when the objective is
a compact reference to owned content. A goal editor omits `submit` and does not
create a visible human message.

The submit receipt commits with the goal mutation. Retrying the same request ID
and original mutation returns its original snapshot and revision, even if the
current state has since advanced or been cleared. Receipt lookup precedes
current-state guards. A retry does not add a second visible objective, resurrect
a goal, or admit work again. Reusing a request ID for different fields fails.
Clients preserve every original mutation field until the outcome is known.

`goalAdmission` describes accepted execution disposition: `scheduled`, `steered`,
or `deferred`. It is informational. Clients never execute a continuation prompt
or create a separate continuation scheduler. Queued ordinary user work takes
priority over a new automatic goal turn.

## Durable context and history

Only an explicit trusted human control sends `origin:"user"`. Missing origin and
`origin:"automatic"` do not create human objective or status evidence. A
budget-only mutation does not invent objective or status authorization. The
engine durably flushes typed instruction evidence before mutating goal state;
text markers and tool output do not acquire this provenance. Compaction and
resume preserve the trusted instruction identities and ordering. The retained
objective limit is 700 UTF-8 bytes of the encoded JSON string value alone.
Larger objectives are omitted whole from this bounded evidence, with explicit
omission metadata. The durable goal and ordinary followup context retain the
full escaped objective, current status, and budget, including stopped goals.

Automatic clear requires `origin:"automatic"` and the matching completed goal
identity. Before clearing, the engine flushes a complete terminal goal snapshot.
`session/history` and `engine/readThreadHistory` expose it in `goalCompletions`
with `goal`, `goalRevision`, and an optional `sourceTurnId`. Automatic cleanup
never records a human revocation.

## Inheritance and eligibility

`session/forkLive` and the persistent fork facade accept `inheritGoal:true`.
Omission leaves the target isolated. Explicit inheritance flushes source
progress, captures one immutable goal snapshot, and copies that snapshot into
the target with execution deferred. A later source edit or clear cannot alter a
fork already being initialized. No source run ownership is inherited. The first
explicit target turn can adopt the goal. Ephemeral forks reject inheritance.

Persistent goal execution is available only to ordinary persistent root sessions.
Side tasks and review sessions cannot own a persistent goal. Nonreview ephemeral
sessions may display goal tool specifications, but execution reports that the
session is ineligible; it never creates goal storage implicitly.

## Interruption ownership

`turn/interrupt` accepts `sessionId`, optional correlation `submissionId`,
optional `origin`, and optional `expectedTurnId`. The expected turn is the
foreground facade submission ID, distinct from the interrupt correlation ID.
The engine validates its reservation before any goal pause and again before
cancelling. A delayed pause cannot commit for a replaced or cancelled reservation.

- `user-stop` is the default. It suppresses continuation, retains the genuine
  user pause attempt, and attempts the durable pause before interrupting.
  The flushed attempt grants retained authorization only when its exact SQLite
  receipt committed. Applied and rejected dispositions remain distinct; resume
  resolves a missing disposition from that immutable receipt without inferring
  intent from current goal state. Cancellation proceeds when the critical 500 ms
  pause budget expires; a late writer loses its reservation authority.
- `system` awaits the ordinary shutdown pause and records no user instruction.
  Pause failure is reported and cancellation still proceeds.
- `targeted-turn` requires `expectedTurnId` and leaves goal status and the user
  Stop latch untouched. It cancels only that reservation signal and preserves
  unrelated background work and queued reservations.

Every interrupt retains its captured reservation even when the caller omits a
fence. A later human reservation invalidates that captured authority. A pending
user or system Stop temporarily blocks automatic admission while ordinary queued
human work remains eligible. The Stop latch is applied only after the final
ownership check. Lazy endpoints pause saved active goals through the durable
writer without constructing a Session or resolving a provider.

The immediate JSON-RPC result remains `SubmissionAck`. A correlated final
protocol envelope carries `turnInterrupt` with `accepted`, optional
`interruptedTurnId`, and optional `goalPauseError`. A stale target reports
`accepted:false` and an explicit error. A pause failure can accompany
`accepted:true` because execution cancellation still succeeded. Clients install
the correlated listener before submitting and treat the final result as the
semantic outcome.

## Provider-free ownership reads

`session/goal` also reports `goalPursuitEligible`, independently of the feature
being enabled. A lazy endpoint classifies its actual resolved root configuration
without constructing a Session or provider. Saved reads use the engine's
persisted ownership marker. Unknown ownership reports `false` and
`goalPursuitEligibilityNotice`; it never infers permission from a saved goal.
Task resource scopes retain legacy goal read, pause, clear, and Stop controls,
while objective, budget, and resume writes fail before authorization is appended.

`session/readGoalObjective` takes the same thread address and a required
`expectedGoalId`. It returns the current goal ID and complete objective text
without provider activation. Owned file references are resolved only through
the current durable goal manifest; callers cannot provide arbitrary paths.
