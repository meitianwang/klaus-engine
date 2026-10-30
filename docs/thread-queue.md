# Durable future-turn input

The app-server facade owns the next-turn queue. Its storage is
`thread-queue.sqlite3` beside the endpoint's user configuration file. This queue
is separate from the current turn's pending steering input. A future-turn entry
never drains inside the current model loop.

The public methods are `thread/queue/list`, `add`, `update`, `delete`, `reorder`,
`start`, `pause`, `resume`, `import`, `clear`, and `discard`. Their request and result
schemas are generated from the method table. Mutations publish
`thread/queue/changed` with `threadId` and the committed `revision`. An endpoint
also watches SQLite `data_version` every ten seconds to discover changes made by
other connections or processes. The watcher ends with its owning connection.

A thread ID is the live session ID. Queue reads and edits work without a live
session and do not enter the turn serial chain. IDs are SQL parameters and are
hashed for advisory-lock filenames; no ID becomes a filesystem path. Runtime
input and client metadata use the exact runtime JSON codec. Prepared image,
document, and media bytes remain in the queue independently of their source
files. The engine does not read attachment paths while adding or dispatching an
entry.

## Persistence and mutation rules

SQLite stores thread revisions, ordered current entries, prepared payloads,
permanent compact identities, temporary removal proofs, and migration records
in separate tables. Lists query the current page; mutations update changed
entries and indexed identities without decoding historical receipts. A valid
v1 document migrates in one transaction; a malformed document is retained and
reported as an error. A thread-scoped advisory lock coordinates queue mutations through
admission, including multiple endpoint connections. Read-only lists bypass that
lock. Database files and lock files are created with private permissions on Unix.

`expectedRevision` provides optional compare-and-set control. A mismatch returns
JSON-RPC error `-32014` with `data.expectedRevision` and `data.revision`. A list
cursor binds its offset to a revision, so a concurrent edit cannot silently skip
or repeat entries. Reorder requires each current entry exactly once. Add can
insert before or after a current backend entry ID.

`clientUserMessageId` is durable idempotency identity. Its compact birth-input
hash survives later edits and consumption. Repeating the original add returns
the current entry or a compact consumed receipt with `queuedSubmission: null`;
reusing that identity for different native input is rejected. No permanent
identity retains input or UI payloads. An
add with `lookupOnly: true` reads the durable receipt without creating an entry,
preparing attachments, acquiring the mutation lock, or publishing a change.
It returns `found: false` when no matching identity exists. This also recovers a
lost add acknowledgement after consumed input's source file has disappeared. An
explicit restore requires `restore: true`, `requireRemovedReceipt: true`, and a
confirmed deletion receipt. Restore validates the original item, creation time,
and pause state, then reuses the original prepared input and full metadata,
advancing the saved entry version.
The interrupt banner may redact its pause reason from a removed client row;
that one omission is accepted and the durable interrupt pause is restored.
Consumed or unconfirmed input cannot be restored. Repeating a confirmed delete
with its mutation ID and matching fingerprint accepts either its original
backend ID or original client-message ID, even after a reconnect. Ordinary
deletes address a current backend ID.

Removal proofs are bounded across the profile by 512 proofs, seven days and
16 MiB of serialized UI removal-proof data. Prepared native input is excluded
from that charge: pending entries and removal proofs share its stored reference.
Consumption and final-proof eviction release unreferenced payloads. Temporary
physical storage can exceed 16 MiB. A single deletion whose UI proof exceeds
16 MiB is rejected atomically. A deletion replay reads its own historical proof,
never a restored or edited current entry; an evicted proof yields only its
permanent compact receipt.
Expired proofs immediately stop authorizing Undo. Physical proof and payload
cleanup occurs during the next committed queue mutation.

Normal new adds and restores allow at most 128 current entries per thread,
64 MiB of serialized UI items in total, and 8 MiB per UI item. Only
`clientMetadata.item` is charged; prepared native bytes are excluded. Native
producers without that UI field retain the 128-entry limit without a UI-byte
charge. Existing rows accepted through migration or import are not rejected by
these admission limits.

`list`, `add`, `update`, `delete`, `start`, and `clear` accept `includeInput`
(default `true`). Setting it to `false` omits `input`, adds `inputOmitted: true`,
and retains the full metadata, pause, dispatch and creation fields. Every entry
includes `inputFingerprint` (`sha256:<hex>`) and `rowVersion`. Metadata reads do
not load prepared bytes. A full result retains `input: []` for an empty input.
Native edits change the fingerprint; entry changes advance the version.

`clear` removes all current entries atomically and returns `cleared` with the
committed revision. It creates no Undo proofs and has no removal-proof byte
limit. Compact cleared identities prevent an old add replay from reviving a
cleared entry. Permanent identities and migrations remain until `discard` or
the owning session is deleted.

`import` atomically transfers a legacy snapshot and addition/removal receipts.
A repeated migration ID is idempotent, and `minimumRevision` preserves monotonic
client snapshots. Imported unconfirmed dispatches remain paused. Legacy removal
proof preserves the original rich item; a restored legacy row without prepared
bytes is marked `clientMetadata.legacyUnprepared` for the host's first explicit
preparation. Imported mutation IDs require the original fingerprint on replay.

`pause` and `resume` accept selected backend IDs or the whole queue. A reason on
resume clears only the matching reason. Resume never clears unconfirmed
admission. A metadata-only update can omit `input`; an explicit null
`pausedReason` clears that field on an editable entry.

## Admission and recovery

The host prepares turn options and explicitly requests `start`. The engine does
not automatically drain future turns. Default start uses the same atomic
idle-only foreground admission gate as `turn/submit`; it does not observe busy
state and then submit an unconditional operation. Review, compaction, closing,
and foreground ownership therefore retain their existing admission rules.

Start requires a live session. Its structured response retains the selected
entry, revision, submission identity, admitted turn identity, and rejection
reason. Optional `submissionId` and `queueMessageId` preserve client observation
and later steering-application correlation. Explicit steer mode requires the
expected foreground turn. Options affect the start branch only. Human steering
adds its history marker to a temporary protocol input; persisted prepared input
and receipts keep the original content.

`expectedInputFingerprint` and `expectedRowVersion` are checked in the final
reservation transaction. A mismatch returns `not-submitted` with reason code
`queue-entry-changed`, preserving the current entry and its pause state.
The mismatch itself never pauses or consumes the entry.

Before admission the engine commits an unconfirmed dispatch marker. A definite
rejection retains the input. Confirmed start or steering acceptance deletes the
future-turn entry and commits a durable consumed receipt. Steering acceptance
still precedes the later `InputApplied` receipt. A crash between admission and
settlement conservatively retains an unconfirmed entry. It is never automatically
resent, and only an explicit user retry may set `retryUnconfirmed: true`.
This is conservative recovery, not a claim of exactly-once execution.

The named interrupt method coordinates its accepted Stop with queue persistence.
The final expected-turn fence is checked under foreground admission, then the
queue pause is committed before the same gate cancels the captured turn. A
stale target never pauses the queue. The persisted pause reason is
`Interrupted before the turn ended.`. Connection loss and ordinary session close
preserve existing failure or directory-boundary pause reasons. Both also
retain queue input and receipts. A committed session deletion clears them inside
the engine before its successful lifecycle reply. `discard` provides the same
queue-only cleanup for an ended ephemeral session without deleting its history.
Ordinary row deletion retains idempotency receipts.
