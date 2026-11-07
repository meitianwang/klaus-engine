# Saved user presentation dependencies

`engine/readThreadHistory` and `engine/readThreadItems` may return a separate
`presentationRecords` array. Its entries are existing `system/user_display`
messages, encoded with their original UUID, subtype and text envelope. Each
entry belongs to a user UUID present in that reply's raw `history`. Only the
last valid retained envelope for each selected user is returned, in source
order. Full turn pages may omit the field because their history already
contains all display envelopes. Empty sidecars are omitted.

The summary anchors, raw item membership, page limits and cursors are unchanged.
In particular, concatenating all limit-one item pages still equals the whole
raw turn. The sidecar is a display dependency, not an additional visible row.
A client builds its user presentation lookup from history and sidecar together.

The materialized index records the target user UUID while accepting each valid
presentation record. Page reads perform keyed lookups only for their selected
users, within the same SQLite read snapshot as the history rows. They do not
scan unrelated system payloads or reopen the rollout. Item-page decoded-byte
accounting includes both arrays and retains the existing refusal text.

Cache schema version 5 adds the association and a resumable compatibility
journal for version-4 projections. Each migration transaction reads at most
256 existing system records. A fixed ending ordinal and committed next ordinal
bound and resume the pass; malformed cached records refuse the page without
advancing the failed batch. Existing rollout bytes, source checkpoints and
cursor epochs remain unchanged. New projections write associations alongside
payloads in their existing batch transaction. Rewinds and cache/thread deletion
remove obsolete associations.
