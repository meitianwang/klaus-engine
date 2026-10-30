# External session import

`engine/importExternalSession` is the only client entry point for imported
history. It requires no live session and does not create a model or read the
external source path. The host normalizes its ecosystem format into text; the
engine owns validation, message IDs, provenance, rollout serialization and
atomic publication.

The declared params/result schemas live in the app-server method table. Params:

```text
{ threadId, cwd, title?,
  source: { providerId, format, path, contentSha256 },
  messages: [{ role, text, timestampMs?, sourceMessageId? }], hostMetadata? }
```

`threadId` is a non-nil, lowercase hyphenated UUID. `cwd` and `source.path` are
absolute paths on the endpoint's platform, without NUL, at most 32768 UTF-8 bytes.
The source path is attribution only. `providerId` matches `[a-z][a-z0-9._-]{0,63}`;
format is `claude-jsonl`, `codex-jsonl`, or `kiro-chat`; SHA-256 is 64 lowercase
hexadecimal characters. The host may derive a stable UUID from realpath plus
source content hash, but the engine does not trust that derivation for conflicts.

Messages preserve order and adjacent roles. Only `user` and `assistant` are
accepted. Empty/whitespace-only text is rejected. Limits are 10000 messages,
1 MiB per text, and 8 MiB total UTF-8 text. Optional timestamps are integers in
`0..=253402300799999`; source message IDs are nonempty, NUL-free strings up to
1024 UTF-8 bytes. A present title is nonempty and at most 4096 UTF-8 bytes.
Host metadata is an object with at most 64 KiB of JSON. The combined durable
header must remain below the store's 64 KiB header-reader limit, and the complete
normalized request must be at most 16 MiB of JSON. Bounds fail explicitly; no
truncation or partial import is performed.

Imported messages contain text blocks only. External tool calls/results may be
explicitly labelled text supplied by the host; they cannot carry native tool,
token, usage, or turn execution records. `engine/readThread` exposes imported
groups with status `imported`, and message items carry `externalSource` with
their optional original message ID/time. No inference duration or completed
Klaus execution is invented. A later resumed turn uses the normal live engine
and appends its actual records to the same thread.

The engine records a canonical request SHA-256 and source in an immutable
`external_session_import` rollout event. Object key order does not affect this
digest. The replaceable `hostMetadata` header attachment cannot overwrite this
provenance. A private candidate contains the complete header, source annotations,
messages and completion receipt. The store fsyncs it, then publishes it with an
atomic no-replace hard link under the same per-thread lifecycle/file lock used
by create, resume, archive and delete. Candidates do not appear in the index.
Only one large import per endpoint is admitted; concurrent requests get an
overload error instead of an unbounded queue.

An existing active or archived ID is never overwritten. An exact retry verifies
the complete original import prefix and returns `alreadyImported: true`, even
after later turns or title/host-metadata changes. Different input or a damaged
prefix fails and preserves the bytes. A lazy/live registered ID may verify an
existing import, but cannot gain new imported history underneath its binding.
Session registration is blocked for that ID during publication. A cancelled
waiter cannot release the filesystem identity lock before its publisher finishes.
An error after the final durability barrier is attempted can leave a complete
published import; retrying the identical request determines the result safely.

Success is `{threadId, persisted:true, alreadyImported, messageCount}`. It does
not subscribe or open the thread. Clients then use normal `engine/listSessions`,
`engine/readThread`, `session/start` with `resume`, and the existing named
archive/unarchive/delete methods. The public facade and actual filesystem
failure/cancellation regressions cover this lifecycle.
