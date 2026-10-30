# Atomic turn submission

Clients that can submit to a session shared with other clients use `turn/submit`.
The facade publishes the request and response schemas in the ordinary method
bundle. It makes one admission decision under `ProtocolServer`'s `RunAdmission`
gate. An accepted start owns a foreground reservation and an execution job before
it is acknowledged. An accepted steer has entered the existing steering queue
while the same gate still protects its exact recipient.

## Request

```json
{
  "method": "turn/submit",
  "id": 42,
  "params": {
    "sessionId": "session-1",
    "mode": "start-or-steer",
    "input": "Inspect the orders page",
    "submissionId": "submission-42",
    "queueMessageId": "c9a02ab1-092b-4206-8b14-bbb7d37d5f20",
    "delivery": "user",
    "displayMetadata": { "displayText": "Inspect Orders" }
  }
}
```

`input` accepts the same string or structured content parts as `turn/start`.
`submissionId` is optional; the facade assigns one when absent. It is a correlation
identity, not a durable deduplication key. Reusing an in-flight identity returns
the existing duplicate-submission JSON-RPC error. Malformed outer request fields,
an unknown mode, or an unknown session use the normal JSON-RPC errors.

| Mode | Idle session | Admitted foreground work |
| --- | --- | --- |
| `start-if-idle` | Start | Refuse with `not-idle` |
| `continue-if-idle` | Continue existing history with `input: []` | Refuse with `not-idle` |
| `steer` | Refuse with `no-active-turn` | Steer if the recipient is steerable |
| `start-or-steer` | Start | Steer if the recipient is steerable |

`expectedTurnId` is required and nonempty for `steer`; omitting it returns
`invalid-input` before input retention or provider execution. `start-or-steer`
may omit it for automatic admission. When supplied it must match the current
foreground admission identity. It fences the whole operation, including the idle
start branch: an old target is refused, and never converted into a new turn. When several raw runs
are already queued, only the front reservation is a possible steer recipient.
A resume which has not claimed a nonempty batch is busy but not steerable.
A turn whose terminal boundary has already closed steering is also not steerable,
even while its reservation is still being retired.

`continue-if-idle` is an explicit recovery action after a stopped or failed turn.
It requires exactly an empty content-parts list and starts the next turn from
existing history without inserting a new user message or replaying the previous
input. It uses the same atomic foreground reservation and never steers or queues
behind an active turn. Ordinary message modes still reject empty input.
`thread/queue/start` accepts only `start-if-idle` or `steer`, because a durable
queue row represents retained message input rather than a continuation.

`options` has the same shape as `turn/start` and applies only to the start branch.
It never changes session settings during steering. Retired per-turn
`permissionMode` is refused on the start branch. `displayMetadata` is opaque,
model-invisible rendering data; the top-level value overrides
`options.displayMetadata` on a start and accompanies the queued message on a
steer. `delivery` (`user` or `delegation`) and canonical UUID `queueMessageId`
apply to steering. Absent delivery retains the existing delegation delivery
behavior. Unified steering explicitly uses `now` queue priority, preserving the
existing human follow-up priority. A started turn retains the existing main-turn
persistence semantics.

## Responses and receipts

```json
{"status":"started","submissionId":"submission-42","turnId":"submission-42"}
```

`started` confirms admission, rather than successful execution or a provider
request. The existing turn lifecycle, result, failure, raw events and
`turn/completed` notifications follow normally. Stop can still cancel an
admitted turn before its provider request.

```json
{"status":"steered","submissionId":"submission-42","turnId":"active-turn-7"}
```

`steered` confirms queue acceptance. It is not an applied receipt. The later
correlated raw event retains the existing shape:

```json
{"submissionId":"submission-42","steered":{"applied":true}}
```

The existing queue snapshot, stop retention, explicit queued continuation,
transcript delivery and rendering metadata contracts remain in effect. The
request's `submissionId` identifies its receipt; `turnId` identifies its recipient;
`queueMessageId` identifies the retained queue message across stop or reconnect.

```json
{
  "status": "not-submitted",
  "submissionId": "submission-42",
  "reason": {
    "code": "expected-turn-mismatch",
    "message": "the expected turn is no longer current",
    "activeTurnId": "active-turn-8"
  }
}
```

A refusal retains no request input or rendering metadata, changes no settings,
and schedules no model turn. `activeTurnId` is included when a current admission
identity is available. For a Review or Compact boundary, optional `turnKind`
is `review` or `compact`, so clients can identify the operation without parsing
the diagnostic `message`.

| Reason code | Meaning |
| --- | --- |
| `no-active-turn` | Steer-only submission found an idle session |
| `expected-turn-mismatch` | The supplied target is no longer the current admission identity |
| `not-idle` | Start-only submission found admitted work |
| `session-closing` | Endpoint or execution queue no longer accepts work |
| `active-turn-not-steerable` | Busy operation cannot receive main-thread input |
| `empty-input` | Empty or whitespace-only input, including empty text parts |
| `invalid-input` | Invalid input, options, payload, delivery or queue identity |

Input structure and emptiness are checked before preparation. Known admission
refusals also return without creating a lazy core. After asynchronous preparation,
the engine repeats the authoritative decision under the gate. Start validation
precedes reservation and execution. Steering validates and inserts synchronously
under that gate, so terminal publication cannot change its recipient between
checking and insertion.

## Non-steerable operations and compatibility

Review and manual Compact retain busy leases from raw operation admission
through their actual worker completion. Both make `start-if-idle` refuse. When
the current operation is Review or Compact, steering returns
`active-turn-not-steerable` with the corresponding `turnKind`. Review keeps its
independent delegate; an existing regular main turn remains steerable while that
delegate runs. Compact keeps its serial execution: waiting behind a regular turn
does not block steering that turn, and the Compact boundary becomes active only
when its worker starts. Their leases are released on failure, cancellation and
dropped dispatch as well as success. Automatic goal admission observes these
leases too.

The existing `turn/start` and `session/steer` request and response shapes remain
available. `turn/start` continues to enqueue serial work; legacy unchecked
`session/steer` retains its compatibility behavior. Raw runs, raw resumes and
automatic goal continuations all reserve under the same foreground gate, so they
cannot slip through the new method's idle decision. Clients using the unified
contract should handle its refusal directly instead of falling back to a legacy
unconditional start or unchecked steer.

This change does not migrate the persistent queue or introduce durable submission
idempotency. Queue ownership and applied receipts retain their existing contracts.
