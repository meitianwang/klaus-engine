# Ordinary worker message recovery

An ordinary worker keeps its identity, direct-parent binding and transcript after a turn completes. Runtime availability is separate from those records.

## Delivery and execution

`send_message` accepts mail without starting an idle worker's next turn. `followup_task` accepts mail and requests a turn. Both use the same delivery path. A running worker consumes mail at its existing input boundaries; delivery does not create concurrent inference for that worker. Mail and the terminal handoff share one owner, and message UUIDs prevent repeated transcript insertion.

Cold discovery restores the worker roster, lifecycle state and transcript availability without constructing every child's model connection. Addressing a cold worker constructs its runtime before accepting mail. Ordinary mail still performs no inference. Follow-up work cannot target the root conversation. Unknown targets return an explicit error.

## Runtime configuration

An existing runtime retains its effective model, role, instruction and execution configuration. Each new query captures the current shared skill generation; publication of a newer generation does not mutate a running query's snapshot.

A cold runtime is constructed field by field:

| Value | Source |
| --- | --- |
| Configuration defaults | The sender's effective query capture |
| Model and model source | The target's persisted selection, resolved against the currently available source adapters |
| Reasoning effort | Persisted target effort, overridden by an explicit effort in the current target role |
| Role | Persisted role identity, loaded from the current registry |
| Base instructions | The target's persisted instruction body; historical records without a body use the current template |
| Role instructions | The current target role, appended to the base instructions |
| Native and MCP tools | Current admitted implementations filtered by the effective role and disabled-tool settings |
| Dynamic tool declarations | Persisted declarations bound to current admitted adapters |
| Skill catalog | The current shared generation captured for the next query |
| Working directory | The sender's effective working directory |
| Execution restrictions | Current policy together with retained task restrictions and permission-origin migration |
| Transcript and fork seed | The target's persisted history and retained seed identities |

The sender capture is taken from the calling query, including when a child sends to another child. A later parent model selection is not substituted during an asynchronous message handoff.

## Historical records and failures

Creation metadata version 5 stores instruction bodies and tool declarations rather than equality proofs for the executable, provider metadata, complete skill catalog, role definition or tool schema. Versions 2 through 4 remain readable. Their existing sandbox, permission-origin, effort and direct-parent migrations still apply. Missing historical instruction bodies cannot be reconstructed from a digest, so they use the current template.

Catalog refreshes and implementation upgrades do not invalidate an otherwise recoverable worker. Missing roles, unavailable model sources, invalid metadata, unreadable transcripts and missing dynamic-tool adapters remain concrete errors. A history read failure must not silently become a successful continuation with an empty transcript. User cancellation, shutdown admission and one-shot-role restrictions remain explicit lifecycle decisions.
