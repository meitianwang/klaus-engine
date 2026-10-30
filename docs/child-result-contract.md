# Child execution results

A child result is complete only after a successful terminal event. Progress before
interruption, failure, or a stream ending without a terminal event is incomplete
activity and must not be returned as a successful final answer.

Ordinary cancellation remains visible in the lifecycle and explicit result
retrieval. It does not send a parent completion notification. An interrupted
background handle reports `interrupted`; `task_output` returns an error result
with `retrieval_status: interrupted`. Failed handles also return an error result
with `retrieval_status: failed`. Available partial activity remains inspectable
and is labelled as incomplete.

Workflow agent calls reject interrupted, failed, and unterminated streams with a
reasoned error. Only successfully completed child calls enter the resume journal.
Parallel and pipeline joins preserve a branch failure as an object containing
`ok: false` and `error`, rather than replacing the failure with `null`. A
non-function entry in a parallel input continues to return `null`.

The public regressions in
`crates/core/tests/regressions/r20261014_subagent_cancellation_outcomes.rs` pin
interruption after progress, explicit retrieval, absent completion notifications,
and workflow joins with interrupted or missing terminal events. Workflow runtime
tests also pin failed branch and thrown stage error values.
