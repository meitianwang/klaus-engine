//! Prepared admission for explicitly synchronous test recorders.
//!
//! Only use this adapter after auditing every append body as immediate. A
//! recorder that delegates to an asynchronous writer must reserve that writer's
//! capacity instead. Pending is a fixture defect and panics; it is never an
//! accepted write or an asynchronous fallback.

use crate::schemas::rollout::RolloutItem;
use crate::store::session_store::{
    AppendEventOptions, CheckpointRecord, CompactionRecord, LiveRollout, PreparedRolloutBatch,
    RollbackRecord,
};
use futures::FutureExt;

/// Preserve an immediate recorder's existing append interception at commit.
/// Deliberate synchronous panic/block probes retain their original behavior.
/// This module is available only with the core test-support surface.
pub fn prepare_immediate_test_rollout_batch(
    recorder: &dyn LiveRollout,
    maximum_records: usize,
) -> std::io::Result<PreparedRolloutBatch<'_>> {
    if maximum_records == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "test rollout admission must reserve at least one record",
        ));
    }
    Ok(PreparedRolloutBatch::new(move |items| {
        assert!(items.len() <= maximum_records);
        async {
            for item in items {
                match item {
                    RolloutItem::Message { message } => recorder.append_message(message).await,
                    RolloutItem::TurnContext { ctx } => recorder.append_turn_context(ctx).await,
                    RolloutItem::Compacted(record) => {
                        recorder
                            .append_compaction(CompactionRecord {
                                summary: record.summary,
                                replaced_range: record.replaced_range,
                                replacement_history: record.replacement_history,
                                source_message_ids: record.source_message_ids,
                                approval_parent_history: record.approval_parent_history,
                            })
                            .await;
                    }
                    RolloutItem::Event(record) => {
                        recorder
                            .append_event(
                                record.event,
                                Some(AppendEventOptions {
                                    turn_id: record.turn_id,
                                    payload: record.payload,
                                }),
                            )
                            .await;
                    }
                    RolloutItem::Rollback(record) => {
                        recorder
                            .append_rollback(RollbackRecord {
                                num_turns: record.num_turns,
                            })
                            .await;
                    }
                    RolloutItem::Checkpoint(record) => {
                        recorder
                            .append_checkpoint(CheckpointRecord {
                                hash: record.hash.map(|hash| {
                                    hash.try_to_utf8()
                                        .expect("fixture checkpoint hash is scalar")
                                }),
                            })
                            .await;
                    }
                    RolloutItem::UsageLedger(record) => recorder.append_usage(record).await,
                    RolloutItem::SubagentLifecycle(record) => {
                        recorder.append_subagent_lifecycle(record.record).await;
                    }
                    RolloutItem::BackgroundProcessLifecycle(record) => {
                        recorder
                            .append_background_process_lifecycle(record.record)
                            .await;
                    }
                    RolloutItem::SessionMeta { .. } => {
                        panic!("a session header is not an append operation");
                    }
                }
            }
        }
        .now_or_never()
        .expect("an immediate test rollout append must not suspend");
    }))
}
