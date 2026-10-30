//! Actual usage receipts for isolated summary requests.

use crate::runtime_values::RuntimeString;
use crate::schemas::message::{AssistantMessage, Usage};
use crate::schemas::rollout::UsageLedgerItem;
use crate::store::receipt_broker::{reserve_receipt, ReceiptBroker, ReceiptSlot};
use crate::types::provider::{CallModel, ModelStreamEvent};
use crate::types::tool::UuidFn;
use futures::StreamExt;
use std::sync::Arc;

type UsageRecorder = Arc<dyn Fn(AssistantMessage) -> Option<UsageLedgerItem> + Send + Sync>;

struct UsageReceipt {
    request_id: RuntimeString,
    model: RuntimeString,
    actual: Option<Usage>,
    record: UsageRecorder,
    slot: Option<ReceiptSlot>,
}

impl Drop for UsageReceipt {
    fn drop(&mut self) {
        if let Some(usage) = self.actual.take() {
            let record = (self.record)(AssistantMessage {
                uuid: self.request_id.clone(),
                model: self.model.clone(),
                content: Vec::new(),
                usage: Some(usage),
            });
            if let (Some(record), Some(slot)) = (record, self.slot.take()) {
                slot.commit(record);
            }
        }
    }
}

/// Each physical summary request records its last actual cumulative receipt once,
/// including a request dropped after usage arrived but before its terminal event.
pub(super) fn track_compaction_usage(
    call_model: CallModel,
    record: UsageRecorder,
    broker: Option<Arc<ReceiptBroker>>,
    uuid: UuidFn,
) -> CallModel {
    let call_model: crate::engine::session::SharedCallModel = Arc::from(call_model);
    Box::new(move |request, signal| {
        let receipt = UsageReceipt {
            request_id: uuid().into(),
            model: request.model.clone(),
            actual: None,
            record: record.clone(),
            slot: None,
        };
        let call_model = call_model.clone();
        let broker = broker.clone();
        Box::pin(async_stream::stream! {
            let mut receipt = receipt;
            receipt.slot = match reserve_receipt(broker.as_ref(), &signal).await {
                Ok(slot) => slot,
                Err(error) => {
                    yield ModelStreamEvent::Error { error: super::super::session_targets_part::usage_admission_error(error) };
                    return;
                }
            };
            // Calling a transport may start work synchronously. Defer even its
            // construction until capacity is reserved on this first poll.
            let mut stream = call_model(request, signal);
            while let Some(event) = stream.next().await {
                match &event {
                    ModelStreamEvent::MessageStart { model } => receipt.model = model.clone(),
                    ModelStreamEvent::UsageSnapshot { usage }
                    | ModelStreamEvent::MessageStop { usage, .. } => receipt.actual = Some(usage.clone()),
                    _ => {}
                }
                yield event;
            }
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability::cost_tracker::{CostTracker, ModelPricing, PricingTable};
    use crate::runtime_values::runtime_codec;
    use crate::types::abort::AbortSignal;
    use crate::types::provider::{ModelRequest, ModelStopReason, ModelStream};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

    fn usage(input: u64, output: u64) -> Usage {
        Usage {
            input_tokens: input,
            output_tokens: output,
            cache_read_input_tokens: 3,
            cache_creation_input_tokens: 2,
            total_tokens: Some(input + output + 5),
        }
    }
    fn request() -> ModelRequest {
        runtime_codec::decode_text(&RuntimeString::from(r#"{"model":"summary-target","system":[],"messages":[],"tools":[],"maxOutputTokens":1024}"#)).unwrap()
    }
    fn tracked(
        events: Vec<ModelStreamEvent>,
        records: Arc<Mutex<Vec<AssistantMessage>>>,
    ) -> CallModel {
        let events = Arc::new(events);
        let call: CallModel = Box::new(move |_, _| -> ModelStream {
            Box::pin(futures::stream::iter((*events).clone()))
        });
        let next_id = Arc::new(AtomicUsize::new(0));
        track_compaction_usage(
            call,
            Arc::new(move |record| {
                records.lock().unwrap().push(record);
                None
            }),
            None,
            Arc::new(move || format!("summary-request-{}", next_id.fetch_add(1, Ordering::SeqCst))),
        )
    }

    #[test]
    fn repeated_snapshots_and_terminal_usage_are_charged_once_per_actual_summary_request() {
        let records = Arc::new(Mutex::new(Vec::new()));
        let call = tracked(
            vec![
                ModelStreamEvent::MessageStart {
                    model: "actual-summary-model".into(),
                },
                ModelStreamEvent::UsageSnapshot {
                    usage: usage(100, 1),
                },
                ModelStreamEvent::UsageSnapshot {
                    usage: usage(100, 1),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::EndTurn,
                    usage: usage(100, 7),
                },
            ],
            records.clone(),
        );
        for _ in 0..2 {
            futures::executor::block_on(call(request(), AbortSignal::new()).collect::<Vec<_>>());
        }
        let records = records.lock().unwrap();
        assert_eq!(records.len(), 2);
        assert_ne!(records[0].uuid, records[1].uuid);
        for record in records.iter() {
            assert_eq!(record.model, "actual-summary-model");
            assert!(
                record.content.is_empty(),
                "accounting cannot add a fabricated summary to history"
            );
            assert_eq!(record.usage, Some(usage(100, 7)));
        }
        let captured_rates: PricingTable = vec![(
            "actual-summary-model".into(),
            ModelPricing {
                input_per_m_tok: 2.0,
                output_per_m_tok: 8.0,
                cache_read_per_m_tok: 1.0,
                cache_write_per_m_tok: 3.0,
            },
        )];
        let mut cost = CostTracker::with_pricing(captured_rates);
        for record in records.iter() {
            cost.record(
                record.uuid.clone(),
                record.uuid.clone(),
                "actual-summary-model",
                record.usage.as_ref().unwrap(),
            );
        }
        let total = cost.total_usage();
        assert_eq!(total.input_tokens, 200);
        assert_eq!(total.output_tokens, 14);
        assert_eq!(total.cache_read_input_tokens, 6);
        assert_eq!(total.cache_creation_input_tokens, 4);
        assert!((cost.total_cost_usd() - 0.00053).abs() < 1e-12);
    }

    #[test]
    fn dropping_a_partial_summary_records_only_the_actual_usage_already_observed() {
        let records = Arc::new(Mutex::new(Vec::new()));
        let call = tracked(
            vec![
                ModelStreamEvent::UsageSnapshot {
                    usage: usage(12, 2),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::EndTurn,
                    usage: usage(12, 9),
                },
            ],
            records.clone(),
        );
        let mut stream = call(request(), AbortSignal::new());
        futures::executor::block_on(stream.next()).unwrap();
        drop(stream);
        assert_eq!(records.lock().unwrap().len(), 1);
        assert_eq!(records.lock().unwrap()[0].usage, Some(usage(12, 2)));
        let stream = call(request(), AbortSignal::new());
        drop(stream);
        assert_eq!(
            records.lock().unwrap().len(),
            1,
            "an unobserved request has no estimated charge"
        );
    }
    #[tokio::test]
    async fn actual_summary_collector_records_usage_when_it_stops_at_the_terminal_event() {
        use crate::context::compact::full_compact::{create_full_compactor, FullCompactorOptions};
        use crate::schemas::content::{ContentBlock, TextBlock};
        use crate::schemas::message::{Message, UserMessage};
        let records = Arc::new(Mutex::new(Vec::new()));
        let call = tracked(
            vec![
                ModelStreamEvent::MessageStart {
                    model: "summary-target".into(),
                },
                ModelStreamEvent::UsageSnapshot {
                    usage: usage(140, 1),
                },
                ModelStreamEvent::TextDelta {
                    text: "## Goals\nContinue the requested workspace inspection.".into(),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::EndTurn,
                    usage: usage(140, 6),
                },
            ],
            records.clone(),
        );
        let compactor = create_full_compactor(FullCompactorOptions {
            call_model: call,
            model: "summary-target".into(),
            max_output_tokens: Some(1024),
            uuid: Box::new(|| "summary-boundary".to_owned()),
        });
        let original = Message::User(UserMessage {
            uuid: "summary-source".into(),
            question_answer: None,
            is_meta: None,
            content: vec![ContentBlock::Text(TextBlock {
                text: "Recorded workspace inspection evidence. "
                    .repeat(300)
                    .into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
        });
        let result = compactor
            .full_compact(
                vec![original],
                &crate::schemas::config::default_config(),
                None,
                false,
            )
            .await;
        assert!(result.compacted);
        let records = records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].model, "summary-target");
        assert_eq!(records[0].usage, Some(usage(140, 6)));
    }

    #[tokio::test]
    async fn partial_summary_drop_accounts_synchronously_and_survives_a_cold_rollout_read() {
        use crate::schemas::rollout::RolloutItem;
        use crate::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
        use crate::store::rollout::parse_rollout_line;
        use crate::store::session_store::{LiveRollout, SessionStore};

        let directory = tempfile::tempdir().unwrap();
        let store = JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(directory.path().to_string_lossy().into_owned()),
            archive_dir: None,
        });
        let meta = match parse_rollout_line(r#"{"kind":"session_meta","schemaVersion":2,"meta":{"id":"partial-summary-receipt","createdAt":1700000000000,"cwd":"/tmp","model":"summary-target"}}"#).unwrap() {
            RolloutItem::SessionMeta { meta, .. } => meta,
            _ => unreachable!(),
        };
        let raw: Arc<dyn LiveRollout> = Arc::from(store.create(meta).await.unwrap());
        let broker = ReceiptBroker::new(raw.clone());
        let cost = Arc::new(Mutex::new(CostTracker::new()));
        let account = cost.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let started = calls.clone();
        let call: CallModel = Box::new(move |_, _| {
            started.fetch_add(1, Ordering::SeqCst);
            Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::UsageSnapshot {
                    usage: usage(12, 2),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::EndTurn,
                    usage: usage(12, 9),
                },
            ]))
        });
        let call = track_compaction_usage(
            call,
            Arc::new(move |assistant| {
                Some(account.lock().unwrap().record(
                    assistant.uuid.clone(),
                    assistant.uuid,
                    "summary-target",
                    assistant.usage.as_ref().unwrap(),
                ))
            }),
            Some(broker.clone()),
            Arc::new(|| "captured-summary-request".to_owned()),
        );
        let mut stream = call(request(), AbortSignal::new());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "transport construction remains lazy"
        );
        assert!(matches!(
            stream.next().await,
            Some(ModelStreamEvent::UsageSnapshot { .. })
        ));
        drop(stream);
        assert_eq!(cost.lock().unwrap().total_usage().input_tokens, 12);
        assert_eq!(cost.lock().unwrap().total_usage().output_tokens, 2);
        broker.flush_committed().await.unwrap();
        raw.flush().await.unwrap();
        broker.shutdown().await.unwrap();
        raw.shutdown().await.unwrap();
        let cold = store.resume("partial-summary-receipt").await.unwrap();
        assert!(
            cold.messages.is_empty(),
            "usage cannot fabricate assistant history"
        );
        let receipts = cold
            .raw_items
            .into_iter()
            .filter_map(|item| match item {
                RolloutItem::UsageLedger(record) => Some(record),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].request_id, "captured-summary-request");
        assert_eq!(receipts[0].usage, usage(12, 2));
    }

    #[tokio::test]
    async fn closed_receipt_admission_never_invokes_an_eager_summary_transport() {
        use crate::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
        use crate::store::rollout::parse_rollout_line;
        use crate::store::session_store::{LiveRollout, SessionStore};
        let directory = tempfile::tempdir().unwrap();
        let store = JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(directory.path().to_string_lossy().into_owned()),
            archive_dir: None,
        });
        let meta = match parse_rollout_line(r#"{"kind":"session_meta","schemaVersion":2,"meta":{"id":"closed-summary-receipt","createdAt":1700000000000,"cwd":"/tmp","model":"summary-target"}}"#).unwrap() {
            crate::schemas::rollout::RolloutItem::SessionMeta { meta, .. } => meta,
            _ => unreachable!(),
        };
        let raw: Arc<dyn LiveRollout> = Arc::from(store.create(meta).await.unwrap());
        let broker = ReceiptBroker::new(raw.clone());
        let calls = Arc::new(AtomicUsize::new(0));
        let started = calls.clone();
        let ids = Arc::new(AtomicUsize::new(0));
        let next_id = ids.clone();
        let call = track_compaction_usage(
            Box::new(move |_, _| {
                started.fetch_add(1, Ordering::SeqCst);
                Box::pin(futures::stream::empty())
            }),
            Arc::new(|_| panic!("unobserved requests cannot be accounted")),
            Some(broker.clone()),
            Arc::new(move || format!("summary-{}", next_id.fetch_add(1, Ordering::SeqCst))),
        );
        let unobserved = call(request(), AbortSignal::new());
        assert_eq!(
            ids.load(Ordering::SeqCst),
            1,
            "request UUID timing remains eager"
        );
        drop(unobserved);
        broker.close_admission();
        let mut stream = call(request(), AbortSignal::new());
        assert!(
            matches!(stream.next().await, Some(ModelStreamEvent::Error { error })
            if error.code.as_ref().is_some_and(|code| code == "usage_receipt_admission_failed"))
        );
        assert!(stream.next().await.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        broker.shutdown().await.unwrap();
        raw.shutdown().await.unwrap();
    }
}
