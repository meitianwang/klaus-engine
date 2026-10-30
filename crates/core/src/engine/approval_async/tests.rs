use super::*;
use crate::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use futures::stream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, oneshot, Notify};
use tokio_util::sync::CancellationToken;

fn config() -> AsyncReviewConfig {
    AsyncReviewConfig::resolve(AsyncLocalConfig::default()).unwrap()
}

fn observation(call_id: &str) -> ToolObservation<'_> {
    ToolObservation {
        call_id,
        originating_item_id: None,
        origin: ToolOrigin::Direct,
        is_code_wrapper: false,
    }
}

fn score(risk: f64, seconds: u64) -> ClassifierScore {
    ClassifierScore {
        action_risk: risk,
        sampled_at: SystemTime::UNIX_EPOCH + Duration::from_secs(seconds),
    }
}

fn cache_check<'a>(call_id: Option<&'a str>, authorization: &'a u64) -> CacheCheck<'a, u64> {
    CacheCheck {
        call_id,
        category: ReviewScope::Shell,
        action_fits: true,
        compaction_compatible: true,
        is_computer_use_js: false,
        current_authorization: Some(authorization),
        retained_context_complete: true,
    }
}

#[test]
fn explicit_catalog_policy_overrides_a_disabled_legacy_feature() {
    let policy = ModelReviewPolicy {
        shell: Some(ReviewMode::Adaptive),
        ..Default::default()
    };
    for legacy in [
        AsyncFeatureConfig::Enabled(false),
        AsyncFeatureConfig::Config(Box::new(AsyncLocalConfig {
            enabled: Some(false),
            ..Default::default()
        })),
    ] {
        assert_eq!(
            PolicyLoader::new(Some(&legacy), false).resolve(Some(&policy), Some(false)),
            policy
        );
        let constrained = PolicyLoader::new(Some(&legacy), true).resolve(Some(&policy), None);
        assert_eq!(constrained.shell, Some(ReviewMode::Synchronous));
        assert!(!constrained.scoring_enabled());
    }
    assert!(!PolicyLoader::new(None, false)
        .resolve(None, None)
        .scoring_enabled());
}

#[test]
fn legacy_scope_and_unknown_catalog_modes_preserve_blocking_review() {
    let enabled = AsyncFeatureConfig::Enabled(true);
    let loader = PolicyLoader::new(Some(&enabled), false);
    let policy = loader.resolve(None, Some(true));
    assert_eq!(policy.computer_use, Some(ReviewMode::Adaptive));
    assert_eq!(policy.shell, Some(ReviewMode::Synchronous));
    assert_eq!(policy.unscored_action, UnscoredAction::Ignore);
    assert!(policy.allows_initial_computer_use());
    assert!(!loader.resolve(None, Some(false)).scoring_enabled());
    let broad = AsyncFeatureConfig::Config(Box::new(AsyncLocalConfig {
        enabled: Some(true),
        review_scope: Some(LocalReviewScope {
            computer_use_only: Some(false),
            sandboxed_exec_commands: Some(true),
        }),
        ..Default::default()
    }));
    let policy = PolicyLoader::new(Some(&broad), false).resolve(None, None);
    assert_eq!(policy.shell, Some(ReviewMode::Adaptive));
    assert_eq!(policy.unscored_action, UnscoredAction::AgeScore);
    assert!(!policy.allows_initial_computer_use());
    let unknown: ModelReviewPolicy =
        serde_json::from_str(r#"{"shell":"future_mode","unscored_action":"future_action"}"#)
            .unwrap();
    assert_eq!(unknown.review_mode(ReviewScope::Shell), ReviewMode::Unknown);
    assert_eq!(unknown.review_mode(ReviewScope::Mcp), ReviewMode::Disabled);
    assert_eq!(unknown.unscored_action, UnscoredAction::InvalidateScore);
    assert!(!ModelReviewPolicy {
        other_tools: ReviewMode::Adaptive,
        ..Default::default()
    }
    .scoring_enabled());
}

#[test]
fn classifier_defaults_and_model_override_reset_are_exact() {
    let defaults = config();
    assert_eq!(defaults.review_threshold, 0.5);
    assert_eq!(defaults.max_tool_call_lag, 2);
    assert_eq!(defaults.max_action_tokens, 10_000);
    assert_eq!(defaults.max_parent_compaction_tokens, 25_000);
    assert_eq!(defaults.max_classifier_instruction_tokens, None);
    assert_eq!(defaults.async_classifier_conversation_token_limit, 100_000);
    assert_eq!(defaults.reasoning_effort, "low");
    assert_eq!(
        (
            defaults.transcript.max_message_entry_tokens,
            defaults.transcript.max_tool_entry_tokens,
            defaults.transcript.max_message_transcript_tokens,
            defaults.transcript.max_tool_transcript_tokens,
            defaults.transcript.max_recent_non_user_entries
        ),
        (2_000, 1_000, 10_000, 10_000, 40)
    );
    assert!(defaults.transcript.include_images);
    let model = AsyncModelConfig {
        classifier_instructions: Some(String::new()),
        async_classifier_mode: Some(AsyncClassifierMode::Conversation),
        max_tool_call_lag: Some(0),
        reasoning_effort: Some("model-specific-effort".into()),
        ..Default::default()
    };
    let inherited = defaults.with_model_defaults(Some(&model)).unwrap();
    assert_eq!(inherited.review_threshold, 0.8);
    assert_eq!(inherited.classifier_mode, AsyncClassifierMode::Conversation);
    assert_eq!(inherited.max_tool_call_lag, 0);
    assert_eq!(inherited.reasoning_effort, "model-specific-effort");
    assert_eq!(inherited.with_model_defaults(None).unwrap(), defaults);
    let local = AsyncReviewConfig::resolve(AsyncLocalConfig {
        review_threshold: Some(0.25),
        max_tool_call_lag: Some(4),
        ..Default::default()
    })
    .unwrap()
    .with_model_defaults(Some(&AsyncModelConfig {
        review_threshold_basis_points: Some(6_000),
        ..model
    }))
    .unwrap();
    assert_eq!(local.review_threshold, 0.25);
    assert_eq!(local.max_tool_call_lag, 4);
}

#[test]
fn invalid_classifier_configuration_is_rejected_before_model_work() {
    for threshold in [f64::NAN, f64::INFINITY, -0.1, 1.1] {
        assert!(AsyncReviewConfig::resolve(AsyncLocalConfig {
            review_threshold: Some(threshold),
            ..Default::default()
        })
        .is_err());
    }
    for max_action_tokens in [0, 99, 100_001] {
        assert!(AsyncReviewConfig::resolve(AsyncLocalConfig {
            max_action_tokens: Some(max_action_tokens),
            ..Default::default()
        })
        .is_err());
    }
    assert!(AsyncReviewConfig::resolve(AsyncLocalConfig {
        async_classifier_conversation_token_limit: Some(0),
        ..Default::default()
    })
    .is_err());
    assert!(config()
        .with_model_defaults(Some(&AsyncModelConfig {
            transcript: Some(AsyncTranscriptModelConfig {
                sources: Some(vec!["unknown".into()]),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .is_err());
    assert!(AsyncReviewConfig::resolve(AsyncLocalConfig {
        transcript: Some(TranscriptOptions {
            max_message_entry_tokens: Some(20_000),
            ..Default::default()
        }),
        ..Default::default()
    })
    .is_err());
}

#[test]
fn optional_review_configuration_is_governed_and_round_trips_through_the_public_config_codec() {
    use crate::runtime_values::runtime_codec;
    use crate::schemas::config::{engine_config_governance_schema, EngineConfig};

    assert!(EngineConfig::default().approval_review.is_none());
    for text in [
        r#"false"#,
        r#"true"#,
        r#"{"enabled":true,"async_classifier_mode":"conversation","review_threshold":0.25,"transcript":{"sources":["tool_calls","reasoning"],"include_images":false}}"#,
        r#"{"max_tool_call_lag":2,"max_action_tokens":100,"review_threshold":0.0}"#,
    ] {
        let value = runtime_json::parse(&RuntimeString::from(text)).unwrap();
        let feature = runtime_codec::decode_value::<AsyncFeatureConfig>(&value).unwrap();
        assert!(runtime_codec::encode_value(&feature)
            .unwrap()
            .semantic_eq_unordered(&value));
        let config_value = format!(r#"{{"approvalReview":{text}}}"#);
        let config: EngineConfig = serde_json::from_str(&config_value).unwrap();
        config.validate().unwrap();
        let encoded = runtime_codec::encode_value(&config).unwrap();
        let round_trip = runtime_codec::decode_value::<EngineConfig>(&encoded).unwrap();
        assert_eq!(round_trip.approval_review, Some(feature));
    }
    let schema = engine_config_governance_schema();
    assert!(schema.spec("approvalReview").is_some());
    for invalid in [
        r#"{"approvalReview":{"max_action_tokens":99}}"#,
        r#"{"approvalReview":{"review_threshold":1.1}}"#,
        r#"{"approvalReview":{"unknown":true}}"#,
        r#"{"approvalReview":"enabled"}"#,
    ] {
        assert!(serde_json::from_str::<EngineConfig>(invalid).is_err());
    }
}

#[test]
fn full_action_keeps_nested_arguments_and_rejects_overflow() {
    let original = runtime_json::parse(&RuntimeString::from(r#"{"tool":"untrusted","body":{"secret":"keep-tail","values":[-0,true,null]},"text":"🦀\"\\\n"}"#)).unwrap();
    let rendered = render_action("inspect_values".into(), original.clone(), 10_000).unwrap();
    let parsed = runtime_json::parse(&rendered).unwrap();
    let mut expected = original.into_object().unwrap();
    expected.insert("tool", RuntimeJson::String("inspect_values".into()));
    assert!(parsed.semantic_eq_unordered(&RuntimeJson::Object(expected)));
    let budget = rendered.to_node_utf8().len().saturating_add(1).div_ceil(4);
    assert!(render_action("inspect_values".into(), parsed.clone(), budget).is_ok());
    assert!(render_action("inspect_values".into(), parsed, budget - 1).is_err());
}

#[test]
fn failed_or_delayed_samples_cannot_replace_newer_evidence() {
    let progress = ScoreProgress::<u64>::default();
    let index = progress.observe(observation("one"));
    assert!(progress.publish(score(0.0, 2), 7, index));
    assert!(!progress.publish(score(0.0, 2), 8, index));
    progress.fail_closed(SystemTime::UNIX_EPOCH + Duration::from_secs(1));
    assert_eq!(progress.inspect(Some("one")).action_risk, Some(0.0));
    progress.fail_closed(SystemTime::UNIX_EPOCH + Duration::from_secs(2));
    assert_eq!(progress.inspect(Some("one")).action_risk, Some(1.0));
    assert!(!progress.publish(score(0.0, 2), 7, index));
    assert!(progress.publish(score(0.0, 3), 7, index));
    let second = progress.observe(observation("two"));
    progress.invalidate(second);
    assert!(progress.inspect(Some("two")).has_unscored_failure);
    progress.fail_closed(SystemTime::UNIX_EPOCH + Duration::from_secs(4));
    assert!(progress.inspect(Some("two")).has_unscored_failure);
    assert!(progress.publish(score(0.0, 5), 7, second));
    assert!(!progress.inspect(Some("two")).has_unscored_failure);
}

#[test]
fn cached_evidence_requires_exact_authorization_order_lag_and_threshold() {
    let progress = ScoreProgress::<u64>::default();
    let first = progress.observe(observation("first"));
    progress.publish(score(0.0, 1), 7, first);
    let cfg = config();
    let policy = ModelReviewPolicy::default();
    assert_eq!(
        cached_evidence(&progress, &cfg, &policy, cache_check(Some("first"), &7)),
        Ok(CachedApproval {
            releases_pending_review: true
        })
    );
    assert_eq!(
        cached_evidence(&progress, &cfg, &policy, cache_check(Some("first"), &8)),
        Err(ReviewFallbackReason::AuthorizationChanged)
    );
    let second = progress.observe(observation("second"));
    progress.publish(score(0.0, 2), 7, second);
    assert_eq!(
        cached_evidence(&progress, &cfg, &policy, cache_check(Some("first"), &7)),
        Err(ReviewFallbackReason::MissingScore)
    );
    progress.publish(score(0.5, 3), 7, second);
    assert_eq!(
        cached_evidence(&progress, &cfg, &policy, cache_check(Some("second"), &7)),
        Err(ReviewFallbackReason::ElevatedRisk)
    );
    progress.publish(score(f64::NAN, 4), 7, second);
    assert_eq!(
        cached_evidence(&progress, &cfg, &policy, cache_check(Some("second"), &7)),
        Err(ReviewFallbackReason::InvalidScore)
    );
    for call in ["third", "fourth", "fifth"] {
        progress.observe(observation(call));
    }
    assert_eq!(
        cached_evidence(&progress, &cfg, &policy, cache_check(Some("second"), &7)),
        Err(ReviewFallbackReason::StaleScore)
    );
}

#[test]
fn initial_computer_use_allowance_never_releases_a_pending_review() {
    let progress = ScoreProgress::<u64>::default();
    progress.observe(observation("js"));
    progress.observe_js_execution();
    let policy = ModelReviewPolicy {
        computer_use: Some(ReviewMode::Adaptive),
        ..Default::default()
    };
    let input = || CacheCheck {
        category: ReviewScope::ComputerUse,
        is_computer_use_js: true,
        current_authorization: None,
        ..cache_check(Some("js"), &7)
    };
    assert_eq!(
        cached_evidence(&progress, &config(), &policy, input()),
        Ok(CachedApproval {
            releases_pending_review: false
        })
    );
    assert_eq!(
        cached_evidence(
            &progress,
            &config(),
            &policy,
            CacheCheck {
                compaction_compatible: false,
                ..input()
            }
        ),
        Err(ReviewFallbackReason::IncompatibleCompaction)
    );
    progress.mark_oversized("js", 1);
    assert_eq!(
        cached_evidence(&progress, &config(), &policy, input()),
        Err(ReviewFallbackReason::ScoringFailure)
    );
}

#[test]
fn active_nested_action_discounts_only_its_own_wrapper() {
    let progress = Arc::new(ScoreProgress::<u64>::default());
    let first = progress.observe(observation("first"));
    progress.publish(score(0.0, 1), 7, first);
    progress.observe(ToolObservation {
        call_id: "wrapper",
        originating_item_id: Some("item"),
        origin: ToolOrigin::Direct,
        is_code_wrapper: true,
    });
    progress.observe(ToolObservation {
        call_id: "nested",
        originating_item_id: Some("item"),
        origin: ToolOrigin::Nested {
            wrapper_item_id: Some("item"),
        },
        is_code_wrapper: false,
    });
    assert_eq!(progress.inspect(Some("nested")).lag, 1);
    assert_eq!(progress.inspect(Some("wrapper")).lag, 2);
    let pending = PendingScore {
        progress: Arc::clone(&progress),
        index: first,
        max_lag: 1,
    };
    assert!(!pending.is_expired());
    progress.finish("nested");
    assert!(pending.is_expired());
}

#[test]
fn unscored_permission_widening_invalidates_while_polling_does_not_age() {
    let progress = ScoreProgress::<u64>::default();
    let policy = ModelReviewPolicy {
        shell: Some(ReviewMode::Adaptive),
        sandboxed_exec_commands: Some(false),
        unscored_action: UnscoredAction::AgeScore,
        ..Default::default()
    };
    let first = progress.observe(observation("first"));
    progress.publish(score(0.0, 1), 7, first);
    let input = |name| ObservationInput {
        action: observation("next"),
        default_namespace: true,
        tool_name: name,
        scope: Some(ReviewScope::Shell),
        explicitly_escalated: false,
        with_additional_permissions: true,
        computer_use_js_execution: false,
        full_access: false,
        user_reviewer: false,
        synchronous_review_required_for_model: false,
    };
    assert_eq!(
        observe_action(&progress, &policy, input("wait")),
        ObservationDisposition::Ignored
    );
    assert_eq!(progress.inspect(None).lag, 0);
    assert_eq!(
        observe_action(&progress, &policy, input("exec_command")),
        ObservationDisposition::Unscored { index: 2 }
    );
    assert!(progress.inspect(None).has_unscored_failure);
}

#[test]
fn stale_classifier_context_is_superseded_without_poisoning_the_score() {
    let progress = ScoreProgress::<u64>::default();
    let first = progress.observe(observation("first"));
    progress.publish(score(0.0, 1), 7, first);
    assert_eq!(
        publish_classification(
            &progress,
            Ok("high"),
            8,
            score(0.0, 2).sampled_at,
            first,
            false,
            true
        ),
        ClassificationPublication::Superseded
    );
    assert_eq!(progress.inspect(None).action_risk, Some(0.0));
    assert_eq!(
        publish_classification(
            &progress,
            Ok("Low"),
            7,
            score(0.0, 3).sampled_at,
            first,
            true,
            true
        ),
        ClassificationPublication::Failed
    );
    assert_eq!(progress.inspect(None).action_risk, Some(1.0));
}

#[tokio::test]
async fn publication_during_the_initial_check_is_not_lost() {
    let progress = ScoreProgress::<u64>::default();
    let index = progress.observe(observation("first"));
    let checks = AtomicUsize::new(0);
    tokio::time::timeout(
        Duration::from_secs(1),
        wait_for_cached_approval(&progress, || {
            let first = checks.fetch_add(1, Ordering::Relaxed) == 0;
            if first {
                progress.publish(score(0.0, 1), 7, index);
            }
            std::future::ready(if first {
                Err(ReviewFallbackReason::MissingScore)
            } else {
                Ok(CachedApproval {
                    releases_pending_review: true,
                })
            })
        }),
    )
    .await
    .unwrap();
    assert_eq!(checks.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn blocking_review_wins_simultaneous_completion_and_cached_winners_are_drained() {
    let cancellation = CancellationToken::new();
    let external = CancellationToken::new();
    assert!(matches!(
        race_review(
            std::future::ready(7),
            std::future::ready(()),
            &cancellation,
            &external,
            tokio::time::Instant::now() + Duration::from_secs(1),
            || std::future::ready(true)
        )
        .await,
        ReviewRace::Synchronous(7)
    ));
    assert!(!cancellation.is_cancelled());
    let drained = AtomicUsize::new(0);
    let review = async {
        cancellation.cancelled().await;
        drained.store(1, Ordering::Relaxed);
        8
    };
    let result = race_review(
        review,
        std::future::ready(()),
        &cancellation,
        &external,
        tokio::time::Instant::now() + Duration::from_secs(1),
        || async { drained.load(Ordering::Relaxed) == 1 },
    )
    .await;
    assert!(matches!(result, ReviewRace::Cached { drained: 8 }));
}

#[test]
fn retained_admission_is_bounded_and_only_newer_compatible_history_commits() {
    let backend = ConversationBackend::<u64, Vec<String>>::default();
    let mut reservations = Vec::new();
    for index in 1..=16 {
        reservations.push(backend.reserve(|| index).1.unwrap());
    }
    assert!(matches!(
        backend.reserve(|| 17).1,
        Err(ClassifierError::QueueFull)
    ));
    let cursor = TranscriptCursor {
        parent_history_version: 1,
        transcript_entry_count: 3,
    };
    assert!(reservations[1].commit(7, vec!["newer".into()], cursor, || true));
    assert!(!reservations[0].commit(7, vec!["older".into()], cursor, || true));
    assert!(reservations[0].fork(&7).is_none());
    assert!(reservations[2].fork(&8).is_none());
    assert_eq!(reservations[2].fork(&7).unwrap().history, ["newer"]);
    drop(backend);
    assert!(!reservations[2].is_current());
    assert!(!reservations[2].commit(7, vec![], cursor, || true));
}

#[test]
fn retained_reset_rebuilds_fresh_evidence_without_applying_the_reset_limit_to_it() {
    let backend = ConversationBackend::<u64, Vec<String>>::default();
    let cursor = TranscriptCursor {
        parent_history_version: 1,
        transcript_entry_count: 3,
    };
    backend
        .reserve(|| 1)
        .1
        .unwrap()
        .commit(7, vec!["committed".into()], cursor, || true);
    let reservation = backend.reserve(|| 2).1.unwrap();
    let mut attempts = Vec::new();
    let prepared = reservation.prepare(&7, cursor, 100, 10_000, |history| {
        attempts.push(history.is_some());
        Some((
            if history.is_some() {
                "retained"
            } else {
                "fresh"
            },
            200,
        ))
    });
    assert_eq!(prepared, Some(("fresh", 200)));
    assert_eq!(attempts, [true, false]);
    assert_eq!(backend.snapshot().unwrap().history, ["committed"]);
}

fn stream_channel<H: Send + 'static>(
    receiver: mpsc::Receiver<SamplingEvent<H>>,
) -> SamplingStream<H> {
    Box::pin(stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|event| (event, receiver))
    }))
}

#[test]
fn snapshot_admission_replaces_a_scored_drain_before_unfinished_work() {
    let sampler = ClassifierSampler::default();
    let active = (0..16).map(|_| sampler.admit()).collect::<Vec<_>>();
    active[6].mark_scored();
    let newest = sampler.admit();
    assert!(active[6].cancellation.is_cancelled());
    assert!(!active[0].cancellation.is_cancelled());
    assert!(!newest.cancellation.is_cancelled());
    let retained = sampler.admit_retained();
    assert!(!active[0].cancellation.is_cancelled());
    assert!(!retained.cancellation.is_cancelled());
}

#[tokio::test]
async fn snapshot_returns_the_first_delta_and_drains_later_disagreement() {
    let sampler = ClassifierSampler::default();
    let (send, receive) = mpsc::channel::<SamplingEvent<()>>(4);
    send.send(SamplingEvent::OutputTextDelta("low".into()))
        .await
        .unwrap();
    let finished = Arc::new(Notify::new());
    let done = Arc::clone(&finished);
    let result = sampler
        .admit()
        .sample_snapshot(
            stream_channel(receive),
            std::future::pending(),
            move || async move {
                done.notify_one();
            },
        )
        .await;
    assert_eq!(result, Ok("low".into()));
    send.send(SamplingEvent::OutputTextDelta("high".into()))
        .await
        .unwrap();
    send.send(SamplingEvent::Completed).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), finished.notified())
        .await
        .unwrap();
}

#[tokio::test]
async fn retained_partial_or_contradictory_output_keeps_the_early_label_without_committing() {
    for completed in [false, true] {
        let sampler = ClassifierSampler::default();
        let mut events = vec![
            SamplingEvent::OutputTextDelta("low".into()),
            SamplingEvent::OutputItem {
                item: "retained-high",
                tokens: 1,
                valid_for_retention: true,
                assistant_text: Some("high".into()),
            },
        ];
        if completed {
            events.push(SamplingEvent::Completed);
        }
        let (ready, receive) = oneshot::channel();
        let output = sampler
            .admit_retained()
            .sample_retained(
                Box::pin(stream::iter(events)),
                Some(ready),
                1_000,
                std::future::pending(),
                || async {},
            )
            .await;
        assert_eq!(receive.await.unwrap(), Ok("low".into()));
        assert!(output.retained_output.is_none());
    }
}

#[test]
fn checkpoint_eligibility_keeps_independent_legacy_and_thread_owned_modes_distinct() {
    let cfg = config();
    let checkpoint = || CompactionCheckpoint {
        item: &"encrypted",
        usable: true,
        model_hash: Some("parent"),
        serialized_bytes: Ok(100),
        estimated_tokens: 100,
    };
    assert!(select_parent_compaction(
        ContextMode::Independent,
        &cfg,
        Some(checkpoint()),
        None,
        None
    )
    .unwrap()
    .item
    .is_none());
    assert!(matches!(
        select_parent_compaction(
            ContextMode::ThreadOwned,
            &cfg,
            Some(checkpoint()),
            None,
            Some("different")
        ),
        Err(CompactionError::RequiresSynchronousReview)
    ));
    assert!(select_parent_compaction(
        ContextMode::Legacy,
        &cfg,
        Some(checkpoint()),
        Some("parent"),
        Some("different")
    )
    .unwrap()
    .item
    .is_none());
    let selected = select_parent_compaction(
        ContextMode::ThreadOwned,
        &cfg,
        Some(checkpoint()),
        None,
        Some("parent"),
    )
    .unwrap();
    assert_eq!(selected.item, Some("encrypted"));
    assert!(matches!(
        select_parent_compaction(
            ContextMode::Legacy,
            &cfg,
            Some(CompactionCheckpoint {
                serialized_bytes: Ok(400_001),
                ..checkpoint()
            }),
            Some("parent"),
            Some("different")
        ),
        Err(CompactionError::Oversized)
    ));
}

#[test]
fn prepared_review_action_budget_preserves_its_existing_identity() {
    let action = runtime_json::parse(&RuntimeString::from(
        r#"{"tool":"registered","arguments":{"tool":"untrusted"},"type":"exec"}"#,
    ))
    .unwrap();
    let rendered = render_review_action(&action, 100).unwrap();
    assert!(runtime_json::parse(&rendered)
        .unwrap()
        .semantic_eq_unordered(&action));
    assert!(render_review_action(&action, 1).is_err());
}

#[tokio::test]
async fn retry_discards_unscored_attempt_items_before_retaining_a_completed_response() {
    let sampler = ClassifierSampler::default();
    let events = vec![
        SamplingEvent::OutputItem {
            item: "old-attempt",
            tokens: 6,
            valid_for_retention: true,
            assistant_text: Some("high".into()),
        },
        SamplingEvent::Restart,
        SamplingEvent::OutputTextDelta("low".into()),
        SamplingEvent::OutputItem {
            item: "new-attempt",
            tokens: 6,
            valid_for_retention: true,
            assistant_text: Some("low".into()),
        },
        SamplingEvent::Completed,
    ];
    let (ready, receive) = oneshot::channel();
    let completed = sampler
        .admit_retained()
        .sample_retained(
            Box::pin(stream::iter(events)),
            Some(ready),
            6,
            std::future::pending(),
            || async {},
        )
        .await;
    assert_eq!(receive.await.unwrap(), Ok("low".to_owned()));
    assert_eq!(completed.result, Ok("low".to_owned()));
    assert_eq!(completed.retained_output, Some(vec!["new-attempt"]));
}

#[tokio::test]
async fn sampler_shutdown_waits_for_the_published_response_cleanup() {
    let sampler = Arc::new(ClassifierSampler::default());
    let cleanup_entered = Arc::new(Notify::new());
    let cleanup_release = Arc::new(Notify::new());
    let entered = cleanup_entered.clone();
    let release = cleanup_release.clone();
    let (send, receive) = mpsc::channel::<SamplingEvent<()>>(1);
    send.send(SamplingEvent::OutputTextDelta("low".into()))
        .await
        .unwrap();
    assert_eq!(
        sampler
            .admit()
            .sample_snapshot(
                stream_channel(receive),
                std::future::pending(),
                move || async move {
                    entered.notify_one();
                    release.notified().await;
                }
            )
            .await,
        Ok("low".to_owned())
    );
    let closing = sampler.clone();
    let shutdown = tokio::spawn(async move {
        closing.shutdown().await;
    });
    cleanup_entered.notified().await;
    assert!(!shutdown.is_finished());
    cleanup_release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), shutdown)
        .await
        .unwrap()
        .unwrap();
}
