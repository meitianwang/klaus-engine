use super::*;
use crate::app_server::wire;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson};
use klaus_agent_core::store::session_store::RolloutMigrationMode;
use klaus_agent_protocol::server::RolloutMaintenance;
use klaus_agent_protocol::ProtocolLifecycle;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use tokio::sync::Notify;

fn report(value: Value) -> RuntimeJson {
    runtime_json::parse(&value.to_string().into()).unwrap()
}
fn maintenance_client(
    root: &std::path::Path,
    hooks: Arc<RolloutMaintenance>,
    config: &str,
) -> TestClient {
    let config_path = root.join("config.toml");
    std::fs::write(&config_path, config).unwrap();
    TestClient::start_full(
        make_create_session(&[]),
        Some(ProtocolLifecycle {
            rollout_maintenance: Some(hooks),
            ..Default::default()
        }),
        Some(root.join("rules.json")),
        Some(config_path),
    )
}

#[tokio::test]
async fn default_features_never_start_store_maintenance() {
    let root = tempfile::tempdir().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let compress_calls = calls.clone();
    let migrate_calls = calls.clone();
    let hooks = Arc::new(RolloutMaintenance {
        compress: Some(Arc::new(move |_cancelled| {
            compress_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(report(json!({}))) })
        })),
        migrate: Some(Arc::new(move |_, _| {
            migrate_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(report(json!({"busy":false,"files":[]}))) })
        })),
        ..Default::default()
    });
    let mut client = maintenance_client(root.path(), hooks, "");
    client.initialize().await;
    client.close().await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn manual_compression_kick_is_immediate_and_coalesces_with_startup() {
    let root = tempfile::tempdir().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let hooks = Arc::new(RolloutMaintenance {
        compress: Some(Arc::new({
            let calls = calls.clone();
            let entered = entered.clone();
            let release = release.clone();
            move |_cancelled| {
                let entered = entered.clone();
                let release = release.clone();
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    entered.notify_one();
                    release.notified().await;
                    Ok(report(json!({"compressed":1,"failed":0})))
                })
            }
        })),
        ..Default::default()
    });
    let mut client = maintenance_client(
        root.path(),
        hooks,
        "[features]\ncold_rollout_compression = true\n",
    );
    client.initialize().await;
    tokio::time::timeout(REPLY_TIMEOUT, entered.notified())
        .await
        .unwrap();
    for id in [2, 3] {
        client
            .send(json!({"id":id,"method":"engine/compressRollouts","params":{}}))
            .await;
        assert_eq!(client.response_for(id).await.0["result"], json!({}));
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the same pending worker owns startup and kicks"
    );
    client
        .send(json!({"id":4,"method":"config/read","params":{}}))
        .await;
    assert!(
        client.response_for(4).await.0.get("result").is_some(),
        "maintenance must not block unrelated RPCs"
    );
    release.notify_one();
    client.close().await;
}

#[tokio::test]
async fn manual_migration_defaults_to_preview_and_preserves_per_file_reports() {
    let root = tempfile::tempdir().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let hooks = Arc::new(RolloutMaintenance {
        migrate: Some(Arc::new({
            let seen = seen.clone();
            move |request, _| {
                let apply = matches!(request.mode, RolloutMigrationMode::Apply);
                seen.lock()
                    .unwrap()
                    .push((apply, request.expected_source_revisions));
                Box::pin(async move {
                    Ok(report(
                        json!({"busy":false,"files":[{"path":"/saved/source.jsonl","sourceRevision":{"length":19},"status":if apply {"applied"} else {"preview"}}]}),
                    ))
                })
            }
        })),
        ..Default::default()
    });
    let mut client = maintenance_client(root.path(), hooks, "");
    client.initialize().await;
    client
        .send(json!({"id":2,"method":"engine/migrateRollouts","params":{}}))
        .await;
    let preview = client.response_for(2).await.0;
    assert_eq!(preview["result"]["files"][0]["status"], "preview");
    client.send(json!({"id":3,"method":"engine/migrateRollouts","params":{"mode":"apply","expectedSourceRevisions":{"/saved/source.jsonl":{"length":19}}}})).await;
    let applied = client.response_for(3).await.0;
    assert_eq!(applied["result"]["files"][0]["status"], "applied");
    {
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert!(!seen[0].0);
        assert!(seen[0].1.is_none());
        assert!(seen[1].0);
        assert!(seen[1].1.as_ref().unwrap().contains_key(
            &klaus_agent_core::runtime_values::RuntimeString::from("/saved/source.jsonl")
        ));
    }
    client.close().await;
}

#[tokio::test]
async fn invalid_maintenance_params_are_rejected_and_store_failures_are_visible() {
    let root = tempfile::tempdir().unwrap();
    let hooks = Arc::new(RolloutMaintenance {
        migrate: Some(Arc::new(|_, _| {
            Box::pin(async { Err("migration source is unavailable".into()) })
        })),
        ..Default::default()
    });
    let mut client = maintenance_client(root.path(), hooks, "");
    client.initialize().await;
    for (offset, (method, params)) in [
        ("engine/compressRollouts", json!({"force":true})),
        ("engine/migrateRollouts", json!({"mode":"force"})),
        (
            "engine/migrateRollouts",
            json!({"expectedSourceRevisions":[]}),
        ),
        (
            "engine/migrateRollouts",
            json!({"expectedSourceRevisions":{"/source":4}}),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let id = offset as u64 + 2;
        client
            .send(json!({"id":id,"method":method,"params":params}))
            .await;
        assert_eq!(
            client.response_for(id).await.0["error"]["code"],
            wire::INVALID_PARAMS
        );
    }
    client
        .send(json!({"id":8,"method":"engine/migrateRollouts","params":{}}))
        .await;
    let failure = client.response_for(8).await.0;
    assert_eq!(failure["error"]["code"], wire::ENGINE_ERROR);
    assert_eq!(
        failure["error"]["message"],
        "migration source is unavailable"
    );
    client.close().await;
}

#[tokio::test]
async fn manual_compression_works_with_feature_off_and_disconnect_waits_for_safe_cleanup() {
    let root = tempfile::tempdir().unwrap();
    let entered = Arc::new(Notify::new());
    let stopped = Arc::new(AtomicBool::new(false));
    let hooks = Arc::new(RolloutMaintenance {
        compress: Some(Arc::new({
            let entered = entered.clone();
            let stopped = stopped.clone();
            move |cancelled| {
                let entered = entered.clone();
                let stopped = stopped.clone();
                Box::pin(async move {
                    entered.notify_one();
                    while !cancelled.load(Ordering::SeqCst) {
                        tokio::task::yield_now().await;
                    }
                    stopped.store(true, Ordering::SeqCst);
                    Err("compression cancelled after safe cleanup".into())
                })
            }
        })),
        ..Default::default()
    });
    let mut client = maintenance_client(root.path(), hooks.clone(), "");
    client.initialize().await;
    client
        .send(json!({"id":2,"method":"engine/compressRollouts","params":{}}))
        .await;
    assert_eq!(client.response_for(2).await.0["result"], json!({}));
    tokio::time::timeout(REPLY_TIMEOUT, entered.notified())
        .await
        .unwrap();
    client.close().await;
    assert!(stopped.load(Ordering::SeqCst));
    assert!(!hooks.compression_pending.load(Ordering::SeqCst));
}

#[tokio::test]
async fn disconnect_cancels_migration_cooperatively_and_drains_its_owner() {
    let root = tempfile::tempdir().unwrap();
    let entered = Arc::new(Notify::new());
    let stopped = Arc::new(AtomicBool::new(false));
    let hooks = Arc::new(RolloutMaintenance {
        migrate: Some(Arc::new({
            let entered = entered.clone();
            let stopped = stopped.clone();
            move |_, cancelled| {
                let entered = entered.clone();
                let stopped = stopped.clone();
                Box::pin(async move {
                    entered.notify_one();
                    while !cancelled.load(Ordering::SeqCst) {
                        tokio::task::yield_now().await;
                    }
                    stopped.store(true, Ordering::SeqCst);
                    Ok(report(
                        json!({"busy":false,"files":[{"status":"cancelled"}]}),
                    ))
                })
            }
        })),
        ..Default::default()
    });
    let mut client = maintenance_client(root.path(), hooks, "");
    client.initialize().await;
    client
        .send(json!({"id":2,"method":"engine/migrateRollouts","params":{"mode":"apply"}}))
        .await;
    tokio::time::timeout(REPLY_TIMEOUT, entered.notified())
        .await
        .unwrap();
    client.close().await;
    assert!(
        stopped.load(Ordering::SeqCst),
        "connection teardown retains the worker until cancellation is acknowledged"
    );
}

#[tokio::test]
async fn background_migration_is_explicitly_enabled_and_applies_without_blocking_handshake() {
    let root = tempfile::tempdir().unwrap();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let hooks = Arc::new(RolloutMaintenance {
        migrate: Some(Arc::new({
            let entered = entered.clone();
            let release = release.clone();
            move |request, _| {
                assert!(matches!(request.mode, RolloutMigrationMode::Apply));
                let entered = entered.clone();
                let release = release.clone();
                Box::pin(async move {
                    entered.notify_one();
                    release.notified().await;
                    Ok(report(json!({"busy":false,"files":[]})))
                })
            }
        })),
        ..Default::default()
    });
    let mut client = maintenance_client(
        root.path(),
        hooks,
        "[features]\nbackground_rollout_migration = true\n",
    );
    client.initialize().await;
    tokio::time::timeout(REPLY_TIMEOUT, entered.notified())
        .await
        .unwrap();
    client
        .send(json!({"id":2,"method":"config/read","params":{}}))
        .await;
    assert!(client.response_for(2).await.0.get("result").is_some());
    release.notify_one();
    client.close().await;
}

#[tokio::test]
async fn both_startup_features_run_in_order_without_losing_the_second_pass() {
    let root = tempfile::tempdir().unwrap();
    let order = Arc::new(Mutex::new(Vec::new()));
    let migration_entered = Arc::new(Notify::new());
    let migration_release = Arc::new(Notify::new());
    let compression_entered = Arc::new(Notify::new());
    let compression_release = Arc::new(Notify::new());
    let hooks = Arc::new(RolloutMaintenance {
        migrate: Some(Arc::new({
            let order = order.clone();
            let entered = migration_entered.clone();
            let release = migration_release.clone();
            move |request, _| {
                assert!(matches!(request.mode, RolloutMigrationMode::Apply));
                let order = order.clone();
                let entered = entered.clone();
                let release = release.clone();
                Box::pin(async move {
                    order.lock().unwrap().push("migration");
                    entered.notify_one();
                    release.notified().await;
                    Ok(report(json!({"busy":false,"files":[]})))
                })
            }
        })),
        compress: Some(Arc::new({
            let order = order.clone();
            let entered = compression_entered.clone();
            let release = compression_release.clone();
            move |_cancelled| {
                let order = order.clone();
                let entered = entered.clone();
                let release = release.clone();
                Box::pin(async move {
                    order.lock().unwrap().push("compression");
                    entered.notify_one();
                    release.notified().await;
                    Ok(report(json!({"compressed":0,"failed":0})))
                })
            }
        })),
        ..Default::default()
    });
    let mut client = maintenance_client(
        root.path(),
        hooks,
        "[features]\nbackground_rollout_migration = true\ncold_rollout_compression = true\n",
    );
    client.initialize().await;
    tokio::time::timeout(REPLY_TIMEOUT, migration_entered.notified())
        .await
        .unwrap();
    assert_eq!(*order.lock().unwrap(), vec!["migration"]);
    migration_release.notify_one();
    tokio::time::timeout(REPLY_TIMEOUT, compression_entered.notified())
        .await
        .unwrap();
    assert_eq!(*order.lock().unwrap(), vec!["migration", "compression"]);
    compression_release.notify_one();
    client.close().await;
}

#[tokio::test]
async fn production_cold_compression_kick_preserves_provider_free_history_reads() {
    use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
    use klaus_agent_core::store::session_store::SessionStore;
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("sessions");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("1700000000000-cold-facade.jsonl");
    std::fs::write(&path, concat!(
        "{\"kind\":\"session_meta\",\"schemaVersion\":3,\"meta\":{\"id\":\"cold-facade\",\"createdAt\":1700000000000,\"cwd\":\"/tmp\",\"model\":\"fixture\"}}\n",
        "{\"kind\":\"message\",\"message\":{\"type\":\"user\",\"uuid\":\"cold-u1\",\"content\":[{\"type\":\"text\",\"text\":\"Cold stored request.\"}]}}\n"
    )).unwrap();
    let cold = std::time::SystemTime::now() - Duration::from_secs(8 * 24 * 60 * 60);
    std::fs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_times(std::fs::FileTimes::new().set_modified(cold))
        .unwrap();
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: Some(root.path().join("archived").to_string_lossy().into_owned()),
    }));
    let lifecycle = crate::lifecycle::protocol_lifecycle_in_test_root(
        make_agent_for_tests(vec![]),
        Some(store),
        root.path().to_string_lossy().into_owned().into(),
    );
    let hooks = lifecycle.rollout_maintenance.as_ref().unwrap().clone();
    let config_path = root.path().join("config.toml");
    std::fs::write(&config_path, "").unwrap();
    let mut client = TestClient::start_full(
        make_create_session(&[]),
        Some(lifecycle),
        Some(root.path().join("rules.json")),
        Some(config_path),
    );
    client.initialize().await;
    client
        .send(json!({"id":2,"method":"engine/compressRollouts","params":{}}))
        .await;
    assert_eq!(client.response_for(2).await.0["result"], json!({}));
    tokio::time::timeout(REPLY_TIMEOUT, async {
        while hooks.compression_pending.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!path.exists());
    assert!(path.with_extension("jsonl.zst").exists());
    client
        .send(
            json!({"id":3,"method":"engine/readThreadHistory","params":{"threadId":"cold-facade"}}),
        )
        .await;
    let history = client.response_for(3).await.0;
    assert!(history.get("error").is_none(), "{history}");
    assert!(
        history.to_string().contains("Cold stored request."),
        "{history}"
    );
    client.close().await;
}

#[tokio::test]
async fn production_store_preview_apply_and_restore_keep_the_thread_identity() {
    use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
    use klaus_agent_core::store::session_store::SessionStore;
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("sessions");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("1700000000000-migration-facade.jsonl");
    let original = concat!(
        "{\"kind\":\"session_meta\",\"schemaVersion\":3,\"meta\":{\"id\":\"migration-facade\",\"createdAt\":1700000000000,\"cwd\":\"/tmp\",\"model\":\"fixture\"}}\n",
        "{\"kind\":\"message\",\"message\":{\"type\":\"user\",\"uuid\":\"u1\",\"content\":[{\"type\":\"text\",\"text\":\"Inspect only.\"}]}}\n"
    );
    std::fs::write(&path, original).unwrap();
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: Some(root.path().join("archived").to_string_lossy().into_owned()),
    }));
    let agent = make_agent_for_tests(vec![]);
    let lifecycle = crate::lifecycle::protocol_lifecycle_in_test_root(
        agent,
        Some(store.clone()),
        root.path().to_string_lossy().into_owned().into(),
    );
    let config_path = root.path().join("config.toml");
    std::fs::write(&config_path, "").unwrap();
    let mut client = TestClient::start_full(
        make_create_session(&[]),
        Some(lifecycle),
        Some(root.path().join("rules.json")),
        Some(config_path),
    );
    client.initialize().await;
    client
        .send(json!({"id":2,"method":"engine/migrateRollouts","params":{}}))
        .await;
    let preview = client.response_for(2).await.0;
    assert!(preview.get("error").is_none(), "{preview}");
    assert_eq!(std::fs::read(&path).unwrap(), original.as_bytes());
    assert!(!preview["result"]["files"].as_array().unwrap().is_empty());
    client
        .send(json!({"id":3,"method":"engine/migrateRollouts","params":{"mode":"apply"}}))
        .await;
    let applied = client.response_for(3).await.0;
    assert!(applied.get("error").is_none(), "{applied}");
    assert!(
        applied["result"]["files"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["status"] == "applied"),
        "{applied}"
    );
    assert_ne!(std::fs::read(&path).unwrap(), original.as_bytes());
    client.send(json!({"id":4,"method":"engine/readThreadHistory","params":{"threadId":"migration-facade"}})).await;
    let restored = client.response_for(4).await.0;
    assert!(restored.get("error").is_none(), "{restored}");
    assert!(restored.to_string().contains("Inspect only."), "{restored}");
    let child = store
        .open_sub_agent(
            "migration-facade",
            "bounded-child",
            klaus_agent_core::store::session_store::SubAgentMeta {
                agent_type: None,
                model: "fixture".into(),
                permission_mode: None,
                effort: None,
                created_at: 1_700_000_000_000,
            },
        )
        .await
        .unwrap();
    child.append_event(
        "rollout_migration".into(),
        Some(klaus_agent_core::store::session_store::AppendEventOptions {
            turn_id: None,
            payload: Some(report(json!({"version":1,"cursorEpoch":"bounded-child-epoch","auditTruncated":true,"fullyNormalized":true,"retainedReasons":[]}))),
        }),
    ).await;
    child.flush().await.unwrap();
    child.shutdown().await.unwrap();
    client.send(json!({"id":5,"method":"engine/readSubagentHistory","params":{"threadId":"migration-facade","agentId":"bounded-child"}})).await;
    let child_history = client.response_for(5).await.0;
    assert!(child_history.get("error").is_none(), "{child_history}");
    assert_eq!(child_history["result"]["auditTruncated"], true);
    client.close().await;
}
