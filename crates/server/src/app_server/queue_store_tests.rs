use super::*;
use serde_json::json;

#[test]
fn borrowed_header_matches_owned_serialization_without_changing_native_input() {
    let metadata: RuntimeValue = serde_json::from_str(
        r#"{"pair":"\ud83d\ude00","lone":"\ud800","number":9007199254740993,"nested":[null,{"x":"value"}]}"#,
    )
    .unwrap();
    let mut row = QueuedSubmission {
        id: "header-row".into(),
        input: vec![metadata.clone()],
        input_omitted: false,
        input_fingerprint: "sha256:input".into(),
        row_version: 17,
        client_user_message_id: None,
        client_metadata: None,
        paused_reason: None,
        created_at: 123,
        dispatching: None,
    };
    for populated in [false, true] {
        if populated {
            row.client_user_message_id = Some("client".into());
            row.client_metadata = Some(metadata.clone());
            row.paused_reason = Some(INTERRUPTED_REASON.into());
            row.dispatching = Some(QueueDispatch {
                submission_id: "submission".into(),
                mode: super::super::wire::TurnSubmitMode::Steer,
            });
        }
        let before = encode(&row).unwrap();
        let mut owned = row.clone();
        owned.input.clear();
        owned.input_omitted = true;
        assert_eq!(header(&row).unwrap(), encode(&owned).unwrap());
        assert_eq!(encode(&row).unwrap(), before);
    }
}

fn proof_fixture(
    store: &Store,
    thread: &str,
    client: &str,
) -> (ThreadQueueAddParams, String, String) {
    let mut input = add_params(client);
    input.thread_id = thread.into();
    let row = store
        .add(input.clone())
        .unwrap()
        .0
        .queued_submission
        .unwrap();
    let mutation = format!("delete-{client}");
    store.delete(serde_json::from_value(json!({
        "threadId":thread,"queuedSubmissionId":row.id,"mutationId":mutation,"includeInput":false
    })).unwrap()).unwrap();
    let c = store.open().unwrap();
    let proof = c
        .query_row(
            "SELECT id FROM queue_proofs WHERE thread_id=?1 AND row_id=?2",
            params![thread, row.id],
            |r| r.get(0),
        )
        .unwrap();
    (input, proof, row.id)
}

#[test]
fn profile_global_count_eviction_releases_native_payload_and_preserves_identity() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(root.path().join("queue.sqlite3"));
    let (mut restore, proof, row_id) = proof_fixture(&store, "first-thread", "old-proof");
    let mut c = store.open().unwrap();
    c.execute(
        "UPDATE queue_proofs SET created_at=?1 WHERE id=?2",
        params![now() - 1000, proof],
    )
    .unwrap();
    // Synthetic proof headers isolate global eviction policy from payload size.
    // The actual proof/payload under observation was created by add and delete.
    let tx = c.transaction().unwrap();
    for i in 0..512 {
        tx.execute(
            "INSERT INTO queue_proofs VALUES(?1,'other-thread',?1,'{}',NULL,1,?2)",
            params![format!("synthetic-{i}"), now() + i],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    let mut trigger = add_params("trigger");
    trigger.thread_id = "third-thread".into();
    store.add(trigger).unwrap();
    assert_eq!(
        c.query_row("SELECT count(*) FROM queue_proofs", [], |r| r
            .get::<_, usize>(0))
            .unwrap(),
        512
    );
    assert_eq!(
        c.query_row(
            "SELECT count(*) FROM queue_proofs WHERE id=?1",
            [&proof],
            |r| r.get::<_, usize>(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(
        c.query_row("SELECT count(*) FROM queue_payloads", [], |r| r
            .get::<_, usize>(0))
            .unwrap(),
        1
    );
    restore.restore = true;
    restore.require_removed_receipt = true;
    restore.input.clear();
    assert!(matches!(store.add(restore.clone()), Err(Error::Invalid(_))));
    let replay = store.delete(serde_json::from_value(json!({
        "threadId":"first-thread","queuedSubmissionId":row_id,"mutationId":"delete-old-proof"
    })).unwrap()).unwrap().0;
    assert!(replay.replayed && replay.deleted && replay.queued_submission.is_none());
    restore.lookup_only = true;
    restore.restore = false;
    let identity = store.lookup(&restore).unwrap();
    assert_eq!(identity.found, Some(true));
    assert!(identity.queued_submission.is_none());
}

#[test]
fn profile_global_ui_bytes_evict_proof_but_pending_reference_keeps_native_payload() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(root.path().join("queue.sqlite3"));
    let (mut restore, proof, row_id) = proof_fixture(&store, "first-thread", "retained");
    restore.restore = true;
    restore.require_removed_receipt = true;
    restore.input.clear();
    store.add(restore).unwrap();
    let c = store.open().unwrap();
    let payload = payload_id(&c, "first-thread", &row_id).unwrap().unwrap();
    c.execute(
        "UPDATE queue_proofs SET created_at=?1,bytes=?2 WHERE id=?3",
        params![now() - 1000, 8 * 1024 * 1024, proof],
    )
    .unwrap();
    c.execute("INSERT INTO queue_proofs VALUES('large-new-proof','other-thread','synthetic','{}',NULL,?1,?2)",
        params![9*1024*1024,now()+100]).unwrap();
    let mut trigger = add_params("trigger");
    trigger.thread_id = "third-thread".into();
    store.add(trigger).unwrap();
    assert_eq!(
        c.query_row(
            "SELECT count(*) FROM queue_proofs WHERE id=?1",
            [&proof],
            |r| r.get::<_, usize>(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(
        c.query_row(
            "SELECT count(*) FROM queue_payloads WHERE id=?1",
            [&payload],
            |r| r.get::<_, usize>(0)
        )
        .unwrap(),
        1
    );
    assert_eq!(store.read("first-thread").unwrap().rows.len(), 1);
    store
        .transact("first-thread", |d| {
            let row = d.rows.remove(0);
            let receipt = d.receipt(row, "started");
            d.remember(receipt, None);
            Ok(((), true))
        })
        .unwrap();
    assert_eq!(
        c.query_row(
            "SELECT count(*) FROM queue_payloads WHERE id=?1",
            [&payload],
            |r| r.get::<_, usize>(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(
        c.query_row(
            "SELECT count(*) FROM queue_proofs WHERE thread_id='other-thread'",
            [],
            |r| r.get::<_, usize>(0)
        )
        .unwrap(),
        1
    );
}

#[test]
fn profile_global_age_expiry_denies_undo_before_mutation_prunes_native_bytes() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(root.path().join("queue.sqlite3"));
    let (mut restore, proof, row_id) = proof_fixture(&store, "first-thread", "expired");
    let c = store.open().unwrap();
    c.execute(
        "UPDATE queue_proofs SET created_at=?1 WHERE id=?2",
        params![now() - PROOF_AGE - 1, proof],
    )
    .unwrap();
    assert_eq!(
        c.query_row("SELECT count(*) FROM queue_payloads", [], |r| r
            .get::<_, usize>(0))
            .unwrap(),
        1
    );
    restore.restore = true;
    restore.require_removed_receipt = true;
    restore.input.clear();
    assert!(matches!(store.add(restore), Err(Error::Invalid(_))));
    let replay = store
        .delete(
            serde_json::from_value(json!({
                "threadId":"first-thread","queuedSubmissionId":row_id,"mutationId":"delete-expired"
            }))
            .unwrap(),
        )
        .unwrap()
        .0;
    assert!(replay.replayed && replay.queued_submission.is_none());
    assert_eq!(
        c.query_row("SELECT count(*) FROM queue_payloads", [], |r| r
            .get::<_, usize>(0))
            .unwrap(),
        1
    );
    let mut trigger = add_params("trigger");
    trigger.thread_id = "other-thread".into();
    store.add(trigger).unwrap();
    assert_eq!(
        c.query_row("SELECT count(*) FROM queue_proofs", [], |r| r
            .get::<_, usize>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        c.query_row("SELECT count(*) FROM queue_payloads", [], |r| r
            .get::<_, usize>(0))
            .unwrap(),
        1
    );
}

#[test]
fn profile_global_expired_legacy_proof_keeps_only_the_original_selector_identity() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(root.path().join("queue.sqlite3"));
    store.import(serde_json::from_value(json!({
        "threadId":"legacy","migrationId":"legacy-receipt","minimumRevision":0,"queuedSubmissions":[],
        "receipts":[{"kind":"removal","mutationId":"old-remove","fingerprint":"old-fingerprint",
            "createdAt":now(),"result":{"removed":{"id":"original-client","item":{"text":"old UI"},"createdAt":42}}}]
    })).unwrap()).unwrap();
    let c = store.open().unwrap();
    c.execute("UPDATE queue_proofs SET created_at=0", [])
        .unwrap();
    let valid = json!({"threadId":"legacy","queuedSubmissionId":"original-client",
        "mutationId":"old-remove","mutationFingerprint":"old-fingerprint"});
    let replay = store
        .delete(serde_json::from_value(valid.clone()).unwrap())
        .unwrap()
        .0;
    assert!(replay.replayed && replay.deleted && replay.queued_submission.is_none());
    let mut wrong = valid;
    wrong["queuedSubmissionId"] = json!("other-client");
    assert!(matches!(
        store.delete(serde_json::from_value(wrong).unwrap()),
        Err(Error::Invalid(_))
    ));
    let compact: String = c
        .query_row(
            "SELECT receipt FROM queue_identities WHERE key='legacy-target:old-remove'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(compact.len() < 1024 && !compact.contains("old UI"));
}

#[test]
fn normal_add_and_restore_capacity_charges_only_ui_items_at_exact_boundaries() {
    let mut params = add_params("boundary");
    params.client_metadata = Some(
        serde_json::from_value(json!({"item":{"text":"x".repeat(MAX_ITEM_BYTES as usize - 11)}}))
            .unwrap(),
    );
    let row = QueuedSubmission {
        id: "boundary".into(),
        input: vec![],
        input_omitted: false,
        input_fingerprint: fingerprint(&[]).unwrap(),
        row_version: 1,
        client_user_message_id: Some("boundary".into()),
        client_metadata: params.client_metadata,
        paused_reason: None,
        created_at: 42,
        dispatching: None,
    };
    assert_eq!(item_bytes(&row).unwrap(), MAX_ITEM_BYTES);
    let mut doc = Document::default();
    assert!(check_capacity(&doc, &row).is_ok());
    doc.rows = vec![row.clone(); 7];
    assert!(check_capacity(&doc, &row).is_ok());
    doc.rows.push(row.clone());
    assert!(matches!(check_capacity(&doc, &row), Err(Error::Invalid(_))));
    let mut external = row;
    external.client_metadata = None;
    assert!(check_capacity(&doc, &external).is_ok());
}

#[test]
fn restore_rejects_full_queue_without_losing_its_deletion_proof() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(root.path().join("queue.sqlite3"));
    let params = add_params("restore");
    let row = store
        .add(params.clone())
        .unwrap()
        .0
        .queued_submission
        .unwrap();
    store
        .delete(
            serde_json::from_value(json!({
                "threadId":"thread","queuedSubmissionId":row.id
            }))
            .unwrap(),
        )
        .unwrap();
    store.import(serde_json::from_value(json!({
        "threadId":"thread","migrationId":"full","minimumRevision":0,
        "queuedSubmissions":(0..128).map(|i|json!({"clientUserMessageId":format!("seed-{i}"),"input":[]})).collect::<Vec<_>>()
    })).unwrap()).unwrap();
    let mut restore = params;
    restore.restore = true;
    restore.require_removed_receipt = true;
    restore.input.clear();
    assert!(matches!(store.add(restore), Err(Error::Invalid(_))));
    let c = store.open().unwrap();
    assert_eq!(
        c.query_row("SELECT count(*) FROM queue_proofs", [], |r| r
            .get::<_, usize>(0))
            .unwrap(),
        1
    );
    assert_eq!(store.read("thread").unwrap().rows.len(), 128);
}

#[test]
fn normalized_payload_reference_moves_to_undo_and_consumption_collects_it() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(root.path().join("queue.sqlite3"));
    let mut input = add_params("image");
    input.input =
        serde_json::from_value(json!([{"type":"image","base64":"a".repeat(8*1024*1024)}])).unwrap();
    input.client_metadata = Some(
        serde_json::from_value(
            json!({"item":{"images":["a".repeat(4*1024*1024-256),"b".repeat(4*1024*1024-256)]}}),
        )
        .unwrap(),
    );
    let row = store
        .add(input.clone())
        .unwrap()
        .0
        .queued_submission
        .unwrap();
    let c = store.open().unwrap();
    let count = || {
        c.query_row("SELECT count(*) FROM queue_payloads", [], |r| {
            r.get::<_, usize>(0)
        })
        .unwrap()
    };
    assert_eq!(count(), 1);
    let delete = serde_json::from_value(
        json!({"threadId":"thread","queuedSubmissionId":row.id,"includeInput":false}),
    )
    .unwrap();
    store.delete(delete).unwrap();
    assert_eq!(count(), 1);
    let bytes: u64 = c
        .query_row("SELECT bytes FROM queue_proofs", [], |r| r.get(0))
        .unwrap();
    assert!(bytes < 9 * 1024 * 1024);
    input.restore = true;
    input.require_removed_receipt = true;
    input.input.clear();
    store.add(input).unwrap();
    assert_eq!(count(), 1);
    store
        .transact("thread", |d| {
            let row = d.rows.remove(0);
            let receipt = d.receipt(row, "started");
            d.remember(receipt, None);
            Ok(((), true))
        })
        .unwrap();
    assert_eq!(count(), 0);
    let identity: String = c
        .query_row("SELECT receipt FROM queue_identities LIMIT 1", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert!(!identity.contains("base64"));
    assert!(identity.len() < 1024);
}

#[test]
fn normalized_clear_trigger_failure_rolls_back_rows_revision_and_identities() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(root.path().join("queue.sqlite3"));
    store.add(add_params("one")).unwrap();
    store.add(add_params("two")).unwrap();
    let c = store.open().unwrap();
    c.execute_batch("CREATE TRIGGER fail_second BEFORE DELETE ON queue_entries WHEN OLD.position=1 BEGIN SELECT RAISE(ABORT,'injected clear failure'); END;").unwrap();
    let clear = serde_json::from_value(json!({"threadId":"thread","includeInput":false})).unwrap();
    assert!(store.clear_rows(clear).is_err());
    let d = store.read("thread").unwrap();
    assert_eq!(d.revision, 2);
    assert_eq!(d.rows.len(), 2);
    assert_eq!(
        store
            .add(add_params("one"))
            .unwrap()
            .0
            .receipt
            .unwrap()
            .deserialize::<serde_json::Value>()
            .unwrap()["kind"],
        "added"
    );
}

#[test]
fn normalized_v1_migration_is_atomic_and_preserves_exact_pending_input() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(root.path().join("queue.sqlite3"));
    let c = store.open().unwrap();
    c.execute(
        "INSERT INTO thread_queues_v1 VALUES('broken',7,'invalid')",
        [],
    )
    .unwrap();
    assert!(store.read("broken").is_err());
    assert_eq!(
        c.query_row(
            "SELECT document FROM thread_queues_v1 WHERE thread_id='broken'",
            [],
            |r| r.get::<_, String>(0)
        )
        .unwrap(),
        "invalid"
    );
    let row: QueuedSubmission = serde_json::from_str(r#"{"id":"old","input":[{"rawRuntimeJson":"{\"type\":\"text\",\"text\":\"\\ud800\"}"}],"createdAt":42,"clientUserMessageId":"old","clientMetadata":{"item":{"text":"old"}}}"#).unwrap();
    let d = Document {
        revision: 9,
        rows: vec![row.clone()],
        ..Default::default()
    };
    c.execute(
        "INSERT INTO thread_queues_v1 VALUES('old',9,?1)",
        [encode(&d).unwrap()],
    )
    .unwrap();
    let migrated = store.read("old").unwrap();
    assert_eq!(migrated.revision, 9);
    assert_eq!(migrated.rows[0].input, row.input);
    assert!(migrated.rows[0].input_fingerprint.starts_with("sha256:"));
    assert_eq!(
        c.query_row(
            "SELECT count(*) FROM thread_queues_v1 WHERE thread_id='old'",
            [],
            |r| r.get::<_, usize>(0)
        )
        .unwrap(),
        0
    );
}

#[test]
fn projected_list_preserves_runtime_bytes_pagination_and_external_revision_fences() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("queue.sqlite3");
    let store = Store::new(path.clone());
    let mut first = add_params("first");
    first.client_metadata = Some(RawJson::from_json(r#"{"item":{"text":"\ud800\udc00\udfff","number":9007199254740993,"negativeZero":-0,"unknown":[null,{"key":"\ud800"}]}}"#.into()).unwrap().to_runtime().unwrap());
    store.add(first).unwrap();
    store.add(add_params("second")).unwrap();
    let params: ThreadQueueListParams =
        serde_json::from_value(json!({"threadId":"thread","includeInput":false,"limit":1}))
            .unwrap();
    let expected = store.list(params.clone()).unwrap();
    let (actual, revision) = store.list_projected(params.clone()).unwrap();
    assert_eq!(
        actual.as_str(),
        RawJson::from_serialize(&expected).unwrap().as_str()
    );
    assert_eq!(revision, expected.revision);
    assert!(actual.as_str().contains("inputOmitted"));
    let next: ThreadQueueListParams = serde_json::from_value(
        json!({"threadId":"thread","includeInput":false,"limit":1,"cursor":expected.next_cursor}),
    )
    .unwrap();
    assert_eq!(
        store.list_projected(next.clone()).unwrap().0.as_str(),
        RawJson::from_serialize(&store.list(next.clone()).unwrap())
            .unwrap()
            .as_str()
    );
    Store::new(path).add(add_params("third")).unwrap();
    assert!(matches!(
        store.list_projected(next),
        Err(Error::Conflict { .. })
    ));
    assert_eq!(store.list_projected(params).unwrap().1, revision + 1);
}

#[test]
fn transaction_preserves_equivalent_header_spelling_and_payload_identity() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(root.path().join("queue.sqlite3"));
    let first = store
        .add(add_params("first"))
        .unwrap()
        .0
        .queued_submission
        .unwrap();
    let connection = store.open().unwrap();
    let (original, payload): (String, String) = connection
        .query_row(
            "SELECT header,payload_id FROM queue_entries WHERE thread_id='thread' AND id=?1",
            [&first.id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let equivalent = format!(" \n{original}\n ");
    connection
        .execute(
            "UPDATE queue_entries SET header=?1 WHERE id=?2",
            params![equivalent, first.id],
        )
        .unwrap();
    store.add(add_params("second")).unwrap();
    let retained = store.entry("thread", &first.id, true).unwrap().unwrap();
    assert_eq!(retained.row_version, first.row_version);
    assert_eq!(retained.input, first.input);
    assert_eq!(retained.client_metadata, first.client_metadata);
    assert_eq!(retained.input_fingerprint, first.input_fingerprint);
    let stored: (String, String) = connection
        .query_row(
            "SELECT header,payload_id FROM queue_entries WHERE id=?1",
            [&first.id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(stored, (equivalent, payload));
}

#[test]
fn transaction_rejects_mismatched_header_identity_without_partial_mutation() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(root.path().join("queue.sqlite3"));
    let first = store
        .add(add_params("first"))
        .unwrap()
        .0
        .queued_submission
        .unwrap();
    let connection = store.open().unwrap();
    let mut wrong = first.clone();
    wrong.id = "different-stored-key".into();
    let malformed = header(&wrong).unwrap();
    connection
        .execute(
            "UPDATE queue_entries SET header=?1 WHERE id=?2",
            params![malformed, first.id],
        )
        .unwrap();
    assert!(
        matches!(store.add(add_params("second")), Err(Error::Storage(message)) if message.contains("identity"))
    );
    assert_eq!(revision(&connection, "thread").unwrap(), 1);
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM queue_entries", [], |row| row
                .get::<_, usize>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT header FROM queue_entries WHERE id=?1",
                [&first.id],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
        malformed
    );
    assert_eq!(
        store.lookup(&add_params("second")).unwrap().found,
        Some(false)
    );
}

fn add_params(client: &str) -> ThreadQueueAddParams {
    serde_json::from_value(json!({"threadId":"thread","input":[{"type":"text","text":client}],
        "clientUserMessageId":client,"clientMetadata":{"item":{"text":client,"files":["/tmp/source"]}},
        "createdAt":42})).unwrap()
}

#[test]
fn persistence_reopens_complete_input_metadata_order_and_confirmed_receipts() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("queue.sqlite3");
    let store = Store::new(path.clone());
    let (first, _) = store.add(add_params("first")).unwrap();
    let (second, _) = store.add(add_params("second")).unwrap();
    drop(store);
    let store = Store::new(path);
    let reopened = store.read("thread").unwrap();
    assert_eq!(reopened.revision, 2);
    assert_eq!(
        reopened.rows[0].id,
        first.queued_submission.as_ref().unwrap().id
    );
    assert_eq!(
        reopened.rows[1].id,
        second.queued_submission.as_ref().unwrap().id
    );
    assert_eq!(reopened.rows[0].input, add_params("first").input);
    assert_eq!(
        reopened.rows[0].client_metadata,
        add_params("first").client_metadata
    );
    let (replay, changed) = store.add(add_params("first")).unwrap();
    assert!(replay.replayed);
    assert!(!changed);
    assert_eq!(replay.revision, 2);
}

#[test]
fn concurrent_connections_do_not_lose_adds_or_client_deduplication() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("queue.sqlite3");
    Store::new(path.clone()).read("thread").unwrap();
    let workers: Vec<_> = (0..32)
        .map(|index| {
            let path = path.clone();
            std::thread::spawn(move || {
                Store::new(path)
                    .add(add_params(&format!("client-{}", index % 16)))
                    .unwrap()
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    let document = Store::new(path).read("thread").unwrap();
    assert_eq!(document.revision, 16);
    assert_eq!(document.rows.len(), 16);
    assert_eq!(
        document
            .rows
            .iter()
            .map(|row| row.id.clone())
            .collect::<HashSet<_>>()
            .len(),
        16
    );
}

#[test]
fn confirmed_delete_restore_uses_original_prepared_input_and_does_not_restore_consumed_input() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(root.path().join("queue.sqlite3"));
    let (added, _) = store.add(add_params("first")).unwrap();
    let original = added.queued_submission.unwrap();
    let delete: ThreadQueueDeleteParams = serde_json::from_value(
        json!({"threadId":"thread","queuedSubmissionId":original.id,"mutationId":"remove-1"}),
    )
    .unwrap();
    let (removed, changed) = store.delete(delete.clone()).unwrap();
    assert!(changed && removed.deleted);
    assert!(store.delete(delete).unwrap().0.replayed);
    let mut restore = add_params("first");
    restore.restore = true;
    restore.require_removed_receipt = true;
    restore.input = Vec::new();
    let (restored, _) = store.add(restore.clone()).unwrap();
    assert_eq!(
        restored.queued_submission.as_ref().unwrap().input,
        original.input
    );
    assert_eq!(restored.queued_submission.as_ref().unwrap().id, original.id);
    store
        .transact("thread", |document| {
            let row = document.rows.remove(0);
            let receipt = document.receipt(row, "started");
            document.remember(receipt, None);
            Ok(((), true))
        })
        .unwrap();
    assert!(matches!(store.add(restore), Err(Error::Invalid(_))));
    let (replayed, changed) = store.add(add_params("first")).unwrap();
    assert!(replayed.replayed && !changed);
    assert!(store.read("thread").unwrap().rows.is_empty());
}

#[test]
fn stale_revision_rejects_mutation_but_successful_unknown_ack_replays_before_cas() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(root.path().join("queue.sqlite3"));
    let mut first = add_params("first");
    first.expected_revision = Some(0);
    first.mutation_id = Some("add-1".into());
    store.add(first.clone()).unwrap();
    assert!(store.add(first).unwrap().0.replayed);
    let mut second = add_params("second");
    second.expected_revision = Some(0);
    assert!(matches!(
        store.add(second),
        Err(Error::Conflict {
            expected: 0,
            actual: 1
        })
    ));
    assert_eq!(store.read("thread").unwrap().rows.len(), 1);
}

#[test]
fn exact_runtime_strings_survive_sqlite_without_scalar_json_projection() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(root.path().join("queue.sqlite3"));
    let params: ThreadQueueAddParams = serde_json::from_str(r#"{"threadId":"thread","input":[{"type":"text","text":"\ud800"}],"clientUserMessageId":"exact","clientMetadata":{"item":{"text":"\udfff","large":9007199254740993}}}"#).unwrap();
    store.add(params.clone()).unwrap();
    let row = store.read("thread").unwrap().rows.remove(0);
    assert_eq!(row.input, params.input);
    assert_eq!(row.client_metadata, params.client_metadata);
}

#[test]
fn unconfirmed_admission_is_durable_and_never_erases_input() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("queue.sqlite3");
    let store = Store::new(path.clone());
    store.add(add_params("first")).unwrap();
    store
        .transact("thread", |document| {
            document.rows[0].dispatching = Some(QueueDispatch {
                submission_id: "unknown".into(),
                mode: super::super::wire::TurnSubmitMode::StartIfIdle,
            });
            document.rows[0].paused_reason = Some("admission-unknown".into());
            Ok(((), true))
        })
        .unwrap();
    drop(store);
    let document = Store::new(path).read("thread").unwrap();
    assert_eq!(
        document.rows[0].dispatching.as_ref().unwrap().submission_id,
        "unknown"
    );
    assert_eq!(
        document.rows[0].paused_reason.as_deref(),
        Some("admission-unknown")
    );
    assert_eq!(document.rows[0].input, add_params("first").input);
}

#[tokio::test]
async fn thread_locks_coordinate_connections_without_blocking_other_threads_or_reads() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(root.path().join("queue.sqlite3"));
    store.add(add_params("first")).unwrap();
    let lock = store.lock("thread").await.unwrap();
    let blocked = tokio::spawn({
        let store = store.clone();
        async move { store.lock("thread").await.unwrap() }
    });
    let other = tokio::time::timeout(std::time::Duration::from_secs(1), store.lock("other"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(store.read("thread").unwrap().rows.len(), 1);
    assert!(!blocked.is_finished());
    drop(other);
    drop(lock);
    tokio::time::timeout(std::time::Duration::from_secs(1), blocked)
        .await
        .unwrap()
        .unwrap();
}

#[test]
fn endpoint_reuses_one_writer_and_reads_observe_external_commits() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("queue.sqlite3");
    let store = Store::new(path.clone());
    for i in 0..8 {
        let p = add_params(&format!("local-{i}"));
        let (added, _) = store.add(p.clone()).unwrap();
        store
            .entry("thread", &added.queued_submission.unwrap().id, true)
            .unwrap();
        let mut lookup = p;
        lookup.lookup_only = true;
        assert_eq!(store.lookup(&lookup).unwrap().found, Some(true));
        assert_eq!(
            store.read_projected("thread", false).unwrap().revision,
            i + 1
        );
    }
    assert_eq!(store.0.writer_opens.load(Ordering::Relaxed), 1);
    let foreign = Store::new(path);
    foreign.add(add_params("foreign")).unwrap();
    let latest = store
        .list(
            serde_json::from_value(serde_json::json!({
                "threadId":"thread","includeInput":false
            }))
            .unwrap(),
        )
        .unwrap();
    assert_eq!(latest.revision, 9);
    assert_eq!(latest.data.len(), 9);
    assert_eq!(store.0.writer_opens.load(Ordering::Relaxed), 1);
    store
        .with_connection(|c| {
            assert_eq!(
                c.query_row("PRAGMA synchronous", [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                2
            );
            assert_eq!(
                c.query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
                    .unwrap(),
                "wal"
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn initialized_list_and_lookup_do_not_wait_for_the_busy_writer_mutex() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(root.path().join("queue.sqlite3"));
    store.add(add_params("committed")).unwrap();
    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let writer = store.clone();
    let writer_task = std::thread::spawn(move || {
        writer.with_connection(|c| {
            c.execute_batch(
                "BEGIN IMMEDIATE; UPDATE queue_threads SET revision=99 WHERE thread_id='thread'",
            )
            .map_err(storage)?;
            held_tx.send(()).unwrap();
            release_rx
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap();
            c.execute_batch("ROLLBACK").map_err(storage)?;
            Ok(())
        })
    });
    held_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    let reader = store.clone();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let reader_task = std::thread::spawn(move || {
        let list = reader.list(
            serde_json::from_value(serde_json::json!({
                "threadId":"thread","includeInput":false
            }))
            .unwrap(),
        );
        let mut lookup = add_params("committed");
        lookup.lookup_only = true;
        result_tx.send((list, reader.lookup(&lookup))).unwrap();
    });
    let result = result_rx.recv_timeout(std::time::Duration::from_secs(2));
    release_tx.send(()).unwrap();
    writer_task.join().unwrap().unwrap();
    reader_task.join().unwrap();
    let (list, lookup) = result.expect("WAL metadata reads must not wait for writer mutex");
    assert_eq!(
        list.unwrap().revision,
        1,
        "uncommitted writer data must stay invisible"
    );
    assert_eq!(lookup.unwrap().revision, 1);
}

#[test]
fn full_input_clear_hydrates_inside_the_existing_writer_transaction() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(root.path().join("queue.sqlite3"));
    let input = add_params("first").input;
    store.add(add_params("first")).unwrap();
    store.add(add_params("second")).unwrap();
    let (cleared, _) = store
        .clear_rows(
            serde_json::from_value(serde_json::json!({
                "threadId":"thread","includeInput":true,"expectedRevision":2
            }))
            .unwrap(),
        )
        .unwrap();
    assert_eq!(cleared.cleared.len(), 2);
    assert_eq!(cleared.cleared[0].input, input);
    assert!(!cleared.cleared[0].input_omitted);
    assert!(store.read("thread").unwrap().rows.is_empty());
    assert_eq!(store.0.writer_opens.load(Ordering::Relaxed), 1);
}

#[test]
fn fresh_reader_rechecks_legacy_after_an_earlier_negative_lookup() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("queue.sqlite3");
    let store = Store::new(path.clone());
    assert!(store.read("later-legacy").unwrap().rows.is_empty());
    let foreign = Store::new(path);
    let connection = foreign.open().unwrap();
    let row: QueuedSubmission = serde_json::from_value(json!({
        "id":"legacy-row","input":[{"type":"text","text":"legacy input"}],
        "clientUserMessageId":"legacy","createdAt":42
    }))
    .unwrap();
    let document = Document {
        revision: 7,
        rows: vec![row.clone()],
        ..Default::default()
    };
    connection
        .execute(
            "INSERT INTO thread_queues_v1 VALUES('later-legacy',7,?1)",
            [encode(&document).unwrap()],
        )
        .unwrap();
    let migrated = store.read("later-legacy").unwrap();
    assert_eq!(migrated.revision, 7);
    assert_eq!(migrated.rows[0].input, row.input);
    assert_eq!(
        connection
            .query_row(
                "SELECT count(*) FROM thread_queues_v1 WHERE thread_id='later-legacy'",
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn sqlite_writer_busy_wait_does_not_serialize_independent_metadata_reads() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("queue.sqlite3");
    let store = Store::new(path.clone());
    store.add(add_params("committed")).unwrap();
    let foreign = Store::new(path);
    let blocker = foreign.open().unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let pending = store.clone();
    let writer = std::thread::spawn(move || {
        pending.with_connection(|connection| {
            started_tx.send(()).unwrap();
            connection
                .execute_batch("BEGIN IMMEDIATE; ROLLBACK")
                .map_err(storage)?;
            Ok(())
        })
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    let reader = store.clone();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let reading = std::thread::spawn(move || {
        result_tx
            .send(reader.read_projected("thread", false))
            .unwrap();
    });
    let result = result_rx.recv_timeout(std::time::Duration::from_secs(2));
    blocker.execute_batch("ROLLBACK").unwrap();
    writer.join().unwrap().unwrap();
    reading.join().unwrap();
    assert_eq!(
        result
            .expect("read must finish while the writer waits for SQLite")
            .unwrap()
            .revision,
        1
    );
}

#[test]
fn autocommit_storage_failure_retires_writer_without_replaying_the_operation() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(root.path().join("queue.sqlite3"));
    store.add(add_params("committed")).unwrap();
    let attempts = std::sync::atomic::AtomicUsize::new(0);
    let result: Result<()> = store.with_connection(|connection| {
        attempts.fetch_add(1, Ordering::Relaxed);
        assert!(connection.is_autocommit());
        Err(storage("injected storage failure"))
    });
    assert!(matches!(result, Err(Error::Storage(_))));
    assert_eq!(attempts.load(Ordering::Relaxed), 1);
    assert!(store.0.writer.lock().unwrap().is_none());
    assert_eq!(store.read("thread").unwrap().revision, 1);
    store.add(add_params("explicit-retry")).unwrap();
    assert_eq!(store.read("thread").unwrap().revision, 2);
    assert_eq!(store.0.writer_opens.load(Ordering::Relaxed), 2);
}

#[test]
fn unfinished_writer_transaction_is_rolled_back_and_connection_retired() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(root.path().join("queue.sqlite3"));
    store.add(add_params("first")).unwrap();
    let error: Result<()> = store.with_connection(|c| {
        c.execute_batch("BEGIN IMMEDIATE; UPDATE queue_threads SET revision=99")
            .map_err(storage)?;
        Err(storage("injected operation failure"))
    });
    assert!(error.is_err());
    assert!(store.0.writer.lock().unwrap().is_none());
    assert_eq!(store.read("thread").unwrap().revision, 1);
    store.add(add_params("second")).unwrap();
    assert_eq!(store.read("thread").unwrap().revision, 2);
    assert_eq!(store.0.writer_opens.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn endpoint_close_releases_writer_and_late_clones_cannot_reopen_it() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("queue.sqlite3");
    let store = Store::new(path.clone());
    store.add(add_params("first")).unwrap();
    let delayed = store.clone();
    store.close();
    assert!(store.0.writer.lock().unwrap().is_none());
    assert!(delayed.add(add_params("late")).is_err());
    assert!(delayed.read("thread").is_err());
    assert!(delayed.watch_connection().is_err());
    assert!(delayed.lock("thread").await.is_err());
    assert_eq!(store.0.writer_opens.load(Ordering::Relaxed), 1);
    let other_endpoint = Store::new(path);
    other_endpoint.add(add_params("other-endpoint")).unwrap();
    assert_eq!(other_endpoint.read("thread").unwrap().revision, 2);
}

#[cfg(unix)]
#[test]
fn physical_database_replacement_is_rejected_before_reusing_the_writer() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("queue.sqlite3");
    let store = Store::new(path.clone());
    store.add(add_params("first")).unwrap();
    std::fs::rename(&path, root.path().join("retired.sqlite3")).unwrap();
    std::fs::write(&path, []).unwrap();
    assert!(store.add(add_params("must-not-write-old-inode")).is_err());
    assert!(store.read("thread").is_err());
    assert!(store.watch_connection().is_err());
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    store.close();
}

#[test]
fn poisoned_writer_retires_uncommitted_work_before_an_explicit_retry() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(root.path().join("queue.sqlite3"));
    store.add(add_params("before")).unwrap();
    let poisoned = store.clone();
    assert!(std::thread::spawn(move || {
        poisoned.with_connection::<()>(|connection| {
            connection
                .execute_batch("BEGIN IMMEDIATE; UPDATE queue_threads SET revision=999;")
                .unwrap();
            panic!("fixture writer failure");
        })
    })
    .join()
    .is_err());
    let error = store.add(add_params("after")).unwrap_err();
    assert!(error.to_string().contains("poisoned"));
    let visible = store.read_projected("thread", false).unwrap();
    assert_eq!(
        visible.revision, 1,
        "uncommitted revision must be rolled back"
    );
    assert_eq!(
        visible.rows.len(),
        1,
        "the failed request does not add a row"
    );
    let (reply, changed) = store.add(add_params("after")).unwrap();
    assert!(changed);
    assert_eq!(
        reply.revision, 2,
        "a later explicit request opens a healthy writer"
    );
}
