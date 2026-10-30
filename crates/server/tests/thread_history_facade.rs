//! Full saved-history facade contract. These source-owned records pin display
//! fidelity without provider activation, continuation locks, or archive writes.

use super::*;
use crate::app_server::wire;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::rollout::parse_rollout_line;
use klaus_agent_core::store::session_store::SessionStore;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn paged_history_hook_errors_never_retry_the_whole_file() {
    for kind in [
        std::io::ErrorKind::InvalidInput,
        std::io::ErrorKind::InvalidData,
    ] {
        let mut lifecycle =
            crate::lifecycle::protocol_lifecycle(make_agent_for_tests(vec![]), None);
        let reads = Arc::new(AtomicUsize::new(0));
        let full_reads = reads.clone();
        lifecycle.read_thread = Some(Arc::new(move |_| {
            full_reads.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Err("whole-file fallback must not execute".into()) })
        }));
        lifecycle.read_thread_page = Some(Arc::new(move |_, request| {
            if request.turn_id.is_some() {
                assert_eq!(request.cursor.as_deref(), Some("cursor"));
                assert_eq!(request.limit, Some(1));
                assert_eq!(request.sort_direction.as_deref(), Some("asc"));
            }
            Box::pin(async move {
                Err(Box::new(std::io::Error::new(kind, "index read refused"))
                    as crate::lifecycle::BoxError)
            })
        }));
        let create: CreateSession =
            Arc::new(|_| Box::pin(async { Err("must not activate".into()) }));
        let mut client = TestClient::start_with(create, Some(lifecycle));
        client.initialize().await;
        for (id, method, params) in [
            (
                901,
                "engine/readThreadHistory",
                json!({"threadId":"saved","cursor":"cursor","limit":1}),
            ),
            (
                902,
                "engine/readThreadItems",
                json!({"threadId":"saved","turnId":"turn","cursor":"cursor","limit":1,"sortDirection":"asc"}),
            ),
        ] {
            let response = read(&mut client, id, method, params).await;
            assert_eq!(
                response["error"]["code"],
                if kind == std::io::ErrorKind::InvalidInput {
                    wire::INVALID_PARAMS
                } else {
                    wire::ENGINE_ERROR
                },
                "{response}"
            );
            assert!(
                response["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("index read refused"),
                "{response}"
            );
        }
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        client.close().await;
    }
}

#[tokio::test]
async fn paged_history_message_windows_keep_projection_dependencies_and_file_changes() {
    let dir = tempfile::tempdir().unwrap();
    let source = fixture();
    let mut rows = source["items"].as_array().unwrap().clone();
    rows.push(json!({"kind":"event","event":"file_change","turnId":"three","payload":{"path":"/tmp/retained.txt","kind":"modify","added":3,"removed":1,"turnDiff":"complete saved diff"}}));
    write_rows(&dir.path().join("1700000000000-history.jsonl"), &rows);
    let mut client = read_only_client(dir.path(), Arc::new(AtomicUsize::new(0)));
    client.initialize().await;
    let response = read(
        &mut client,
        910,
        "engine/readThreadHistory",
        json!({"threadId":"history","itemsView":"notLoaded","limit":100}),
    )
    .await;
    assert_eq!(response["result"]["history"], json!([]), "{response}");
    assert_eq!(
        response["result"]["fileChanges"][0]["files"][0]["turnDiff"], "complete saved diff",
        "{response}"
    );
    let expected = klaus_agent_core::store::thread_history::project_thread_history(
        rows.iter()
            .filter_map(|row| parse_rollout_line(&row.to_string()))
            .collect(),
    );
    for turn in response["result"]["turns"].as_array().unwrap() {
        let turn_id = turn["id"].as_str().unwrap();
        let mut cursor = None;
        let mut actual = Vec::new();
        loop {
            let reply=read(&mut client,911,"engine/readThreadItems",json!({"threadId":"history","turnId":turn_id,"cursor":cursor,"limit":1,"sortDirection":"asc"})).await;
            let result = &reply["result"];
            assert!(reply.get("error").is_none(), "{reply}");
            for row in result["history"].as_array().unwrap() {
                if row["uuid"] == "user-one" {
                    assert_eq!(
                        result["presentationRecords"][0]["uuid"], "display-one",
                        "{reply}"
                    );
                } else if row["type"] != "user" {
                    assert!(result.get("presentationRecords").is_none(), "{reply}");
                }
            }
            actual.extend(result["history"].as_array().unwrap().iter().cloned());
            cursor = result["page"]["nextCursor"].as_str().map(str::to_string);
            if result["page"]["hasMore"] == false {
                break;
            }
        }
        let encoded =
            klaus_agent_core::runtime_values::runtime_codec::encode_value(&expected).unwrap();
        let value = serde_json::from_str::<Value>(
            &klaus_agent_core::runtime_values::runtime_json::stringify(&encoded)
                .try_to_utf8()
                .unwrap(),
        )
        .unwrap();
        let wanted = value["history"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| value["historyTurnIds"][row["uuid"].as_str().unwrap()] == turn_id)
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(actual, wanted, "turn {turn_id}");
    }
    client.close().await;
}

fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../core/tests/fixtures/complete_thread_history.json"
    ))
    .unwrap()
}

#[tokio::test]
async fn paged_history_large_image_references_are_substituted_before_the_page_guard() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let image = dir.path().join("saved.png");
    std::fs::write(&image, b"source bytes").unwrap();
    let path = dir.path().join("1700000000000-history.jsonl");
    let mut file = std::fs::File::create(&path).unwrap();
    writeln!(file, "{}", fixture()["items"][0]).unwrap();
    writeln!(file, "{}", fixture()["items"][2]).unwrap();
    file.write_all(b"{\"kind\":\"message\",\"message\":{\"type\":\"user\",\"uuid\":\"large-image\",\"content\":[{\"type\":\"image\",\"mimeType\":\"image/png\",\"base64\":\"").unwrap();
    let chunk = vec![b'a'; 64 * 1024];
    for _ in 0..257 {
        file.write_all(&chunk).unwrap();
    }
    writeln!(
        file,
        "\",\"sourcePath\":{}}}]}}}}",
        json!(image.to_string_lossy())
    )
    .unwrap();
    file.flush().unwrap();
    let before = std::fs::metadata(&path).unwrap().len();
    let mut client = read_only_client(dir.path(), Arc::new(AtomicUsize::new(0)));
    client.initialize().await;
    let request = json!({"threadId":"history","turnId":"one","limit":1});
    let reply = read(&mut client, 920, "engine/readThreadItems", request.clone()).await;
    assert_eq!(
        reply["result"]["history"][0]["content"][0],
        json!({"type":"image","mimeType":"image/png","path":image.to_string_lossy()}),
        "{reply}"
    );
    std::fs::remove_file(&image).unwrap();
    let reply = read(&mut client, 921, "engine/readThreadItems", request).await;
    assert_eq!(reply["error"]["code"], wire::INVALID_PARAMS, "{reply}");
    assert!(
        reply["error"]["message"]
            .as_str()
            .unwrap()
            .contains("decoded message length too large"),
        "{reply}"
    );
    assert_eq!(std::fs::metadata(&path).unwrap().len(), before);
    client.close().await;
}

fn write_rows(path: &std::path::Path, rows: &[Value]) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    for row in rows {
        assert!(
            parse_rollout_line(&row.to_string()).is_some(),
            "valid source-owned record: {row}"
        );
    }
    std::fs::write(
        path,
        rows.iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n",
    )
    .unwrap();
}

fn read_only_client(dir: &std::path::Path, activations: Arc<AtomicUsize>) -> TestClient {
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let lifecycle = crate::lifecycle::protocol_lifecycle(make_agent_for_tests(vec![]), Some(store));
    let create: CreateSession = Arc::new(move |_| {
        activations.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err("saved history must not initialize a provider".into()) })
    });
    TestClient::start_with(create, Some(lifecycle))
}

fn source_entry_count(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .filter(|name| name != "threads-index.sqlite3" && name != "threads-index.sqlite3.lock")
        .count()
}

async fn read(client: &mut TestClient, id: u64, method: &str, params: Value) -> Value {
    client
        .send(json!({"id":id,"method":method,"params":params}))
        .await;
    client.response_for(id).await.0
}

fn assert_complete_history(result: &Value, source: &Value) {
    let history = result["history"].as_array().expect("full message array");
    for (key, expected) in source["expected"].as_object().unwrap() {
        if key == "messageIds" {
            assert_eq!(
                json!(history
                    .iter()
                    .map(|message| &message["uuid"])
                    .collect::<Vec<_>>()),
                *expected
            );
        } else {
            assert_eq!(result[key], *expected, "saved history field {key}");
        }
    }
    let rows = source["items"].as_array().unwrap();
    for uuid in ["display-one", "user-one", "assistant-one"] {
        assert_eq!(
            history
                .iter()
                .find(|message| message["uuid"] == uuid)
                .unwrap(),
            &rows
                .iter()
                .find(|row| row["message"]["uuid"] == uuid)
                .unwrap()["message"],
            "complete original message {uuid}",
        );
    }
    let result = history
        .iter()
        .find(|message| message["uuid"] == "result-one")
        .unwrap();
    assert_eq!(
        result["output"],
        json!({"exit_code":130,"stdout":"Stopped","fixture":"structured"})
    );
    assert_eq!(result["nativeToolOutput"], json!({"fixture":"native"}));
    assert_eq!(result["extraUserBlocks"][0]["filename"], "tool.pdf");
}

#[tokio::test]
async fn full_saved_history_facade_preserves_content_and_never_activates_archived_parent() {
    let dir = tempfile::tempdir().unwrap();
    let source = fixture();
    let rows = source["items"].as_array().unwrap();
    let archived = dir.path().join("archived");
    let parent = archived.join("1700000000000-history.jsonl");
    let child_dir = parent.with_extension("").join("subagents");
    let child = child_dir.join("agent-reviewer.jsonl");
    write_rows(&parent, rows);
    let mut child_rows = rows.clone();
    child_rows.push(json!({"kind":"event","event":"tools_surfaced","payload":{"names":["Read"]}}));
    write_rows(&child, &child_rows);
    let meta = json!({"agentType":"reviewer","model":"fixture-model","permissionMode":"default","effort":"high","createdAt":1700000000000_i64});
    let sidecar = child_dir.join("agent-reviewer.meta.json");
    std::fs::write(&sidecar, meta.to_string()).unwrap();
    let before: Vec<_> = [&parent, &child, &sidecar]
        .into_iter()
        .map(|path| std::fs::read(path).unwrap())
        .collect();
    let activations = Arc::new(AtomicUsize::new(0));
    let mut client = read_only_client(dir.path(), activations.clone());
    client.initialize().await;

    let main = read(
        &mut client,
        10,
        "engine/readThreadHistory",
        json!({"threadId":"history"}),
    )
    .await;
    assert_complete_history(&main["result"], &source);
    assert_eq!(main["result"]["threadId"], "history");
    let child_reply = read(
        &mut client,
        11,
        "engine/readSubagentHistory",
        json!({"threadId":"history","agentId":"reviewer"}),
    )
    .await;
    assert_complete_history(&child_reply["result"], &source);
    assert_eq!(child_reply["result"]["threadId"], "history");
    assert_eq!(child_reply["result"]["agentId"], "reviewer");
    assert_eq!(child_reply["result"]["meta"], meta);
    assert_eq!(child_reply["result"]["surfacedTools"], json!(["Read"]));
    let missing_child = read(
        &mut client,
        12,
        "engine/readSubagentHistory",
        json!({"threadId":"history","agentId":"missing"}),
    )
    .await;
    assert_eq!(missing_child["result"]["history"], json!([]));
    assert_eq!(missing_child["result"]["meta"], Value::Null);
    assert_eq!(missing_child["result"]["surfacedTools"], json!([]));
    assert_eq!(missing_child["result"]["agentNotifications"], json!([]));
    assert_eq!(activations.load(Ordering::SeqCst), 0);
    assert_eq!(
        [&parent, &child, &sidecar]
            .into_iter()
            .map(|path| std::fs::read(path).unwrap())
            .collect::<Vec<_>>(),
        before
    );
    assert_eq!(source_entry_count(dir.path()), 1);
    assert_eq!(std::fs::read_dir(&archived).unwrap().count(), 2);
    assert_eq!(std::fs::read_dir(&child_dir).unwrap().count(), 2);
    client.close().await;
}

#[tokio::test]
async fn full_saved_history_facade_validates_ids_and_preserves_large_single_messages() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = fixture();
    let long = "完整记录🦀".repeat(70000);
    source["items"][4]["message"]["content"][0]["text"] = json!(long);
    let path = dir.path().join("1700000000000-history.jsonl");
    write_rows(&path, source["items"].as_array().unwrap());
    let activations = Arc::new(AtomicUsize::new(0));
    let mut client = read_only_client(dir.path(), activations.clone());
    client.initialize().await;
    let reply = read(
        &mut client,
        10,
        "engine/readThreadHistory",
        json!({"threadId":"history"}),
    )
    .await;
    assert_complete_history(&reply["result"], &source);
    for (index, method, params) in [
        (11, "engine/readThreadHistory", json!({"threadId":""})),
        (
            12,
            "engine/readThreadHistory",
            json!({"threadId":"history","itemsView":"bogus"}),
        ),
        (
            13,
            "engine/readSubagentHistory",
            json!({"threadId":"history","agentId":" "}),
        ),
        (
            14,
            "engine/readSubagentHistory",
            json!({"threadId":"history"}),
        ),
    ] {
        assert_eq!(
            read(&mut client, index, method, params).await["error"]["code"],
            wire::INVALID_PARAMS
        );
    }
    for (index, method, params) in [
        (
            15,
            "engine/readThreadHistory",
            json!({"threadId":"missing"}),
        ),
        (
            16,
            "engine/readSubagentHistory",
            json!({"threadId":"missing","agentId":"reviewer"}),
        ),
    ] {
        assert_eq!(
            read(&mut client, index, method, params).await["error"]["code"],
            wire::UNKNOWN_SESSION
        );
    }
    assert_eq!(activations.load(Ordering::SeqCst), 0);
    assert_eq!(source_entry_count(dir.path()), 1);
    client.close().await;
}

#[tokio::test]
async fn paged_history_pages_by_turn_and_refuses_unknown_cursors() {
    let dir = tempfile::tempdir().unwrap();
    let source = fixture();
    let rows = source["items"].as_array().unwrap();
    let parent = dir.path().join("1700000000000-history.jsonl");
    write_rows(&parent, rows);
    let mut client = read_only_client(dir.path(), Arc::new(AtomicUsize::new(0)));
    client.initialize().await;

    let first = read(
        &mut client,
        20,
        "engine/readThreadHistory",
        json!({"threadId":"history","limit":1}),
    )
    .await;
    let page = &first["result"]["page"];
    assert_eq!(page["order"], "newest_first");
    assert_eq!(page["hasMore"], true);
    let newest = first["result"]["history"].as_array().unwrap();
    assert!(!newest.is_empty(), "the newest turn carries its rows");
    let cursor = page["nextCursor"].as_str().unwrap().to_string();

    let second = read(
        &mut client,
        21,
        "engine/readThreadHistory",
        json!({"threadId":"history","limit":1,"cursor":cursor}),
    )
    .await;
    let older = second["result"]["history"].as_array().unwrap();
    assert!(
        !older.is_empty(),
        "the cursor page carries the previous turn"
    );
    let newest_uuids: Vec<_> = newest.iter().map(|row| row["uuid"].clone()).collect();
    let older_uuids: Vec<_> = older.iter().map(|row| row["uuid"].clone()).collect();
    assert!(
        older_uuids.iter().all(|uuid| !newest_uuids.contains(uuid)),
        "pages do not overlap"
    );

    let whole = read(
        &mut client,
        22,
        "engine/readThreadHistory",
        json!({"threadId":"history"}),
    )
    .await;
    let all = whole["result"]["history"].as_array().unwrap().len();
    assert!(
        all > newest.len() + older.len(),
        "the unpaged read still carries the turns the two pages left out"
    );

    let unknown = read(
        &mut client,
        23,
        "engine/readThreadHistory",
        json!({"threadId":"history","cursor":"no-such-turn"}),
    )
    .await;
    assert_eq!(unknown["error"]["code"], wire::INVALID_PARAMS);
    let bad_view = read(
        &mut client,
        24,
        "engine/readThreadHistory",
        json!({"threadId":"history","itemsView":"bogus"}),
    )
    .await;
    assert_eq!(bad_view["error"]["code"], wire::INVALID_PARAMS);

    let summary = read(
        &mut client,
        25,
        "engine/readThreadHistory",
        json!({"threadId":"history","itemsView":"summary"}),
    )
    .await;
    let summary_rows = summary["result"]["history"].as_array().unwrap();
    let whole_rows = whole["result"]["history"].as_array().unwrap();
    assert!(
        summary_rows.len() < whole_rows.len(),
        "summary keeps only the first user and last assistant row per turn"
    );
    assert!(
        summary_rows
            .iter()
            .all(|row| row["type"] == "user" || row["type"] == "assistant"),
        "summary rows are messages only"
    );
    assert_eq!(
        summary["result"]["presentationRecords"][0]["uuid"],
        "display-one"
    );
    assert_eq!(
        summary["result"]["presentationRecords"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let empty = read(
        &mut client,
        26,
        "engine/readThreadHistory",
        json!({"threadId":"history","itemsView":"notLoaded"}),
    )
    .await;
    assert_eq!(empty["result"]["history"], json!([]));
    assert!(
        empty["result"]["page"]["nextCursor"].is_string()
            || empty["result"]["page"]["hasMore"] == false
    );
}

#[tokio::test]
async fn history_rows_carry_source_paths_instead_of_bytes_when_the_file_survives() {
    let dir = tempfile::tempdir().unwrap();
    let source = fixture();
    let mut rows = source["items"].as_array().unwrap().clone();
    let image = dir.path().join("shot.png");
    std::fs::write(&image, b"hello").unwrap();
    let gone = dir.path().join("gone.png");
    for row in rows.iter_mut() {
        if row["kind"] != "message" {
            continue;
        }
        for block in row["message"]["content"]
            .as_array_mut()
            .into_iter()
            .flatten()
        {
            if block["type"] == "image" {
                block
                    .as_object_mut()
                    .unwrap()
                    .insert("sourcePath".into(), json!(image.to_string_lossy()));
            }
        }
    }
    // A second image row whose source file never existed keeps its bytes.
    rows.push(json!({"kind":"message","message":{"type":"user","uuid":"user-gone","content":[{"type":"text","text":"lost"},{"type":"image","mimeType":"image/png","base64":"aGVsbG8=","sourcePath":gone.to_string_lossy()}]}}));
    let parent = dir.path().join("1700000000000-history.jsonl");
    write_rows(&parent, &rows);
    let mut client = read_only_client(dir.path(), Arc::new(AtomicUsize::new(0)));
    client.initialize().await;

    let reply = read(
        &mut client,
        30,
        "engine/readThreadHistory",
        json!({"threadId":"history"}),
    )
    .await;
    let history = reply["result"]["history"].as_array().unwrap();
    let kept = history
        .iter()
        .find(|row| row["uuid"] == "user-one")
        .unwrap();
    let kept_image = kept["content"]
        .as_array()
        .unwrap()
        .iter()
        .find(|block| block["type"] == "image")
        .unwrap();
    assert_eq!(kept_image["path"], json!(image.to_string_lossy()));
    assert!(
        kept_image["base64"].is_null(),
        "the bytes stay off the wire"
    );
    let lost = history
        .iter()
        .find(|row| row["uuid"] == "user-gone")
        .unwrap();
    let lost_image = lost["content"]
        .as_array()
        .unwrap()
        .iter()
        .find(|block| block["type"] == "image")
        .unwrap();
    assert_eq!(
        lost_image["base64"],
        json!("aGVsbG8="),
        "a missing file keeps the inline copy"
    );
    assert!(lost_image["path"].is_null());
}
