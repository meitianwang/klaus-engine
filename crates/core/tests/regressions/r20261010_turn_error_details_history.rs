//! Defect: resumed histories retained only a failure string and provider kind.
//! Root cause: full terminal diagnostics were not indexed or replayed.
//! Regression scope: complete, optional, read-only turn-error diagnostics.

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::NoopCompactor;
use klaus_agent_core::engine::session::{Input, TurnIndex, TurnOptions};
use klaus_agent_core::provider::classify::{classify_error, RawProviderError};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeEncode, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::rollout::RolloutItem;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::rollout::parse_rollout_line;
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::store::thread_history::project_thread_history;
use klaus_agent_core::types::errors::ProviderMediaOrigin;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::ModelStreamEvent;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn exact(value: &Value) -> RuntimeJson {
    runtime_codec::decode_text(&RuntimeString::from(value.to_string())).unwrap()
}
fn encode(value: &impl RuntimeEncode) -> Value {
    serde_json::from_slice(&runtime_codec::encode_bytes(value).unwrap()).unwrap()
}
fn items(rows: &[Value]) -> Vec<RolloutItem> {
    rows.iter()
        .map(|row| parse_rollout_line(&row.to_string()).expect("valid history fixture"))
        .collect()
}
fn detail(kind: &str, message: &str, request_id: &str, status: Option<u16>) -> Value {
    let mut provider = json!({"kind":kind,"message":message,"code":if kind == "prompt_too_long" { "context_length_exceeded" } else { "gateway_body_limit" },"type":if kind == "prompt_too_long" { "invalid_request_error" } else { "request_too_large" },"requestId":request_id});
    if let Some(status) = status {
        provider["status"] = json!(status);
    }
    json!({"code":if kind == "prompt_too_long" { "prompt_too_long" } else { "provider_error" },"message":message,"kind":kind,"requestId":request_id,"providerError":provider,"recoveryReason":"reactive-compaction-disabled"})
}
fn rows(root: &Path) -> (Vec<Value>, Value) {
    let bytes = detail(
        "request_too_large",
        "Proxy rejected a 12 MB body",
        "req_bytes",
        Some(413),
    );
    let window = detail(
        "prompt_too_long",
        "The token context window is exceeded",
        "req_window",
        Some(413),
    );
    let sse = detail(
        "request_too_large",
        "Stream payload is too large",
        "req_sse",
        None,
    );
    let mut private = bytes.clone();
    private["media_origins"] = json!([{ "private":"do-not-expose" }]);
    private["providerError"]["mediaOrigins"] = json!([{ "private":"do-not-expose" }]);
    private["futureDiagnostic"] =
        json!({"kept":true,"rows":[{"media_origins":["do-not-expose"],"futureValue":17}]});
    let mut expected_bytes = bytes.clone();
    expected_bytes["futureDiagnostic"] = json!({"kept":true,"rows":[{"futureValue":17}]});
    let mut source = vec![
        json!({"kind":"session_meta","schemaVersion":2,"meta":{"id":"history","createdAt":1700000000000_i64,"cwd":root.to_string_lossy(),"model":"fixture","title":"Failure history"}}),
    ];
    for (id, payload) in [
        (
            "bytes",
            json!({"status":"failed","error":bytes["message"],"kind":"request_too_large","errorDetails":private}),
        ),
        (
            "window",
            json!({"status":"failed","error":window["message"],"kind":"prompt_too_long","providerError":window["providerError"],"recoveryReason":"reactive-compaction-disabled"}),
        ),
        (
            "sse",
            json!({"status":"failed","error":sse["message"],"kind":"request_too_large","providerError":sse["providerError"],"requestId":null,"recoveryReason":"reactive-compaction-disabled"}),
        ),
        (
            "legacy",
            json!({"status":"failed","error":"Legacy network failure","kind":"network"}),
        ),
        (
            "empty",
            json!({"status":"failed","error":"Empty detail failure","kind":"network","errorDetails":{}}),
        ),
    ] {
        source.push(json!({"kind":"turn_context","ctx":{"cwd":root.to_string_lossy(),"model":"fixture","permissionMode":"default","turnId":id}}));
        source.push(json!({"kind":"message","message":{"type":"user","uuid":format!("{id}-user"),"content":[{"type":"text","text":"Inspect the original failure"},{"type":"image","mimeType":"image/png","base64":"aGVsbG8=","sourcePath":root.join("history.png").to_string_lossy()}]}}));
        source.push(json!({"kind":"event","event":"turn_lifecycle","turnId":id,"payload":payload}));
    }
    (
        source,
        json!({"bytes":expected_bytes,"window":window,"sse":sse}),
    )
}
fn write_fixture(root: &Path, source: &[Value], archived: bool) -> PathBuf {
    let dir = if archived {
        root.join("archived")
    } else {
        root.to_path_buf()
    };
    std::fs::create_dir_all(&dir).unwrap();
    items(source);
    let path = dir.join("1700000000000-history.jsonl");
    std::fs::write(
        &path,
        source
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n",
    )
    .unwrap();
    path
}

#[tokio::test]
async fn public_store_history_reads_preserve_full_diagnostics_without_rewriting_sources() {
    for archived in [false, true] {
        let root =
            std::env::temp_dir().join(format!("klaus-error-history-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let media = root.join("history.png");
        std::fs::write(&media, b"immutable historical image bytes").unwrap();
        let (source, expected) = rows(&root);
        let file = write_fixture(&root, &source, archived);
        let before = std::fs::read(&file).unwrap();
        let before_media = std::fs::read(&media).unwrap();
        let store = JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(root.to_string_lossy().into_owned()),
            archive_dir: None,
        });
        for _ in 0..3 {
            let snapshot = store.read_thread("history").await.unwrap();
            assert_eq!(snapshot.archived, archived);
            assert_eq!(snapshot.raw_items, items(&source));
            let result = encode(&project_thread_history(snapshot.raw_items));
            assert_eq!(result["turnErrorDetails"], expected);
            assert_eq!(result["turnErrors"]["bytes"], "Proxy rejected a 12 MB body");
            assert_eq!(result["turnErrorKinds"]["window"], "prompt_too_long");
            assert!(result["turnErrorDetails"]["sse"]["providerError"]
                .get("status")
                .is_none());
            assert_eq!(result["turnErrorDetails"]["sse"]["requestId"], "req_sse");
            assert!(!result["turnErrors"]["sse"]
                .as_str()
                .unwrap()
                .contains("req_sse"));
            assert!(result["turnErrorDetails"].get("legacy").is_none());
            assert!(result["turnErrorDetails"].get("empty").is_none());
            assert!(!result.to_string().contains("do-not-expose"));
            assert!(result["history"][0]["content"][1]
                .get("sourcePath")
                .is_some());
            assert_eq!(std::fs::read(&file).unwrap(), before);
            assert_eq!(std::fs::read(&media).unwrap(), before_media);
        }
        assert!(!file.with_extension("jsonl.lock").exists());
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn optional_details_and_unknown_codec_fields_remain_compatible() {
    for field in [
        Value::Null,
        json!({}),
        json!({"empty":{},"internal":{"providerError":{"media_origins":[{"private":true}]}}}),
    ] {
        let input = exact(&json!({"turnErrorDetails":field,"futureEnvelope":{"supported":true}}));
        let index: TurnIndex = runtime_codec::decode_value(&input).unwrap();
        assert!(encode(&index).get("turnErrorDetails").is_none());
    }
    let future = json!({"code":"future_engine_code","message":"Future diagnostic","kind":"future_provider_kind","providerError":{"kind":"future_provider_kind","message":"Future diagnostic","type":"future_type","futureExtension":[1,{"retained":true}]},"futureDiagnostic":{"retained":true}});
    let input = exact(&json!({"turnErrorDetails":{"future":future},"ignoredEnvelope":true}));
    let index: TurnIndex = runtime_codec::decode_value(&input).unwrap();
    assert_eq!(encode(&index)["turnErrorDetails"]["future"], future);
}

#[test]
fn legacy_flat_diagnostics_are_preserved_without_inventing_http_status() {
    let source = vec![
        json!({"kind":"turn_context","ctx":{"cwd":"/tmp","model":"fixture","permissionMode":"default","turnId":"flat"}}),
        json!({"kind":"message","message":{"type":"user","uuid":"flat-user","content":[{"type":"text","text":"Read"}]}}),
        json!({"kind":"event","event":"turn_lifecycle","turnId":"flat","payload":{"status":"failed","error":"Older flat diagnostic","kind":"request_too_large","errorCode":"provider_error","requestId":"req_flat","providerCode":"body_limit","providerType":"request_too_large","recoveryReason":"disabled"}}),
    ];
    let before = items(&source);
    let output = encode(&project_thread_history(before.clone()));
    assert_eq!(before, items(&source));
    assert_eq!(
        output["turnErrorDetails"]["flat"],
        json!({"code":"provider_error","message":"Older flat diagnostic","kind":"request_too_large","requestId":"req_flat","providerCode":"body_limit","providerType":"request_too_large","recoveryReason":"disabled"})
    );
    assert!(output["turnErrorDetails"]["flat"].get("status").is_none());
}

#[test]
fn old_histories_without_structured_details_omit_the_new_field() {
    let source = vec![
        json!({"kind":"turn_context","ctx":{"cwd":"/tmp","model":"fixture","permissionMode":"default","turnId":"old"}}),
        json!({"kind":"message","message":{"type":"user","uuid":"old-user","content":[{"type":"text","text":"Read"}]}}),
        json!({"kind":"event","event":"turn_lifecycle","turnId":"old","payload":{"status":"failed","error":"Old failure","kind":"network"}}),
    ];
    let output = encode(&project_thread_history(items(&source)));
    assert!(output.get("turnErrorDetails").is_none());
    assert_eq!(output["turnErrors"], json!({"old":"Old failure"}));
    assert_eq!(output["turnErrorKinds"], json!({"old":"network"}));
}

#[test]
fn repeated_lifecycle_records_replace_one_detail_entry_instead_of_decorating_messages() {
    let first = detail(
        "request_too_large",
        "First rejection",
        "req_first",
        Some(413),
    );
    let second = detail("request_too_large", "Last rejection", "req_last", None);
    let source = vec![
        json!({"kind":"turn_context","ctx":{"cwd":"/tmp","model":"fixture","permissionMode":"default","turnId":"repeat"}}),
        json!({"kind":"message","message":{"type":"user","uuid":"repeat-user","content":[{"type":"text","text":"Read"}]}}),
        json!({"kind":"event","event":"turn_lifecycle","turnId":"repeat","payload":{"status":"failed","error":"First rejection","errorDetails":first}}),
        json!({"kind":"event","event":"turn_lifecycle","turnId":"repeat","payload":{"status":"failed","error":"Last rejection","errorDetails":second}}),
    ];
    let output = encode(&project_thread_history(items(&source)));
    assert_eq!(output["turnErrorDetails"].as_object().unwrap().len(), 1);
    assert_eq!(output["turnErrorDetails"]["repeat"], second);
    assert_eq!(output["turnErrors"]["repeat"], "Last rejection");
}

#[tokio::test]
async fn public_session_terminal_resume_and_read_keep_the_same_complete_error() {
    for (status, code, error_type, message) in [
        (
            Some(413),
            "gateway_body_limit",
            "request_too_large",
            "Proxy rejected bytes",
        ),
        (
            Some(413),
            "context_length_exceeded",
            "invalid_request_error",
            "Token context window exceeded",
        ),
        (
            None,
            "request_too_large",
            "request_too_large",
            "SSE request is too large",
        ),
    ] {
        let process = RuntimeProcessContext::initialize(
            super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
        )
        .unwrap();
        let root = std::env::temp_dir().join(format!(
            "klaus-session-error-details-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let media = root.join("immutable.png");
        std::fs::write(&media, b"immutable media fixture").unwrap();
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(root.join("sessions").to_string_lossy().into_owned()),
            archive_dir: None,
        }));
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let mut provider = classify_error(&RawProviderError {
            status,
            code: Some(code.into()),
            body_type: Some(error_type.into()),
            message: message.into(),
            normalized_request_id: Some("req_original".into()),
            ..Default::default()
        });
        provider.media_origins = Some(vec![ProviderMediaOrigin {
            wire_message_index: 0,
            wire_content_index: 0,
            wire_output_index: None,
            message_index: 0,
            content_index: Some(0),
            extra_user_blocks: false,
            output_index: None,
        }]);
        let mut options =
            super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
        options.cwd = Some(root.to_string_lossy().into_owned());
        options.compactor = Some(Arc::new(NoopCompactor));
        options.config.as_mut().unwrap().as_object_mut().unwrap().extend(json!({"model":"fixture","contextWindow":1_000_000,"maxOutputTokens":1000,"maxRetries":0,"prewarm":false,"autoMemory":false,"agentMemory":false,"idleMicrocompact":false,"earlyMicrocompact":false,"speculativeCompaction":false,"loopDetection":false,"goal":false}).as_object().unwrap().clone());
        options.deps.call_model = Some(Box::new(move |_, _| {
            seen.fetch_add(1, Ordering::SeqCst);
            Box::pin(futures::stream::iter(vec![ModelStreamEvent::Error {
                error: provider.clone(),
            }]))
        }));
        let agent = Agent::new(options);
        let sopts = || StartSessionOptions {
            cwd: Some(root.to_string_lossy().into_owned().into()),
            user_config_dir: Some(root.to_string_lossy().into_owned().into()),
            ..Default::default()
        };
        let session = agent.open_session(store.clone(), sopts()).await.unwrap();
        let events: Vec<_> = session
            .run_streamed(
                Input::Text("Fail this fixture request.".into()),
                TurnOptions::default(),
            )
            .events
            .collect()
            .await;
        let expected = events
            .iter()
            .find_map(|event| match event {
                EngineEvent::TurnFailed { error, .. } => Some(encode(error)),
                _ => None,
            })
            .expect("fixture ends on a typed failure");
        let live = encode(&session.turn_index_snapshot());
        let turn_id = live["turnErrorDetails"]
            .as_object()
            .unwrap()
            .keys()
            .next()
            .unwrap()
            .clone();
        assert_eq!(live["turnErrorDetails"][&turn_id], expected);
        assert_eq!(expected["providerError"]["type"], error_type);
        assert_eq!(expected["providerError"]["code"], code);
        assert_eq!(expected["requestId"], "req_original");
        assert_eq!(expected["message"], message);
        let expected_recovery = if code == "context_length_exceeded" {
            "context-window-full-next-turn-compaction-required"
        } else {
            "reactive-compaction-disabled"
        };
        assert_eq!(expected["recoveryReason"], expected_recovery);
        if let Some(status) = status {
            assert_eq!(expected["providerError"]["status"], status);
        } else {
            assert!(expected["providerError"].get("status").is_none());
        }
        assert!(!expected.to_string().contains("media_origins"));
        let thread_id = session.id.try_to_utf8().unwrap();
        session.flush_rollout().await.unwrap();
        let snapshot = store.read_thread(&thread_id).await.unwrap();
        assert_eq!(
            encode(&project_thread_history(snapshot.raw_items.clone()))["turnErrorDetails"]
                [&turn_id],
            expected
        );
        let lifecycle = snapshot
            .raw_items
            .iter()
            .rev()
            .find_map(|item| match item {
                RolloutItem::Event(event) if event.event == "turn_lifecycle" => {
                    event.payload.as_ref()
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(encode(lifecycle)["errorDetails"], expected);
        session.shutdown_and_wait().await.unwrap();
        let original = store.read_thread(&thread_id).await.unwrap().raw_items;
        let file = std::fs::read_dir(root.join("sessions"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.extension().and_then(|extension| extension.to_str()) == Some("jsonl"))
            .expect("the public session owns one rollout file");
        let before_file = std::fs::read(&file).unwrap();
        let before_media = std::fs::read(&media).unwrap();
        let resumed = agent
            .resume_session(store.clone(), &thread_id, sopts())
            .await
            .unwrap();
        resumed.flush_rollout().await.unwrap();
        let resumed_file = std::fs::read(&file).unwrap();
        assert!(resumed_file.starts_with(&before_file));
        for _ in 0..2 {
            assert_eq!(
                encode(&resumed.turn_index_snapshot())["turnErrorDetails"][&turn_id],
                expected
            );
            let read = store.read_thread(&thread_id).await.unwrap();
            assert_eq!(&read.raw_items[..original.len()], original.as_slice());
            assert_eq!(read.raw_items.len(), original.len() + 1);
            assert_eq!(
                encode(&read.raw_items[original.len()]),
                json!({"kind":"event","event":"resumed"})
            );
            assert_eq!(
                project_thread_history(read.raw_items.clone()).history,
                project_thread_history(snapshot.raw_items.clone()).history
            );
            assert_eq!(
                std::fs::read(&file).unwrap(),
                resumed_file,
                "pure reads must not append records"
            );
            assert_eq!(
                encode(&project_thread_history(read.raw_items))["turnErrorDetails"][&turn_id],
                expected
            );
            assert_eq!(std::fs::read(&media).unwrap(), before_media);
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "resume and history must not call the model"
        );
        resumed.shutdown_and_wait().await.unwrap();
        let legacy_root = root.join("legacy-sessions");
        std::fs::create_dir_all(&legacy_root).unwrap();
        let mut legacy = original.clone();
        for item in &mut legacy {
            if let RolloutItem::Event(event) = item {
                if event.event == "turn_lifecycle" {
                    if let Some(RuntimeJson::Object(payload)) = &mut event.payload {
                        payload.remove("errorDetails");
                        payload.remove("providerError");
                        payload.remove("recoveryReason");
                    }
                }
            }
        }
        let legacy_file = legacy_root.join(format!("1700000000000-{thread_id}.jsonl"));
        let mut legacy_bytes = Vec::new();
        for item in &legacy {
            legacy_bytes.extend_from_slice(&runtime_codec::encode_bytes(item).unwrap());
            legacy_bytes.push(b'\n');
        }
        std::fs::write(&legacy_file, &legacy_bytes).unwrap();
        let legacy_store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(legacy_root.to_string_lossy().into_owned()),
            archive_dir: None,
        }));
        let legacy_session = agent
            .resume_session(legacy_store.clone(), &thread_id, sopts())
            .await
            .unwrap();
        legacy_session.flush_rollout().await.unwrap();
        let legacy_read = encode(&legacy_session.turn_index_snapshot());
        assert!(legacy_read.get("turnErrorDetails").is_none());
        assert_eq!(legacy_read["turnErrors"][&turn_id], message);
        let legacy_after = std::fs::read(&legacy_file).unwrap();
        assert!(legacy_after.starts_with(&legacy_bytes));
        let legacy_items = legacy_store
            .read_thread(&thread_id)
            .await
            .unwrap()
            .raw_items;
        assert_eq!(&legacy_items[..legacy.len()], legacy.as_slice());
        assert_eq!(legacy_items.len(), legacy.len() + 1);
        assert_eq!(
            encode(&legacy_items[legacy.len()]),
            json!({"kind":"event","event":"resumed"})
        );
        assert_eq!(std::fs::read(&file).unwrap(), resumed_file);
        assert_eq!(std::fs::read(&media).unwrap(), before_media);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        legacy_session.shutdown_and_wait().await.unwrap();
        process.shutdown().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}
