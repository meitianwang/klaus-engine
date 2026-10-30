//! Incident 20261022: caller history consumed the 512-entry response allocation
//! budget before any model request. Keep input history and response budgets separate.
use super::*;
use base64::{engine::general_purpose::STANDARD, Engine};
use sha2::{Digest, Sha256};

fn long_input(count: usize) -> Vec<Value> {
    (0..count)
        .map(|index| {
            json!({"role":if index % 2 == 0 {"user"} else {"assistant"},
                "content":format!("History item {index}")})
        })
        .chain([json!({"role":"user","content":"Reply exactly HISTORY_OK. Do not use tools."})])
        .collect()
}

#[tokio::test]
async fn caller_history_over_512_entries_reaches_the_model_and_completes() {
    let _guard = test_lock().lock().await;
    for count in [520, 4094] {
        verify_caller_history(count).await;
    }
}

async fn verify_caller_history(count: usize) {
    let temp = tempfile::tempdir().unwrap();
    set_config_root(temp.path());
    let (api, api_task) = unary_fixture().await;
    let (inference, captured, inference_task) =
        model_step_fixture("unused".into(), "terminal-streaming").await;
    adopt_credential(
        json!({"accessToken":"fixture-token","clientIdentity":"fixture-machine",
        "backendUrl":api,"inferenceUrl":inference}),
    );
    let (provider, auth) = resolve("auto").await.unwrap();
    let mut body = provider_body();
    body["model"] = provider["model"].clone();
    body["input"] = json!(long_input(count));
    let response = reqwest::Client::new()
        .post(format!(
            "{}/responses",
            provider["baseUrl"].as_str().unwrap()
        ))
        .bearer_auth(auth["token"].as_str().unwrap())
        .json(&body)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let events = response.text().await.unwrap();
    assert!(events.contains("response.completed"), "{events}");
    assert!(events.contains("FACADE_TOOL_OK"), "{events}");
    let observed = captured.lock().unwrap();
    assert_eq!(observed.len(), 1);
    let messages = observed[0]["messages"].as_array().unwrap();
    assert_eq!(
        messages.len(),
        count + 2,
        "all history plus the tool binding"
    );
    for (index, message) in messages.iter().skip(1).take(count).enumerate() {
        let content = &message["content"];
        let text = content.as_str().or_else(|| content[0]["text"].as_str());
        assert_eq!(text, Some(format!("History item {index}").as_str()));
    }
    drop(observed);
    logout(None).unwrap();
    api_task.abort();
    inference_task.abort();
}

#[test]
fn caller_history_does_not_expand_the_upstream_blob_allowance() {
    let mut blobs = step_history::Blobs::default();
    for index in 0..600 {
        blobs.seed(format!("caller-{index}").into_bytes()).unwrap();
    }
    for index in 0..512 {
        let bytes = format!("upstream-{index}").into_bytes();
        let key = STANDARD.encode(Sha256::digest(&bytes));
        blobs.insert(key.clone(), bytes.clone()).unwrap();
        blobs.insert(key, bytes).unwrap();
    }
    let extra = b"upstream-over-budget".to_vec();
    assert_eq!(
        blobs.insert(STANDARD.encode(Sha256::digest(&extra)), extra),
        Err("cursor_blob_limit".into())
    );
    assert_eq!(
        blobs.seed(vec![0; step_history::MAX_BLOB + 1]),
        Err("cursor_context_length_exceeded".into())
    );
}

#[test]
fn oversized_caller_history_is_a_context_error() {
    let input = json!({"input":long_input(step_history::MAX_HISTORY_ROOTS),"tools":[]});
    let chat = responses::normalize(&input).unwrap();
    let error = step_history::History::new(&chat).err().unwrap();
    assert_eq!(error, "cursor_context_length_exceeded");
    let public = responses::public_error(&error);
    assert_eq!(public["error"]["type"], "context_length_exceeded");
    assert_eq!(public["message"], "cursor_context_length_exceeded");
}

#[tokio::test]
#[ignore = "requires an explicitly supplied live account profile"]
async fn caller_history_over_512_entries_live_grok() {
    let root = std::env::var("KLAUS_CURSOR_HISTORY_PROBE_ROOT")
        .expect("supply an isolated account profile explicitly");
    let _guard = test_lock().lock().await;
    set_config_root(std::path::Path::new(&root));
    let selection = "grok-4.7[context=256k,fast=true,reasoning_effort=high]";
    let (provider, auth) = resolve(selection).await.unwrap();
    let body = json!({"model":provider["model"],"stream":true,
        "input":long_input(536),"tools":[],"max_output_tokens":128});
    let began = std::time::Instant::now();
    let response = reqwest::Client::new()
        .post(format!(
            "{}/responses",
            provider["baseUrl"].as_str().unwrap()
        ))
        .bearer_auth(auth["token"].as_str().unwrap())
        .json(&body)
        .timeout(std::time::Duration::from_secs(50))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let events = response.text().await.unwrap();
    println!(
        "live model={selection} inputMessages=537 status={status} elapsedMs={} completed={}",
        began.elapsed().as_millis(),
        events.contains("response.completed")
    );
    println!("live response={events}");
    assert!(status.is_success());
    assert!(events.contains("response.completed"));
    assert!(events.contains("HISTORY_OK"));
}
