//! Live diagnostic probe, ignored by default: asks the Cursor backend whether a
//! model that our catalog marks blind actually reads an attached image. It
//! bypasses the engine vision gate on purpose by calling the source adapter
//! directly, so the answer describes the wire, not our declaration. Opt in with
//! KLAUS_CURSOR_LIVE_PROBE=1 plus a copied real credential; without both the
//! test skips itself.

use serde_json::{json, Value};

/// A 32x32 solid orange PNG: unmistakable to any vision-capable model and
/// meaningless to guess from the question text.
const ORANGE_PNG_BASE64: &str = "iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAIAAAD8GO2jAAAAKUlEQVR4nO3NsQ0AAAjDsJ7N9/AE3SxljrOTat07AAAAAAAAAAAAeOwA3UXcTIdilLsAAAAASUVORK5CYII=";

const PROBE_KEY: &str = "grok-4.7[context=256k,fast=false,reasoning_effort=high]";

#[tokio::test]
#[ignore = "live network probe against the Cursor backend; opt in with KLAUS_CURSOR_LIVE_PROBE=1"]
async fn cursor_grok47_live_vision_probe() {
    if std::env::var("KLAUS_CURSOR_LIVE_PROBE").unwrap_or_default() != "1" {
        eprintln!("PROBE skipped: KLAUS_CURSOR_LIVE_PROBE is not set");
        return;
    }
    let _guard = super::test_lock().lock().await;
    let home = std::env::var("HOME").expect("HOME");
    let store = std::path::Path::new(&home).join(".klaus/cursor-subscription-oauth.json");
    let stored = match std::fs::read(&store) {
        Ok(bytes) => bytes,
        Err(error) => {
            eprintln!(
                "PROBE skipped: no credential at {}: {error}",
                store.display()
            );
            return;
        }
    };
    // Work on a copy so a refresh can never touch the live store.
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join("cursor-subscription-oauth.json"), stored).unwrap();
    super::set_config_root(temp.path());

    let (credential, generation) = super::credential::snapshot().expect("credential snapshot");

    // Declaration layer, raw: what the Cursor service catalog itself carries.
    let (raw, _) = super::credential::unary(
        "/aiserver.v1.AiService/AvailableModels",
        json!({"includeLongContextModels": true, "useModelParameters": true}),
    )
    .await
    .expect("raw catalog fetch");
    let raw_entry = raw["models"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|model| {
            model["serverModelName"]
                .as_str()
                .or_else(|| model["name"].as_str())
                .is_some_and(|id| id == "grok-4.7")
        })
        .cloned();
    match &raw_entry {
        Some(entry) => {
            let image_fields: Value = json!(entry
                .as_object()
                .map(|object| object
                    .iter()
                    .filter(|(key, _)| {
                        let lower = key.to_ascii_lowercase();
                        lower.contains("image") || lower.contains("vision") || lower == "is_vl"
                    })
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect::<serde_json::Map<String, Value>>())
                .unwrap_or_default());
            println!("PROBE raw_image_fields={image_fields}");
            println!("PROBE raw_entry={entry}");
        }
        None => println!("PROBE raw_entry=missing"),
    }

    // Declaration layer, parsed: what our catalog and capabilities expose.
    let entries = super::catalog::fetch_models(true)
        .await
        .expect("parsed catalog");
    let entry = entries
        .iter()
        .find(|entry| entry["key"].as_str() == Some(PROBE_KEY))
        .or_else(|| {
            entries.iter().find(|entry| {
                entry["key"]
                    .as_str()
                    .is_some_and(|key| key.starts_with("grok-4.7["))
            })
        })
        .cloned()
        .expect("a grok-4.7 catalog entry");
    let key = entry["key"].as_str().unwrap().to_string();
    println!("PROBE parsed_entry={entry}");
    println!(
        "PROBE declared_capabilities={}",
        super::model_capabilities(&key).await
    );

    // Behaviour layer: one image, one unanswerable-without-vision question.
    let body = json!({
        "model": key,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": "An image is attached to this message. Look at the image and answer in one line: what is its dominant color? If you cannot see any image at all, reply with exactly: NO_IMAGE_VISIBLE."},
                {"type": "image_url", "image_url": {"url": format!("data:image/png;base64,{ORANGE_PNG_BASE64}")}}
            ]
        }],
        "tools": []
    });
    let mut output = super::responses::Output::new(&key);
    let (tx, mut rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(1024);
    let drain = tokio::spawn(async move {
        let mut frames = 0usize;
        while rx.recv().await.is_some() {
            frames += 1;
        }
        frames
    });
    let started = std::time::Instant::now();
    let result =
        super::step::invoke(&body, &entry, &credential, generation, &mut output, &tx).await;
    drop(tx);
    let frames = drain.await.unwrap();
    match result {
        Ok(certified) => {
            println!(
                "PROBE result=ok elapsed_ms={} frames={frames}",
                started.elapsed().as_millis()
            );
            println!("PROBE reply_text={}", certified.text);
            println!(
                "PROBE reply_reasoning_len={} usage={}",
                certified.reasoning.len(),
                certified.usage
            );
        }
        Err(error) => {
            println!(
                "PROBE result=err elapsed_ms={} frames={frames} error={error}",
                started.elapsed().as_millis()
            );
        }
    }
}
