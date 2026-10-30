//! Connection-owned live dictation. Audio is bounded and never replayed from a file.
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use klaus_agent_core::types::abort::AbortSignal;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, watch};

use super::{wire, AppServerState};

#[derive(Default)]
pub(super) struct DictationSessions {
    active: Mutex<HashMap<String, Arc<Handle>>>,
}
struct Input {
    sender: Option<mpsc::Sender<Vec<u8>>>,
    next_sequence: u64,
    bytes: usize,
}
struct Handle {
    signal: AbortSignal,
    admission: tokio::sync::Semaphore,
    input: tokio::sync::Mutex<Input>,
    sequence: AtomicU64,
    done: watch::Receiver<Option<Result<(), String>>>,
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}
async fn emit(state: &AppServerState, handle: &Handle, operation_id: &str, event: Value) {
    if handle.signal.is_cancelled() || state.connection_closed.load(Ordering::SeqCst) {
        return;
    }
    let mut payload = event;
    payload["operationId"] = json!(operation_id);
    payload["seq"] = json!(handle.sequence.fetch_add(1, Ordering::SeqCst) + 1);
    tokio::select! {
        biased;
        _ = handle.signal.cancelled() => {},
        _ = state.notify("engine/dictationEvent", payload) => {},
    }
}

async fn start(
    state: &Arc<AppServerState>,
    params: wire::DictationStartParams,
) -> Result<Value, String> {
    if !valid_id(&params.operation_id) {
        return Err("dictation-operation-id-invalid".into());
    }
    if ![
        "claude",
        "codex",
        "workbuddy",
        "trae",
        "antigravity",
        "qoder",
        "qoderwork",
        "qwenwork",
    ]
    .contains(&params.source.as_str())
    {
        return Err("dictation-streaming-source-unavailable".into());
    }
    let language = crate::app_server::subscription_speech::language(params.language.as_deref())?;
    if language.is_some() && params.source != "claude" {
        return Err("dictation-language-unavailable".into());
    }
    if let Some(refinement) = &params.refinement {
        refinement.validate()?;
        if ["claude", "codex", "workbuddy", "antigravity", "trae"].contains(&params.source.as_str())
        {
            return Err("dictation-refinement-unavailable".into());
        }
    }
    let (sender, input) = mpsc::channel(4);
    let (ready_tx, ready) = oneshot::channel();
    let (done_tx, done) = watch::channel(None);
    let handle = Arc::new(Handle {
        signal: AbortSignal::new(),
        admission: tokio::sync::Semaphore::new(4),
        input: tokio::sync::Mutex::new(Input {
            sender: Some(sender),
            next_sequence: 1,
            bytes: 0,
        }),
        sequence: AtomicU64::new(0),
        done,
    });
    {
        let mut active = state.dictation.active.lock().unwrap();
        if active.contains_key(&params.operation_id) {
            return Err("dictation-operation-id-in-use".into());
        }
        if active.len() >= 4 {
            return Err("dictation-operation-limit".into());
        }
        active.insert(params.operation_id.clone(), handle.clone());
    }
    let root = crate::app_server::model_sources::config_root_of(&state.config_path);
    let speech_options = state.subscription_speech_options.clone();
    let claude_usage_options = state.claude_usage_options.clone();
    let credential_store = state.credential_store.clone();
    match params.source.as_str() {
        "claude" | "codex" => {}
        "workbuddy" => crate::app_server::workbuddy::set_config_root(&root),
        "trae" => crate::app_server::trae::set_config_root(&root),
        "antigravity" => crate::app_server::antigravity::set_config_root(&root),
        "qoder" => crate::app_server::qoder::set_config_root(&root),
        "qoderwork" => crate::app_server::qoderwork::set_config_root(&root),
        "qwenwork" => crate::app_server::qwenwork::set_config_root(&root),
        _ => unreachable!(),
    }
    let source = params.source.clone();
    let weak = Arc::downgrade(state);
    let operation_id = params.operation_id.clone();
    let worker = handle.clone();
    tokio::spawn(async move {
        let weak_partial = weak.clone();
        let partial_handle = worker.clone();
        let partial_id = operation_id.clone();
        let publish: crate::app_server::qoder::speech::TextCallback = Arc::new(move |text| {
            let state = weak_partial.upgrade();
            let handle = partial_handle.clone();
            let id = partial_id.clone();
            Box::pin(async move {
                if let Some(state) = state {
                    emit(&state, &handle, &id, json!({"type":"partial", "text":text})).await;
                }
            })
        });
        let recognized: Option<crate::app_server::qoder::speech::TextCallback> =
            params.refinement.as_ref().map(|_| {
                let weak = weak.clone();
                let handle = worker.clone();
                let id = operation_id.clone();
                Arc::new(move |text| {
                    let state = weak.upgrade();
                    let handle = handle.clone();
                    let id = id.clone();
                    Box::pin(async move {
                        if let Some(state) = state {
                            emit(
                                &state,
                                &handle,
                                &id,
                                json!({"type":"recognized", "text":text}),
                            )
                            .await;
                        }
                    }) as futures::future::BoxFuture<'static, ()>
                }) as crate::app_server::qoder::speech::TextCallback
            });
        let stream = async {
            match source.as_str() {
                "claude" => crate::app_server::subscription_speech::stream_claude_dictation(
                    crate::app_server::subscription_speech::ClaudeDictationOptions {
                        root: root.clone(),
                        store: credential_store,
                        speech: speech_options.clone(),
                        usage: claude_usage_options,
                        language,
                    },
                    input,
                    ready_tx,
                    publish,
                    worker.signal.clone(),
                )
                .await
                .map(crate::app_server::qoder::speech::SpeechResult::unrefined),
                "workbuddy" => crate::app_server::workbuddy::stream_dictation(
                    input,
                    ready_tx,
                    publish,
                    worker.signal.clone(),
                )
                .await
                .map(crate::app_server::qoder::speech::SpeechResult::unrefined),
                "codex" => crate::app_server::codex_dictation::stream_dictation(
                    &root,
                    &speech_options.codex_stream_url,
                    input,
                    ready_tx,
                    publish,
                    worker.signal.clone(),
                )
                .await
                .map(crate::app_server::qoder::speech::SpeechResult::unrefined),
                "trae" => crate::app_server::trae::audio::stream_dictation(
                    input,
                    ready_tx,
                    publish,
                    worker.signal.clone(),
                )
                .await
                .map(crate::app_server::qoder::speech::SpeechResult::unrefined),
                "antigravity" => crate::app_server::antigravity::stream_dictation(
                    input,
                    ready_tx,
                    publish,
                    worker.signal.clone(),
                )
                .await
                .map(crate::app_server::qoder::speech::SpeechResult::unrefined),
                "qwenwork" => {
                    crate::app_server::qwenwork::capabilities::stream_dictation(
                        input,
                        ready_tx,
                        publish,
                        worker.signal.clone(),
                        params.refinement,
                        recognized,
                    )
                    .await
                }
                _ => {
                    let source = if source == "qoder" {
                        crate::app_server::qoder::speech::Source::Qoder
                    } else {
                        crate::app_server::qoder::speech::Source::QoderWork
                    };
                    crate::app_server::qoder::speech::stream_dictation(
                        source,
                        input,
                        ready_tx,
                        publish,
                        worker.signal.clone(),
                        params.refinement,
                        recognized,
                    )
                    .await
                    .map_err(|error| source.source_error(&error))
                }
            }
        };
        let result = tokio::select! {
            biased;
            _ = worker.signal.cancelled() => Err("dictation-cancelled".into()),
            result = stream => result,
        };
        if let Some(state) = weak.upgrade() {
            match &result {
                Ok(result) => emit(
                    &state,
                    &worker,
                    &operation_id,
                    json!({"type":"final", "text":result.text, "rawText":result.raw_text, "polished":result.polished, "refinementComplete":result.refinement_complete}),
                ).await,
                Err(code) => emit(
                    &state,
                    &worker,
                    &operation_id,
                    json!({"type":"error", "code":code}),
                ).await,
            }
            let mut active = state.dictation.active.lock().unwrap();
            if active
                .get(&operation_id)
                .is_some_and(|current| Arc::ptr_eq(current, &worker))
            {
                active.remove(&operation_id);
            }
        }
        done_tx.send_replace(Some(result.map(|_| ())));
    });
    match tokio::time::timeout(Duration::from_secs(35), ready).await {
        Ok(Ok(generation)) => {
            let owned = state
                .dictation
                .active
                .lock()
                .unwrap()
                .get(&params.operation_id)
                .is_some_and(|current| Arc::ptr_eq(current, &handle));
            if handle.signal.is_cancelled() || handle.done.borrow().is_some() || !owned {
                return Err("dictation-cancelled".into());
            }
            Ok(
                json!({"operationId":params.operation_id, "sampleRate":16000, "channels":1, "encoding":"s16le", "streaming":true, "credentialGeneration":generation}),
            )
        }
        _ => {
            handle.signal.cancel();
            let reason = handle
                .done
                .borrow()
                .as_ref()
                .and_then(|result| result.as_ref().err())
                .cloned();
            Err(reason.unwrap_or_else(|| "dictation-start-failed".into()))
        }
    }
}

async fn append(
    state: &AppServerState,
    params: wire::DictationAudioParams,
) -> Result<Value, String> {
    let handle = state
        .dictation
        .active
        .lock()
        .unwrap()
        .get(&params.operation_id)
        .cloned()
        .ok_or("dictation-session-not-owned")?;
    append_handle(&handle, params).await
}

async fn append_handle(
    handle: &Handle,
    params: wire::DictationAudioParams,
) -> Result<Value, String> {
    let _admission = handle
        .admission
        .try_acquire()
        .map_err(|_| "dictation-audio-overflow")?;
    if params.audio_base64.len() > 32768_usize.div_ceil(3) * 4 {
        return Err("dictation-audio-invalid".into());
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(&params.audio_base64)
        .map_err(|_| "dictation-audio-invalid")?;
    if bytes.is_empty() || bytes.len() > 32768 || !bytes.len().is_multiple_of(2) {
        return Err("dictation-audio-invalid".into());
    }
    let mut input = handle.input.lock().await;
    if params.seq != input.next_sequence {
        return Err("dictation-audio-sequence".into());
    }
    if input.bytes.saturating_add(bytes.len()) > 16000 * 2 * 300 {
        return Err("dictation-audio-overflow".into());
    }
    let sender = input.sender.as_ref().ok_or("dictation-session-ended")?;
    let length = bytes.len();
    tokio::select! {
        biased;
        _ = handle.signal.cancelled() => return Err("dictation-cancelled".into()),
        result = tokio::time::timeout(Duration::from_secs(5), sender.send(bytes)) => {
            result.map_err(|_| "dictation-audio-write-timeout")?.map_err(|_| "dictation-session-ended")?;
        }
    }
    input.bytes += length;
    input.next_sequence += 1;
    Ok(json!({"seq":params.seq}))
}

async fn end(state: &AppServerState, params: wire::DictationEndParams) -> Result<Value, String> {
    let handle = state
        .dictation
        .active
        .lock()
        .unwrap()
        .get(&params.operation_id)
        .cloned();
    let Some(handle) = handle else {
        return Ok(json!({"complete":true}));
    };
    if params.cancel {
        handle.signal.cancel();
    }
    handle.input.lock().await.sender.take();
    if !params.cancel {
        return Ok(json!({"complete":false}));
    }
    let mut done = handle.done.clone();
    let complete = tokio::time::timeout(Duration::from_secs(5), async {
        while done.borrow().is_none() {
            if done.changed().await.is_err() {
                break;
            }
        }
    })
    .await
    .is_ok();
    Ok(json!({"complete":complete}))
}

pub(super) async fn dispatch(state: Arc<AppServerState>, id: Value, method: String, params: Value) {
    let admission = if method == "engine/dictationEnd" {
        None
    } else {
        match super::HeavyRequestGuard::acquire(&state, &id).await {
            Some(guard) => Some(guard),
            None => return,
        }
    };
    tokio::spawn(async move {
        let _admission = admission;
        let result = match method.as_str() {
            "engine/dictationStart" => match serde_json::from_value(params) {
                Ok(params) => start(&state, params).await,
                Err(error) => Err(error.to_string()),
            },
            "engine/dictationAppendAudio" => match serde_json::from_value(params) {
                Ok(params) => append(&state, params).await,
                Err(error) => Err(error.to_string()),
            },
            "engine/dictationEnd" => match serde_json::from_value(params) {
                Ok(params) => end(&state, params).await,
                Err(error) => Err(error.to_string()),
            },
            _ => Err("dictation-method-unknown".into()),
        };
        match result {
            Ok(result) => state.send(wire::response(&id, result)).await,
            Err(error) => {
                state
                    .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
                    .await
            }
        }
    });
}

pub(super) async fn stop_all(state: &AppServerState) {
    let handles: Vec<_> = state
        .dictation
        .active
        .lock()
        .unwrap()
        .values()
        .cloned()
        .collect();
    for handle in handles {
        handle.signal.cancel();
        handle.input.lock().await.sender.take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stalled_consumer_has_bounded_append_admission_and_cancellation_drains_waiters() {
        let (sender, _receiver) = mpsc::channel(1);
        sender.send(vec![0, 0]).await.unwrap();
        let (_done_sender, done) = watch::channel(None);
        let handle = Arc::new(Handle {
            signal: AbortSignal::new(),
            admission: tokio::sync::Semaphore::new(4),
            input: tokio::sync::Mutex::new(Input {
                sender: Some(sender),
                next_sequence: 1,
                bytes: 0,
            }),
            sequence: AtomicU64::new(0),
            done,
        });
        let mut waiters = Vec::new();
        for _ in 0..4 {
            let handle = handle.clone();
            waiters.push(tokio::spawn(async move {
                append_handle(
                    &handle,
                    wire::DictationAudioParams {
                        operation_id: "capture".into(),
                        seq: 1,
                        audio_base64: "AAA=".into(),
                    },
                )
                .await
            }));
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while handle.admission.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            append_handle(
                &handle,
                wire::DictationAudioParams {
                    operation_id: "capture".into(),
                    seq: 1,
                    audio_base64: "invalid".into()
                }
            )
            .await
            .unwrap_err(),
            "dictation-audio-overflow"
        );
        handle.signal.cancel();
        for waiter in waiters {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), waiter)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap_err(),
                "dictation-cancelled"
            );
        }
        assert_eq!(handle.admission.available_permits(), 4);
        assert_eq!(handle.input.lock().await.bytes, 0);
    }
}
