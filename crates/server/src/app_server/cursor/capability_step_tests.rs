use super::*;

fn barrier(step: &SearchStep) -> Value {
    json!({"name":format!("{OWNER}-barrier"),"args":{"hold":"pending"},"toolCallId":step.barrier_id,"providerIdentifier":OWNER,"toolName":"barrier","serverIdentifier":OWNER})
}
fn call(step: &SearchStep, native: bool) -> Value {
    if native {
        json!({"webSearchToolCall":{"args":{"searchTerm":step.query,"toolCallId":step.native_id}},"toolCallId":step.native_id})
    } else {
        json!({"mcpToolCall":{"args":barrier(step)},"toolCallId":step.barrier_id})
    }
}
fn started(step: &SearchStep, native: bool) -> Value {
    json!({"interactionUpdate":{"toolCallStarted":{"callId":if native{&step.native_id}else{&step.barrier_id},"toolCall":call(step,native),"modelCallId":"invocation"}}})
}
fn approval(step: &SearchStep) -> Value {
    json!({"interactionQuery":{"webSearchRequestQuery":{"args":{"searchTerm":step.query,"toolCallId":step.native_id}}}})
}
fn held(step: &SearchStep) -> Value {
    json!({"execServerMessage":{"mcpArgs":barrier(step),"acceptHookAdditionalContexts":true}})
}
fn completed(step: &SearchStep) -> Value {
    let mut call = call(step, true);
    call["webSearchToolCall"]["result"] = json!({"success":{"references":[{"title":"Web search results","chunk":"Links:\n1. [Example](https://example.com/page)\n\nA supported answer."}]}});
    json!({"interactionUpdate":{"toolCallCompleted":{"callId":step.native_id,"toolCall":call,"modelCallId":"invocation"}}})
}

#[test]
fn pending_search_binds_one_exact_query_and_holds_barrier_before_approval() {
    let mut step = SearchStep::new("caller exact query").unwrap();
    let request=step.request(&json!({"legacySlug":"fixture-high","requestedModel":{"modelId":"fixture","maxMode":false,"parameters":[{"id":"effort","value":"high"}]}})).unwrap();
    assert_eq!(request["runRequest"]["conversationState"]["mode"], 2);
    assert_eq!(
        request["runRequest"]["requestedModel"]["parameters"][0]["value"],
        "high"
    );
    assert_eq!(
        request["runRequest"]["conversationState"]["pendingToolCalls"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(request["runRequest"].get("prompt").is_none());
    assert!(step.receive(&approval(&step)).unwrap().is_empty());
    assert!(step.receive(&started(&step, false)).unwrap().is_empty());
    let approved = step.receive(&held(&step)).unwrap();
    assert_eq!(approved.len(), 1);
    assert_eq!(approved[0]["interactionResponse"]["id"], 0);
    assert!(approved[0]["interactionResponse"]
        .get("webSearchRequestResponse")
        .is_some());
    assert!(step.receive(&started(&step, true)).unwrap().is_empty());
    assert!(step.receive(&completed(&step)).unwrap().is_empty());
    let result = step.take_result().unwrap();
    assert_eq!(result["results"][0]["url"], "https://example.com/page");
    assert!(result["results"][0]["snippet"]
        .as_str()
        .unwrap()
        .contains("supported answer"));
    assert!(result["answer"].as_str().unwrap().contains("Links:"));
}

#[test]
fn pending_search_rejects_foreign_exec_model_events_changed_queries_and_early_completion() {
    for packet in [
        json!({"execServerMessage":{"readArgs":{"path":"/private/file"}}}),
        json!({"execServerMessage":{"shellArgs":{"command":"echo rejected"}}}),
        json!({"interactionUpdate":{"textDelta":{"text":"unexpected model"}}}),
        json!({"interactionUpdate":{"thinkingDelta":{"text":"unexpected model"}}}),
        json!({"interactionUpdate":{"stepStarted":{}}}),
        json!({"execServerControlMessage":{}}),
        json!({"interactionQuery":{"writeFileQuery":{}}}),
        json!({"interactionUpdate":{"heartbeat":{},"toolCallStarted":{}}}),
    ] {
        let mut step = SearchStep::new("query").unwrap();
        assert!(step.receive(&packet).is_err());
        assert!(step.take_result().is_none());
    }
    let mut step = SearchStep::new("query").unwrap();
    let mut wrong = approval(&step);
    wrong["interactionQuery"]["webSearchRequestQuery"]["args"]["searchTerm"] = json!("other");
    assert!(step.receive(&wrong).is_err());
    assert!(step.receive(&completed(&step)).is_err());
    let mut step = SearchStep::new("query").unwrap();
    let mut wrong = held(&step);
    wrong["execServerMessage"]["mcpArgs"]["providerIdentifier"] = json!("foreign");
    assert!(step.receive(&wrong).is_err());
}

#[test]
fn search_reference_defaults_preserve_answers_and_only_real_safe_citations() {
    assert_eq!(project_search(&json!({})).unwrap()["results"], json!([]));
    let result =
        project_search(&json!({"references":[{"chunk":"Changed prose without citations."}]}))
            .unwrap();
    assert_eq!(result["results"], json!([]));
    assert_eq!(result["answer"], "Changed prose without citations.");
    let result=project_search(&json!({"references":[{"chunk":"[Unsafe](javascript:alert)\n[Credentials](https://user:pass@example.com)\n[Good](https://example.com/page)\n[Repeat](https://example.com/page)"}]})).unwrap();
    assert_eq!(result["results"].as_array().unwrap().len(), 1);
    assert!(project_search(&json!({"references":[{"url":"file:///private"}]})).is_err());
    assert!(project_search(&json!({"references":null})).is_err());
}

#[test]
fn a_result_never_publishes_with_already_received_partial_trailing_frame() {
    let mut step = SearchStep::new("query").unwrap();
    step.result = Some(json!({"results":[]}));
    let mut decoder = bridge::Decoder::default();
    assert!(decoder.push(&[0, 0, 0]).unwrap().is_empty());
    assert_eq!(
        certified_result(&mut step, &decoder).unwrap_err(),
        "cursor_native_operation_response_invalid"
    );
    assert!(snippet(&"字".repeat(32768)).len() <= 32768);
}

pub(crate) struct StepFixture {
    pub origin: String,
    pub started: std::sync::Arc<tokio::sync::Notify>,
    pub closed: std::sync::Arc<tokio::sync::Notify>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for StepFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(crate) async fn fixture(hang: bool, image: bool) -> StepFixture {
    use http_body_util::{BodyExt, StreamBody};
    use hyper::{body::Frame, Response};
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use std::convert::Infallible;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let started_signal = std::sync::Arc::new(tokio::sync::Notify::new());
    let closed = std::sync::Arc::new(tokio::sync::Notify::new());
    let (first, done) = (started_signal.clone(), closed.clone());
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let service = hyper::service::service_fn(
            move |request: hyper::Request<hyper::body::Incoming>| {
                let (first, done) = (first.clone(), done.clone());
                async move {
                    assert_eq!(request.uri().path(), "/agent.v1.AgentService/Run");
                    let (tx, rx) = mpsc::channel::<Bytes>(4);
                    tokio::spawn(async move {
                        let mut input = request.into_body();
                        let mut decoder = bridge::Decoder::default();
                        async fn next(
                            input: &mut hyper::body::Incoming,
                            decoder: &mut bridge::Decoder,
                        ) -> Value {
                            loop {
                                let chunk =
                                    input.frame().await.unwrap().unwrap().into_data().unwrap();
                                let mut values = decoder.push(&chunk).unwrap();
                                if !values.is_empty() {
                                    assert_eq!(values.len(), 1);
                                    return values.remove(0).1;
                                }
                            }
                        }
                        let request = next(&mut input, &mut decoder).await;
                        let run = &request["runRequest"];
                        assert!(run.get("prompt").is_none());
                        assert_eq!(run["canCreateCloudSubagents"], false);
                        let pending: Value = serde_json::from_str(
                            run["conversationState"]["pendingToolCalls"][0]
                                .as_str()
                                .unwrap(),
                        )
                        .unwrap();
                        let roots = run["conversationState"]["rootPromptMessagesJson"]
                            .as_array()
                            .unwrap();
                        assert_eq!(roots.len(), 3);
                        // The first numeric ID is omitted by protobuf JSON.
                        tx.send(
                            bridge::encode_frame(
                                json!({"kvServerMessage":{"getBlobArgs":{"blobId":roots[0]}}}),
                            )
                            .unwrap(),
                        )
                        .await
                        .unwrap();
                        let reply = next(&mut input, &mut decoder).await;
                        assert_eq!(reply["kvClientMessage"]["id"], 0);
                        assert!(reply["kvClientMessage"].get("getBlobResult").is_some());
                        first.notify_one();
                        if hang {
                            let _ = tx.closed().await;
                            done.notify_one();
                            return;
                        }
                        let native = pending["content"][0]["toolCallId"].as_str().unwrap();
                        if image {
                            let args = &pending["content"][0]["args"]["arguments"];
                            let typed = json!({"description":args["description"],"filePath":args["filename"],"aspectRatio":args["aspect_ratio"],"referenceImagePaths":args["reference_image_paths"]});
                            let call =
                                json!({"toolCallId":native,"generateImageToolCall":{"args":typed}});
                            tx.send(bridge::encode_frame(json!({"interactionUpdate":{"partialToolCall":{"callId":native,"toolCall":{"toolCallId":native,"generateImageToolCall":{}},"modelCallId":"fixture"}},"ttftBreakdown":{"preStreamSetupMs":1.25,"serverFirstTokenMs":2.5,"waitForFirstEventMs":3.75}})).unwrap()).await.unwrap();
                            tx.send(bridge::encode_frame(json!({"interactionUpdate":{"toolCallStarted":{"callId":native,"toolCall":call,"modelCallId":"fixture"}}})).unwrap()).await.unwrap();
                            for path in args["reference_image_paths"]
                                .as_array()
                                .into_iter()
                                .flatten()
                            {
                                tx.send(bridge::encode_frame(json!({"execServerMessage":{"readArgs":{"path":path,"toolCallId":native}}})).unwrap()).await.unwrap();
                                let read = next(&mut input, &mut decoder).await;
                                assert_eq!(
                                    read["execClientMessage"]["readResult"]["success"]["path"],
                                    *path
                                );
                                assert!(read["execClientMessage"]["readResult"]["success"]["data"]
                                    .as_str()
                                    .unwrap()
                                    .starts_with("iVBOR"));
                            }
                            let path = std::path::Path::new(
                                run["action"]["resumeAction"]["requestContext"]["env"]
                                    ["artifactsFolder"]
                                    .as_str()
                                    .unwrap(),
                            )
                            .join("assets")
                            .join(args["filename"].as_str().unwrap());
                            tx.send(bridge::encode_frame(json!({"execServerMessage":{"writeArgs":{"path":path,"fileBytes":STANDARD.encode(b"\x89PNG\r\n\x1a\nfixture-image")}}})).unwrap()).await.unwrap();
                        } else {
                            let barrier_id = pending["content"][1]["toolCallId"].as_str().unwrap();
                            let query = pending["content"][0]["args"]["arguments"]["search_term"]
                                .as_str()
                                .unwrap();
                            let mut step = SearchStep::new(query).unwrap();
                            step.native_id = native.into();
                            step.barrier_id = barrier_id.into();
                            for packet in [
                                started(&step, true),
                                started(&step, false),
                                approval(&step),
                                held(&step),
                            ] {
                                tx.send(bridge::encode_frame(packet).unwrap())
                                    .await
                                    .unwrap();
                            }
                            let approved = next(&mut input, &mut decoder).await;
                            assert!(approved["interactionResponse"]
                                .get("webSearchRequestResponse")
                                .is_some());
                            tx.send(bridge::encode_frame(completed(&step)).unwrap())
                                .await
                                .unwrap();
                        }
                        // A write/MCP execution result would let the remote loop
                        // continue; the provider must close instead of replying.
                        tokio::time::timeout(std::time::Duration::from_secs(2), async {
                            while let Some(Ok(frame)) = input.frame().await {
                                if let Ok(bytes) = frame.into_data() {
                                    assert!(
                                        bytes.is_empty(),
                                        "no execution result may be acknowledged"
                                    );
                                }
                            }
                            tx.closed().await;
                        })
                        .await
                        .expect("both stream halves closed");
                        done.notify_one();
                    });
                    let output = futures::stream::unfold(rx, |mut rx| async move {
                        rx.recv()
                            .await
                            .map(|bytes| (Ok::<_, Infallible>(Frame::data(bytes)), rx))
                    });
                    Ok::<_, Infallible>(
                        Response::builder()
                            .header("content-type", "application/connect+json")
                            .body(StreamBody::new(output))
                            .unwrap(),
                    )
                }
            },
        );
        let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
            .serve_connection(TokioIo::new(socket), service)
            .await;
    });
    StepFixture {
        origin,
        started: started_signal,
        closed,
        task,
    }
}

#[tokio::test]
async fn http2_search_closes_held_barrier_before_return_and_cancel_epoch_interrupt_it() {
    let _guard = super::super::test_lock().lock().await;
    let root = tempfile::tempdir().unwrap();
    super::super::set_config_root(root.path());
    for mode in ["success", "cancel", "epoch"] {
        let fixture = fixture(mode != "success", false).await;
        super::super::adopt_credential(
            json!({"accessToken":"fixture","clientIdentity":"fixture","inferenceUrl":fixture.origin}),
            None,
        );
        let (credential, generation) = credential::snapshot().unwrap();
        let signal = klaus_agent_core::types::abort::AbortSignal::new();
        let abort = signal.clone();
        let task = tokio::spawn(async move {
            super::super::capabilities::cancellable(&signal,std::time::Duration::from_secs(5),search(&credential,generation,&json!({"legacySlug":"fixture-high","requestedModel":{"modelId":"fixture","maxMode":false,"parameters":[]}}),"query")).await
        });
        fixture.started.notified().await;
        if mode == "cancel" {
            abort.cancel();
        } else if mode == "epoch" {
            super::super::adopt_credential(
                json!({"accessToken":"new-account","clientIdentity":"fixture"}),
                None,
            );
        }
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        match mode {
            "success" => assert_eq!(
                result.unwrap()["results"][0]["url"],
                "https://example.com/page"
            ),
            "cancel" => assert_eq!(result.unwrap_err(), "cursor_operation_cancelled"),
            _ => assert_eq!(result.unwrap_err(), "cursor_credential_changed"),
        };
        tokio::time::timeout(std::time::Duration::from_secs(2), fixture.closed.notified())
            .await
            .unwrap();
    }
}
