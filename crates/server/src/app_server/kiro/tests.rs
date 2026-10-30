use super::{auth, bridge, catalog};
use serde_json::{json, Value};
fn credential() -> auth::Credential {
    serde_json::from_value(json!({"accessToken":"fixture-token","authMethod":"social","provider":"Google","profileArn":"arn:aws:codewhisperer:us-east-1:123:profile/test"})).unwrap()
}
#[test]
fn native_parameter_refusals_keep_safe_diagnostics_separate_from_authentication() {
    let refused = json!({"error":{"code":"unsupported_parameter","param":"max_output_tokens","message":"ignored echoed contents"}});
    let (code, native) = super::errors::classify(400, &refused);
    assert_eq!(code, "kiro_request_parameter_unsupported");
    assert_eq!(
        super::errors::diagnostic(code, &native, &refused),
        "kiro_request_parameter_unsupported (unsupported_parameter; parameter=max_output_tokens)"
    );
    let unsafe_param = json!({"error":{"param":"Bearer secret echoed by service"}});
    assert_eq!(
        super::errors::diagnostic(code, &native, &unsafe_param),
        "kiro_request_parameter_unsupported (unsupported_parameter)"
    );
    assert_eq!(super::errors::classify(403, &json!({"error":{"code":"access_denied","message":"The bearer token included in the request is invalid."}})).0, "kiro_login_expired");
    assert_eq!(
        super::errors::classify(
            403,
            &json!({"error":{"code":"access_denied","message":"A different service refusal"}})
        )
        .0,
        "kiro_request_rejected"
    );
}
#[test]
fn inventory_preserves_exact_identity_and_account_declarations() {
    let raw = vec![
        json!({"modelId":"shared-id","modelName":"Shared model","status":"UNAVAILABLE","consentStatus":"PENDING","supportedInputTypes":["TEXT","IMAGE"],"tokenLimits":{"maxInputTokens":200000,"maxOutputTokens":64000}}),
        json!({"modelId":"text-only","modelName":"Text only","supportedInputTypes":["TEXT"],"tokenLimits":{"maxInputTokens":164000,"maxOutputTokens":32000}}),
    ];
    let rows = catalog::normalize(&raw, Some("shared-id"));
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["key"], "shared-id");
    assert_eq!(rows[0]["capabilities"]["contextWindow"], 200000);
    assert_eq!(rows[0]["providerMetadata"]["status"], "UNAVAILABLE");
    assert_eq!(rows[0]["supportsVision"], true);
    assert_eq!(rows[1]["supportsVision"], false);
    assert_eq!(rows[0]["efforts"], json!([]));
    assert!(!rows[0]["supportsThinking"].as_bool().unwrap());
    assert!(super::super::conversation_catalog::is_conversation_model(
        "kiro", &rows[0]
    ));
}
#[test]
fn native_reasoning_schema_declares_only_exact_options() {
    let rows = catalog::normalize(
        &[
            json!({"modelId":"reasoning-model","additionalModelRequestFieldsSchema":{"properties":{"reasoning":{"properties":{"effort":{"enum":["low","high"],"default":"high"},"type":{"enum":["adaptive"]}}}}}}),
        ],
        None,
    );
    assert_eq!(rows[0]["efforts"], json!([]));
    assert_eq!(rows[0]["kiroVariants"][2]["nativeEffort"], "high");
    assert_eq!(rows[0]["kiroVariants"][0]["nativeEffort"], "high");
    assert_eq!(rows[0]["capabilities"]["thinking"]["adaptive"], true);
}
#[test]
fn response_adapter_keeps_native_ids_tools_and_image_data() {
    let input = json!({"model":"shared-id","tool_choice":"auto","parallel_tool_calls":true,"max_output_tokens":256,"input":[{"type":"message","role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,YWJj"}]},{"type":"function_call_output","call_id":"call-1","output":"answer"}],"tools":[{"type":"function","name":"fixture_marker","parameters":{"type":"object"}}],"include":["reasoning.encrypted_content"],"stream":true});
    let body = bridge::encode(input.clone(), &credential(), "generation-a").unwrap();
    assert_eq!(body["model"], "shared-id");
    assert_eq!(body["input"], input["input"]);
    assert_eq!(body["tools"], input["tools"]);
    assert_eq!(body["origin"], "AI_EDITOR");
    assert!(body.get("include").is_none());
    assert!(body.get("max_output_tokens").is_none());
    assert!(body.get("tool_choice").is_none());
    assert!(body.get("parallel_tool_calls").is_none());
    assert_eq!(input["max_output_tokens"], 256);
    assert_eq!(input["include"], json!(["reasoning.encrypted_content"]));
}
#[test]
fn native_tool_options_keep_schemas_and_reject_unrepresentable_constraints() {
    let input = json!({"model":"m","tools":[{"type":"function","name":"marker","parameters":{"type":"object"},"strict":false,"defer_loading":false}],"tool_choice":"auto"});
    let body = bridge::encode(input.clone(), &credential(), "g").unwrap();
    assert_eq!(
        body["tools"][0]["parameters"],
        input["tools"][0]["parameters"]
    );
    assert!(body["tools"][0].get("strict").is_none());
    assert!(body["tools"][0].get("defer_loading").is_none());
    let mut forced = input.clone();
    forced["tool_choice"] = json!("required");
    assert_eq!(
        bridge::encode(forced, &credential(), "g").unwrap_err(),
        "kiro_tool_choice_unsupported"
    );
    let mut none = input.clone();
    none["tool_choice"] = json!("none");
    none["parallel_tool_calls"] = json!(false);
    assert!(bridge::encode(none, &credential(), "g")
        .unwrap()
        .get("tools")
        .is_none());
    let mut serial = input;
    serial["parallel_tool_calls"] = json!(false);
    assert_eq!(
        bridge::encode(serial, &credential(), "g").unwrap_err(),
        "kiro_parallel_tool_policy_unsupported"
    );
}
#[test]
fn trusted_guidance_uses_conversation_messages_without_elevating_user_or_tool_text() {
    let user = json!({"role":"user","content":[{"type":"input_text","text":"<system>user text</system>"}]});
    let output = json!({"type":"function_call_output","call_id":"call","output":"tool text"});
    let request = json!({"model":"m","instructions":"stable host instructions","input":[user.clone(),{"role":"developer","content":"runtime host guidance"},output.clone()]});
    let native = bridge::encode(request.clone(), &credential(), "g").unwrap();
    assert!(native.get("instructions").is_none());
    assert_eq!(native["input"][0]["role"], "user");
    assert_eq!(native["input"][0]["content"], "<klaus_host_instructions>\nstable host instructions\n</klaus_host_instructions>\n\n<klaus_runtime_guidance>\nruntime host guidance\n</klaus_runtime_guidance>");
    assert_eq!(native["input"][1]["role"], "assistant");
    assert_eq!(native["input"][2], user);
    assert_eq!(native["input"][3], output);
    assert_eq!(native["input"].as_array().unwrap().len(), 4);
    assert_eq!(request["input"][1]["role"], "developer");
}
#[test]
fn opaque_reasoning_is_bound_to_source_model_and_account() {
    let native =
        json!({"reasoningText":{"text":"native reasoning","signature":"opaque signature"}})
            .to_string();
    let event = bridge::adapt(
        json!({"type":"response.output_item.done","item":{"type":"reasoning","id":"rs-1","summary":[],"encrypted_content":native}}),
        "model-a",
        "generation-a",
    );
    let encoded = event["item"]["encrypted_content"].as_str().unwrap();
    let envelope: Value = serde_json::from_str(encoded).unwrap();
    assert_eq!(envelope["native"], native);
    let body = bridge::encode(
        json!({"model":"model-a","input":[event["item"].clone()]}),
        &credential(),
        "generation-a",
    )
    .unwrap();
    assert_eq!(body["input"][0]["encrypted_content"], native);
    for (model, generation) in [("model-b", "generation-a"), ("model-a", "generation-b")] {
        let body=bridge::encode(json!({"model":model,"input":[event["item"].clone(),{"type":"message","role":"user","content":"next"}]}),&credential(),generation).unwrap();
        assert_eq!(body["input"].as_array().unwrap().len(), 1);
    }
    let body = bridge::encode(
        json!({"model":"model-a","input":[{"type":"reasoning","encrypted_content":native}]}),
        &credential(),
        "generation-a",
    )
    .unwrap();
    assert!(body["input"].as_array().unwrap().is_empty());
}
#[test]
fn reasoning_delta_conversion_is_local_and_preserves_terminal() {
    let input = json!({"type":"response.reasoning_text.delta","delta":"thinking","item_id":"rs-1"});
    let output = bridge::adapt(input.clone(), "model-a", "generation-a");
    assert_eq!(output["type"], "response.reasoning_summary_text.delta");
    assert_eq!(input["type"], "response.reasoning_text.delta");
    let terminal = json!({"type":"response.completed","response":{"status":"completed","usage":{"kiro:credits":{"value":0.25}}}});
    assert_eq!(
        bridge::adapt(terminal.clone(), "model-a", "generation-a"),
        terminal
    );
}
#[test]
fn social_headers_and_region_are_source_scoped() {
    let c = credential();
    let h = super::headers(&c).unwrap();
    assert_eq!(h["authorization"], "Bearer fixture-token");
    assert!(h.get("tokentype").is_none());
    assert_eq!(super::region(&c), "us-east-1");
    let mut other = c.clone();
    other.auth_method = "IdC".into();
    assert_eq!(super::headers(&other).unwrap()["tokentype"], "SSO_OIDC");
    assert!(super::headers(&c).unwrap().get("tokentype").is_none());
}

#[tokio::test]
async fn imported_credentials_expire_without_refreshing_or_modifying_external_state() {
    let root = tempfile::tempdir().unwrap();
    super::set_config_root(root.path());
    let external = root.path().join("external-kiro.json");
    std::fs::write(&external, b"external grant sentinel").unwrap();
    let mut c = credential();
    c.refresh_token = Some("external-refresh-token".into());
    c.expires_at = Some("2020-01-01T00:00:00Z".into());
    let status = auth::adopt(c, false).await.unwrap();
    assert_eq!(status["needsLogin"], true);
    assert_eq!(status["canRefresh"], false);
    assert_eq!(
        auth::credential(None).await.err().unwrap(),
        "kiro_login_expired"
    );
    let generation = auth::generation().unwrap();
    let status = auth::adopt(credential(), false).await.unwrap();
    assert_eq!(status["loggedIn"], true);
    assert_eq!(
        auth::credential(Some(&generation)).await.err().unwrap(),
        "kiro_credential_changed"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(root.path().join("kiro-auth.json"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    auth::logout().await.unwrap();
    assert_eq!(auth::status()["loggedIn"], false);
    assert_eq!(
        std::fs::read(&external).unwrap(),
        b"external grant sentinel"
    );
}

#[tokio::test]
async fn login_ownership_cancel_and_connection_close_leave_committed_credentials_intact() {
    use std::sync::{atomic::AtomicBool, Arc};
    let root = tempfile::tempdir().unwrap();
    super::set_config_root(root.path());
    let result = super::login::start(7, Arc::new(AtomicBool::new(true))).await;
    assert_eq!(result["error"], "kiro_login_cancelled");
    let first = super::login::start(7, Arc::new(AtomicBool::new(false))).await;
    assert!(first["expiresAt"].as_i64().unwrap() > auth::now());
    assert_eq!(first["interval"], 1);
    let id = first["flowId"].as_str().unwrap();
    let uri = url::Url::parse(first["authUrl"].as_str().unwrap()).unwrap();
    assert_eq!(uri.origin().ascii_serialization(), "https://app.kiro.dev");
    let query: std::collections::HashMap<_, _> = uri.query_pairs().into_owned().collect();
    assert_eq!(query["code_challenge_method"], "S256");
    assert_eq!(query["redirect_from"], "KiroIDE");
    assert_eq!(
        super::login::poll(8, id).await["error"],
        "kiro_login_cancelled"
    );
    assert_eq!(super::login::poll(7, id).await["pending"], true);
    super::login::cancel(8, id).await;
    assert_eq!(super::login::poll(7, id).await["pending"], true);
    super::login::cancel(7, id).await;
    assert_eq!(
        super::login::poll(7, id).await["error"],
        "kiro_login_cancelled"
    );
    auth::adopt(credential(), false).await.unwrap();
    super::login::cancel(7, id).await;
    assert_eq!(auth::status()["loggedIn"], true);
    let second = super::login::start(7, Arc::new(AtomicBool::new(false))).await;
    let id = second["flowId"].as_str().unwrap();
    drop(super::login::ConnectionGuard::new(7));
    assert_eq!(
        super::login::poll(7, id).await["error"],
        "kiro_login_cancelled"
    );
    assert_eq!(auth::status()["loggedIn"], true);
}

async fn read_http_request(socket: &mut tokio::net::TcpStream) -> (String, Value) {
    use tokio::io::AsyncReadExt;
    let mut bytes = Vec::new();
    loop {
        let mut chunk = [0; 4096];
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&chunk[..n]);
        if let Some(header_end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            let headers = std::str::from_utf8(&bytes[..header_end]).unwrap();
            let length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|s| s.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            if bytes.len() >= header_end + 4 + length {
                let body = if length == 0 {
                    Value::Null
                } else {
                    serde_json::from_slice(&bytes[header_end + 4..header_end + 4 + length]).unwrap()
                };
                return (headers.lines().next().unwrap().to_owned(), body);
            }
        }
    }
}
#[tokio::test]
async fn direct_login_uses_pkce_callback_path_and_renews_only_its_independent_grant() {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
    use sha2::{Digest, Sha256};
    use std::sync::{atomic::AtomicBool, Arc};
    use tokio::io::AsyncWriteExt;
    let root = tempfile::tempdir().unwrap();
    super::set_config_root(root.path());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let auth_url = format!("http://{}", listener.local_addr().unwrap());
    std::env::set_var("KLAUS_TEST", "1");
    std::env::set_var("KLAUS_KIRO_AUTH_URL", &auth_url);
    let (sent_tx, mut sent_rx) = tokio::sync::mpsc::channel(2);
    let server = tokio::spawn(async move {
        for index in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut socket).await;
            sent_tx.send(request).await.unwrap();
            let body=json!({"accessToken":format!("independent-token-{index}"),"refreshToken":format!("independent-refresh-{index}"),"profileArn":if index==0{"arn:aws:codewhisperer:us-east-1:123:profile/independent"}else{"unexpected-other-profile"},"expiresIn":3600}).to_string();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        }
    });
    let flow = super::login::start(27, Arc::new(AtomicBool::new(false))).await;
    let flow_id = flow["flowId"].as_str().unwrap().to_owned();
    let uri = url::Url::parse(flow["authUrl"].as_str().unwrap()).unwrap();
    let params: std::collections::HashMap<_, _> = uri.query_pairs().into_owned().collect();
    let loopback = url::Url::parse(&params["redirect_uri"]).unwrap();
    let mut browser =
        tokio::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, loopback.port().unwrap()))
            .await
            .unwrap();
    browser.write_all(format!("GET /signin/callback?state={}&login_option=google&code=fixture-code HTTP/1.1\r\nHost: localhost:{}\r\n\r\n",params["state"],loopback.port().unwrap()).as_bytes()).await.unwrap();
    let (line, body) = tokio::time::timeout(std::time::Duration::from_secs(5), sent_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(line, "POST /oauth/token HTTP/1.1");
    assert_eq!(body["code"], "fixture-code");
    assert_eq!(
        body["redirect_uri"],
        format!(
            "{}/signin/callback?login_option=google",
            params["redirect_uri"]
        )
    );
    assert_eq!(
        URL_SAFE_NO_PAD.encode(Sha256::digest(
            body["code_verifier"].as_str().unwrap().as_bytes()
        )),
        params["code_challenge"]
    );
    for _ in 0..100 {
        if super::login::poll(27, &flow_id).await["status"] == "complete" {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(super::login::poll(27, &flow_id).await["status"], "complete");
    let generation = auth::generation().unwrap();
    let mut stored: Value =
        serde_json::from_slice(&std::fs::read(root.path().join("kiro-auth.json")).unwrap())
            .unwrap();
    stored["credential"]["expiresAt"] = json!("2020-01-01T00:00:00Z");
    std::fs::write(
        root.path().join("kiro-auth.json"),
        serde_json::to_vec(&stored).unwrap(),
    )
    .unwrap();
    let credential = auth::credential(Some(&generation)).await.unwrap();
    assert_eq!(credential.access_token, "independent-token-1");
    assert_eq!(
        credential.profile_arn.as_deref(),
        Some("arn:aws:codewhisperer:us-east-1:123:profile/independent")
    );
    assert_eq!(auth::generation().as_deref(), Some(generation.as_str()));
    let (line, body) = sent_rx.recv().await.unwrap();
    assert_eq!(line, "POST /refreshToken HTTP/1.1");
    assert_eq!(body["refreshToken"], "independent-refresh-0");
    super::login::cancel(27, &flow_id).await;
    assert_eq!(auth::status()["loggedIn"], true);
    server.await.unwrap();
    std::env::remove_var("KLAUS_KIRO_AUTH_URL");
    std::env::remove_var("KLAUS_TEST");
}

#[test]
fn exact_native_effort_members_do_not_change_the_base_wire_identity() {
    let rows = catalog::normalize(
        &[
            json!({"modelId":"exact-model","additionalModelRequestFieldsSchema":{"properties":{"output_config":{"properties":{"effort":{"enum":["low","high","max"],"default":"high"}}},"thinking":{"properties":{"type":{"enum":["disabled","adaptive"],"default":"adaptive"}}}}}}),
        ],
        None,
    );
    assert_eq!(
        rows[0]["kiroVariants"][3]["value"],
        "kiro:exact-model::effort=max"
    );
    assert_eq!(catalog::expanded_rows(&rows).len(), 5);
    let body = bridge::encode(
        json!({"model":"exact-model::effort=max","input":[],"reasoning":{"effort":"low"}}),
        &credential(),
        "generation-a",
    )
    .unwrap();
    let body = bridge::native_options(body, &rows[0], "exact-model::effort=max").unwrap();
    assert_eq!(body["model"], "exact-model");
    assert_eq!(body["reasoning"], json!({"type":"adaptive","effort":"max"}));
    assert!(body.get("output_config").is_none());
    assert!(bridge::native_options(
        json!({"model":"exact-model"}),
        &rows[0],
        "exact-model::effort=none"
    )
    .is_err());
    assert!(catalog::selection_parts("exact-model::effort=%6dax").is_err());
    let plain = bridge::native_options(
        json!({"model":"plain","reasoning":{"effort":"high"}}),
        &json!({}),
        "plain",
    )
    .unwrap();
    assert!(plain.get("reasoning").is_none());
}
#[test]
fn refusal_classification_keeps_rate_quota_model_and_iam_separate() {
    for (status, native, expected) in [
        (
            403,
            json!({"__type":"AccessDeniedException"}),
            "kiro_request_rejected",
        ),
        (
            403,
            json!({"reason":"MODEL_NOT_SUBSCRIBED"}),
            "kiro_model_access_denied",
        ),
        (
            402,
            json!({"reason":"INSUFFICIENT_CREDITS"}),
            "kiro_quota_exhausted",
        ),
        (
            429,
            json!({"reason":"MONTHLY_REQUEST_COUNT","type":"too_many_requests"}),
            "kiro_rate_limited",
        ),
        (
            400,
            json!({"code":"context_length_exceeded"}),
            "kiro_context_length_exceeded",
        ),
        (401, json!({"code":"invalid_token"}), "kiro_login_expired"),
        (
            404,
            json!({"error":{"code":"model_not_found","type":"not_found","param":"model","message":"Invalid model ID: fixture"}}),
            "kiro_model_not_found",
        ),
        (
            404,
            json!({"error":{"code":"route_not_found"}}),
            "kiro_request_rejected",
        ),
    ] {
        assert_eq!(super::errors::classify(status, &native).0, expected);
    }
}
#[tokio::test]
async fn terminal_policy_is_bound_to_the_source_and_keeps_other_requests_unchanged() {
    use futures::StreamExt;
    use klaus_agent_core::{
        runtime_values::{runtime_codec, RuntimeString},
        types::{
            abort::AbortSignal,
            errors::{ProviderError, ProviderErrorKind},
            provider::{
                ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
                ProviderCapabilities, ProviderInfo,
            },
        },
    };
    use std::sync::{Arc, Mutex};
    struct Scripted {
        inner: klaus_agent_core::testing::FakeProvider,
        requests: Mutex<Vec<ModelRequest>>,
        events: Vec<ModelStreamEvent>,
    }
    impl ModelProvider for Scripted {
        fn info(&self) -> &ProviderInfo {
            self.inner.info()
        }
        fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
            self.inner.capabilities(model)
        }
        fn stream(&self, request: ModelRequest, _signal: AbortSignal) -> ModelStream {
            self.requests.lock().unwrap().push(request);
            Box::pin(futures::stream::iter(self.events.clone()))
        }
    }
    let request:ModelRequest=runtime_codec::decode_text(&RuntimeString::from(json!({"model":"shared-model-id","system":[],"messages":[],"tools":[{"name":"fixture_marker","description":"Return a marker","inputSchema":{"type":"object"}}],"maxOutputTokens":321}).to_string())).unwrap();
    for (reason, expected) in [
        ("max_output_tokens", None),
        ("content_filter", Some(ProviderErrorKind::Withheld)),
        ("model_not_found", Some(ProviderErrorKind::InvalidRequest)),
        (
            "compaction_required_for_model_handoff",
            Some(ProviderErrorKind::PromptTooLong),
        ),
    ] {
        let inner = Arc::new(Scripted {
            inner: klaus_agent_core::testing::fake_provider(),
            requests: Mutex::new(Vec::new()),
            events: vec![ModelStreamEvent::Error {
                error: ProviderError {
                    kind: ProviderErrorKind::Network,
                    message: "Incomplete response".into(),
                    status: None,
                    code: Some(if reason == "model_not_found" {
                        "kiro_model_not_found".into()
                    } else {
                        "response_incomplete".into()
                    }),
                    error_type: Some(reason.into()),
                    request_id: None,
                    media_origins: None,
                    media_repair_complete: false,
                    retry_after_ms: None,
                },
            }],
        });
        let ordinary = inner
            .stream(request.clone(), AbortSignal::new())
            .collect::<Vec<_>>()
            .await;
        let adapted = super::provider::decorate(inner.clone())
            .stream(request.clone(), AbortSignal::new())
            .collect::<Vec<_>>()
            .await;
        assert!(
            matches!(&ordinary[0],ModelStreamEvent::Error{error}if error.kind==ProviderErrorKind::Network)
        );
        match expected {
            None => assert!(matches!(
                &adapted[0],
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::MaxTokens,
                    ..
                }
            )),
            Some(kind) => {
                assert!(matches!(&adapted[0],ModelStreamEvent::Error{error}if error.kind==kind))
            }
        }
        let requests = inner.requests.lock().unwrap();
        assert_eq!(requests.as_slice(), &[request.clone(), request.clone()]);
        assert_eq!(requests[1].tools[0].name, "fixture_marker");
        assert_eq!(requests[1].max_output_tokens, 321);
    }
}

#[test]
fn thinking_only_models_can_disable_and_reenable_without_an_effort_enum() {
    let rows = catalog::normalize(
        &[
            json!({"modelId":"thinking-only","additionalModelRequestFieldsSchema":{"properties":{"thinking":{"properties":{"type":{"enum":["adaptive","disabled"],"default":"adaptive"}}}}}}),
        ],
        None,
    );
    for kind in ["disabled", "adaptive"] {
        let selection = format!("thinking-only::effort={kind}");
        assert!(catalog::expanded_rows(&rows)
            .iter()
            .any(|r| r["key"] == selection));
        let body =
            bridge::native_options(json!({"model":"thinking-only"}), &rows[0], &selection).unwrap();
        assert_eq!(body["reasoning"]["type"], kind);
        assert!(body["reasoning"].get("effort").is_none());
    }
}
#[test]
fn native_catalog_keeps_exact_members_without_synthesizing_template_ids() {
    let rows = catalog::normalize(
        &[
            json!({"modelId":"fixture","additionalModelRequestFieldsSchema":{"properties":{"reasoning":{"properties":{"effort":{"enum":["default","high"],"default":"default"}}}}}}),
        ],
        None,
    );
    assert_eq!(rows[0]["kiroVariants"][0]["nativeEffort"], Value::Null);
    assert!(rows[0]["kiroVariants"]
        .as_array()
        .unwrap()
        .iter()
        .all(|v| v["nativeEffort"] != "default"));
    let native = vec![
        json!({"modelId":"native-restricted","modelName":"Native name","available":false,"supportedInputTypes":["TEXT","IMAGE"]}),
    ];
    let rows = catalog::normalize(&native, None);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["key"], "native-restricted");
    assert_eq!(rows[0]["display_name"], "Native name");
    assert_eq!(rows[0]["available"], false);
    assert!(super::super::conversation_catalog::is_conversation_model(
        "kiro", &rows[0]
    ));
    assert!(rows[0]["capabilities"].get("contextWindow").is_none());
    assert_eq!(rows[0]["supportsVision"], true);
}
#[test]
fn native_catalog_maps_rate_multiplier_to_price_factor() {
    let rows = catalog::normalize(
        &[
            json!({"modelId":"rate-string","modelName":"Rate string","rateMultiplier":"0.5x","rateUnit":"credit"}),
            json!({"modelId":"rate-number","modelName":"Rate number","rateMultiplier":2,"rateUnit":"credit"}),
            json!({"modelId":"rate-bare","modelName":"Rate bare","rateMultiplier":"1.5"}),
            json!({"modelId":"rate-none","modelName":"Rate none","rateMultiplier":"","rateUnit":"credit"}),
        ],
        None,
    );
    assert_eq!(rows[0]["priceFactor"].as_f64(), Some(0.5));
    assert_eq!(rows[1]["priceFactor"].as_f64(), Some(2.0));
    assert_eq!(rows[2]["priceFactor"].as_f64(), Some(1.5));
    assert!(rows[3].get("priceFactor").is_none());
    assert_eq!(rows[0]["providerMetadata"]["rateUnit"], "credit");
}
#[test]
fn source_search_character_limit_and_expired_token_document_are_exact() {
    assert!(super::valid_search_query(&"中".repeat(100)));
    assert!(!super::valid_search_query(&"中".repeat(201)));
    assert_eq!(
        super::errors::classify(403, &json!({"message":"Invalid token","reason":null})).0,
        "kiro_login_expired"
    );
    assert_eq!(
        super::errors::classify(
            403,
            &json!({"message":"General permission refusal","reason":null})
        )
        .0,
        "kiro_request_rejected"
    );
}
