use super::{auth, bridge, catalog};
use serde_json::{json, Value};

fn fixture() -> Value {
    json!({"config_info_list":[
        {"config_name":"chat-a","config_source":1,"config_switch":true,"is_default":true,"usage":"chat_completion",
         "context_window_tokens":{"dev":256000,"max":1000000},"display_config":{"display_name":"Chat A","model_capability":"chat_model","multimodal":true},
         "extra_config":"{\"native_function_call\":true}","reasoning_effort_config":{"options":["light","high"],"default_level":"high","support_thinking":true},
         "model_detail_list":[
            {"model_name":"chat-a__dev","max_tokens":32000,"encrypted_model_params":"private-routing-material"},
            {"model_name":"chat-a__max","max_tokens":64000,"encrypted_model_params":"private-max-routing-material"}
         ]},
        {"config_name":"restricted","config_switch":false,"usage":"chat_completion","context_window_tokens":{"dev":128000},
         "display_config":{"display_name":"Restricted"},"model_detail_list":[{"model_name":"restricted__dev","max_tokens":8000}]},
        {"config_name":"helper","config_switch":true,"is_invisible_to_user":true,"usage":"summary","display_config":{},"model_detail_list":[{"model_name":"helper-native"}]}
    ]})
}

#[test]
fn cloud_protocol_revision_selects_the_current_inventory() {
    let headers = super::cloud_headers(&credential()).unwrap();
    assert_eq!(headers["x-ide-version-code"], "20260924");
    assert_eq!(headers["x-ide-version"], "3.4.7");
    let mut config = fixture();
    config["config_info_list"][0]["reasoning_effort_config"] = json!({
        "support_thinking": true, "options": ["light", "high", "extra_high"], "default_level": "extra_high"
    });
    let rows = catalog::normalize(&config);
    assert_eq!(rows[0]["traeVariants"][0]["nativeEffort"], "extra_high");
    assert_eq!(
        rows[0]["traeVariants"][3]["value"],
        "trae:chat-a::effort=extra_high"
    );
}

#[test]
fn max_mode_catalog_preserves_limits_and_hides_private_routing() {
    let rows = catalog::normalize(&fixture());
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["capabilities"]["contextWindow"], 1000000);
    assert_eq!(rows[0]["capabilities"]["maxOutputTokens"], 64000);
    assert_eq!(rows[0]["providerMetadata"]["wireModelName"], "chat-a__max");
    for variant in rows[0]["traeVariants"].as_array().unwrap() {
        assert_eq!(variant["contextTokens"], 1000000);
    }
    assert_eq!(rows[1]["capabilities"]["contextWindow"], 128000);
    assert_eq!(
        rows[1]["providerMetadata"]["wireModelName"],
        "restricted__dev"
    );
    assert_eq!(rows[0]["supportsVision"], true);
    assert_eq!(rows[0]["supportsToolCalls"], true);
    let serialized = serde_json::to_string(&rows).unwrap();
    assert!(!serialized.contains("private-routing-material"));
    assert!(!serialized.contains("private-max-routing-material"));
    assert_eq!(rows[1]["available"], false);
    assert!(super::super::conversation_catalog::is_conversation_model(
        "trae", &rows[1]
    ));
    assert!(!super::super::conversation_catalog::is_conversation_model(
        "trae", &rows[2]
    ));
}

#[test]
fn max_mode_request_route_preserves_native_effort_and_private_parameters() {
    let payload = fixture();
    let config = &payload["config_info_list"][0];
    let (detail, mode) = catalog::request_route(config).unwrap();
    assert_eq!(mode, "max");
    let body = bridge::encode(
        &json!({"messages":[{"role":"user","content":"hello"}]}),
        config,
        detail,
        Some("high"),
    )
    .unwrap();
    assert_eq!(body["model_name"], "chat-a__max");
    assert_eq!(
        body["encrypted_model_params"],
        "private-max-routing-material"
    );
    assert_eq!(body["reasoning_effort_level"], "high");

    let (dev_only, mode) = catalog::request_route(&payload["config_info_list"][1]).unwrap();
    assert_eq!(mode, "dev");
    assert_eq!(dev_only["model_name"], "restricted__dev");
}

#[test]
fn native_effort_members_preserve_base_and_exact_service_values() {
    let rows = catalog::normalize(&fixture());
    assert_eq!(rows[0]["traeVariants"][0]["value"], "trae:chat-a");
    assert_eq!(rows[0]["traeVariants"][0]["nativeEffort"], "high");
    assert_eq!(
        rows[0]["traeVariants"][1]["value"],
        "trae:chat-a::effort=light"
    );
    assert_eq!(rows[0]["traeVariants"][1]["nativeEffort"], "light");
    assert_eq!(rows[0]["efforts"], json!([]));
    assert_eq!(
        catalog::selection_parts("chat-a::effort=light").unwrap(),
        ("chat-a", Some("light".into()))
    );
    assert!(catalog::selection_parts("chat-a::effort=").is_err());
    assert!(catalog::selection_parts("chat-a::effort=%6cight").is_err());
    assert_eq!(catalog::expanded_rows(&rows).len(), 5);
}

#[test]
fn explicit_empty_terminal_reason_is_scoped_and_requires_complete_output() {
    let output = b"event: output\ndata: {\"response\":\"TRAE_OK\"}\n\n";
    let done = b"event: done\ndata: {\"finish_reason\":\"\"}\n\n";
    let mut state = bridge::StreamState::new("DeepSeek-V4-Flash-Official");
    state.event(output).unwrap();
    assert!(!state.finished);
    let terminal = state.event(done).unwrap();
    assert_eq!(terminal[0]["choices"][0]["finish_reason"], "stop");
    assert!(state.finished);
    let mut other = bridge::StreamState::new("chat-a");
    other.event(output).unwrap();
    assert_eq!(
        other.event(done).unwrap_err(),
        "trae_stream_stop_reason_missing"
    );
    let mut empty = bridge::StreamState::new("DeepSeek-V4-Flash-Official");
    assert_eq!(
        empty.event(done).unwrap_err(),
        "trae_stream_stop_reason_missing"
    );
    empty.event(output).unwrap();
    assert_eq!(
        empty.event(b"event: done\ndata: {}\n\n").unwrap_err(),
        "trae_stream_stop_reason_missing"
    );
    for (arguments, valid) in [("{", false), ("{}", true)] {
        let mut call = bridge::StreamState::new("DeepSeek-V4-Flash-Official");
        call.event(format!("event: output\ndata: {}\n\n", json!({"tool_calls":[{"index":0,"id":"call-one","function_call":{"name":"fixture_marker","arguments":arguments}}]})).as_bytes()).unwrap();
        if valid {
            assert_eq!(
                call.event(done).unwrap()[0]["choices"][0]["finish_reason"],
                "tool_calls"
            );
        } else {
            assert_eq!(
                call.event(done).unwrap_err(),
                "trae_stream_stop_reason_missing"
            );
        }
    }
}

#[test]
fn quota_preserves_missing_usage_and_independent_pack_expiries() {
    let source = json!({"usage_summary":{"total_amount":4500,"consumed_amount":0},"user_entitlement_pack_list":[
        {"group_name":"Membership","expire_time":1800000000,"status":1,"entitlement_base_info":{"entitlement_id":"one","quota":{"credits_limit":4000}},"usage":{}},
        {"group_name":"Bonus","expire_time":1801000000,"status":1,"entitlement_base_info":{"entitlement_id":"two","quota":{"credits_limit":500}},"usage":{"credits_amount":20}},
        {"group_name":"Unlimited","entitlement_base_info":{"entitlement_id":"three","quota":{"credits_limit":-1}},"usage":{}}
    ]});
    let projected = catalog::normalize_usage(
        &source,
        &json!({"user_pay_identity_str":"Pro"}),
        Some("account"),
    );
    assert!(projected["windows"][0].get("used").is_none());
    assert!(projected["windows"][0].get("remaining").is_none());
    assert_eq!(projected["windows"][0]["expiresAt"], 1800000000000i64);
    assert!(projected["windows"][0].get("resetsAt").is_none());
    assert_eq!(projected["windows"][1]["used"], 20);
    assert_eq!(projected["windows"][2]["unlimited"], true);
    assert!(projected["windows"][2].get("total").is_none());
    assert_eq!(projected["credits"]["remaining"], 4500);
}

#[test]
fn personal_credit_balance_preserves_fractional_consumption_and_pack_defaults() {
    let status = json!({"user_pay_identity_str":"Pro"});
    let summary = catalog::normalize_usage(
        &json!({"usage_summary":{"total_amount":8850,"consumed_amount":517.02}}),
        &status,
        None,
    );
    assert_eq!(summary["credits"]["used"], 517.02);
    assert_eq!(summary["credits"]["remaining"], 8332.98);
    let packs = json!({"usage_summary":{"total_amount":8850},"user_entitlement_pack_list":[
        {"status":0,"entitlement_base_info":{"quota":{"credits_limit":4000}},"usage":{}},
        {"status":1,"entitlement_base_info":{"quota":{"credits_limit":4850}},"usage":{"credits_amount":517.0196}}
    ]});
    let projected = catalog::normalize_usage(&packs, &status, None);
    assert!((projected["credits"]["remaining"].as_f64().unwrap() - 8332.9804).abs() < 0.000001);
    assert_eq!(projected["windows"][1]["used"], 517.0196);
    let incomplete = catalog::normalize_usage(
        &json!({"usage_summary":{"total_amount":8850}}),
        &status,
        None,
    );
    assert!(incomplete["credits"].get("remaining").is_none());
    let spent = catalog::normalize_usage(
        &json!({"usage_summary":{"total_amount":10,"consumed_amount":10.5}}),
        &status,
        None,
    );
    assert_eq!(spent["credits"]["remaining"], 0.0);
    let exact = catalog::normalize_usage(
        &json!({"user_entitlement_pack_list":[
            {"entitlement_base_info":{"quota":{"credits_limit":9007199254740993i64}},"usage":{"credits_amount":9007199254740992i64}}
        ]}),
        &status,
        None,
    );
    assert_eq!(exact["credits"]["remaining"], 1.0);
}

pub(super) fn credential() -> auth::Credential {
    auth::Credential {
        access_token: "test-access".into(),
        refresh_token: Some("test-refresh".into()),
        expires_at: Some(auth::now() + 3_600_000),
        refresh_expires_at: Some(auth::now() + 7_200_000),
        account: Some("test account".into()),
        user_id: Some("test-user".into()),
        device_id: "123456789".into(),
        machine_id: "test-machine".into(),
        device_private_key: None,
        device_public_key: None,
        client_version: "3.4.7".into(),
        region: "cn".into(),
    }
}
pub(super) fn root_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[tokio::test]
async fn adopted_credentials_never_rotate_the_external_refresh_token() {
    let _guard = root_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    super::set_config_root(temp.path());
    let mut value = credential();
    value.expires_at = Some(auth::now() - 1);
    auth::adopt(value).await.unwrap();
    assert_eq!(
        auth::credential(None).await.err().as_deref(),
        Some("trae_login_expired")
    );
    assert_eq!(auth::status()["authMethod"], "local");
    assert_eq!(auth::status()["canRefresh"], false);
    let saved = std::fs::read(temp.path().join("trae-auth.json")).unwrap();
    assert!(String::from_utf8(saved.clone())
        .unwrap()
        .contains("test-refresh"));
    assert!(!auth::status().to_string().contains("test-refresh"));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(temp.path().join("trae-auth.json"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    assert_eq!(
        std::fs::read(temp.path().join("trae-auth.json")).unwrap(),
        saved
    );
}

#[tokio::test]
async fn replacement_and_logout_fence_the_previous_account_generation() {
    let _guard = root_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    super::set_config_root(temp.path());
    auth::adopt(credential()).await.unwrap();
    let first = auth::generation().unwrap();
    auth::adopt(credential()).await.unwrap();
    assert!(!auth::is_current(&first));
    assert_eq!(
        auth::credential(Some(&first)).await.err().as_deref(),
        Some("trae_credential_changed")
    );
    auth::logout().await.unwrap();
    assert_eq!(auth::status()["signedIn"], false);
    assert!(!temp.path().join("trae-auth.json").exists());
}

#[tokio::test]
async fn invalid_credential_does_not_replace_a_connected_account() {
    let _guard = root_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    super::set_config_root(temp.path());
    auth::adopt(credential()).await.unwrap();
    let first = auth::generation().unwrap();
    let mut invalid = credential();
    invalid.access_token = "bad\r\nheader".into();
    assert!(auth::adopt(invalid).await.is_err());
    assert!(auth::is_current(&first));
    let mut invalid = credential();
    invalid.region = "unknown".into();
    assert!(auth::adopt(invalid).await.is_err());
    assert!(auth::is_current(&first));
}

#[test]
fn independent_device_keys_produce_verifiable_asn1_proofs() {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use ring::signature::{UnparsedPublicKey, ECDSA_P256_SHA256_ASN1};
    let (private, public) = auth::generated_keys().unwrap();
    let key = auth::signing_key(&private).unwrap();
    let public = STANDARD
        .decode(
            public
                .lines()
                .filter(|line| !line.starts_with("-----"))
                .collect::<String>(),
        )
        .unwrap();
    let data = b"POST\n/trae/api/v3/oauth/ExchangeToken\nclient\nrefresh\n1\nnonce";
    let signed = key.sign(&ring::rand::SystemRandom::new(), data).unwrap();
    UnparsedPublicKey::new(&ECDSA_P256_SHA256_ASN1, &public[public.len() - 65..])
        .verify(data, signed.as_ref())
        .unwrap();
    assert!(auth::signing_key("not a private key").is_err());
}

#[test]
fn local_device_proof_requires_a_matching_p256_pair_and_cannot_carry_access_grants() {
    let (private, public) = auth::generated_keys().unwrap();
    let mut proof = auth::LocalDeviceProof {
        device_id: "12345678".into(),
        machine_id: "existing-machine".into(),
        device_private_key: private,
        device_public_key: public,
    };
    auth::validate_device_proof(&proof).unwrap();
    let (_, foreign) = auth::generated_keys().unwrap();
    proof.device_public_key = foreign;
    assert_eq!(
        auth::validate_device_proof(&proof).unwrap_err(),
        "trae_device_key_mismatch"
    );
    assert!(serde_json::from_value::<auth::LocalDeviceProof>(json!({"deviceId":"123","machineId":"machine","devicePrivateKey":"private","devicePublicKey":"public","accessToken":"external-token"})).is_err());
    assert!(serde_json::from_value::<auth::LocalDeviceProof>(json!({"deviceId":"123","machineId":"machine","devicePrivateKey":"private","devicePublicKey":"public","refreshToken":"external-refresh"})).is_err());
}

#[test]
fn native_requests_preserve_tool_result_identity_and_typed_content() {
    let body=bridge::encode(&json!({"messages":[
        {"role":"system","content":"system"},{"role":"assistant","tool_calls":[{"id":"call-a","function":{"name":"echo","arguments":"{\"x\":1}"}}]},
        {"role":"tool","tool_call_id":"call-a","content":"result"}],"tools":[{"type":"function","function":{"name":"echo","parameters":{"type":"object","properties":{}}}}]}),
        &fixture()["config_info_list"][0],&fixture()["config_info_list"][0]["model_detail_list"][0],Some("light")).unwrap();
    assert_eq!(
        body["messages"][0]["content"],
        json!([{"type":"text","text":"system"}])
    );
    assert_eq!(
        body["messages"][1]["tool_calls"][0]["function_call"]["name"],
        "echo"
    );
    assert_eq!(body["messages"][2]["tool_call_id"], "call-a");
    assert_eq!(body["reasoning_effort_level"], "light");
    assert!(body.get("reasoning_effort").is_none());
    assert_eq!(body["messages"][2]["name"], "echo");
    assert_eq!(
        serde_json::from_str::<Value>(body["tools"][0]["function"]["parameters"].as_str().unwrap())
            .unwrap(),
        json!({"type":"object","properties":{}})
    );
}

#[test]
fn tool_message_content_never_carries_empty_parts_on_the_wire() {
    // The service rejects a tool message whose content is an empty part array
    // (4001) and drops a lone empty text part before pairing (4027). Encode
    // must therefore emit neither shape: media parts survive on the tool
    // message the way the native wire carries them, empty text parts are
    // removed, and a content array that ends up empty gets an explanatory
    // text part.
    let encode = |messages: Value| {
        bridge::encode(
            &json!({"messages": messages}),
            &fixture()["config_info_list"][0],
            &fixture()["config_info_list"][0]["model_detail_list"][0],
            None,
        )
        .unwrap()
    };
    let assert_invariants = |body: &Value| {
        for message in body["messages"].as_array().unwrap() {
            if message["role"] != json!("tool") {
                continue;
            }
            let parts = message["content"].as_array().unwrap();
            assert!(!parts.is_empty(), "tool content must not be empty");
            assert!(
                !parts.iter().any(|part| {
                    part["type"].as_str() == Some("text")
                        && part["text"].as_str().unwrap_or_default().trim().is_empty()
                }),
                "tool content must not carry an empty text part: {message}"
            );
        }
    };
    // Image-only result: pixels stay on the tool message as image_url parts.
    let body = encode(json!([
        {"role":"assistant","tool_calls":[{"id":"call-img","function":{"name":"read","arguments":"{\"path\":\"/x.png\"}"}}]},
        {"role":"tool","tool_call_id":"call-img","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,QQ=="}}]}
    ]));
    assert_invariants(&body);
    assert_eq!(
        body["messages"][1]["content"],
        json!([{"type":"image_url","image_url":{"url":"data:image/png;base64,QQ=="}}])
    );
    // An empty text part beside an image is stripped; the image survives.
    let body = encode(json!([
        {"role":"assistant","tool_calls":[{"id":"call-img","function":{"name":"read","arguments":"{\"path\":\"/x.png\"}"}}]},
        {"role":"tool","tool_call_id":"call-img","content":[{"type":"text","text":""},{"type":"image_url","image_url":{"url":"data:image/png;base64,QQ=="}}]}
    ]));
    assert_invariants(&body);
    assert_eq!(body["messages"][1]["content"].as_array().unwrap().len(), 1);
    // A content array that would end up empty gets explanatory text.
    let body = encode(json!([
        {"role":"assistant","tool_calls":[{"id":"call-nil","function":{"name":"read","arguments":"{\"path\":\"/x\"}"}}]},
        {"role":"tool","tool_call_id":"call-nil","content":[]}
    ]));
    assert_invariants(&body);
    assert_eq!(
        body["messages"][1]["content"],
        json!([{"type":"text","text":"(This tool result contained no textual output.)"}])
    );
    // A textual result is left untouched.
    let body = encode(json!([
        {"role":"assistant","tool_calls":[{"id":"call-txt","function":{"name":"read","arguments":"{\"path\":\"/x\"}"}}]},
        {"role":"tool","tool_call_id":"call-txt","content":[{"type":"text","text":"body"}]}
    ]));
    assert_eq!(
        body["messages"][1]["content"],
        json!([{"type":"text","text":"body"}])
    );
}

#[test]
fn http_success_with_native_error_is_a_failed_stream() {
    let mut state = bridge::StreamState::new("chat-a");
    let chunks=state.event(b"event: error\ndata: {\"code\":4001,\"error\":\"\",\"message\":\"invalid param\",\"extra\":null}\n\n").unwrap();
    assert!(chunks[0].to_string().contains("trae_request_rejected"));
    assert!(state.finished);
}

#[test]
fn interrupted_stream_chunks_carry_the_retryable_service_envelope() {
    let chunk = bridge::stream_interrupted("trae_stream_transport_failed", "chat-a", "account-a");
    assert!(chunk.starts_with("data: "));
    assert!(chunk.contains("klaus.trae.error.v1"));
    assert!(chunk.contains("trae_service_unavailable"));
    assert!(chunk.contains("trae_stream_transport_failed"));
    let incomplete = bridge::stream_interrupted("trae_stream_incomplete", "chat-a", "account-a");
    assert!(incomplete.contains("trae_stream_incomplete"));
}

#[test]
fn control_frames_allow_empty_or_plain_heartbeats_but_invalid_output_is_rejected() {
    let mut state = bridge::StreamState::new("chat-a");
    for event in [
        b"event: heartbeat\ndata: \n\n".as_slice(),
        b"event: heartbeat\ndata: ping\n\n",
        b"event: metadata\ndata: not-json\n\n",
        b"event:progress_notice\ndata:;Processing_1792959586444266353_GKnNAj\n\n",
    ] {
        assert!(state.event(event).unwrap().is_empty());
        assert!(!state.finished);
    }
    assert_eq!(
        state
            .event(b"event: output\ndata: not-json\n\n")
            .unwrap_err(),
        "trae_stream_event_invalid"
    );
    assert_eq!(
        state
            .event(b"event: error\ndata: not-json\n\n")
            .unwrap_err(),
        "trae_stream_event_invalid"
    );
    assert_eq!(
        state.event(b"event: unknown\ndata: {}\n\n").unwrap_err(),
        "trae_stream_event_unsupported"
    );
    assert_eq!(
        state.event(b"event: done\ndata: {}\n\n").unwrap_err(),
        "trae_stream_stop_reason_missing"
    );
}

fn tool_fragment(state: &mut bridge::StreamState, calls: Value) -> Value {
    state
        .event(format!("event: output\ndata: {}\n\n", json!({"tool_calls": calls})).as_bytes())
        .unwrap()
        .remove(0)
}

#[test]
fn argument_deltas_preserve_unicode_repeated_chunks_and_native_identity() {
    let mut state = bridge::StreamState::new("deepseek-v4.1-flash");
    let fragments = [
        r#"{"prompt":""#,
        "喵",
        "喵",
        r#"，\"小猫\"\n窗边","size":"1024x1024"}"#,
    ];
    let mut emitted = String::new();
    for (index, fragment) in fragments.iter().enumerate() {
        let delta = tool_fragment(
            &mut state,
            json!([{"index":0,"id":if index==0 {"call-one"} else {""},"type":if index==0 {"function"} else {""},
            "function_call":{"name":if index==0 {"render_image"} else {""},"arguments":fragment,"partial_arguments":null,"namespace":if index==0 {json!("native-scope")} else {Value::Null}}}]),
        );
        emitted.push_str(
            delta["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"]
                .as_str()
                .unwrap(),
        );
    }
    let expected = fragments.concat();
    assert_eq!(emitted, expected);
    assert_eq!(
        serde_json::from_str::<Value>(&emitted).unwrap()["prompt"],
        "喵喵，\"小猫\"\n窗边"
    );
    tool_fragment(
        &mut state,
        json!([{"index":0,"function_call":{"arguments":"","partial_arguments":null}}]),
    );
    let extra = state
        .event(b"event: extra_info\ndata: {\"content\":\"opaque\"}\n\n")
        .unwrap();
    let marker = extra[0]["choices"][0]["delta"]["reasoning_content"]
        .as_str()
        .unwrap();
    let context: Value =
        serde_json::from_str(marker.strip_prefix(super::provider::marker()).unwrap()).unwrap();
    let raw = &context["toolCalls"][0];
    assert_eq!(raw["id"], "call-one");
    assert_eq!(raw["type"], "function");
    assert_eq!(raw["function_call"]["name"], "render_image");
    assert_eq!(raw["function_call"]["namespace"], "native-scope");
    assert_eq!(raw["function_call"]["arguments"], expected);
    let done = state
        .event(b"event: done\ndata: {\"finish_reason\":\"tool_calls\"}\n\n")
        .unwrap();
    assert_eq!(done[0]["choices"][0]["finish_reason"], "tool_calls");
}

#[test]
fn partial_arguments_ignore_empty_placeholders_and_validate_snapshot_confirmations() {
    let mut state = bridge::StreamState::new("chat-a");
    for partial in ["{\"x\":", "1}"] {
        let delta = tool_fragment(
            &mut state,
            json!([{"index":0,"id":"call-one","type":"function","function_call":{"name":"echo","arguments":"","partial_arguments":partial}}]),
        );
        assert_eq!(
            delta["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"],
            partial
        );
    }
    for _ in 0..2 {
        let delta = tool_fragment(
            &mut state,
            json!([{"index":0,"function_call":{"arguments":"{\"x\":1}","partial_arguments":null}}]),
        );
        assert_eq!(
            delta["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"],
            ""
        );
    }
    assert_eq!(state.event(b"event: output\ndata: {\"tool_calls\":[{\"index\":0,\"function_call\":{\"arguments\":\"{\\\"x\\\":2}\"}}]}\n\n").unwrap_err(), "trae_stream_tool_arguments_changed");
}

#[test]
fn interleaved_argument_deltas_remain_scoped_to_each_tool_index() {
    let mut state = bridge::StreamState::new("chat-a");
    tool_fragment(
        &mut state,
        json!([
            {"index":0,"id":"a","type":"function","function_call":{"name":"first","arguments":"{\"value\":"}},
            {"index":1,"id":"b","type":"function","function_call":{"name":"second","arguments":"{\"value\":"}}
        ]),
    );
    let output = tool_fragment(
        &mut state,
        json!([
            {"index":1,"function_call":{"arguments":"\"cat\"}"}},
            {"index":0,"function_call":{"arguments":"\"猫\"}"}}
        ]),
    );
    assert_eq!(output["choices"][0]["delta"]["tool_calls"][0]["index"], 1);
    assert_eq!(output["choices"][0]["delta"]["tool_calls"][1]["index"], 0);
    let extra = state
        .event(b"event: extra_info\ndata: {\"content\":\"opaque\"}\n\n")
        .unwrap();
    let marker = extra[0]["choices"][0]["delta"]["reasoning_content"]
        .as_str()
        .unwrap();
    let context: Value =
        serde_json::from_str(marker.strip_prefix(super::provider::marker()).unwrap()).unwrap();
    assert_eq!(
        context["toolCalls"][0]["function_call"]["arguments"],
        "{\"value\":\"猫\"}"
    );
    assert_eq!(
        context["toolCalls"][1]["function_call"]["arguments"],
        "{\"value\":\"cat\"}"
    );
}

#[test]
fn partial_tool_arguments_emit_only_new_bytes_and_preserve_native_replay_metadata() {
    let mut state = bridge::StreamState::new("chat-a");
    let first = state.event(b"event: output\ndata: {\"tool_calls\":[{\"index\":0,\"id\":\"c1\",\"type\":\"function\",\"function_call\":{\"name\":\"echo\",\"partial_arguments\":\"{\\\"x\\\":\"},\"namespace\":\"original\"}]}\n\n").unwrap();
    assert_eq!(
        first[0]["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"],
        "{\"x\":"
    );
    let second = state.event(b"event: output\ndata: {\"tool_calls\":[{\"index\":0,\"function_call\":{\"partial_arguments\":\"1}\"},\"namespace\":\"original\"}]}\n\n").unwrap();
    assert_eq!(
        second[0]["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"],
        "1}"
    );
    let full = state.event(b"event: output\ndata: {\"tool_calls\":[{\"index\":0,\"id\":\"c1\",\"type\":\"function\",\"function_call\":{\"name\":\"echo\",\"arguments\":\"{\\\"x\\\":1}\",\"namespace\":\"n\"}}]}\n\n").unwrap();
    assert_eq!(
        full[0]["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"],
        ""
    );
    let extra = state
        .event(b"event: extra_info\ndata: {\"model\":\"native\",\"content\":\"opaque\"}\n\n")
        .unwrap();
    let marker = extra[0]["choices"][0]["delta"]["reasoning_content"]
        .as_str()
        .unwrap();
    let context: Value =
        serde_json::from_str(marker.strip_prefix(super::provider::marker()).unwrap()).unwrap();
    assert_eq!(
        context["toolCalls"][0]["function_call"]["arguments"],
        "{\"x\":1}"
    );
    assert_eq!(context["toolCalls"][0]["function_call"]["namespace"], "n");
    assert!(
        super::provider::marker_context(marker, "other", context["account"].as_str().unwrap())
            .is_none()
    );
}

#[tokio::test]
async fn image_edit_is_an_explicit_independent_capability() {
    let _guard = root_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    super::set_config_root(temp.path());
    assert_eq!(
        super::capabilities().await.unwrap()["imageEdit"]["available"],
        false
    );
    auth::adopt(credential()).await.unwrap();
    let capabilities = super::capabilities().await.unwrap();
    assert_eq!(capabilities["imageEdit"]["available"], true);
    assert_eq!(capabilities["imageEdit"]["models"][0]["id"], "");
    assert_eq!(capabilities["imageEdit"]["referenceImages"], true);
}
