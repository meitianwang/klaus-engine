//! Output-level round-trip snapshots for the handwritten manual codecs and
//! adapter families owned by this crate. Each covered family constructs
//! representative sample values (Option absent/present, empty collections,
//! special characters), encodes them through the exact runtime codec,
//! snapshots the encoded JSON with insta (re-record via `cargo insta review`
//! or `INSTA_UPDATE=always`), and — where a decoder exists — decodes the
//! snapshot back and asserts round-trip equality. Decode-only carriers pin
//! their decoded semantics and rejection messages instead.
//!
//! Families covered here: exec-server-path-uri, exec-server-byte-chunk-base64,
//! fork-context, process-output-chunk-base64, agent-definition-isolation,
//! config-scalar-runtime-string-vec, tool-spec-hybrid, hooks-file,
//! mcp-oauth-server-key-material, ordered-json,
//! provider-behavior-strict-provider-info and
//! provider-behavior-strict-scripted-turns, rollout-required-nullable-string.

use std::collections::BTreeMap;

use klaus_agent_core::command_runtime::exec_server::protocol::{ByteChunk, PathUri};
use klaus_agent_core::command_runtime::process::{ExecOutputStream, ProcessOutputChunk};
use klaus_agent_core::engine::agent_definitions::AgentDefinition;
use klaus_agent_core::extensions::command_loader::CommandLoadOptions;
use klaus_agent_core::extensions::hooks_loader::parse_hooks_json;
use klaus_agent_core::extensions::plugins::loader::load_one_plugin;
use klaus_agent_core::hooks::types::HookHandler;
use klaus_agent_core::mcp::oauth_provider::{mcp_oauth_server_key, McpOAuthConfig};
use klaus_agent_core::runtime_values::{
    runtime_codec, runtime_json, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeString,
};
use klaus_agent_core::schemas::config::ClearToolInputs;
use klaus_agent_core::schemas::config::ContextManagementConfig;
use klaus_agent_core::schemas::rollout::CheckpointItem;
use klaus_agent_core::testing::provider_behavior::TaskProfileContract;
use klaus_agent_core::types::provider::{ModelStreamEvent, ProviderInfo, ToolSpec};
use klaus_agent_core::types::tool::{ForkContext, PositiveForkNumber};

fn encoded_pretty<T: RuntimeEncode + ?Sized>(value: &T) -> String {
    let json = runtime_codec::encode_value(value).expect("sample encodes");
    runtime_json::stringify_pretty(&json)
        .try_to_utf8()
        .expect("encoded wire JSON is scalar text")
}

fn decode_from<T: RuntimeDecode>(text: &str) -> Result<T, RuntimeCodecError> {
    runtime_codec::decode_text(&RuntimeString::from(text))
}

fn roundtrip<T: RuntimeEncode + RuntimeDecode + PartialEq + std::fmt::Debug>(value: &T) -> String {
    let encoded = encoded_pretty(value);
    let decoded: T = decode_from(&encoded).expect("snapshot JSON decodes back");
    assert_eq!(&decoded, value, "encode/decode round-trip must be lossless");
    encoded
}

// ---- exec-server-path-uri (manualCodec) ------------------------------------

#[test]
fn path_uri_roundtrip_snapshots() {
    let plain = PathUri::parse("file:///workspace/src/lib.rs").expect("plain file URI parses");
    let escaped =
        PathUri::parse("file:///workspace/a%20b/%E6%B5%8B%E8%AF%95.txt").expect("escaped parses");
    insta::assert_snapshot!(
        "path_uri_encoded",
        format!("{}\n{}", roundtrip(&plain), roundtrip(&escaped))
    );

    let scheme_error = decode_from::<PathUri>("\"https://example.com/x\"").unwrap_err();
    let shape_error = decode_from::<PathUri>("42").unwrap_err();
    insta::assert_snapshot!(
        "path_uri_decode_rejections",
        format!("{scheme_error}\n{shape_error}")
    );
}

// ---- exec-server-byte-chunk-base64 (serdeAdapter) ---------------------------

#[test]
fn byte_chunk_base64_roundtrip_snapshots() {
    let empty = ByteChunk(Vec::new());
    let binary = ByteChunk(vec![0x00, 0x01, 0xfe, 0xff, b'A']);
    insta::assert_snapshot!(
        "byte_chunk_encoded",
        format!("{}\n{}", roundtrip(&empty), roundtrip(&binary))
    );

    let invalid = decode_from::<ByteChunk>("\"@@not-base64@@\"").unwrap_err();
    let non_ascii = decode_from::<ByteChunk>("\"π\"").unwrap_err();
    insta::assert_snapshot!(
        "byte_chunk_decode_rejections",
        format!("{invalid}\n{non_ascii}")
    );
}

// ---- fork-context (manualCodec) ---------------------------------------------

#[test]
fn fork_context_roundtrip_snapshots() {
    let all = ForkContext::All;
    let none = ForkContext::None;
    let recent = ForkContext::Recent(std::num::NonZeroUsize::new(3).expect("non-zero"));
    let fractional =
        ForkContext::PositiveNumber(PositiveForkNumber::new(0.5).expect("positive finite"));
    insta::assert_snapshot!(
        "fork_context_encoded",
        format!(
            "{}\n{}\n{}\n{}",
            roundtrip(&all),
            roundtrip(&none),
            roundtrip(&recent),
            roundtrip(&fractional)
        )
    );

    // A huge integral count exceeds usize and stays a PositiveNumber.
    let huge: ForkContext = decode_from("1e20").expect("huge positive number decodes");
    assert!(matches!(huge, ForkContext::PositiveNumber(_)));

    let zero = decode_from::<ForkContext>("0").unwrap_err();
    let negative = decode_from::<ForkContext>("-2").unwrap_err();
    let string = decode_from::<ForkContext>("\"three\"").unwrap_err();
    insta::assert_snapshot!(
        "fork_context_decode_rejections",
        format!("{zero}\n{negative}\n{string}")
    );
}

// ---- process-output-chunk-base64 (serdeAdapter) -----------------------------

#[test]
fn process_output_chunk_roundtrip_snapshots() {
    let empty = ProcessOutputChunk {
        seq: 0,
        stream: ExecOutputStream::Stdout,
        chunk: Vec::new(),
    };
    let binary = ProcessOutputChunk {
        seq: 7,
        stream: ExecOutputStream::Stderr,
        chunk: vec![b'h', b'i', 0x00, 0xff],
    };
    insta::assert_snapshot!(
        "process_output_chunk_encoded",
        format!("{}\n{}", roundtrip(&empty), roundtrip(&binary))
    );

    let invalid =
        decode_from::<ProcessOutputChunk>("{\"seq\":1,\"stream\":\"pty\",\"chunk\":\"!!\"}")
            .unwrap_err();
    insta::assert_snapshot!("process_output_chunk_decode_rejection", invalid.to_string());
}

// ---- agent-definition-isolation (serdeAdapter) ------------------------------

#[test]
fn agent_definition_isolation_roundtrip_snapshots() {
    let minimal = AgentDefinition {
        name: RuntimeString::from("reviewer"),
        description: RuntimeString::from("reviews code"),
        ..AgentDefinition::default()
    };
    let isolated = AgentDefinition {
        name: RuntimeString::from("worker"),
        description: RuntimeString::from("runs in a worktree"),
        tools: Some(vec![RuntimeString::from("bash")]),
        disallowed_tools: Some(Vec::new()),
        model: Some(RuntimeString::from("opus")),
        isolation: Some(klaus_agent_core::types::tool::Isolation::Worktree),
        max_turns: Some(12),
        background: Some(true),
        ..AgentDefinition::default()
    };

    // AgentDefinition has no PartialEq; assert losslessness via re-encoding.
    let minimal_encoded = encoded_pretty(&minimal);
    let isolated_encoded = encoded_pretty(&isolated);
    for encoded in [&minimal_encoded, &isolated_encoded] {
        let decoded: AgentDefinition = decode_from(encoded).expect("agent definition decodes");
        assert_eq!(&encoded_pretty(&decoded), encoded);
    }
    insta::assert_snapshot!(
        "agent_definition_encoded",
        format!("{minimal_encoded}\n{isolated_encoded}")
    );

    let invalid_isolation = decode_from::<AgentDefinition>(
        "{\"name\":\"x\",\"description\":\"y\",\"isolation\":\"container\"}",
    )
    .unwrap_err();
    let unknown_field =
        decode_from::<AgentDefinition>("{\"name\":\"x\",\"description\":\"y\",\"extra\":1}")
            .unwrap_err();
    insta::assert_snapshot!(
        "agent_definition_decode_rejections",
        format!("{invalid_isolation}\n{unknown_field}")
    );
}

// ---- config-scalar-runtime-string-vec (serdeAdapter) ------------------------

#[test]
fn context_management_config_roundtrip_snapshots() {
    let empty = ContextManagementConfig::default();
    let populated = ContextManagementConfig {
        trigger_input_tokens: Some(120_000),
        trigger_tool_uses: Some(24),
        keep_tool_uses: Some(3),
        clear_at_least_input_tokens: None,
        clear_tool_inputs: Some(ClearToolInputs::All(false)),
        exclude_tools: vec![RuntimeString::from("bash"), RuntimeString::from("读文件")],
    };
    insta::assert_snapshot!(
        "context_management_config_encoded",
        format!("{}\n{}", roundtrip(&empty), roundtrip(&populated))
    );
}

// ---- tool-spec-hybrid (hybrid) ----------------------------------------------

#[test]
fn tool_spec_hybrid_decode_snapshots() {
    let canonical: ToolSpec = decode_from(
        "{\"name\":\"grep\",\"description\":\"search\",\"inputSchema\":{\"type\":\"object\"},\
         \"format\":{\"type\":\"grammar\",\"syntax\":\"lark\",\"definition\":\"start: WORD\"}}",
    )
    .expect("canonical tool spec decodes");
    let legacy_freeform: ToolSpec = decode_from(
        "{\"name\":\"free\",\"description\":\"legacy\",\"kind\":\"freeform\",\"format\":\"raw\"}",
    )
    .expect("legacy schema-less freeform decodes");
    let external_function: ToolSpec =
        decode_from("{\"Function\":{\"name\":\"fx\",\"description\":\"ext\",\"inputSchema\":{}}}")
            .expect("external Function envelope decodes");
    insta::assert_snapshot!(
        "tool_spec_decoded",
        format!("{canonical:#?}\n{legacy_freeform:#?}\n{external_function:#?}")
    );

    // The canonical decode result must re-encode losslessly.
    let reencoded = encoded_pretty(&canonical);
    let redecoded: ToolSpec = decode_from(&reencoded).expect("re-encoded spec decodes");
    assert_eq!(redecoded, canonical);
    insta::assert_snapshot!("tool_spec_reencoded", reencoded);

    let legacy_without_kind =
        decode_from::<ToolSpec>("{\"name\":\"bad\",\"description\":\"legacy\",\"format\":\"raw\"}")
            .unwrap_err();
    let missing_schema =
        decode_from::<ToolSpec>("{\"name\":\"bad\",\"description\":\"no schema\"}").unwrap_err();
    insta::assert_snapshot!(
        "tool_spec_decode_rejections",
        format!("{legacy_without_kind}\n{missing_schema}")
    );
}

// ---- hooks-file (manualCodec, decode-only) -----------------------------------

fn render_hooks(hooks: &[klaus_agent_core::hooks::types::Hook]) -> String {
    hooks
        .iter()
        .map(|hook| {
            let handler = match &hook.handler {
                HookHandler::Command {
                    command,
                    timeout_ms,
                } => format!("command={command:?} timeout_ms={timeout_ms:?}"),
                HookHandler::Fn(_) => "fn".to_string(),
            };
            format!(
                "{:?} matcher={:?} async={} {handler}",
                hook.event, hook.matcher, hook.is_async
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn hooks_file_decode_snapshots() {
    let hooks = parse_hooks_json(
        r#"{
            "PostToolUse": [
                {"matcher": "Edit", "hooks": [
                    {"type": "command", "command": "fmt --check", "timeout": 2.5, "async": true},
                    {"type": "prompt", "command": "skipped"}
                ]},
                {"hooks": [{"type": "command", "command": "lint"}]}
            ],
            "NotARealEvent": [{"hooks": [{"type": "command", "command": "never"}]}],
            "PreToolUse": [{"hooks": [{"type": "command", "command": "审计 $FILE"}]}]
        }"#,
    );
    insta::assert_snapshot!("hooks_file_decoded", render_hooks(&hooks));

    assert!(parse_hooks_json("not json").is_empty());
    assert!(parse_hooks_json("[]").is_empty());
}

// ---- mcp-oauth-server-key-material (derivedHelper, encode-only) --------------

#[test]
fn mcp_oauth_server_key_material_snapshots() {
    let without_headers = mcp_oauth_server_key(&McpOAuthConfig {
        transport: "http".to_string(),
        name: "docs".to_string(),
        url: "https://example.com/mcp".to_string(),
        headers: None,
    });
    let mut headers = BTreeMap::new();
    headers.insert("X-Tenant".to_string(), "团队/α".to_string());
    headers.insert("Authorization".to_string(), "Bearer …".to_string());
    let with_headers = mcp_oauth_server_key(&McpOAuthConfig {
        transport: "sse".to_string(),
        name: "docs".to_string(),
        url: "https://example.com/mcp".to_string(),
        headers: Some(headers),
    });
    insta::assert_snapshot!(
        "mcp_oauth_server_key_material",
        format!("{without_headers}\n{with_headers}")
    );
}

// ---- ordered-json (manualCodec) ----------------------------------------------

#[tokio::test]
async fn ordered_json_preserves_manifest_document_order() {
    let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("ordered-json-plugin");
    let manifest_dir = root.join(".codex-plugin");
    std::fs::create_dir_all(&manifest_dir).expect("plugin manifest dir");
    // The inline hooks entry decodes through OrderedJson; its document order
    // (PostToolUse before PreToolUse, i.e. NOT alphabetical) must survive the
    // decode -> encode round-trip into the loaded hook list.
    std::fs::write(
        manifest_dir.join("plugin.json"),
        r#"{
            "name": "ordered-json-fixture",
            "hooks": [{
                "PostToolUse": [{"hooks": [{"type": "command", "command": "second"}]}],
                "PreToolUse": [{"hooks": [{"type": "command", "command": "first"}]}]
            }]
        }"#,
    )
    .expect("plugin manifest write");

    let plugin = load_one_plugin(
        root.to_str().expect("utf-8 tmp path"),
        CommandLoadOptions::default(),
    )
    .await
    .expect("ordered-json fixture plugin loads");
    assert_eq!(plugin.name, "ordered-json-fixture");
    insta::assert_snapshot!("ordered_json_manifest_hooks", render_hooks(&plugin.hooks));
}

// ---- provider-behavior-strict-provider-info + strict-scripted-turns ----------

#[test]
fn task_profile_contract_roundtrip_snapshots() {
    let contract = TaskProfileContract {
        provider_info: ProviderInfo {
            name: RuntimeString::from("anthropic"),
            base_url: None,
            env_key: Some(RuntimeString::from("ANTHROPIC_API_KEY")),
            default_model: RuntimeString::from("claude-opus-4-8"),
        },
        model: RuntimeString::from("claude-opus-4-8"),
        scripted_turns: vec![
            vec![ModelStreamEvent::TextDelta {
                text: RuntimeString::from("héllo 世界"),
            }],
            Vec::new(),
        ],
        visible_name_overrides: BTreeMap::new(),
    };
    insta::assert_snapshot!("task_profile_contract_encoded", roundtrip(&contract));

    // strict-provider-info: unknown providerInfo fields are rejected.
    let loose_provider_info = decode_from::<TaskProfileContract>(
        "{\"providerInfo\":{\"name\":\"a\",\"defaultModel\":\"m\",\"extra\":true},\
         \"model\":\"m\",\"scriptedTurns\":[],\"visibleNameOverrides\":{}}",
    )
    .unwrap_err();
    // strict-scripted-turns: unknown event keys are rejected.
    let loose_event = decode_from::<TaskProfileContract>(
        "{\"providerInfo\":{\"name\":\"a\",\"defaultModel\":\"m\"},\"model\":\"m\",\
         \"scriptedTurns\":[[{\"type\":\"text_delta\",\"text\":\"x\",\"extra\":1}]],\
         \"visibleNameOverrides\":{}}",
    )
    .unwrap_err();
    insta::assert_snapshot!(
        "task_profile_contract_decode_rejections",
        format!("{loose_provider_info}\n{loose_event}")
    );
}

// ---- rollout-required-nullable-string (serdeAdapter) --------------------------

#[test]
fn checkpoint_item_roundtrip_snapshots() {
    let missing_snapshot = CheckpointItem { hash: None };
    let present = CheckpointItem {
        hash: Some(RuntimeString::from("0123abcd")),
    };
    insta::assert_snapshot!(
        "checkpoint_item_encoded",
        format!("{}\n{}", roundtrip(&missing_snapshot), roundtrip(&present))
    );
}
