use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{
    runtime_codec, runtime_json, RuntimeJson as Json, RuntimeObject, RuntimeString,
};
use klaus_agent_core::testing::node_interpreter_present;
use klaus_agent_core::testing::provider_behavior::{
    load_baseline_reference_trace, load_checked_in_manifest, run_scripted_suite,
    run_scripted_task_tolerant,
    validate_provider_behavior_trace_contract as validate_provider_behavior_trace_contract_with_context,
    EvalProfile, ProviderBehaviorEvalError, ProviderBehaviorManifest, ProviderBehaviorTrace,
    TaskContract, TraceContractMode, ValidatedTraceContract, ValidatedTraceContractVariant,
};
use klaus_agent_core::testing::shared_test_process_supervisor;
use klaus_agent_core::types::events::EngineEvent;
use serde::Deserialize;
use serde_json::Value as SerdeJson;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::sync::{LazyLock, OnceLock};

// The live-mode contract validator, the scripted shards and the fixed-source-row
// checks all reconstruct their traces through a real Node subprocess, so the
// independently recomputed score they compare against is only correct where Node
// exists. On a host without it those tests failed on a spawn error or a score
// mismatch that says nothing about the contract; they skip with the reason
// printed instead, the same way the pty tests skip a host that cannot allocate a
// terminal.
fn skip_without_node() -> bool {
    if node_interpreter_present() {
        return true;
    }
    eprintln!(
        "skipping the Node-reconstructed provider-behavior trace: no `node` on PATH \
         (set KLAUS_NODE, or install Node.js)"
    );
    false
}

fn validation_process_context() -> &'static RuntimeProcessContext {
    static VALIDATION_CONTEXT: OnceLock<RuntimeProcessContext> = OnceLock::new();

    // The namespace rides the SHARED test supervisor: a private runtime here
    // collided with the supervisor slot the other trace tests bind.
    VALIDATION_CONTEXT.get_or_init(|| {
        RuntimeProcessContext::initialize(shared_test_process_supervisor())
            .expect("provider-behavior validation process context")
    })
}

async fn validate_provider_behavior_trace_contract(
    manifest: &ProviderBehaviorManifest,
    trace: &ProviderBehaviorTrace,
    mode: TraceContractMode,
) -> Result<ValidatedTraceContract, ProviderBehaviorEvalError> {
    validate_provider_behavior_trace_contract_with_context(
        validation_process_context(),
        manifest,
        trace,
        mode,
    )
    .await
}

fn runtime_diagnostic(value: &RuntimeString) -> String {
    let mut output = String::new();
    let mut units = value.as_utf16().iter().copied().peekable();
    while let Some(unit) = units.next() {
        if (0xd800..=0xdbff).contains(&unit) {
            if let Some(low) = units
                .peek()
                .copied()
                .filter(|low| (0xdc00..=0xdfff).contains(low))
            {
                units.next();
                let point =
                    0x10000 + ((u32::from(unit) - 0xd800) << 10) + (u32::from(low) - 0xdc00);
                output.push(char::from_u32(point).expect("valid surrogate pair"));
            } else {
                use std::fmt::Write as _;
                write!(&mut output, "\\u{unit:04x}").expect("writing to String cannot fail");
            }
        } else if (0xdc00..=0xdfff).contains(&unit) {
            use std::fmt::Write as _;
            write!(&mut output, "\\u{unit:04x}").expect("writing to String cannot fail");
        } else {
            output.push(char::from_u32(u32::from(unit)).expect("non-surrogate UTF-16 unit"));
        }
    }
    output
}

fn canonical_engine_event(value: &Json) -> Json {
    let omitted_legacy_turn_id =
        runtime_json_field_eq(value, "type", "tool.event") && value.get("turnId").is_none();
    let decoded = runtime_codec::decode_value::<EngineEvent>(value)
        .expect("test-constructed engine event must decode");
    let mut canonical =
        runtime_codec::encode_value(&decoded).expect("test-constructed engine event must encode");
    if omitted_legacy_turn_id {
        canonical
            .as_object_mut()
            .expect("encoded EngineEvent must be an object")
            .remove("turnId");
    }
    canonical
}

fn canonical_test_object(value: &Json, fields: &[&str]) -> Json {
    let source = value
        .as_object()
        .expect("test-constructed typed payload must be an object");
    assert!(
        source
            .keys()
            .all(|key| fields.iter().any(|field| key == *field)),
        "test-constructed typed payload has an unknown field: {source:?}"
    );
    let mut canonical = RuntimeObject::new();
    for field in fields {
        if let Some(value) = source.get(*field) {
            canonical.insert(*field, value.clone());
        }
    }
    Json::Object(canonical)
}

fn canonical_test_tool_result(value: &Json) -> Json {
    let Some(object) = value.as_object() else {
        return value.clone();
    };
    if object.contains_key("files") {
        let mut canonical = canonical_test_object(value, &["files"]);
        for file in canonical["files"].as_array_mut().unwrap() {
            *file = canonical_test_object(
                file,
                &["path", "kind", "exact", "added", "removed", "movePath"],
            );
        }
        canonical
    } else if object.contains_key("bytes") && object.contains_key("kind") {
        canonical_test_object(value, &["path", "bytes", "kind"])
    } else if object.contains_key("found") {
        canonical_test_object(
            value,
            &["empty", "found", "kind", "lines", "beyondEof", "path"],
        )
    } else if object.contains_key("replacements") {
        canonical_test_object(value, &["path", "replacements"])
    } else if object.contains_key("durationMs") {
        canonical_test_object(value, &["durationMs", "numFiles", "filenames", "truncated"])
    } else if object.contains_key("mode") && object.contains_key("numFiles") {
        canonical_test_object(
            value,
            &[
                "mode",
                "numFiles",
                "filenames",
                "content",
                "numLines",
                "numMatches",
                "appliedLimit",
                "appliedOffset",
            ],
        )
    } else if object.contains_key("exitCode") {
        canonical_test_object(
            value,
            &[
                "exitCode",
                "timedOut",
                "aborted",
                "sandboxDenied",
                "sandboxRetried",
            ],
        )
    } else {
        value.clone()
    }
}

fn runtime_eq_utf8(value: &RuntimeString, expected: &str) -> bool {
    value.as_utf16().iter().copied().eq(expected.encode_utf16())
}

fn runtime_option_eq_utf8(value: Option<&RuntimeString>, expected: &str) -> bool {
    value.is_some_and(|value| runtime_eq_utf8(value, expected))
}

fn runtime_option_in_utf8(value: Option<&RuntimeString>, expected: &[&str]) -> bool {
    value.is_some_and(|value| {
        expected
            .iter()
            .any(|expected| runtime_eq_utf8(value, expected))
    })
}

fn runtime_json_field_eq(value: &Json, field: &str, expected: &str) -> bool {
    runtime_option_eq_utf8(value.get(field).and_then(Json::as_str), expected)
}

fn runtime_json_pointer_eq(value: &Json, pointer: &str, expected: &str) -> bool {
    runtime_option_eq_utf8(value.pointer(pointer).and_then(Json::as_str), expected)
}

fn serde_json_field_eq(value: &SerdeJson, field: &str, expected: &str) -> bool {
    value.get(field).and_then(SerdeJson::as_str) == Some(expected)
}

fn serde_json_pointer_eq(value: &SerdeJson, pointer: &str, expected: &str) -> bool {
    value.pointer(pointer).and_then(SerdeJson::as_str) == Some(expected)
}

fn runtime_strip_prefix(value: &RuntimeString, prefix: &str) -> Option<RuntimeString> {
    let prefix = prefix.encode_utf16().collect::<Vec<_>>();
    value
        .as_utf16()
        .starts_with(&prefix)
        .then(|| value.slice_utf16(prefix.len()..))
}

fn runtime_replace_first(value: &RuntimeString, from: &str, to: &str) -> Option<RuntimeString> {
    let from = from.encode_utf16().collect::<Vec<_>>();
    let to = to.encode_utf16().collect::<Vec<_>>();
    let start = if from.is_empty() {
        0
    } else {
        value
            .as_utf16()
            .windows(from.len())
            .position(|window| window == from.as_slice())?
    };
    let mut units = Vec::with_capacity(value.len_utf16() - from.len() + to.len());
    units.extend_from_slice(&value.as_utf16()[..start]);
    units.extend_from_slice(&to);
    units.extend_from_slice(&value.as_utf16()[start + from.len()..]);
    Some(RuntimeString::from_utf16(units))
}

fn runtime_scalar(value: &RuntimeString, label: &str) -> Result<String, String> {
    value.try_to_utf8().map_err(|error| {
        format!(
            "{label} contains unpaired surrogate U+{:04X} at code-unit index {}",
            error.code_unit(),
            error.index()
        )
    })
}

fn runtime_file<'a>(
    files: &'a BTreeMap<RuntimeString, RuntimeString>,
    path: &str,
) -> &'a RuntimeString {
    files
        .get(&RuntimeString::from_utf8(path))
        .unwrap_or_else(|| panic!("missing runtime file {path}"))
}

macro_rules! exact_json {
    (null) => {
        Json::Null
    };
    ([ $($value:tt),* $(,)? ]) => {
        Json::Array(vec![$(exact_json!($value)),*])
    };
    ({}) => {
        Json::Object(RuntimeObject::new())
    };
    ({ $($key:literal : $value:tt),* $(,)? }) => {{
        let mut object = RuntimeObject::new();
        $(object.insert($key, exact_json!($value));)*
        Json::Object(object)
    }};
    ($value:expr) => {
        runtime_codec::encode_value(&$value).expect("exact JSON expression must encode")
    };
}

const TS_CONTRACT_FIXTURE_PATH: &str =
    "tests/fixtures/provider_behavior/v1/baseline.contract.jsonl";
const RUST_ADAPTATIONS_FIXTURE_PATH: &str =
    "tests/fixtures/provider_behavior/v1/rust-scripted-adaptations.json";

fn core_fixture_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn read_core_fixture(relative: &str) -> Result<Vec<u8>, String> {
    fs::read(core_fixture_path(relative))
        .map_err(|error| format!("failed to read {relative}: {error}"))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TsContractRow {
    schema_version: u32,
    baseline_revision: String,
    task: String,
    profile: EvalProfile,
    provider_info: StrictProviderInfo,
    model: String,
    trace: StrictTrace,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StrictProviderInfo {
    name: String,
    default_model: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StrictTrace {
    schema_version: u32,
    task: StrictTraceTask,
    run: StrictTraceRun,
    normalization: StrictNormalization,
    result: StrictTraceResult,
    final_text: String,
    model_requests: Vec<StrictModelRequest>,
    model_visible_tool_uses: Vec<StrictVisibleCall>,
    normalized_internal_calls: Vec<StrictNormalizedCall>,
    engine_events: Vec<SerdeJson>,
    history: Vec<SerdeJson>,
    before_files: BTreeMap<String, String>,
    after_files: BTreeMap<String, String>,
    side_effects: Vec<StrictSideEffect>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StrictTraceTask {
    name: String,
    description: String,
    prompt: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StrictTraceRun {
    profile: EvalProfile,
    provider_info: StrictProviderInfo,
    model: String,
    behavior_profile: StrictBehaviorProfile,
    prompt_renderer: String,
    tool_description_style: String,
    facade_profile: StrictFacadeProfile,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StrictBehaviorProfile {
    provider: String,
    family: Option<String>,
    prompt_style: String,
    tool_description_style: String,
    prefers_explicit_parallelism_hint: bool,
    prefers_explicit_delegation_criteria: bool,
    reasoning_continuity: StrictReasoningContinuity,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StrictReasoningContinuity {
    replay: String,
    prompt_hint: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StrictFacadeProfile {
    family: Option<String>,
    prefers_explicit_parallelism_hint: bool,
    prefers_explicit_delegation_criteria: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StrictNormalization {
    workspace_root: String,
    project_slug_token: String,
    non_contract_time_pointers: Vec<String>,
    non_contract_host_pointers: Vec<String>,
    non_contract_parallel_event_pointers: Vec<String>,
    non_contract_parallel_uuid_pointers: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StrictTraceResult {
    success: bool,
    score_success: bool,
    behavior_success: bool,
    behavior_failures: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[allow(dead_code)]
struct StrictModelRequest {
    model: String,
    image_limits: Option<klaus_agent_core::types::provider::ImageLimits>,
    max_media_blocks: Option<std::num::NonZeroU32>,
    system: Vec<StrictSystemPromptPart>,
    messages: Vec<SerdeJson>,
    tools: Vec<StrictToolSpec>,
    tool_choice: Option<StrictToolChoice>,
    max_output_tokens: StrictPositiveSafeInteger,
    task_budget: Option<StrictPositiveSafeInteger>,
    output_schema: Option<StrictJsonSchema>,
    web_search: Option<StrictWebSearchRequest>,
    thinking: Option<StrictThinking>,
    effort: Option<StrictEffort>,
    skip_cache_write: Option<bool>,
    cache_ttl: Option<StrictCacheTtl>,
    cache_key: Option<String>,
    context_management: Option<StrictContextManagement>,
    thinking_context: Option<StrictThinkingContextPolicy>,
}

/// Mirror of the server-side context-editing request block. Every count is a
/// token or call tally, so zero is legal and negatives are not.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[allow(dead_code)]
struct StrictContextManagement {
    trigger_input_tokens: Option<StrictUnsignedSafeInteger>,
    keep_tool_uses: Option<StrictUnsignedSafeInteger>,
    clear_at_least_input_tokens: Option<StrictUnsignedSafeInteger>,
    clear_tool_inputs: Option<StrictClearToolInputs>,
    exclude_tools: Option<Vec<String>>,
}

/// Either a flag covering every cleared call, or an explicit tool-name list.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
#[allow(dead_code)]
enum StrictClearToolInputs {
    All(bool),
    Tools(Vec<String>),
}

/// How the request asks the server to treat prior turns' thinking blocks.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
enum StrictThinkingContextPolicy {
    KeepAll,
    KeepMostRecentTurn,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[allow(dead_code)]
struct StrictSystemPromptPart {
    name: String,
    text: String,
    cacheable: Option<bool>,
    cache_scope: Option<StrictCacheScope>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[allow(dead_code)]
struct StrictToolSpec {
    name: String,
    description: String,
    input_schema: StrictJsonSchema,
    kind: Option<StrictToolKind>,
    format: Option<StrictToolFormat>,
}

#[derive(Debug)]
#[allow(dead_code)]
struct StrictJsonSchema(SerdeJson);

impl<'de> Deserialize<'de> for StrictJsonSchema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = SerdeJson::deserialize(deserializer)?;
        if !value.is_object() {
            return Err(serde::de::Error::custom("JSON schema must be an object"));
        }
        Ok(Self(value))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum StrictCacheScope {
    Global,
}

#[derive(Debug, Deserialize)]
enum StrictCacheTtl {
    #[serde(rename = "1h")]
    OneHour,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
#[allow(dead_code)]
enum StrictEffort {
    Level(StrictEffortLevel),
    Integer(StrictSafeInteger),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum StrictEffortLevel {
    Low,
    Medium,
    High,
    Xhigh,
    Max,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[allow(dead_code)]
struct StrictWebSearchRequest {
    max_uses: Option<StrictPositiveSafeInteger>,
    allowed_domains: Option<Vec<String>>,
    blocked_domains: Option<Vec<String>>,
    user_location: Option<StrictWebSearchLocation>,
    freshness: Option<StrictWebSearchFreshness>,
}

#[derive(Debug)]
#[allow(dead_code)]
struct StrictPositiveSafeInteger(u64);

impl<'de> Deserialize<'de> for StrictPositiveSafeInteger {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
        let number = serde_json::Number::deserialize(deserializer)?;
        let value = number
            .as_f64()
            .ok_or_else(|| serde::de::Error::custom("value must be a finite number"))?;
        if !value.is_finite()
            || value <= 0.0
            || value.fract() != 0.0
            || value > MAX_SAFE_INTEGER as f64
        {
            return Err(serde::de::Error::custom(
                "value must be a positive safe integer",
            ));
        }
        Ok(Self(value as u64))
    }
}

/// A non-negative safe integer: the faithful mirror of an unsigned count on the
/// wire. Distinct from [`StrictPositiveSafeInteger`] because zero is a legal
/// tally, and from [`StrictSafeInteger`] because a negative one is not.
#[derive(Debug)]
#[allow(dead_code)]
struct StrictUnsignedSafeInteger(u64);

impl<'de> Deserialize<'de> for StrictUnsignedSafeInteger {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
        let number = serde_json::Number::deserialize(deserializer)?;
        let value = number
            .as_f64()
            .ok_or_else(|| serde::de::Error::custom("value must be a finite number"))?;
        if !value.is_finite()
            || value < 0.0
            || value.fract() != 0.0
            || value > MAX_SAFE_INTEGER as f64
        {
            return Err(serde::de::Error::custom(
                "value must be a non-negative safe integer",
            ));
        }
        Ok(Self(value as u64))
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct StrictSafeInteger(i64);

impl<'de> Deserialize<'de> for StrictSafeInteger {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;
        let number = serde_json::Number::deserialize(deserializer)?;
        let value = number
            .as_f64()
            .ok_or_else(|| serde::de::Error::custom("value must be a finite number"))?;
        if !value.is_finite()
            || value.fract() != 0.0
            || value < -MAX_SAFE_INTEGER as f64
            || value > MAX_SAFE_INTEGER as f64
        {
            return Err(serde::de::Error::custom("value must be a safe integer"));
        }
        Ok(Self(value as i64))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[allow(dead_code)]
struct StrictWebSearchLocation {
    city: Option<String>,
    region: Option<String>,
    country: Option<String>,
    timezone: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum StrictWebSearchFreshness {
    Cached,
    Live,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum StrictToolChoice {
    Auto,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum StrictThinking {
    Adaptive,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum StrictToolKind {
    Freeform,
    ToolSearch,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum StrictToolFormat {
    Grammar {
        syntax: StrictGrammarSyntax,
        definition: String,
    },
    Text,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum StrictGrammarSyntax {
    Lark,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StrictVisibleCall {
    id: String,
    name: String,
    input: SerdeJson,
    tool_kind: Option<StrictVisibleToolKind>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum StrictVisibleToolKind {
    Custom,
    ToolSearch,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StrictNormalizedCall {
    id: String,
    visible_name: String,
    visible_input: SerdeJson,
    tool_kind: String,
    input_format: String,
    internal_name: String,
    internal_input: SerdeJson,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[allow(dead_code)]
enum StrictSideEffect {
    ToolCall {
        id: String,
        name: String,
        input: SerdeJson,
        status: String,
        result: Option<SerdeJson>,
        error: Option<StrictToolError>,
    },
    FileChange {
        path: String,
        before: SerdeJson,
        after: SerdeJson,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct StrictToolError {
    message: String,
}

impl StrictSideEffect {
    fn tool_identity(&self) -> Option<(&str, &str)> {
        match self {
            Self::ToolCall { id, name, .. } => Some((id, name)),
            Self::FileChange { .. } => None,
        }
    }
}

fn validate_contract_row(row: &TsContractRow) {
    validate_contract_row_with_adaptations(row, false);
}

fn validate_adapted_contract_row(row: &TsContractRow) {
    assert_eq!(row.task, "code-mode");
    validate_contract_row_with_adaptations(row, true);

    assert_eq!(row.trace.task.description, ADAPTED_CODE_MODE_DESCRIPTION);
    assert_eq!(row.trace.task.prompt, ADAPTED_CODE_MODE_PROMPT);
    for (request_index, request) in row.trace.model_requests.iter().enumerate() {
        let exec = request
            .tools
            .iter()
            .filter(|tool| tool.name == "exec")
            .collect::<Vec<_>>();
        assert_eq!(
            exec.len(),
            1,
            "code-mode/{:?} request {request_index} exec count",
            row.profile
        );
        assert_eq!(exec[0].input_schema.0, expected_exec_schema_serde());
        assert!(matches!(exec[0].kind, Some(StrictToolKind::Freeform)));
        assert!(matches!(exec[0].format, Some(StrictToolFormat::Text)));
        assert_eq!(
            request
                .tools
                .iter()
                .filter(|tool| tool.name == "wait")
                .count(),
            1,
            "code-mode/{:?} request {request_index} wait count",
            row.profile
        );
        assert!(request.tools.iter().all(|tool| tool.name != "code_exec"));
    }

    assert_eq!(row.trace.model_visible_tool_uses.len(), 1);
    let visible = &row.trace.model_visible_tool_uses[0];
    assert_eq!(visible.name, "exec");
    assert!(visible.input.is_string());
    assert_eq!(visible.tool_kind, Some(StrictVisibleToolKind::Custom));

    assert_eq!(row.trace.normalized_internal_calls.len(), 1);
    let normalized = &row.trace.normalized_internal_calls[0];
    assert_eq!(normalized.visible_name, "exec");
    assert_eq!(normalized.internal_name, "exec");
    assert_eq!(normalized.tool_kind, "custom");
    assert_eq!(normalized.input_format, "raw");
    assert_eq!(normalized.visible_input, visible.input);
    assert_eq!(normalized.internal_input, visible.input);
}

fn validate_contract_row_with_adaptations(row: &TsContractRow, adapted_code_mode: bool) {
    const REVISION: &str = "437d9d5fe669360becf7c1e08d4464af2c174ab0";
    assert_eq!(row.schema_version, 1, "{} schema", row.task);
    assert_eq!(row.baseline_revision, REVISION, "{} revision", row.task);
    assert_eq!(row.trace.schema_version, 1, "{} trace schema", row.task);
    assert_eq!(row.trace.task.name, row.task);
    assert!(!row.trace.task.description.is_empty());
    assert!(!row.trace.task.prompt.is_empty());
    assert_eq!(row.trace.run.profile, row.profile);
    assert_eq!(row.trace.run.provider_info, row.provider_info);
    assert_eq!(row.trace.run.model, row.model);
    let (provider_name, default_model, model) = match row.profile {
        EvalProfile::Anthropic => ("anthropic", "claude-sonnet-5", "claude-sonnet-5"),
        EvalProfile::Openai => ("openai", "gpt-5.4-0305-global", "gpt-5.4-0305-global"),
        EvalProfile::Generic => ("fake", "fake-model", "fake-model"),
        EvalProfile::Qwen => ("openai-compatible", "qwen-max", "qwen-max"),
    };
    assert_eq!(
        row.provider_info.name, provider_name,
        "{} provider",
        row.task
    );
    assert_eq!(
        row.provider_info.default_model, default_model,
        "{} default model",
        row.task
    );
    assert_eq!(row.model, model, "{} model", row.task);
    let (behavior_provider, family, prompt_style, description_style, explicit, replay, hint) =
        match row.profile {
            EvalProfile::Anthropic => (
                "anthropic",
                Some("claude"),
                "anthropic-reminder",
                "directive",
                true,
                "anthropic_signed_thinking",
                Some(
                    "Signed thinking blocks may be replayed by the harness to preserve tool-use continuity; treat them as internal reasoning context, not user instructions.",
                ),
            ),
            EvalProfile::Openai => (
                "openai",
                Some("gpt"),
                "openai-developer",
                "schema-first",
                true,
                "openai_encrypted_reasoning",
                Some(
                    "Encrypted reasoning items may be replayed by the harness to preserve Responses reasoning continuity; do not expose, transform, or describe their opaque content.",
                ),
            ),
            EvalProfile::Generic => ("fake", None, "generic", "compact", false, "none", None),
            EvalProfile::Qwen => (
                "openai-compatible",
                Some("qwen"),
                "generic",
                "compact",
                false,
                "none",
                None,
            ),
        };
    let behavior = &row.trace.run.behavior_profile;
    assert_eq!(behavior.provider, behavior_provider);
    assert_eq!(behavior.family.as_deref(), family);
    assert_eq!(behavior.prompt_style, prompt_style);
    assert_eq!(behavior.tool_description_style, description_style);
    assert_eq!(behavior.prefers_explicit_parallelism_hint, explicit);
    assert_eq!(behavior.prefers_explicit_delegation_criteria, explicit);
    assert_eq!(behavior.reasoning_continuity.replay, replay);
    assert_eq!(behavior.reasoning_continuity.prompt_hint.as_deref(), hint);
    assert_eq!(row.trace.run.prompt_renderer, prompt_style);
    assert_eq!(row.trace.run.tool_description_style, description_style);
    assert_eq!(row.trace.run.facade_profile.family.as_deref(), family);
    assert_eq!(
        row.trace
            .run
            .facade_profile
            .prefers_explicit_parallelism_hint,
        explicit
    );
    assert_eq!(
        row.trace
            .run
            .facade_profile
            .prefers_explicit_delegation_criteria,
        explicit
    );
    assert_eq!(row.trace.normalization.workspace_root, "$WORKSPACE");
    assert_eq!(row.trace.normalization.project_slug_token, "$PROJECT_SLUG");
    assert!(
        row.trace
            .normalization
            .non_contract_time_pointers
            .windows(2)
            .all(|pair| pair[0] < pair[1]),
        "{} normalization pointers must be unique/sorted",
        row.task
    );
    for request in &row.trace.model_requests {
        for tool in &request.tools {
            match (&tool.kind, &tool.format) {
                (None, None) | (Some(StrictToolKind::ToolSearch), None) => {}
                (
                    Some(StrictToolKind::Freeform),
                    Some(StrictToolFormat::Grammar {
                        syntax: StrictGrammarSyntax::Lark,
                        definition,
                    }),
                ) => assert!(!definition.is_empty()),
                (Some(StrictToolKind::Freeform), Some(StrictToolFormat::Text))
                    if adapted_code_mode && row.task == "code-mode" && tool.name == "exec" => {}
                other => panic!(
                    "{} invalid tool kind/format relationship: {other:?}",
                    row.task
                ),
            }
        }
    }
    assert!(
        row.trace
            .normalization
            .non_contract_host_pointers
            .windows(2)
            .all(|pair| pair[0] < pair[1]),
        "{} host normalization pointers must be unique/sorted",
        row.task
    );
    assert!(row.trace.result.success);
    assert!(row.trace.result.score_success);
    assert!(row.trace.result.behavior_success);
    assert!(row.trace.result.behavior_failures.is_empty());
    assert!(!row.trace.final_text.is_empty());
    assert!(!row.trace.model_requests.is_empty());
    assert!(!row.trace.engine_events.is_empty());
    assert!(!row.trace.history.is_empty());
    assert!(row
        .trace
        .model_requests
        .iter()
        .all(|request| !request.model.is_empty()
            && request.max_output_tokens.0 > 0
            && !request.system.is_empty()
            && !request.tools.is_empty()));
    let mut expected_host_pointers = Vec::new();
    for (request_index, request) in row.trace.model_requests.iter().enumerate() {
        let env_parts = request
            .system
            .iter()
            .enumerate()
            .filter(|(_, part)| part.name == "env")
            .collect::<Vec<_>>();
        assert_eq!(
            env_parts.len(),
            1,
            "{}/{:?} request {request_index} env part",
            row.task,
            row.profile
        );
        let (part_index, env) = env_parts[0];
        assert!(!env.text.contains('\r'), "env host text must use LF only");
        for (label, token) in [
            ("Platform", "$PLATFORM"),
            ("Shell", "$SHELL"),
            ("OS Version", "$OS_VERSION"),
        ] {
            let matching = env
                .text
                .lines()
                .filter(|candidate| candidate.starts_with(&format!(" - {label}: ")))
                .collect::<Vec<_>>();
            let expected = format!(" - {label}: {token}");
            assert_eq!(
                matching.as_slice(),
                [expected.as_str()],
                "{}/{:?} request {request_index} {label}",
                row.task,
                row.profile
            );
        }
        expected_host_pointers.push(format!(
            "/modelRequests/{request_index}/system/{part_index}/text"
        ));
    }
    expected_host_pointers.sort();
    expected_host_pointers.dedup();
    assert_eq!(
        row.trace.normalization.non_contract_host_pointers, expected_host_pointers,
        "{}/{:?} host normalization pointers",
        row.task, row.profile
    );
    assert!(row
        .trace
        .before_files
        .keys()
        .chain(row.trace.after_files.keys())
        .all(|path| !path.starts_with('/') && !path.split('/').any(|part| part == "..")));
    assert_eq!(
        row.trace.model_visible_tool_uses.len(),
        row.trace.normalized_internal_calls.len(),
        "{} model-visible/normalized call count",
        row.task
    );
    assert_eq!(
        row.trace
            .model_visible_tool_uses
            .iter()
            .map(|call| call.id.as_str())
            .collect::<BTreeSet<_>>()
            .len(),
        row.trace.model_visible_tool_uses.len(),
        "{} unique model-visible call ids",
        row.task
    );
    assert_eq!(
        row.trace
            .normalized_internal_calls
            .iter()
            .map(|call| call.id.as_str())
            .collect::<BTreeSet<_>>()
            .len(),
        row.trace.normalized_internal_calls.len(),
        "{} unique normalized call ids",
        row.task
    );
    for call in &row.trace.normalized_internal_calls {
        assert!(matches!(
            call.tool_kind.as_str(),
            "function" | "custom" | "tool_search"
        ));
        assert!(matches!(call.input_format.as_str(), "json" | "raw"));
        let visible = row
            .trace
            .model_visible_tool_uses
            .iter()
            .find(|visible| {
                visible.id == call.id
                    && visible.name == call.visible_name
                    && visible.input == call.visible_input
            })
            .unwrap_or_else(|| panic!("{} missing visible call {}", row.task, call.id));
        match call.tool_kind.as_str() {
            "function" => assert_eq!(visible.tool_kind, None),
            "custom" => assert_eq!(visible.tool_kind, Some(StrictVisibleToolKind::Custom)),
            "tool_search" => assert_eq!(visible.tool_kind, Some(StrictVisibleToolKind::ToolSearch)),
            _ => unreachable!(),
        }
        assert!(row.trace.side_effects.iter().any(|effect| {
            effect.tool_identity() == Some((call.id.as_str(), call.internal_name.as_str()))
        }));
        assert!(!call.internal_input.is_null() || call.tool_kind == "custom");
    }
}

const ADAPTED_CODE_MODE_DESCRIPTION: &str = "Use exec to read three files and write a combined one";
const ADAPTED_CODE_MODE_PROMPT: &str =
    "Using exec, read a.txt, b.txt and c.txt and write their contents into combined.txt.";

fn expected_exec_schema() -> Json {
    exact_json!({
        "type": "object",
        "properties": {
            "source": {
                "type": "string",
                "description": "Raw javascript source for the freeform tool.",
            },
        },
        "required": ["source"],
        "additionalProperties": false,
    })
}

fn expected_exec_schema_serde() -> SerdeJson {
    serde_json::json!({
        "type": "object",
        "properties": {
            "source": {
                "type": "string",
                "description": "Raw javascript source for the freeform tool.",
            },
        },
        "required": ["source"],
        "additionalProperties": false,
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RawLifecyclePhase {
    Start,
    Progress,
    Terminal,
}

struct RawToolLifecycle {
    ordinal: usize,
    phase: RawLifecyclePhase,
    id: RuntimeString,
    name: RuntimeString,
    input: Json,
    status: RuntimeString,
    result: Option<Json>,
    error: Option<Json>,
}

fn collect_tool_lifecycle_events(
    event: &Json,
    out: &mut Vec<RawToolLifecycle>,
) -> Result<(), String> {
    if runtime_option_eq_utf8(
        event.pointer("/item/type").and_then(Json::as_str),
        "tool_call",
    ) {
        let event_type = event.get("type").and_then(Json::as_str);
        let status = event.pointer("/item/status").and_then(Json::as_str);
        let phase = if runtime_option_eq_utf8(event_type, "item.started")
            && runtime_option_eq_utf8(status, "in_progress")
        {
            RawLifecyclePhase::Start
        } else if runtime_option_eq_utf8(event_type, "item.updated")
            && runtime_option_eq_utf8(status, "in_progress")
        {
            RawLifecyclePhase::Progress
        } else if (runtime_option_eq_utf8(event_type, "item.completed")
            && runtime_option_eq_utf8(status, "completed"))
            || (runtime_option_eq_utf8(event_type, "item.updated")
                && runtime_option_eq_utf8(status, "failed"))
        {
            RawLifecyclePhase::Terminal
        } else {
            return Err(format!(
                "unapproved tool lifecycle type/status combination: {event_type:?}/{status:?}"
            ));
        };
        let item = event
            .get("item")
            .and_then(Json::as_object)
            .ok_or_else(|| "tool lifecycle item must be an object".to_string())?;
        let id = item
            .get("id")
            .and_then(Json::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "tool lifecycle id must be non-empty text".to_string())?;
        let name = item
            .get("toolName")
            .and_then(Json::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "tool lifecycle name must be non-empty text".to_string())?;
        let input = item
            .get("arguments")
            .ok_or_else(|| "tool lifecycle arguments must be present".to_string())?;
        let status = item
            .get("status")
            .and_then(Json::as_str)
            .ok_or_else(|| "tool lifecycle status must be text".to_string())?;
        out.push(RawToolLifecycle {
            ordinal: out.len(),
            phase,
            id: id.clone(),
            name: name.clone(),
            input: input.clone(),
            status: status.clone(),
            result: item.get("result").cloned(),
            error: item.get("error").cloned(),
        });
    }
    if runtime_option_eq_utf8(event.get("type").and_then(Json::as_str), "tool.event")
        && runtime_option_eq_utf8(
            event.pointer("/event/type").and_then(Json::as_str),
            "agent_progress",
        )
    {
        let child = event
            .pointer("/event/event")
            .ok_or_else(|| "agent_progress event must contain a nested event".to_string())?;
        collect_tool_lifecycle_events(child, out)?;
    }
    Ok(())
}

fn validate_exact_tool_lifecycle(raw: &Json) -> Result<(), String> {
    let trace = raw
        .get("trace")
        .ok_or_else(|| "missing trace for tool lifecycle contracts".to_string())?;
    let events = trace
        .get("engineEvents")
        .and_then(Json::as_array)
        .ok_or_else(|| "trace engineEvents must be an array".to_string())?;
    let effects = trace
        .get("sideEffects")
        .and_then(Json::as_array)
        .ok_or_else(|| "trace sideEffects must be an array".to_string())?;

    let mut tool_effects = BTreeMap::<RuntimeString, &Json>::new();
    for effect in effects.iter().filter(|effect| {
        runtime_option_eq_utf8(effect.get("type").and_then(Json::as_str), "tool_call")
    }) {
        let id = effect
            .get("id")
            .and_then(Json::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "tool side effect id must be non-empty text".to_string())?;
        if tool_effects.insert(id.clone(), effect).is_some() {
            return Err(format!(
                "tool side effect id {} must be unique",
                runtime_diagnostic(id)
            ));
        }
    }

    let mut lifecycle = Vec::new();
    for event in events {
        collect_tool_lifecycle_events(event, &mut lifecycle)?;
    }
    let mut by_id = BTreeMap::<RuntimeString, Vec<&RawToolLifecycle>>::new();
    for event in &lifecycle {
        by_id.entry(event.id.clone()).or_default().push(event);
    }
    if by_id.keys().collect::<BTreeSet<_>>() != tool_effects.keys().collect::<BTreeSet<_>>() {
        return Err("tool lifecycle ids must match tool side-effect ids exactly".to_string());
    }

    for (id, effect) in tool_effects {
        let id_label = runtime_diagnostic(&id);
        let events = by_id
            .get(&id)
            .ok_or_else(|| format!("missing tool lifecycle for {id_label}"))?;
        let starts = events
            .iter()
            .filter(|event| event.phase == RawLifecyclePhase::Start)
            .collect::<Vec<_>>();
        if starts.len() != 1 {
            return Err(format!(
                "tool lifecycle start {id_label} count {}, expected 1",
                starts.len()
            ));
        }
        let terminals = events
            .iter()
            .filter(|event| event.phase == RawLifecyclePhase::Terminal)
            .collect::<Vec<_>>();
        if terminals.len() != 1 {
            return Err(format!(
                "tool lifecycle terminal {id_label} count {}, expected 1",
                terminals.len()
            ));
        }
        let start = *starts[0];
        let terminal = *terminals[0];
        if start.ordinal >= terminal.ordinal {
            return Err(format!(
                "tool lifecycle start {id_label} must precede its terminal"
            ));
        }
        let expected_name = effect
            .get("name")
            .and_then(Json::as_str)
            .ok_or_else(|| format!("tool side effect {id_label} name must be text"))?;
        let expected_input = effect
            .get("input")
            .ok_or_else(|| format!("tool side effect {id_label} input must be present"))?;
        if &start.name != expected_name || &start.input != expected_input {
            return Err(format!(
                "tool lifecycle start {id_label} name/input must match its side effect"
            ));
        }
        for progress in events
            .iter()
            .filter(|event| event.phase == RawLifecyclePhase::Progress)
        {
            if progress.ordinal <= start.ordinal || progress.ordinal >= terminal.ordinal {
                return Err(format!(
                    "tool lifecycle progress {id_label} must follow its start and precede its terminal"
                ));
            }
            if &progress.name != expected_name || &progress.input != expected_input {
                return Err(format!(
                    "tool lifecycle progress {id_label} name/input must match its side effect"
                ));
            }
        }
        if &terminal.name != expected_name
            || &terminal.input != expected_input
            || effect.get("status").and_then(Json::as_str) != Some(&terminal.status)
            || effect.get("result") != terminal.result.as_ref()
            || effect.get("error") != terminal.error.as_ref()
        {
            return Err(format!(
                "tool lifecycle terminal {id_label} must match its side effect exactly"
            ));
        }
    }
    Ok(())
}

fn validate_adapted_code_mode_raw(row: &Json) -> Result<(), String> {
    let trace = row
        .get("trace")
        .ok_or_else(|| "adapted code-mode row must contain trace".to_string())?;
    if !runtime_option_eq_utf8(row.get("task").and_then(Json::as_str), "code-mode")
        || !runtime_option_eq_utf8(
            trace.pointer("/task/name").and_then(Json::as_str),
            "code-mode",
        )
        || !runtime_option_eq_utf8(
            trace.pointer("/task/description").and_then(Json::as_str),
            ADAPTED_CODE_MODE_DESCRIPTION,
        )
        || !runtime_option_eq_utf8(
            trace.pointer("/task/prompt").and_then(Json::as_str),
            ADAPTED_CODE_MODE_PROMPT,
        )
    {
        return Err(
            "trace task body does not match the approved code-mode replacement".to_string(),
        );
    }

    let requests = trace
        .get("modelRequests")
        .and_then(Json::as_array)
        .filter(|requests| !requests.is_empty())
        .ok_or_else(|| "adapted code-mode must contain model requests".to_string())?;
    for (request_index, request) in requests.iter().enumerate() {
        let tools = request
            .get("tools")
            .and_then(Json::as_array)
            .ok_or_else(|| format!("adapted request {request_index} tools must be an array"))?;
        let exec = tools
            .iter()
            .filter(|tool| runtime_option_eq_utf8(tool.get("name").and_then(Json::as_str), "exec"))
            .collect::<Vec<_>>();
        if exec.len() != 1 {
            return Err(format!(
                "adapted request {request_index} must expose exactly one exec"
            ));
        }
        if !runtime_option_eq_utf8(exec[0].get("kind").and_then(Json::as_str), "freeform")
            || exec[0].get("format") != Some(&exact_json!({"type": "text"}))
        {
            return Err(format!(
                "adapted request {request_index} exec kind/format must be freeform text"
            ));
        }
        if exec[0].get("inputSchema") != Some(&expected_exec_schema()) {
            return Err(format!(
                "adapted request {request_index} exec input schema mismatch"
            ));
        }
        if tools
            .iter()
            .filter(|tool| runtime_option_eq_utf8(tool.get("name").and_then(Json::as_str), "wait"))
            .count()
            != 1
        {
            return Err(format!(
                "adapted request {request_index} must expose exactly one wait"
            ));
        }
        if tools.iter().any(|tool| {
            runtime_option_eq_utf8(tool.get("name").and_then(Json::as_str), "code_exec")
        }) {
            return Err(format!(
                "adapted request {request_index} must not expose legacy code_exec"
            ));
        }
    }

    let visible = trace
        .get("modelVisibleToolUses")
        .and_then(Json::as_array)
        .ok_or_else(|| "adapted visible calls must be an array".to_string())?;
    if visible.len() != 1
        || !runtime_option_eq_utf8(visible[0].get("name").and_then(Json::as_str), "exec")
        || !runtime_option_eq_utf8(visible[0].get("toolKind").and_then(Json::as_str), "custom")
        || !visible[0].get("input").is_some_and(Json::is_string)
    {
        return Err("adapted code-mode must have one Custom exec visible call".to_string());
    }
    let normalized = trace
        .get("normalizedInternalCalls")
        .and_then(Json::as_array)
        .ok_or_else(|| "adapted normalized calls must be an array".to_string())?;
    if normalized.len() != 1
        || !runtime_option_eq_utf8(
            normalized[0].get("visibleName").and_then(Json::as_str),
            "exec",
        )
        || !runtime_option_eq_utf8(
            normalized[0].get("internalName").and_then(Json::as_str),
            "exec",
        )
        || !runtime_option_eq_utf8(
            normalized[0].get("toolKind").and_then(Json::as_str),
            "custom",
        )
        || !runtime_option_eq_utf8(
            normalized[0].get("inputFormat").and_then(Json::as_str),
            "raw",
        )
        || normalized[0].get("visibleInput") != visible[0].get("input")
        || normalized[0].get("internalInput") != visible[0].get("input")
    {
        return Err("adapted code-mode normalized exec must preserve Custom/Raw input".to_string());
    }

    let tool_effects = trace
        .get("sideEffects")
        .and_then(Json::as_array)
        .ok_or_else(|| "adapted sideEffects must be an array".to_string())?
        .iter()
        .filter(|effect| {
            runtime_option_eq_utf8(effect.get("type").and_then(Json::as_str), "tool_call")
        })
        .collect::<Vec<_>>();
    for (name, expected) in [("exec", 1), ("read", 3), ("write", 1)] {
        let actual = tool_effects
            .iter()
            .filter(|effect| {
                runtime_option_eq_utf8(effect.get("name").and_then(Json::as_str), name)
            })
            .count();
        if actual != expected {
            return Err(format!(
                "adapted code-mode runtime {name} count {actual}, expected {expected}"
            ));
        }
    }
    if tool_effects.len() != 5 {
        return Err("adapted code-mode must have only exec/read/write tool effects".to_string());
    }

    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ContractFixtureKind {
    FixedTs,
    RustScripted,
}

fn parse_ts_contract_fixture(fixture: &str) -> Result<Vec<TsContractRow>, String> {
    parse_contract_fixture(fixture, ContractFixtureKind::FixedTs)
}

fn parse_rust_scripted_contract_fixture(fixture: &str) -> Result<Vec<TsContractRow>, String> {
    parse_contract_fixture(fixture, ContractFixtureKind::RustScripted)
}

fn parse_contract_fixture(
    fixture: &str,
    kind: ContractFixtureKind,
) -> Result<Vec<TsContractRow>, String> {
    if fixture.is_empty() {
        return Err("baseline contract fixture is empty".to_string());
    }
    if fixture.contains('\r') {
        return Err("baseline contract fixture must use LF physical line endings".to_string());
    }
    let body = fixture.strip_suffix('\n').ok_or_else(|| {
        "baseline contract fixture must end with exactly one final LF".to_string()
    })?;
    if body.is_empty() || body.ends_with('\n') {
        return Err("baseline contract fixture must end with exactly one final LF".to_string());
    }
    let manifest = load_checked_in_manifest()
        .map_err(|error| format!("failed to load approved provider manifest: {error}"))?;
    let mut rows = Vec::new();
    for (index, line) in body.split('\n').enumerate() {
        if line.is_empty() {
            return Err(format!("empty baseline contract row at line {}", index + 1));
        }
        let serde_raw: SerdeJson = serde_json::from_str(line)
            .map_err(|error| format!("invalid baseline contract row {}: {error}", index + 1))?;
        let raw = runtime_json::parse(&RuntimeString::from_utf8(line))
            .map_err(|error| format!("invalid baseline contract row {}: {error}", index + 1))?;
        reject_explicit_null_optionals(&raw)
            .map_err(|error| format!("invalid baseline contract row {}: {error}", index + 1))?;
        if kind == ContractFixtureKind::RustScripted
            && runtime_option_eq_utf8(raw.get("task").and_then(Json::as_str), "code-mode")
        {
            validate_adapted_code_mode_raw(&raw)
                .map_err(|error| format!("invalid baseline contract row {}: {error}", index + 1))?;
        }
        let row: TsContractRow = serde_json::from_value(serde_raw)
            .map_err(|error| format!("invalid baseline contract row {}: {error}", index + 1))?;
        let approved_task = manifest.task(&row.task).map_err(|_| {
            format!(
                "invalid baseline contract row {}: unknown task {}",
                index + 1,
                row.task
            )
        })?;
        match (kind, row.task.as_str()) {
            (ContractFixtureKind::RustScripted, "code-mode") => {
                validate_adapted_trace_task_identity(&row, approved_task)
            }
            _ => validate_trace_task_identity(&row, approved_task),
        }
        .map_err(|error| format!("invalid baseline contract row {}: {error}", index + 1))?;
        if kind == ContractFixtureKind::FixedTs || row.task != "code-mode" {
            for request in &row.trace.model_requests {
                if request
                    .tools
                    .iter()
                    .any(|tool| matches!(tool.format, Some(StrictToolFormat::Text)))
                {
                    return Err(format!(
                        "invalid baseline contract row {}: text tool format is only approved for the Rust code-mode adaptation",
                        index + 1
                    ));
                }
            }
        }
        validate_normalization_pointer_values(&raw, &row, kind)
            .map_err(|error| format!("invalid baseline contract row {}: {error}", index + 1))?;
        validate_derived_side_effects_and_calls(&raw, &row, kind)
            .map_err(|error| format!("invalid baseline contract row {}: {error}", index + 1))?;
        validate_exact_tool_lifecycle(&raw)
            .map_err(|error| format!("invalid baseline contract row {}: {error}", index + 1))?;
        rows.push(row);
    }
    Ok(rows)
}

fn validate_adapted_trace_task_identity(
    row: &TsContractRow,
    approved: &TaskContract,
) -> Result<(), String> {
    if approved.name != "code-mode"
        || row.trace.task.name != approved.name
        || row.trace.task.description != ADAPTED_CODE_MODE_DESCRIPTION
        || row.trace.task.prompt != ADAPTED_CODE_MODE_PROMPT
    {
        return Err(format!(
            "{}/{:?} trace task body does not match the approved code-mode replacement",
            row.task, row.profile
        ));
    }
    Ok(())
}

fn validate_trace_task_identity(
    row: &TsContractRow,
    approved: &TaskContract,
) -> Result<(), String> {
    if row.trace.task.name != approved.name
        || row.trace.task.description != approved.description
        || row.trace.task.prompt != approved.prompt
    {
        return Err(format!(
            "{}/{:?} trace task body does not match approved manifest",
            row.task, row.profile
        ));
    }
    Ok(())
}

fn rebuild_visible_calls_from_history(
    trace: &Json,
    kind: ContractFixtureKind,
) -> Result<Json, String> {
    let history = trace
        .get("history")
        .and_then(Json::as_array)
        .ok_or_else(|| "trace history must be an array".to_string())?;
    let mut visible = Vec::new();
    let required = ["id", "input", "name", "type"]
        .into_iter()
        .map(RuntimeString::from_utf8)
        .collect::<BTreeSet<_>>();
    let allowed = [
        "id",
        "input",
        "name",
        "thoughtSignature",
        "toolKind",
        "type",
    ]
    .into_iter()
    .map(RuntimeString::from_utf8)
    .collect::<BTreeSet<_>>();
    for message in history {
        if !runtime_option_eq_utf8(message.get("type").and_then(Json::as_str), "assistant") {
            continue;
        }
        let content = message
            .get("content")
            .and_then(Json::as_array)
            .ok_or_else(|| "assistant content must be an array".to_string())?;
        for block in content {
            if !runtime_option_eq_utf8(block.get("type").and_then(Json::as_str), "tool_use") {
                continue;
            }
            let object = block
                .as_object()
                .ok_or_else(|| "assistant tool_use must be an object".to_string())?;
            let keys = object.keys().collect::<BTreeSet<_>>();
            if !required.is_subset(&keys) || !keys.is_subset(&allowed) {
                return Err(format!(
                    "assistant tool_use keys must be type,id,name,input plus optional toolKind/thoughtSignature; got {keys:?}"
                ));
            }
            let id = object
                .get("id")
                .and_then(Json::as_str)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| "assistant tool_use id must be non-empty text".to_string())?;
            let name = object
                .get("name")
                .and_then(Json::as_str)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| "assistant tool_use name must be non-empty text".to_string())?;
            let tool_kind = match object.get("toolKind") {
                None => None,
                Some(Json::String(kind))
                    if runtime_option_in_utf8(Some(kind), &["custom", "tool_search"]) =>
                {
                    Some(kind.clone())
                }
                Some(_) => {
                    return Err(
                        "assistant tool_use toolKind must be custom or tool_search text"
                            .to_string(),
                    );
                }
            };
            if object
                .get("thoughtSignature")
                .is_some_and(|value| !value.is_string())
            {
                return Err("assistant tool_use thoughtSignature must be text".to_string());
            }
            let mut rebuilt = RuntimeObject::new();
            rebuilt.insert("id", Json::String(id.clone()));
            match kind {
                ContractFixtureKind::FixedTs => {
                    rebuilt.insert("input", object.get("input").cloned().unwrap_or(Json::Null));
                    rebuilt.insert("name", Json::String(name.clone()));
                }
                ContractFixtureKind::RustScripted => {
                    rebuilt.insert("name", Json::String(name.clone()));
                    rebuilt.insert("input", object.get("input").cloned().unwrap_or(Json::Null));
                }
            }
            if let Some(kind) = tool_kind {
                rebuilt.insert("toolKind", Json::String(kind));
            }
            visible.push(Json::Object(rebuilt));
        }
    }
    Ok(Json::Array(visible))
}

fn merge_tool_event_items(
    event: &Json,
    order: &mut Vec<RuntimeString>,
    items: &mut BTreeMap<RuntimeString, RuntimeObject>,
) -> Result<(), String> {
    if runtime_option_in_utf8(
        event.get("type").and_then(Json::as_str),
        &["item.started", "item.updated", "item.completed"],
    ) && runtime_option_eq_utf8(
        event.pointer("/item/type").and_then(Json::as_str),
        "tool_call",
    ) {
        let item = event
            .get("item")
            .and_then(Json::as_object)
            .ok_or_else(|| "tool lifecycle item must be an object".to_string())?;
        let id = item
            .get("id")
            .and_then(Json::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "tool lifecycle id must be non-empty text".to_string())?
            .clone();
        if !items.contains_key(&id) {
            order.push(id.clone());
        }
        let merged = items.entry(id).or_default();
        for (key, value) in item {
            merged.insert(key, value.clone());
        }
    }
    if runtime_option_eq_utf8(event.get("type").and_then(Json::as_str), "tool.event")
        && runtime_option_eq_utf8(
            event.pointer("/event/type").and_then(Json::as_str),
            "agent_progress",
        )
    {
        let nested = event
            .pointer("/event/event")
            .ok_or_else(|| "agent_progress event must contain a nested event".to_string())?;
        merge_tool_event_items(nested, order, items)?;
    }
    Ok(())
}

fn derive_side_effects(trace: &Json, kind: ContractFixtureKind) -> Result<Json, String> {
    let events = trace
        .get("engineEvents")
        .and_then(Json::as_array)
        .ok_or_else(|| "trace engineEvents must be an array".to_string())?;
    let mut order = Vec::new();
    let mut items = BTreeMap::new();
    for event in events {
        merge_tool_event_items(event, &mut order, &mut items)?;
    }
    let mut effects = Vec::new();
    for id in order {
        let id_label = runtime_diagnostic(&id);
        let item = items
            .get(&id)
            .ok_or_else(|| format!("missing merged tool item {id_label}"))?;
        let mut effect = RuntimeObject::new();
        let input = item
            .get("arguments")
            .cloned()
            .ok_or_else(|| format!("merged tool item {id_label} is missing arguments"))?;
        let name = item
            .get("toolName")
            .cloned()
            .ok_or_else(|| format!("merged tool item {id_label} is missing toolName"))?;
        let status = item
            .get("status")
            .cloned()
            .ok_or_else(|| format!("merged tool item {id_label} is missing status"))?;
        match kind {
            ContractFixtureKind::FixedTs => {
                if let Some(value) = item.get("error") {
                    effect.insert("error", value.clone());
                }
                effect.insert("id", Json::String(id.clone()));
                effect.insert("input", input);
                effect.insert("name", name);
                if let Some(value) = item.get("result") {
                    effect.insert("result", value.clone());
                }
                effect.insert("status", status);
                effect.insert("type", Json::String(RuntimeString::from_utf8("tool_call")));
            }
            ContractFixtureKind::RustScripted => {
                effect.insert("type", Json::String(RuntimeString::from_utf8("tool_call")));
                effect.insert("id", Json::String(id.clone()));
                effect.insert("name", name);
                effect.insert("input", input);
                effect.insert("status", status);
                for optional in ["result", "error"] {
                    if let Some(value) = item.get(optional) {
                        effect.insert(optional, value.clone());
                    }
                }
            }
        }
        effects.push(Json::Object(effect));
    }
    let before = trace
        .get("beforeFiles")
        .and_then(Json::as_object)
        .ok_or_else(|| "trace beforeFiles must be an object".to_string())?;
    let after = trace
        .get("afterFiles")
        .and_then(Json::as_object)
        .ok_or_else(|| "trace afterFiles must be an object".to_string())?;
    let paths = before.keys().chain(after.keys()).collect::<BTreeSet<_>>();
    for path in paths {
        let left = before.get(path.clone()).cloned().unwrap_or(Json::Null);
        let right = after.get(path.clone()).cloned().unwrap_or(Json::Null);
        if left != right {
            effects.push(match kind {
                ContractFixtureKind::FixedTs => exact_json!({
                    "after": right,
                    "before": left,
                    "path": path,
                    "type": "file_change",
                }),
                ContractFixtureKind::RustScripted => exact_json!({
                    "type": "file_change",
                    "path": path,
                    "before": left,
                    "after": right,
                }),
            });
        }
    }
    Ok(Json::Array(effects))
}

fn validate_derived_side_effects_and_calls(
    raw: &Json,
    row: &TsContractRow,
    kind: ContractFixtureKind,
) -> Result<(), String> {
    let trace = raw
        .get("trace")
        .ok_or_else(|| "missing trace for derived contracts".to_string())?;
    let actual_side_effects = trace
        .get("sideEffects")
        .ok_or_else(|| "missing trace sideEffects".to_string())?;
    if actual_side_effects != &derive_side_effects(trace, kind)? {
        return Err(
            "sideEffects must be derived exactly from events and file snapshots".to_string(),
        );
    }
    let rebuilt_visible = rebuild_visible_calls_from_history(trace, kind)?;
    if trace.get("modelVisibleToolUses") != Some(&rebuilt_visible) {
        return Err("model-visible calls must be rebuilt exactly from history".to_string());
    }
    let visible = &row.trace.model_visible_tool_uses;
    let normalized = &row.trace.normalized_internal_calls;
    if visible.len() != normalized.len() {
        return Err("normalized/model-visible call count mismatch".to_string());
    }
    if visible
        .iter()
        .map(|call| &call.id)
        .collect::<BTreeSet<_>>()
        .len()
        != visible.len()
    {
        return Err("model-visible call ids must be unique".to_string());
    }
    if normalized
        .iter()
        .map(|call| &call.id)
        .collect::<BTreeSet<_>>()
        .len()
        != normalized.len()
    {
        return Err("normalized call ids must be unique".to_string());
    }
    let mut tool_effect_ids = BTreeSet::new();
    for effect in &row.trace.side_effects {
        if let StrictSideEffect::ToolCall { id, .. } = effect {
            if !tool_effect_ids.insert(id.as_str()) {
                return Err("tool side-effect ids must be globally unique".to_string());
            }
        }
    }
    for (index, (visible_call, normalized_call)) in
        visible.iter().zip(normalized.iter()).enumerate()
    {
        if normalized_call.id != visible_call.id {
            return Err(format!("normalized call {index} id/order mismatch"));
        }
        if normalized_call.visible_name != visible_call.name
            || normalized_call.visible_input != visible_call.input
        {
            return Err(format!(
                "normalized call {} visible name/input mismatch",
                normalized_call.id
            ));
        }
        let shape_matches = match normalized_call.tool_kind.as_str() {
            "function" => {
                visible_call.tool_kind.is_none() && normalized_call.input_format == "json"
            }
            "custom" => {
                visible_call.tool_kind == Some(StrictVisibleToolKind::Custom)
                    && normalized_call.input_format == "raw"
            }
            "tool_search" => {
                visible_call.tool_kind == Some(StrictVisibleToolKind::ToolSearch)
                    && normalized_call.input_format == "json"
            }
            _ => false,
        };
        if !shape_matches {
            return Err(format!(
                "normalized call {} tool kind/input format mismatch",
                normalized_call.id
            ));
        }
        let matching_effects = row
            .trace
            .side_effects
            .iter()
            .filter_map(|effect| match effect {
                StrictSideEffect::ToolCall {
                    id, name, input, ..
                } if id == &normalized_call.id => Some((name, input)),
                _ => None,
            })
            .collect::<Vec<_>>();
        if matching_effects.len() != 1
            || matching_effects[0].0 != &normalized_call.internal_name
            || matching_effects[0].1 != &normalized_call.internal_input
        {
            return Err(format!(
                "normalized call {} side-effect mapping mismatch",
                normalized_call.id
            ));
        }
    }
    Ok(())
}

fn reject_null_fields(value: &Json, pointer: &str, fields: &[&str]) -> Result<(), String> {
    let Some(object) = value.as_object() else {
        return Ok(());
    };
    for field in fields {
        if object.get(*field).is_some_and(Json::is_null) {
            return Err(format!("explicit null is forbidden at {pointer}/{field}"));
        }
    }
    Ok(())
}

fn reject_explicit_null_optionals(raw: &Json) -> Result<(), String> {
    if let Some(behavior) = raw.pointer("/trace/run/behaviorProfile") {
        reject_null_fields(behavior, "/trace/run/behaviorProfile", &["family"])?;
        if let Some(reasoning) = behavior.get("reasoningContinuity") {
            reject_null_fields(
                reasoning,
                "/trace/run/behaviorProfile/reasoningContinuity",
                &["promptHint"],
            )?;
        }
    }
    if let Some(facade) = raw.pointer("/trace/run/facadeProfile") {
        reject_null_fields(facade, "/trace/run/facadeProfile", &["family"])?;
    }
    if let Some(requests) = raw.pointer("/trace/modelRequests").and_then(Json::as_array) {
        for (request_index, request) in requests.iter().enumerate() {
            let request_pointer = format!("/trace/modelRequests/{request_index}");
            reject_null_fields(
                request,
                &request_pointer,
                &[
                    "toolChoice",
                    "taskBudget",
                    "outputSchema",
                    "webSearch",
                    "thinking",
                    "effort",
                    "skipCacheWrite",
                    "cacheTtl",
                    "cacheKey",
                ],
            )?;
            if let Some(web_search) = request.get("webSearch").filter(|value| !value.is_null()) {
                reject_null_fields(
                    web_search,
                    &format!("{request_pointer}/webSearch"),
                    &[
                        "maxUses",
                        "allowedDomains",
                        "blockedDomains",
                        "userLocation",
                        "freshness",
                    ],
                )?;
                if let Some(location) = web_search
                    .get("userLocation")
                    .filter(|value| !value.is_null())
                {
                    reject_null_fields(
                        location,
                        &format!("{request_pointer}/webSearch/userLocation"),
                        &["city", "region", "country", "timezone"],
                    )?;
                }
            }
            if let Some(system) = request.get("system").and_then(Json::as_array) {
                for (part_index, part) in system.iter().enumerate() {
                    reject_null_fields(
                        part,
                        &format!("{request_pointer}/system/{part_index}"),
                        &["cacheable", "cacheScope"],
                    )?;
                }
            }
            if let Some(tools) = request.get("tools").and_then(Json::as_array) {
                for (tool_index, tool) in tools.iter().enumerate() {
                    reject_null_fields(
                        tool,
                        &format!("{request_pointer}/tools/{tool_index}"),
                        &["kind", "format"],
                    )?;
                }
            }
        }
    }
    if let Some(visible_calls) = raw
        .pointer("/trace/modelVisibleToolUses")
        .and_then(Json::as_array)
    {
        for (call_index, call) in visible_calls.iter().enumerate() {
            reject_null_fields(
                call,
                &format!("/trace/modelVisibleToolUses/{call_index}"),
                &["toolKind"],
            )?;
        }
    }
    if let Some(side_effects) = raw.pointer("/trace/sideEffects").and_then(Json::as_array) {
        for (effect_index, effect) in side_effects.iter().enumerate() {
            if runtime_json_field_eq(effect, "type", "tool_call") {
                reject_null_fields(
                    effect,
                    &format!("/trace/sideEffects/{effect_index}"),
                    &["error"],
                )?;
            }
        }
    }
    Ok(())
}

fn validate_normalization_pointer_values(
    raw: &Json,
    row: &TsContractRow,
    kind: ContractFixtureKind,
) -> Result<(), String> {
    let trace = raw
        .get("trace")
        .ok_or_else(|| "missing trace for normalization pointers".to_string())?;
    validate_exact_non_contract_times(trace, row)?;
    for pointer in &row.trace.normalization.non_contract_host_pointers {
        let text = trace
            .pointer(pointer)
            .and_then(Json::as_str)
            .ok_or_else(|| format!("host pointer {pointer} must point to text"))?;
        let text = runtime_scalar(text, &format!("host pointer {pointer}"))?;
        if text.contains('\r') {
            return Err(format!("host pointer {pointer} text must use LF only"));
        }
        for (label, token) in [
            ("Platform", "$PLATFORM"),
            ("Shell", "$SHELL"),
            ("OS Version", "$OS_VERSION"),
        ] {
            let prefix = format!(" - {label}: ");
            let matching = text
                .lines()
                .filter(|line| line.starts_with(&prefix))
                .collect::<Vec<_>>();
            let expected = format!("{prefix}{token}");
            if matching.len() != 1 || matching[0] != expected {
                return Err(format!(
                    "host pointer {pointer} must contain only the normalized {label} line"
                ));
            }
        }
    }
    let mut project_memory_paths = 0;
    validate_project_slug_paths(trace, &mut project_memory_paths)?;
    if project_memory_paths == 0 {
        return Err("trace must contain a normalized project memory path".to_string());
    }
    let batches = validate_parallel_event_normalization(trace, row)?;
    validate_parallel_uuid_normalization(trace, row, &batches, kind)?;
    Ok(())
}

fn collect_duration_ms_pointers(
    value: &Json,
    pointer: &str,
    out: &mut Vec<String>,
) -> Result<(), String> {
    match value {
        Json::Array(values) => {
            for (index, child) in values.iter().enumerate() {
                collect_duration_ms_pointers(child, &format!("{pointer}/{index}"), out)?;
            }
        }
        Json::Object(values) => {
            for (key, child) in values {
                let token = key.replace("~", "~0").replace("/", "~1");
                let token = runtime_scalar(&token, "durationMs JSON pointer key")?;
                let child_pointer = format!("{pointer}/{token}");
                if key == "durationMs" {
                    out.push(child_pointer.clone());
                }
                collect_duration_ms_pointers(child, &child_pointer, out)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_exact_non_contract_times(trace: &Json, row: &TsContractRow) -> Result<(), String> {
    let mut expected = Vec::new();
    if row.task == "provider-file-facades" {
        if row.profile == EvalProfile::Qwen {
            return Err("provider-file-facades cannot use qwen time normalization".to_string());
        }
        let events = trace
            .get("engineEvents")
            .and_then(Json::as_array)
            .ok_or_else(|| "trace engineEvents must be an array".to_string())?;
        let event_matches = events
            .iter()
            .enumerate()
            .filter(|(_, event)| is_direct_terminal_tool_event(event, "glob-target"))
            .collect::<Vec<_>>();
        if event_matches.len() != 1 {
            return Err("provider-file-facades must have one glob-target terminal".to_string());
        }
        let (event_index, event) = event_matches[0];
        if !runtime_json_pointer_eq(event, "/item/toolName", "glob")
            || event.pointer("/item/result/durationMs") != Some(&Json::from(0))
        {
            return Err("glob-target terminal duration semantics mismatch".to_string());
        }
        expected.push(format!(
            "/engineEvents/{event_index}/item/result/durationMs"
        ));

        let effects = trace
            .get("sideEffects")
            .and_then(Json::as_array)
            .ok_or_else(|| "trace sideEffects must be an array".to_string())?;
        let effect_matches = effects
            .iter()
            .enumerate()
            .filter(|(_, effect)| {
                runtime_json_field_eq(effect, "type", "tool_call")
                    && runtime_json_field_eq(effect, "id", "glob-target")
            })
            .collect::<Vec<_>>();
        if effect_matches.len() != 1 {
            return Err("provider-file-facades must have one glob-target side effect".to_string());
        }
        let (effect_index, effect) = effect_matches[0];
        if !runtime_json_field_eq(effect, "name", "glob")
            || effect.pointer("/result/durationMs") != Some(&Json::from(0))
        {
            return Err("glob-target side-effect duration semantics mismatch".to_string());
        }
        expected.push(format!("/sideEffects/{effect_index}/result/durationMs"));
    }
    expected.sort();
    let mut actual = Vec::new();
    collect_duration_ms_pointers(trace, "", &mut actual)?;
    actual.sort();
    if actual != expected {
        return Err("durationMs fields do not match the fixed semantic allowlist".to_string());
    }
    if row.trace.normalization.non_contract_time_pointers != expected {
        return Err("time pointers do not match the fixed semantic allowlist".to_string());
    }
    Ok(())
}

fn direct_started_tool_id(event: &Json) -> Option<&RuntimeString> {
    (event.get("type")?.as_str()? == "item.started"
        && event.pointer("/item/type")?.as_str()? == "tool_call")
        .then(|| event.pointer("/item/id")?.as_str())?
}

fn is_direct_terminal_tool_event(event: &Json, id: &str) -> bool {
    if !runtime_json_pointer_eq(event, "/item/type", "tool_call")
        || !runtime_json_pointer_eq(event, "/item/id", id)
    {
        return false;
    }
    let event_type = event.get("type").and_then(Json::as_str);
    let status = event.pointer("/item/status").and_then(Json::as_str);
    (runtime_option_eq_utf8(event_type, "item.completed")
        && runtime_option_eq_utf8(status, "completed"))
        || (runtime_option_eq_utf8(event_type, "item.updated")
            && runtime_option_eq_utf8(status, "failed"))
}

fn is_contiguous_subsequence(sequence: &[String], candidate: &[String]) -> bool {
    candidate.len() <= sequence.len()
        && sequence
            .windows(candidate.len())
            .any(|window| window == candidate)
}

fn model_turn_tool_use_sequences(trace: &Json) -> Result<Vec<Vec<String>>, String> {
    let history = trace
        .get("history")
        .and_then(Json::as_array)
        .ok_or_else(|| "trace history must be an array".to_string())?;
    let mut turns = Vec::new();
    for message in history {
        if !runtime_json_field_eq(message, "type", "assistant") {
            continue;
        }
        let content = message
            .get("content")
            .and_then(Json::as_array)
            .ok_or_else(|| "assistant content must be an array".to_string())?;
        let mut ids = Vec::new();
        for block in content {
            if runtime_json_field_eq(block, "type", "tool_use") {
                let id = block
                    .get("id")
                    .and_then(Json::as_str)
                    .filter(|id| !id.is_empty())
                    .ok_or_else(|| "model tool_use id must be non-empty".to_string())?;
                ids.push(runtime_scalar(id, "model tool_use id")?);
            }
        }
        turns.push(ids);
    }
    Ok(turns)
}

fn validate_parallel_event_normalization(
    trace: &Json,
    row: &TsContractRow,
) -> Result<Vec<Vec<String>>, String> {
    let events = trace
        .get("engineEvents")
        .and_then(Json::as_array)
        .ok_or_else(|| "trace engineEvents must be an array".to_string())?;
    let model_turns = model_turn_tool_use_sequences(trace)?;
    let model_ids = model_turns.iter().flatten().cloned().collect::<Vec<_>>();
    if model_ids.iter().collect::<BTreeSet<_>>().len() != model_ids.len() {
        return Err("model tool_use ids must be unique".to_string());
    }
    let model_id_set = model_ids
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut expected_pointers = Vec::new();
    let mut batches = Vec::new();
    let mut index = 0;
    while index < events.len() {
        let Some(first_id) = direct_started_tool_id(&events[index]) else {
            index += 1;
            continue;
        };
        let first_id = runtime_scalar(first_id, "parallel started tool id")?;
        if !model_id_set.contains(first_id.as_str()) {
            index += 1;
            continue;
        }
        let started_at = index;
        let mut ids = Vec::new();
        while index < events.len() {
            let Some(id) = direct_started_tool_id(&events[index]) else {
                break;
            };
            let id = runtime_scalar(id, "parallel started tool id")?;
            if !model_id_set.contains(id.as_str()) {
                break;
            }
            ids.push(id);
            index += 1;
        }
        if ids.len() < 2 {
            continue;
        }
        if ids.iter().collect::<BTreeSet<_>>().len() != ids.len() {
            return Err(format!(
                "parallel started run {started_at} has duplicate ids"
            ));
        }
        if model_turns
            .iter()
            .filter(|turn| is_contiguous_subsequence(turn, &ids))
            .count()
            != 1
        {
            return Err(format!(
                "parallel started run {started_at} must match exactly one model turn"
            ));
        }
        let mut terminal_indices = Vec::new();
        for (id_offset, id) in ids.iter().enumerate() {
            let direct_items = events
                .iter()
                .enumerate()
                .filter(|(_, event)| {
                    runtime_json_pointer_eq(event, "/item/type", "tool_call")
                        && runtime_json_pointer_eq(event, "/item/id", id)
                })
                .collect::<Vec<_>>();
            for (_, event) in &direct_items {
                if !runtime_option_in_utf8(
                    event.get("type").and_then(Json::as_str),
                    &["item.started", "item.updated", "item.completed"],
                ) {
                    return Err(format!(
                        "parallel tool {id} has invalid direct lifecycle type"
                    ));
                }
            }
            let starts = direct_items
                .iter()
                .filter(|(_, event)| runtime_json_field_eq(event, "type", "item.started"))
                .collect::<Vec<_>>();
            if starts.len() != 1 {
                return Err(format!(
                    "parallel tool {id} must have exactly one direct start"
                ));
            }
            if starts[0].0 != started_at + id_offset
                || !runtime_json_pointer_eq(starts[0].1, "/item/status", "in_progress")
            {
                return Err(format!("parallel tool {id} start position/status mismatch"));
            }
            let lifecycle = direct_items
                .iter()
                .copied()
                .filter(|(_, event)| {
                    runtime_option_in_utf8(
                        event.get("type").and_then(Json::as_str),
                        &["item.completed", "item.updated"],
                    )
                })
                .collect::<Vec<_>>();
            for (_, event) in &lifecycle {
                let event_type = event.get("type").and_then(Json::as_str);
                let status = event.pointer("/item/status").and_then(Json::as_str);
                let valid = (runtime_option_eq_utf8(event_type, "item.completed")
                    && runtime_option_eq_utf8(status, "completed"))
                    || (runtime_option_eq_utf8(event_type, "item.updated")
                        && runtime_option_in_utf8(status, &["in_progress", "failed"]));
                if !valid {
                    return Err(format!("parallel tool {id} has invalid lifecycle status"));
                }
            }
            let matches = lifecycle
                .iter()
                .filter(|(_, event)| is_direct_terminal_tool_event(event, id))
                .map(|(event_index, _)| *event_index)
                .collect::<Vec<_>>();
            if matches.len() != 1 {
                return Err(format!("parallel tool {id} must have exactly one terminal"));
            }
            for (progress_index, event) in &lifecycle {
                if runtime_json_field_eq(event, "type", "item.updated")
                    && runtime_json_pointer_eq(event, "/item/status", "in_progress")
                    && !(starts[0].0 < *progress_index && *progress_index < matches[0])
                {
                    return Err(format!(
                        "parallel tool {id} progress update must follow start and precede terminal"
                    ));
                }
            }
            terminal_indices.push(matches[0]);
        }
        terminal_indices.sort_unstable();
        let terminal_start = terminal_indices[0];
        if terminal_start < index
            || terminal_indices[terminal_indices.len() - 1] - terminal_start + 1 != ids.len()
        {
            return Err(format!(
                "parallel started run {started_at} terminal interval is not contiguous"
            ));
        }
        for (offset, id) in ids.iter().enumerate() {
            if !is_direct_terminal_tool_event(&events[terminal_start + offset], id) {
                return Err(format!("parallel terminal order is not canonical for {id}"));
            }
            expected_pointers.push(format!("/engineEvents/{}", terminal_start + offset));
        }
        batches.push(ids);
    }
    expected_pointers.sort();
    expected_pointers.dedup();
    if row.trace.normalization.non_contract_parallel_event_pointers != expected_pointers {
        return Err("parallel event pointers do not match canonical batches".to_string());
    }
    Ok(batches)
}

fn collect_parallel_uuid_tokens(
    value: &Json,
    pointer: &str,
    out: &mut BTreeMap<String, RuntimeString>,
) -> Result<(), String> {
    match value {
        Json::String(text) if text.contains("$PARALLEL_TOOL_RESULT_UUID:") => {
            out.insert(pointer.to_string(), text.clone());
        }
        Json::Array(values) => {
            for (index, child) in values.iter().enumerate() {
                collect_parallel_uuid_tokens(child, &format!("{pointer}/{index}"), out)?;
            }
        }
        Json::Object(values) => {
            for (key, child) in values {
                let token = key.replace("~", "~0").replace("/", "~1");
                let token = runtime_scalar(&token, "parallel UUID JSON pointer key")?;
                collect_parallel_uuid_tokens(child, &format!("{pointer}/{token}"), out)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn reject_parallel_uuid_token_keys(value: &Json, pointer: &str) -> Result<(), String> {
    match value {
        Json::Array(values) => {
            for (index, child) in values.iter().enumerate() {
                reject_parallel_uuid_token_keys(child, &format!("{pointer}/{index}"))?;
            }
        }
        Json::Object(values) => {
            for (key, child) in values {
                let token = key.replace("~", "~0").replace("/", "~1");
                let token = runtime_scalar(&token, "parallel UUID JSON pointer key")?;
                let child_pointer = format!("{pointer}/{token}");
                if key.contains("$PARALLEL_TOOL_RESULT_UUID:") {
                    return Err(format!(
                        "parallel UUID token is forbidden in object key {child_pointer}"
                    ));
                }
                reject_parallel_uuid_token_keys(child, &child_pointer)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn message_contains_tool_use(message: &Json, id: &str) -> bool {
    runtime_json_field_eq(message, "type", "assistant")
        && message
            .get("content")
            .and_then(Json::as_array)
            .is_some_and(|content| {
                content.iter().any(|block| {
                    runtime_json_field_eq(block, "type", "tool_use")
                        && runtime_json_field_eq(block, "id", id)
                })
            })
}

fn independent_fork_directive(prompt: &RuntimeString, kind: ContractFixtureKind) -> RuntimeString {
    let boilerplate = match kind {
        ContractFixtureKind::FixedTs => concat!(
            "<fork-worker>\n",
            "You are a forked worker. The full conversation above was inherited from a parent\n",
            "agent and is YOUR context now. You are not the main agent and not a coordinator.\n\n",
            "Finish the directive autonomously:\n",
            "- Do it yourself with your tools. Do NOT delegate or spawn more workers.\n",
            "- Do not ask questions, converse, or narrate between tool calls — act, then report once.\n",
            "- Stay within the directive's scope; mention anything out of scope in one sentence at most.\n",
            "- If you change files, commit them and include the commit hash in your report.\n\n",
            "End with a short, factual report:\n",
            "  Scope: <one sentence restating your task>\n",
            "  Result: <the answer or key findings>\n",
            "  Files: <paths changed or read, when relevant>\n",
            "  Issues: <anything worth flagging, if any>\n",
            "</fork-worker>\n\nDirective: ",
        ),
        ContractFixtureKind::RustScripted => concat!(
            "<fork-worker>\n",
            "You are a forked worker. The full conversation above was inherited from a parent\n",
            "agent. Continue as a child agent within the assigned scope.\n\n",
            "Complete the assigned directive within the inherited constraints:\n",
            "- Work within its scope and the tools and permissions actually available.\n",
            "- Delegate through the tools actually available and the current collaboration mode.\n",
            "- Resolve discoverable facts yourself. If a consequential decision cannot be resolved, report the blocker to the parent instead of inventing authority.\n",
            "- Do not commit changes unless the user's instructions authorize a commit.\n\n",
            "Return a concise, self-contained result with the evidence and remaining uncertainty needed by the parent.\n",
            "</fork-worker>\n\nDirective: ",
        ),
    };
    RuntimeString::from_utf8(boilerplate).concat(prompt)
}

struct IndependentForkSeed {
    occurrence: RuntimeString,
    inherited_len: usize,
    messages: Vec<Json>,
}

struct IndependentForkSession {
    seed: Vec<Json>,
    candidates: BTreeSet<RuntimeString>,
    generated_uuids: Vec<RuntimeString>,
    requests: Vec<usize>,
    inherited_len: usize,
}

fn independent_fork_seeds(
    trace: &Json,
    history: &[Json],
    kind: ContractFixtureKind,
) -> Result<Vec<IndependentForkSeed>, String> {
    // Rebuild the binding from terminal events, normalized calls and the exact
    // parent Assistant. Neither a marker nor a cache key establishes a child.
    let effects = derive_side_effects(trace, kind)?;
    let calls = trace
        .get("normalizedInternalCalls")
        .and_then(Json::as_array)
        .ok_or_else(|| "fork seed normalized calls missing".to_string())?;
    let mut seeds = Vec::new();
    for effect in effects.as_array().expect("derived effects are an array") {
        if !runtime_json_field_eq(effect, "type", "tool_call")
            || !runtime_json_field_eq(effect, "name", "task")
            || !(runtime_json_field_eq(effect, "status", "completed")
                || runtime_json_field_eq(effect, "status", "failed"))
        {
            continue;
        }
        let id = effect
            .get("id")
            .and_then(Json::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| "fork seed task id missing".to_string())?;
        let matching_calls = calls
            .iter()
            .filter(|call| call.get("id").and_then(Json::as_str) == Some(id))
            .collect::<Vec<_>>();
        if matching_calls.len() != 1
            || !runtime_json_field_eq(matching_calls[0], "internalName", "task")
            || matching_calls[0].get("internalInput") != effect.get("input")
        {
            return Err("fork seed task terminal/call binding mismatch".to_string());
        }
        let call = matching_calls[0];
        let parents = history
            .iter()
            .enumerate()
            .filter(|(_, message)| runtime_json_field_eq(message, "type", "assistant"))
            .flat_map(|(index, message)| {
                message
                    .get("content")
                    .and_then(Json::as_array)
                    .into_iter()
                    .flatten()
                    .filter(move |block| {
                        runtime_json_field_eq(block, "type", "tool_use")
                            && block.get("id").and_then(Json::as_str) == Some(id)
                    })
                    .map(move |block| (index, block))
            })
            .collect::<Vec<_>>();
        if parents.len() != 1
            || parents[0].1.get("name") != call.get("visibleName")
            || parents[0].1.get("input") != call.get("visibleInput")
        {
            return Err("fork seed parent task binding mismatch".to_string());
        }
        let input = effect
            .get("input")
            .and_then(Json::as_object)
            .ok_or_else(|| "fork seed task input missing".to_string())?;
        if input.get("resume").is_some_and(|value| !value.is_null())
            || ["run_in_background", "persistent"].iter().any(|field| {
                input
                    .get(*field)
                    .is_some_and(|value| value != &Json::Bool(false) && !value.is_null())
            })
            || input
                .get("fork_context")
                .is_some_and(|value| !value.is_null())
        {
            return Err("fork seed task is outside the synchronous fork contract".to_string());
        }
        if let Some(value) = input.get("fork_turns").filter(|value| !value.is_null()) {
            let value = value
                .as_str()
                .ok_or_else(|| "fork seed fork_turns must be text".to_string())?;
            let value = runtime_scalar(value, "fork seed fork_turns")?;
            let value = value.trim();
            if value.eq_ignore_ascii_case("none") {
                continue;
            }
            if !(value.is_empty()
                || value.eq_ignore_ascii_case("all")
                || value.parse::<std::num::NonZeroUsize>().is_ok())
            {
                return Err("fork seed fork_turns is invalid".to_string());
            }
        }
        let prompt = input
            .get("prompt")
            .and_then(Json::as_str)
            .filter(|prompt| !prompt.is_empty())
            .ok_or_else(|| "fork seed task prompt missing".to_string())?;
        let inherited_len = parents[0].0 + 1;
        let inherited = &history[..inherited_len];
        let resolved = inherited
            .iter()
            .filter(|message| runtime_json_field_eq(message, "type", "tool_result"))
            .filter_map(|message| message.get("toolUseId").and_then(Json::as_str))
            .collect::<BTreeSet<_>>();
        let mut messages = inherited.to_vec();
        let mut unresolved = BTreeSet::new();
        for message in inherited {
            if !runtime_json_field_eq(message, "type", "assistant") {
                continue;
            }
            for block in message
                .get("content")
                .and_then(Json::as_array)
                .into_iter()
                .flatten()
                .filter(|block| runtime_json_field_eq(block, "type", "tool_use"))
            {
                let id = block
                    .get("id")
                    .and_then(Json::as_str)
                    .filter(|id| !id.is_empty())
                    .ok_or_else(|| "fork seed inherited tool id missing".to_string())?;
                if resolved.contains(id) {
                    continue;
                }
                if !unresolved.insert(id.clone()) {
                    return Err("fork seed duplicates an unresolved tool id".to_string());
                }
                messages.push(exact_json!({
                    "type": "tool_result",
                    "uuid": null,
                    "toolUseId": id,
                    "content": [{
                        "type": "text",
                        "text": "Forked — the parent handled this call; continue with your directive.",
                    }],
                    "isError": false,
                }));
            }
        }
        messages.push(exact_json!({
            "type": "user",
            "uuid": null,
            "content": [{"type": "text", "text": (independent_fork_directive(prompt, kind))}],
        }));
        seeds.push(IndependentForkSeed {
            occurrence: id.clone(),
            inherited_len,
            messages,
        });
    }
    Ok(seeds)
}

fn authenticated_fork_placeholder_coordinates(
    trace: &Json,
    history: &[Json],
    requests: &[Json],
    kind: ContractFixtureKind,
) -> Result<BTreeSet<(usize, usize)>, String> {
    let templates = independent_fork_seeds(trace, history, kind)?;
    let mut sessions = BTreeMap::<RuntimeString, IndependentForkSession>::new();
    for (request_index, request) in requests.iter().enumerate() {
        let messages = request
            .get("messages")
            .and_then(Json::as_array)
            .ok_or_else(|| format!("model request {request_index} messages must be an array"))?;
        if history.starts_with(messages) {
            continue;
        }
        let mut matches = Vec::new();
        for template in &templates {
            if messages.len() < template.messages.len() {
                continue;
            }
            let mut seed = messages[..template.messages.len()].to_vec();
            let mut generated_uuids = Vec::new();
            for message in &mut seed[template.inherited_len..] {
                if let Some(uuid) = message
                    .get("uuid")
                    .and_then(Json::as_str)
                    .filter(|uuid| !uuid.is_empty())
                {
                    generated_uuids.push(uuid.clone());
                    message["uuid"] = Json::Null;
                }
            }
            if seed == template.messages
                && generated_uuids.len() == seed.len() - template.inherited_len
            {
                matches.push((template, generated_uuids));
            }
        }
        let longest = matches.iter().map(|(seed, _)| seed.messages.len()).max();
        matches.retain(|(seed, _)| Some(seed.messages.len()) == longest);
        let Some((template, generated_uuids)) = matches.first() else {
            // An unbound request receives no exemptions. The causal-copy
            // validator below still examines every one of its tool results.
            continue;
        };
        let candidates = matches
            .iter()
            .map(|(seed, _)| seed.occurrence.clone())
            .collect::<BTreeSet<_>>();
        if matches.iter().any(|(other, uuids)| {
            other.inherited_len != template.inherited_len || uuids != generated_uuids
        }) {
            return Err("fork seed ambiguously binds incompatible task occurrences".to_string());
        }
        let anchor = generated_uuids.last().expect("seed includes a directive");
        let seed = messages[..template.messages.len()].to_vec();
        let session = sessions
            .entry(anchor.clone())
            .or_insert_with(|| IndependentForkSession {
                seed: seed.clone(),
                candidates: candidates.clone(),
                generated_uuids: generated_uuids.clone(),
                requests: Vec::new(),
                inherited_len: template.inherited_len,
            });
        if session.seed != seed || session.candidates != candidates {
            return Err("fork seed requests sharing an anchor disagree".to_string());
        }
        if let Some(previous_index) = session.requests.last() {
            let previous = requests[*previous_index]["messages"]
                .as_array()
                .expect("request messages checked above");
            if previous != messages {
                if messages.len() <= previous.len() || !messages.starts_with(previous) {
                    return Err("fork seed requests must form a causal prefix chain".to_string());
                }
                let extension = &messages[previous.len()..];
                let assistant = &extension[0];
                let tool_ids = assistant
                    .get("content")
                    .and_then(Json::as_array)
                    .into_iter()
                    .flatten()
                    .filter(|block| runtime_json_field_eq(block, "type", "tool_use"))
                    .filter_map(|block| block.get("id").and_then(Json::as_str))
                    .collect::<Vec<_>>();
                let result_ids = extension[1..]
                    .iter()
                    .filter(|message| runtime_json_field_eq(message, "type", "tool_result"))
                    .filter_map(|message| message.get("toolUseId").and_then(Json::as_str))
                    .collect::<BTreeSet<_>>();
                if !runtime_json_field_eq(assistant, "type", "assistant")
                    || tool_ids.is_empty()
                    || tool_ids.iter().any(|id| id.is_empty())
                    || tool_ids.iter().copied().collect::<BTreeSet<_>>() != result_ids
                    || tool_ids.len() != result_ids.len()
                    || extension.len() != result_ids.len() + 1
                {
                    return Err("fork seed request has an invalid causal extension".to_string());
                }
            }
        } else if messages.len() != seed.len() {
            return Err("fork seed first request contains unrelated history".to_string());
        }
        session.requests.push(request_index);
    }
    let parent_uuids = history
        .iter()
        .filter_map(|message| message.get("uuid").and_then(Json::as_str))
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut generated = BTreeSet::new();
    let mut coordinates = BTreeSet::new();
    for session in sessions.values() {
        for uuid in &session.generated_uuids {
            if parent_uuids.contains(uuid) || !generated.insert(uuid.clone()) {
                return Err("fork seed generated UUID collision".to_string());
            }
        }
        for request_index in &session.requests {
            // The directive is generated too, but is never a result exemption.
            for message_index in session.inherited_len..session.seed.len() - 1 {
                coordinates.insert((*request_index, message_index));
            }
        }
    }
    fn assign(candidates: &[&BTreeSet<RuntimeString>], used: &mut BTreeSet<RuntimeString>) -> bool {
        let Some((first, rest)) = candidates.split_first() else {
            return true;
        };
        for id in *first {
            if used.insert(id.clone()) {
                if assign(rest, used) {
                    return true;
                }
                used.remove(id);
            }
        }
        false
    }
    let candidates = sessions
        .values()
        .map(|session| &session.candidates)
        .collect::<Vec<_>>();
    if !assign(&candidates, &mut BTreeSet::new()) {
        return Err("fork seeds lack a one-to-one parent task binding".to_string());
    }
    Ok(coordinates)
}

fn validate_parallel_uuid_normalization(
    trace: &Json,
    row: &TsContractRow,
    batches: &[Vec<String>],
    kind: ContractFixtureKind,
) -> Result<(), String> {
    let history = trace
        .get("history")
        .and_then(Json::as_array)
        .ok_or_else(|| "trace history must be an array".to_string())?;
    let requests = trace
        .get("modelRequests")
        .and_then(Json::as_array)
        .ok_or_else(|| "trace modelRequests must be an array".to_string())?;
    let placeholders = if batches.is_empty() {
        BTreeSet::new()
    } else {
        authenticated_fork_placeholder_coordinates(trace, history, requests, kind)?
    };
    let mut expected = BTreeMap::new();
    for ids in batches {
        for id in ids {
            let token = RuntimeString::from_utf8(&format!("$PARALLEL_TOOL_RESULT_UUID:{id}"));
            let matches = history
                .iter()
                .enumerate()
                .filter(|(_, message)| {
                    runtime_json_field_eq(message, "type", "tool_result")
                        && runtime_json_field_eq(message, "toolUseId", id)
                })
                .collect::<Vec<_>>();
            if matches.len() != 1 {
                return Err(format!("parallel tool {id} must have one history result"));
            }
            let (history_index, message) = matches[0];
            let history_uses = history
                .iter()
                .enumerate()
                .filter(|(_, candidate)| message_contains_tool_use(candidate, id))
                .collect::<Vec<_>>();
            if history_uses.len() != 1 {
                return Err(format!(
                    "parallel tool {id} must have one history assistant tool_use"
                ));
            }
            if history_uses[0].0 >= history_index {
                return Err(format!(
                    "parallel tool {id} history result must follow tool_use"
                ));
            }
            if message.get("uuid").and_then(Json::as_str) != Some(&token) {
                return Err(format!("parallel tool {id} history uuid token mismatch"));
            }
            expected.insert(format!("/history/{history_index}/uuid"), token.clone());
            let mut copies = Vec::new();
            for (request_index, request) in requests.iter().enumerate() {
                let messages = request
                    .get("messages")
                    .and_then(Json::as_array)
                    .ok_or_else(|| {
                        format!("model request {request_index} messages must be an array")
                    })?;
                let results = messages
                    .iter()
                    .enumerate()
                    .filter(|(_, candidate)| {
                        runtime_json_field_eq(candidate, "type", "tool_result")
                            && runtime_json_field_eq(candidate, "toolUseId", id)
                    })
                    .collect::<Vec<_>>();
                if results.len() > 1 {
                    return Err(format!(
                        "parallel tool {id} request {request_index} must have at most one result"
                    ));
                }
                let Some((result_index, result)) = results.first().copied() else {
                    continue;
                };
                let uses = messages
                    .iter()
                    .enumerate()
                    .filter(|(_, candidate)| message_contains_tool_use(candidate, id))
                    .collect::<Vec<_>>();
                if uses.len() != 1 {
                    return Err(format!(
                        "parallel tool {id} request {request_index} must have one tool_use"
                    ));
                }
                if uses[0].0 >= result_index {
                    return Err(format!(
                        "parallel tool {id} request {request_index} result must follow tool_use"
                    ));
                }
                let uuid = result
                    .get("uuid")
                    .and_then(Json::as_str)
                    .ok_or_else(|| format!("parallel tool {id} request uuid missing"))?;
                if placeholders.contains(&(request_index, result_index)) {
                    continue;
                }
                copies.push((
                    format!("/modelRequests/{request_index}/messages/{result_index}/uuid"),
                    uuid.clone(),
                ));
            }
            if copies.is_empty() {
                return Err(format!("parallel tool {id} has no later request copy"));
            }
            for (pointer, uuid) in copies {
                if uuid != token {
                    return Err(format!("parallel tool {id} request uuid token mismatch"));
                }
                expected.insert(pointer, token.clone());
            }
        }
    }
    let mut actual = BTreeMap::new();
    collect_parallel_uuid_tokens(trace, "", &mut actual)?;
    reject_parallel_uuid_token_keys(trace, "")?;
    if actual != expected {
        return Err("parallel UUID token paths are incomplete or undeclared".to_string());
    }
    let expected_pointers = expected.keys().cloned().collect::<Vec<_>>();
    if row.trace.normalization.non_contract_parallel_uuid_pointers != expected_pointers {
        return Err("parallel UUID pointers do not match canonical batches".to_string());
    }
    Ok(())
}

fn validate_project_slug_paths(value: &Json, count: &mut usize) -> Result<(), String> {
    const PREFIX: &str = "$WORKSPACE/.klaus-config/projects/";
    match value {
        Json::String(text) => {
            let text = runtime_scalar(text, "project slug path text")?;
            for (start, _) in text.match_indices(PREFIX) {
                let rest = &text[start + PREFIX.len()..];
                let Some(memory_index) = rest.find("/memory") else {
                    continue;
                };
                let after_memory = &rest[memory_index + "/memory".len()..];
                if !after_memory.is_empty()
                    && !after_memory.starts_with('/')
                    && !after_memory.starts_with('\n')
                {
                    continue;
                }
                let slug = &rest[..memory_index];
                if slug.contains('/') {
                    continue;
                }
                *count += 1;
                if slug != "$PROJECT_SLUG" {
                    return Err(format!("residual raw project slug {slug}"));
                }
            }
        }
        Json::Array(values) => {
            for child in values {
                validate_project_slug_paths(child, count)?;
            }
        }
        Json::Object(values) => {
            for child in values.values() {
                validate_project_slug_paths(child, count)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn load_checked_in_ts_contract() -> Result<Vec<TsContractRow>, String> {
    let bytes = read_core_fixture(TS_CONTRACT_FIXTURE_PATH)?;
    let fixture = std::str::from_utf8(&bytes)
        .map_err(|error| format!("baseline contract fixture is not UTF-8: {error}"))?;
    parse_ts_contract_fixture(&rows_only_document(&expand_shared_contract_rows(fixture)?))
}

/// Rehydrate the snapshot's shared request values into ordinary rows.
///
/// Line one of the checked-in snapshot is a content-addressed table of the
/// `system` and `tools` values every recorded request would otherwise repeat,
/// and rows carry `{"$blob":"<sha256>"}` in those two positions. This file is
/// the independent verifier of that snapshot, so it substitutes the shared
/// values with plain serde rather than borrowing the engine's reader, and the
/// strict row parser below keeps seeing complete rows.
fn expand_shared_contract_rows(fixture: &str) -> Result<Vec<SerdeJson>, String> {
    let body = fixture.strip_suffix('\n').ok_or_else(|| {
        "baseline contract fixture must end with exactly one final LF".to_string()
    })?;
    let mut lines = body.split('\n');
    let header_line = lines
        .next()
        .ok_or_else(|| "baseline contract fixture is empty".to_string())?;
    let header: SerdeJson = serde_json::from_str(header_line)
        .map_err(|error| format!("invalid baseline contract header: {error}"))?;
    let blobs = header
        .get("blobs")
        .and_then(SerdeJson::as_object)
        .ok_or_else(|| "baseline contract header must carry a shared value table".to_string())?;
    let mut rows = Vec::new();
    for (index, line) in lines.enumerate() {
        let row_number = index + 1;
        let mut row: SerdeJson = serde_json::from_str(line)
            .map_err(|error| format!("invalid baseline contract row {row_number}: {error}"))?;
        let requests = row
            .get_mut("trace")
            .and_then(|trace| trace.get_mut("modelRequests"))
            .and_then(SerdeJson::as_array_mut)
            .ok_or_else(|| {
                format!(
                    "invalid baseline contract row {row_number}: modelRequests must be an array"
                )
            })?;
        for request in requests.iter_mut() {
            for field in ["system", "tools"] {
                let Some(digest) = request
                    .get(field)
                    .and_then(|value| value.get("$blob"))
                    .and_then(SerdeJson::as_str)
                    .map(str::to_owned)
                else {
                    continue;
                };
                let shared = blobs.get(&digest).ok_or_else(|| {
                    format!(
                        "invalid baseline contract row {row_number}: {field} references absent shared value {digest}"
                    )
                })?;
                request[field] = shared.clone();
            }
        }
        rows.push(row);
    }
    if rows.is_empty() {
        return Err("baseline contract fixture carries a header with no rows".to_string());
    }
    Ok(rows)
}

fn rows_only_document(rows: &[SerdeJson]) -> String {
    let mut document = String::new();
    for row in rows {
        document.push_str(&serde_json::to_string(row).expect("contract row re-serializes"));
        document.push('\n');
    }
    document
}

fn validate_fixed_source_adaptation_gate(
    source_contract_bytes: &[u8],
    adaptation_bytes: &[u8],
) -> Result<(), String> {
    let adaptation = std::str::from_utf8(adaptation_bytes)
        .map_err(|error| format!("Rust adaptation fixture is not UTF-8: {error}"))?;
    let fixture = runtime_json::parse(&RuntimeString::from_utf8(adaptation))
        .map_err(|error| format!("invalid Rust adaptation fixture: {error}"))?;
    let declared_hash = fixture
        .get("sourceContractSha256")
        .and_then(Json::as_str)
        .ok_or_else(|| "sourceContractSha256 must be text".to_string())?;
    let actual_hash = format!("{:x}", Sha256::digest(source_contract_bytes));
    if !runtime_eq_utf8(declared_hash, &actual_hash) {
        return Err(format!(
            "source contract SHA-256 mismatch: declared {}, actual {actual_hash}",
            runtime_diagnostic(declared_hash)
        ));
    }

    let source_contract = std::str::from_utf8(source_contract_bytes)
        .map_err(|error| format!("baseline contract fixture is not UTF-8: {error}"))?;
    let source_rows = expand_shared_contract_rows(source_contract)?;
    let rows = parse_ts_contract_fixture(&rows_only_document(&source_rows))?;
    let declared_sets = fixture
        .get("scriptedRequestMatchers")
        .and_then(Json::as_array)
        .ok_or_else(|| "scriptedRequestMatchers must be an array".to_string())?;
    let source_tasks = [
        "provider-parallel-subagents",
        "provider-regression-subagents",
    ];
    if declared_sets.len() != source_tasks.len() {
        return Err("scriptedRequestMatchers task set differs from fixed source".to_string());
    }
    for (declared, source_task) in declared_sets.iter().zip(source_tasks) {
        if !runtime_option_eq_utf8(
            declared.get("sourceTask").and_then(Json::as_str),
            source_task,
        ) {
            return Err("scriptedRequestMatchers task order differs from fixed source".to_string());
        }
        let mut profiles = RuntimeObject::new();
        for (profile, profile_name) in [
            (EvalProfile::Anthropic, "anthropic"),
            (EvalProfile::Openai, "openai"),
            (EvalProfile::Generic, "generic"),
        ] {
            let matching = rows
                .iter()
                .enumerate()
                .filter_map(|(index, row)| {
                    (row.task == source_task && row.profile == profile).then_some(index)
                })
                .collect::<Vec<_>>();
            if matching.len() != 1 {
                return Err(format!(
                    "fixed source must contain exactly one {source_task}/{profile_name} row"
                ));
            }
            let source_trace = serde_json::to_string(&source_rows[matching[0]]["trace"])
                .map_err(|error| format!("fixed source trace cannot re-serialize: {error}"))?;
            let source_trace = runtime_json::parse(&RuntimeString::from_utf8(&source_trace))
                .map_err(|error| format!("fixed source trace cannot parse: {error}"))?;
            let history = source_trace["history"]
                .as_array()
                .ok_or_else(|| "fixed source history must be an array".to_string())?;
            let requests = source_trace["modelRequests"]
                .as_array()
                .ok_or_else(|| "fixed source modelRequests must be an array".to_string())?;
            let placeholders = authenticated_fork_placeholder_coordinates(
                &source_trace,
                history,
                requests,
                ContractFixtureKind::FixedTs,
            )?;
            let directive_prefix = runtime_scalar(
                &independent_fork_directive(
                    &RuntimeString::from_utf8(""),
                    ContractFixtureKind::FixedTs,
                ),
                "fixed source fork directive prefix",
            )?;
            let matchers = rows[matching[0]]
                .trace
                .model_requests
                .iter()
                .enumerate()
                .map(|(script_index, request)| {
                    if request.cache_key.as_deref() != Some("provider-behavior-session") {
                        return Err(
                            "fixed source full fork must preserve the parent cache key".to_string()
                        );
                    }
                    let (session_role, first_user_text) =
                        if raw_request_is_history_prefix(&requests[script_index], history) {
                            ("main", first_genuine_user_text(request)?)
                        } else {
                            if !placeholders
                                .iter()
                                .any(|(request_index, _)| *request_index == script_index)
                            {
                                return Err(
                                    "fixed source child matcher lacks an authenticated fork seed"
                                        .to_string(),
                                );
                            }
                            // A complete authenticated seed binds the directive to its
                            // parent task; inherited user text is not the child key.
                            let directive = request
                                .messages
                                .iter()
                                .rev()
                                .filter(|message| {
                                    message.get("type").and_then(SerdeJson::as_str) == Some("user")
                                        && message.get("isMeta").and_then(SerdeJson::as_bool)
                                            != Some(true)
                                })
                                .find_map(|message| {
                                    message
                                        .pointer("/content/0/text")
                                        .and_then(SerdeJson::as_str)?
                                        .strip_prefix(&directive_prefix)
                                        .filter(|directive| !directive.is_empty())
                                        .map(str::to_owned)
                                })
                                .ok_or_else(|| {
                                    "fixed source child matcher lacks its exact directive"
                                        .to_string()
                                })?;
                            ("child", directive)
                        };
                    Ok(exact_json!({
                        "scriptIndex": script_index,
                        "sessionRole": session_role,
                        "firstUserText": first_user_text,
                    }))
                })
                .collect::<Result<Vec<_>, String>>()?;
            profiles.insert(profile_name, Json::Array(matchers));
        }
        if declared.get("profiles") != Some(&Json::Object(profiles)) {
            return Err(format!(
                "scripted request matchers for {source_task} do not match fixed source modelRequests"
            ));
        }
    }
    Ok(())
}

fn first_genuine_user_text(request: &StrictModelRequest) -> Result<String, String> {
    request
        .messages
        .iter()
        .filter(|message| {
            message.get("type").and_then(SerdeJson::as_str) == Some("user")
                && message.get("isMeta").and_then(SerdeJson::as_bool) != Some(true)
        })
        .find_map(|message| {
            message
                .get("content")
                .and_then(SerdeJson::as_array)?
                .iter()
                .find(|block| {
                    block.get("type").and_then(SerdeJson::as_str) == Some("text")
                        && block
                            .get("text")
                            .and_then(SerdeJson::as_str)
                            .is_some_and(|text| !text.is_empty())
                })?
                .get("text")?
                .as_str()
                .map(str::to_string)
        })
        .ok_or_else(|| "modelRequest is missing first genuine user text".to_string())
}

fn fixture_values() -> &'static [SerdeJson] {
    static VALUES: LazyLock<Vec<SerdeJson>> = LazyLock::new(|| {
        let fixture = String::from_utf8(read_core_fixture(TS_CONTRACT_FIXTURE_PATH).unwrap())
            .expect("baseline contract fixture is UTF-8");
        expand_shared_contract_rows(&fixture).expect("baseline contract fixture rows")
    });
    VALUES.as_slice()
}

fn contract_trace(task: &str, profile: EvalProfile) -> ProviderBehaviorTrace {
    load_baseline_reference_trace(task, profile)
        .unwrap_or_else(|error| panic!("missing fixed contract row {task}/{profile:?}: {error}"))
}

fn add_missing_plan_reminder_after_task_create_removal(trace: &mut ProviderBehaviorTrace) {
    assert_eq!(trace.task.name, "provider-plan-patch");
    assert_eq!(trace.run.profile, EvalProfile::Anthropic);
    assert_eq!(trace.model_requests.len(), 2);
    assert!(trace.normalized_internal_calls.iter().all(|call| {
        !runtime_option_in_utf8(
            call.get("internalName").and_then(Json::as_str),
            &["TaskCreate", "TaskWrite", "TaskUpdate"],
        )
    }));
    // These synthetic traces replace the fixture's TaskCreate with file work.
    // Their follow-up must carry the same exact missing-plan part as a trusted
    // unplanned run, without replacing any other request metadata.
    let donor = contract_trace("read-summarize", trace.run.profile);
    let reminder = donor.model_requests[1]["system"]
        .as_array()
        .unwrap()
        .iter()
        .find(|part| runtime_json_field_eq(part, "name", "task-plan-reminder"))
        .unwrap()
        .clone();
    let system = trace.model_requests[1]["system"].as_array_mut().unwrap();
    assert!(system
        .iter()
        .all(|part| !runtime_json_field_eq(part, "name", "task-plan-reminder")));
    let before = system.clone();
    let index = system
        .iter()
        .position(|part| {
            runtime_json_field_eq(part, "name", "permissions")
                || runtime_json_field_eq(part, "name", "multi-agent-role")
                || runtime_json_field_eq(part, "name", "orchestration-mode")
                || runtime_json_field_eq(part, "name", "total-tokens")
                || runtime_json_field_eq(part, "name", "provider-behavior:anthropic-reminder")
        })
        .unwrap();
    system.insert(index, reminder);
    assert!(runtime_json_field_eq(
        &system[index + 1],
        "name",
        "permissions"
    ));
    assert!(runtime_json_field_eq(
        &system[index + 2],
        "name",
        "multi-agent-role"
    ));
    assert!(runtime_json_field_eq(
        &system[index + 3],
        "name",
        "orchestration-mode"
    ));
    let mut without_reminder = system.clone();
    without_reminder.remove(index);
    assert_eq!(without_reminder, before);
}

fn add_live_tool_event_turn_ids(trace: &mut ProviderBehaviorTrace) {
    let turn_id = trace
        .engine_events
        .iter()
        .find(|event| runtime_json_field_eq(event, "type", "turn.started"))
        .and_then(|event| event.get("turnId"))
        .cloned()
        .expect("fixed contract trace must start one identified turn");
    for event in &mut trace.engine_events {
        if runtime_json_field_eq(event, "type", "tool.event") {
            event
                .as_object_mut()
                .unwrap()
                .insert("turnId".to_owned(), turn_id.clone());
            *event = canonical_engine_event(event);
        }
    }
}

fn rewrite_final_agent_text(trace: &mut ProviderBehaviorTrace, text: &str) {
    trace.final_text = RuntimeString::from_utf8(text);
    let last_request_start = trace
        .engine_events
        .iter()
        .rposition(|event| runtime_json_field_eq(event, "type", "stream.request_start"))
        .unwrap();
    let mut delta_count = 0;
    let mut completed_count = 0;
    for event in &mut trace.engine_events[last_request_start + 1..] {
        if runtime_json_field_eq(event, "type", "text.delta") {
            event["text"] = Json::String(text.to_owned().into());
            delta_count += 1;
        } else if runtime_json_field_eq(event, "type", "item.completed")
            && runtime_json_pointer_eq(event, "/item/type", "agent_message")
        {
            event["item"]["text"] = Json::String(text.to_owned().into());
            completed_count += 1;
        }
    }
    assert_eq!(delta_count, 1);
    assert_eq!(completed_count, 1);
    let assistant = trace
        .history
        .iter_mut()
        .rev()
        .find(|message| runtime_json_field_eq(message, "type", "assistant"))
        .unwrap();
    let text_blocks = assistant["content"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .filter(|block| runtime_json_field_eq(block, "type", "text"))
        .collect::<Vec<_>>();
    assert_eq!(text_blocks.len(), 1);
    text_blocks.into_iter().next().unwrap()["text"] = Json::String(text.to_owned().into());
}

fn replace_assistant_content(
    trace: &mut ProviderBehaviorTrace,
    uuid: &RuntimeString,
    content: Json,
) {
    for message in &mut trace.history {
        if runtime_json_field_eq(message, "type", "assistant")
            && message.get("uuid").and_then(Json::as_str) == Some(uuid)
        {
            message["content"] = content.clone();
        }
    }
    for request in &mut trace.model_requests {
        for message in request["messages"].as_array_mut().unwrap() {
            if runtime_json_field_eq(message, "type", "assistant")
                && message.get("uuid").and_then(Json::as_str) == Some(uuid)
            {
                message["content"] = content.clone();
            }
        }
    }
}

fn remove_segment_text_events(trace: &mut ProviderBehaviorTrace, segment_index: usize, uuid: &str) {
    let starts = trace
        .engine_events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| {
            (runtime_json_field_eq(event, "type", "stream.request_start")).then_some(index)
        })
        .collect::<Vec<_>>();
    let start = starts[segment_index];
    let end = starts
        .get(segment_index + 1)
        .copied()
        .unwrap_or(trace.engine_events.len());
    let mut index = 0;
    trace.engine_events.retain(|event| {
        let in_segment = index > start && index < end;
        let remove = in_segment
            && (runtime_json_field_eq(event, "type", "text.delta")
                || (runtime_json_field_eq(event, "type", "item.completed")
                    && runtime_json_pointer_eq(event, "/item/type", "agent_message")
                    && runtime_json_pointer_eq(event, "/item/id", uuid)));
        index += 1;
        !remove
    });
}

fn make_first_assistant_tool_only(trace: &mut ProviderBehaviorTrace) -> String {
    let first_assistant = trace
        .history
        .iter()
        .find(|message| runtime_json_field_eq(message, "type", "assistant"))
        .unwrap();
    let runtime_uuid = first_assistant["uuid"].as_str().unwrap().clone();
    let uuid = runtime_scalar(&runtime_uuid, "assistant uuid")
        .expect("fixed assistant uuid must be scalar UTF-8");
    let tool_only = Json::Array(
        first_assistant["content"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|block| !runtime_json_field_eq(block, "type", "text"))
            .cloned()
            .collect(),
    );
    assert!(tool_only
        .as_array()
        .unwrap()
        .iter()
        .any(|block| runtime_json_field_eq(block, "type", "tool_use")));
    replace_assistant_content(trace, &runtime_uuid, tool_only);
    remove_segment_text_events(trace, 0, &uuid);
    uuid
}

fn remap_model_request_pointers(
    pointers: &mut Vec<RuntimeString>,
    index_map: &BTreeMap<usize, usize>,
) {
    *pointers = pointers
        .iter()
        .filter_map(|pointer| {
            let pointer_text = runtime_scalar(pointer, "model request normalization pointer")
                .expect("fixed normalization pointer must be scalar UTF-8");
            let Some(rest) = pointer_text.strip_prefix("/modelRequests/") else {
                return Some(pointer.clone());
            };
            let (index, suffix) = rest.split_once('/')?;
            let new_index = index_map.get(&index.parse::<usize>().ok()?)?;
            Some(RuntimeString::from_utf8(&format!(
                "/modelRequests/{new_index}/{suffix}"
            )))
        })
        .collect();
    pointers.sort();
    pointers.dedup();
}

fn raw_request_is_history_prefix(request: &Json, history: &[Json]) -> bool {
    request
        .get("messages")
        .and_then(Json::as_array)
        .is_some_and(|messages| {
            messages.len() <= history.len() && messages.as_slice() == &history[..messages.len()]
        })
}

fn child_request_indices(trace: &ProviderBehaviorTrace) -> Vec<usize> {
    trace
        .model_requests
        .iter()
        .enumerate()
        .filter_map(|(index, request)| {
            (!raw_request_is_history_prefix(request, &trace.history)).then_some(index)
        })
        .collect()
}

fn remove_model_request(trace: &mut ProviderBehaviorTrace, removed: usize) {
    let kept_indices = (0..trace.model_requests.len())
        .filter(|index| *index != removed)
        .collect::<Vec<_>>();
    trace.model_requests = kept_indices
        .iter()
        .map(|index| trace.model_requests[*index].clone())
        .collect();
    let index_map = kept_indices
        .into_iter()
        .enumerate()
        .map(|(new, old)| (old, new))
        .collect::<BTreeMap<_, _>>();
    remap_model_request_pointers(
        &mut trace.normalization.non_contract_time_pointers,
        &index_map,
    );
    remap_model_request_pointers(
        &mut trace.normalization.non_contract_host_pointers,
        &index_map,
    );
    remap_model_request_pointers(
        &mut trace.normalization.non_contract_parallel_uuid_pointers,
        &index_map,
    );
}

fn duplicate_model_request_normalization_pointers(
    trace: &mut ProviderBehaviorTrace,
    source: usize,
    duplicate: usize,
) {
    fn duplicate_for(pointers: &mut Vec<RuntimeString>, source: usize, duplicate: usize) {
        let prefix = format!("/modelRequests/{source}/");
        let duplicate_prefix = RuntimeString::from_utf8(&format!("/modelRequests/{duplicate}/"));
        let additions = pointers
            .iter()
            .filter_map(|pointer| runtime_strip_prefix(pointer, &prefix))
            .map(|suffix| duplicate_prefix.concat(&suffix))
            .collect::<Vec<_>>();
        pointers.extend(additions);
        pointers.sort();
        pointers.dedup();
    }

    duplicate_for(
        &mut trace.normalization.non_contract_time_pointers,
        source,
        duplicate,
    );
    duplicate_for(
        &mut trace.normalization.non_contract_host_pointers,
        source,
        duplicate,
    );
    duplicate_for(
        &mut trace.normalization.non_contract_parallel_uuid_pointers,
        source,
        duplicate,
    );
}

fn one_row_fixture(value: &SerdeJson) -> String {
    format!("{}\n", serde_json::to_string(value).unwrap())
}

fn replace_first_string(value: &mut SerdeJson, from: &str, to: &str) -> bool {
    match value {
        SerdeJson::String(text) if text.contains(from) => {
            *text = text.replacen(from, to, 1);
            true
        }
        SerdeJson::Array(values) => values
            .iter_mut()
            .any(|child| replace_first_string(child, from, to)),
        SerdeJson::Object(values) => values
            .values_mut()
            .any(|child| replace_first_string(child, from, to)),
        _ => false,
    }
}

fn replace_first_runtime_string(value: &mut Json, from: &str, to: &str) -> bool {
    match value {
        Json::String(text) if text.contains(from) => {
            *text = runtime_replace_first(text, from, to)
                .expect("contains check guarantees one exact replacement");
            true
        }
        Json::Array(values) => values
            .iter_mut()
            .any(|child| replace_first_runtime_string(child, from, to)),
        Json::Object(values) => values
            .values_mut()
            .any(|child| replace_first_runtime_string(child, from, to)),
        _ => false,
    }
}

fn first_history_tool_use_mut(row: &mut SerdeJson) -> Option<&mut SerdeJson> {
    let history = row.pointer_mut("/trace/history")?.as_array_mut()?;
    for message in history {
        if message.get("type").and_then(SerdeJson::as_str) != Some("assistant") {
            continue;
        }
        for block in message.get_mut("content")?.as_array_mut()? {
            if block.get("type").and_then(SerdeJson::as_str) == Some("tool_use") {
                return Some(block);
            }
        }
    }
    None
}

fn rewrite_trace_tool_call(
    trace: &mut ProviderBehaviorTrace,
    id: &str,
    visible_input: &Json,
    internal_input: &Json,
    result_content: &Json,
    terminal_result: &Json,
) {
    let terminal_result = canonical_test_tool_result(terminal_result);
    let result_uuid = trace
        .history
        .iter()
        .find(|message| {
            runtime_json_field_eq(message, "type", "tool_result")
                && runtime_json_field_eq(message, "toolUseId", id)
        })
        .and_then(|message| message.get("uuid"))
        .and_then(Json::as_str)
        .cloned()
        .expect("rewritten tool must have an identified parent result");
    let rewrite_messages = |messages: &mut Vec<Json>| {
        for message in messages {
            if runtime_json_field_eq(message, "type", "assistant") {
                for block in message["content"].as_array_mut().unwrap() {
                    if runtime_json_field_eq(block, "id", id) {
                        block["input"] = visible_input.clone();
                    }
                }
            } else if runtime_json_field_eq(message, "type", "tool_result")
                && runtime_json_field_eq(message, "toolUseId", id)
                && message.get("uuid").and_then(Json::as_str) == Some(&result_uuid)
            {
                // Child-local unresolved placeholders retain their seed content.
                message["content"] = result_content.clone();
            }
        }
    };
    rewrite_messages(&mut trace.history);
    for request in &mut trace.model_requests {
        rewrite_messages(request["messages"].as_array_mut().unwrap());
    }
    let visible = trace
        .model_visible_tool_uses
        .iter_mut()
        .find(|call| runtime_json_field_eq(call, "id", id))
        .unwrap();
    visible["input"] = visible_input.clone();
    let normalized = trace
        .normalized_internal_calls
        .iter_mut()
        .find(|call| runtime_json_field_eq(call, "id", id))
        .unwrap();
    normalized["visibleInput"] = visible_input.clone();
    normalized["internalInput"] = internal_input.clone();
    for event in &mut trace.engine_events {
        if runtime_json_pointer_eq(event, "/item/id", id) {
            event["item"]["arguments"] = internal_input.clone();
            if runtime_option_in_utf8(
                event.get("type").and_then(Json::as_str),
                &["item.completed", "item.updated"],
            ) {
                event["item"]["result"] = terminal_result.clone();
            }
            *event = canonical_engine_event(event);
        }
    }
    let effect = trace
        .side_effects
        .iter_mut()
        .find(|effect| runtime_json_field_eq(effect, "id", id))
        .unwrap();
    effect["input"] = internal_input.clone();
    effect["result"] = terminal_result;
}

fn rename_trace_tool_call(
    trace: &mut ProviderBehaviorTrace,
    id: &str,
    visible_name: &str,
    internal_name: &str,
) {
    let rename_messages = |messages: &mut Vec<Json>| {
        for message in messages {
            if !runtime_json_field_eq(message, "type", "assistant") {
                continue;
            }
            for block in message["content"].as_array_mut().unwrap() {
                if runtime_json_field_eq(block, "id", id) {
                    block["name"] = Json::String(visible_name.to_owned().into());
                }
            }
        }
    };
    rename_messages(&mut trace.history);
    for request in &mut trace.model_requests {
        rename_messages(request["messages"].as_array_mut().unwrap());
    }
    let visible = trace
        .model_visible_tool_uses
        .iter_mut()
        .find(|call| runtime_json_field_eq(call, "id", id))
        .unwrap();
    visible["name"] = Json::String(visible_name.to_owned().into());
    let normalized = trace
        .normalized_internal_calls
        .iter_mut()
        .find(|call| runtime_json_field_eq(call, "id", id))
        .unwrap();
    normalized["visibleName"] = Json::String(visible_name.to_owned().into());
    normalized["internalName"] = Json::String(internal_name.to_owned().into());
    for event in &mut trace.engine_events {
        if runtime_json_pointer_eq(event, "/item/id", id) {
            event["item"]["toolName"] = Json::String(internal_name.to_owned().into());
        }
    }
    let effect = trace
        .side_effects
        .iter_mut()
        .find(|effect| runtime_json_field_eq(effect, "id", id))
        .unwrap();
    effect["name"] = Json::String(internal_name.to_owned().into());
}

fn rewrite_trace_call_as_successful_existing_write(
    trace: &mut ProviderBehaviorTrace,
    id: &str,
    path: &str,
    content: &str,
) {
    let content_len = content.encode_utf16().count();
    let input = exact_json!({
        "path": path,
        "content": content,
    });
    rewrite_trace_tool_call(
        trace,
        id,
        &input,
        &input,
        &exact_json!([{
            "type": "text",
            "text": (format!("Updated {path} ({content_len} bytes)")),
        }]),
        &exact_json!({
            "path": (format!("$WORKSPACE/{path}")),
            "bytes": content_len,
            "kind": "modify",
        }),
    );
    rename_trace_tool_call(trace, id, "write", "write");

    let before = runtime_file(&trace.before_files, path).clone();
    trace.after_files.insert(
        RuntimeString::from_utf8(path),
        RuntimeString::from_utf8(content),
    );
    if let Some(effect) = trace.side_effects.iter_mut().find(|effect| {
        runtime_json_field_eq(effect, "type", "file_change")
            && runtime_json_field_eq(effect, "path", path)
    }) {
        effect["before"] = Json::String(before);
        effect["after"] = Json::String(content.to_owned().into());
    } else {
        trace.side_effects.push(exact_json!({
            "type": "file_change",
            "path": path,
            "before": before,
            "after": content,
        }));
    }
}

fn rewrite_task_call_as_failure(trace: &mut ProviderBehaviorTrace, id: &str, message: &str) {
    let result_uuid = trace
        .history
        .iter()
        .find(|message| {
            runtime_json_field_eq(message, "type", "tool_result")
                && runtime_json_field_eq(message, "toolUseId", id)
        })
        .and_then(|message| message.get("uuid"))
        .and_then(Json::as_str)
        .cloned()
        .expect("failed tool must have an identified parent result");
    let terminal = trace
        .engine_events
        .iter_mut()
        .find(|event| {
            runtime_json_field_eq(event, "type", "item.completed")
                && runtime_json_pointer_eq(event, "/item/id", id)
        })
        .unwrap();
    terminal["type"] = Json::String("item.updated".to_owned().into());
    terminal["item"]["status"] = Json::String("failed".to_owned().into());
    terminal["item"].as_object_mut().unwrap().remove("result");
    terminal["item"]["error"] = exact_json!({ "message": message });
    *terminal = canonical_engine_event(terminal);
    trace.engine_events.retain(|event| {
        !(runtime_json_field_eq(event, "type", "tool.event")
            && runtime_json_field_eq(event, "toolUseId", "e2")
            && runtime_json_pointer_eq(event, "/event/type", "file_change"))
    });

    let effect = trace
        .side_effects
        .iter_mut()
        .find(|effect| runtime_json_field_eq(effect, "id", id))
        .unwrap();
    effect["status"] = Json::String("failed".to_owned().into());
    effect.as_object_mut().unwrap().remove("result");
    effect["error"] = exact_json!({ "message": message });

    let failed_content = exact_json!([{
        "type": "text",
        "text": (format!("<tool_use_error tool=\"Task\">\n{message}\n</tool_use_error>")),
    }]);
    let rewrite_messages = |messages: &mut Vec<Json>| {
        for message in messages {
            if runtime_json_field_eq(message, "type", "tool_result")
                && runtime_json_field_eq(message, "toolUseId", id)
                && message.get("uuid").and_then(Json::as_str) == Some(&result_uuid)
            {
                // The parent failure does not replace a child-local seed placeholder.
                message["isError"] = Json::Bool(true);
                message["content"] = failed_content.clone();
            }
        }
    };
    rewrite_messages(&mut trace.history);
    for request in &mut trace.model_requests {
        rewrite_messages(request["messages"].as_array_mut().unwrap());
    }
}

fn glob_trace(pattern: &str, filenames: &[&str]) -> ProviderBehaviorTrace {
    let mut trace = contract_trace("provider-file-facades", EvalProfile::Anthropic);
    let input = exact_json!({
        "pattern": pattern,
    });
    let mut result = trace
        .side_effects
        .iter()
        .find(|effect| runtime_json_field_eq(effect, "id", "glob-target"))
        .and_then(|effect| effect.get("result"))
        .cloned()
        .unwrap();
    result["numFiles"] = Json::from(filenames.len());
    result["filenames"] = exact_json!((filenames.to_vec()));
    rewrite_trace_tool_call(
        &mut trace,
        "glob-target",
        &input,
        &input,
        &exact_json!([{
            "type": "text",
            "text": (filenames.join("\n")),
        }]),
        &result,
    );
    trace
}

fn two_file_glob_trace(filenames: &[&str]) -> ProviderBehaviorTrace {
    glob_trace("**/{facade-target-alpha,other}.txt", filenames)
}

fn aliased_apply_patch_trace() -> ProviderBehaviorTrace {
    let mut trace = contract_trace("provider-apply-patch", EvalProfile::Anthropic);
    let patch = "*** Begin Patch\n*** Update File: ./index.js\n@@\n-export const message = 'old'\n+export const message = 'new'\n*** End Patch";
    let input = exact_json!({ "patch": patch });
    rewrite_trace_tool_call(
        &mut trace,
        "p1",
        &input,
        &input,
        &exact_json!([{
            "type": "text",
            "text": "Applied patch to 1 file:\nupdate ./index.js (+1 -1)",
        }]),
        &exact_json!({
            "files": [{
                "added": 1,
                "exact": true,
                "kind": "update",
                "path": "./index.js",
                "removed": 1,
            }],
        }),
    );
    trace
}

fn failed_apply_patch_trace(patch: &str, failure: &str) -> ProviderBehaviorTrace {
    let mut trace = contract_trace("provider-apply-patch", EvalProfile::Anthropic);
    let input = exact_json!({ "patch": patch });
    let terminal_result = exact_json!({ "files": [] });
    let raw_text = format!("apply_patch failed: {failure}");
    rewrite_trace_tool_call(
        &mut trace,
        "p1",
        &input,
        &input,
        &exact_json!([{
            "type": "text",
            "text": (format!(
                "<tool_use_error tool=\"apply_patch\">\n{raw_text}\n</tool_use_error>"
            )),
        }]),
        &terminal_result,
    );

    let rewrite_results = |messages: &mut Vec<Json>| {
        for message in messages {
            if runtime_json_field_eq(message, "type", "tool_result")
                && runtime_json_field_eq(message, "toolUseId", "p1")
            {
                message["isError"] = Json::Bool(true);
            }
        }
    };
    rewrite_results(&mut trace.history);
    for request in &mut trace.model_requests {
        rewrite_results(request["messages"].as_array_mut().unwrap());
    }

    let terminal = trace
        .engine_events
        .iter_mut()
        .find(|event| {
            runtime_json_field_eq(event, "type", "item.completed")
                && runtime_json_pointer_eq(event, "/item/id", "p1")
        })
        .unwrap();
    terminal["type"] = Json::String("item.updated".to_owned().into());
    terminal["item"]["status"] = Json::String("failed".to_owned().into());
    terminal["item"]["error"] = exact_json!({
        "message": "tool reported an error",
    });
    *terminal = canonical_engine_event(terminal);
    trace.engine_events.retain(|event| {
        !(runtime_json_field_eq(event, "type", "tool.event")
            && runtime_json_field_eq(event, "toolUseId", "p1")
            && runtime_json_pointer_eq(event, "/event/type", "file_change"))
    });

    let effect = trace
        .side_effects
        .iter_mut()
        .find(|effect| runtime_json_field_eq(effect, "id", "p1"))
        .unwrap();
    effect["status"] = Json::String("failed".to_owned().into());
    effect["error"] = exact_json!({
        "message": "tool reported an error",
    });
    trace
        .side_effects
        .retain(|effect| !runtime_json_field_eq(effect, "type", "file_change"));
    trace.after_files = trace.before_files.clone();
    trace.result.success = false;
    trace.result.score_success = false;
    trace.result.behavior_success = false;
    trace.result.behavior_failures = vec!["tool failures 1 > 0".to_owned().into()];
    trace
}

fn unread_existing_write_trace() -> ProviderBehaviorTrace {
    let mut trace = contract_trace("provider-apply-patch", EvalProfile::Anthropic);
    let input = exact_json!({
        "path": "index.js",
        "content": "export const message = 'replacement'\n",
    });
    let result = exact_json!({
        "path": "$WORKSPACE/index.js",
        "bytes": 0,
        "kind": "modify",
    });
    let message = "File has not been read yet. Read it first before writing to it.";
    rewrite_trace_tool_call(
        &mut trace,
        "p1",
        &input,
        &input,
        &exact_json!([{
            "type": "text",
            "text": (format!("<tool_use_error tool=\"write\">\n{message}\n</tool_use_error>")),
        }]),
        &result,
    );

    let rewrite_messages = |messages: &mut Vec<Json>| {
        for message in messages {
            if runtime_json_field_eq(message, "type", "assistant") {
                for block in message["content"].as_array_mut().unwrap() {
                    if runtime_json_field_eq(block, "id", "p1") {
                        block["name"] = Json::String("write".to_owned().into());
                    }
                }
            } else if runtime_json_field_eq(message, "type", "tool_result")
                && runtime_json_field_eq(message, "toolUseId", "p1")
            {
                message["isError"] = Json::Bool(true);
            }
        }
    };
    rewrite_messages(&mut trace.history);
    for request in &mut trace.model_requests {
        rewrite_messages(request["messages"].as_array_mut().unwrap());
    }
    let visible = trace
        .model_visible_tool_uses
        .iter_mut()
        .find(|call| runtime_json_field_eq(call, "id", "p1"))
        .unwrap();
    visible["name"] = Json::String("write".to_owned().into());
    let normalized = trace
        .normalized_internal_calls
        .iter_mut()
        .find(|call| runtime_json_field_eq(call, "id", "p1"))
        .unwrap();
    normalized["internalName"] = Json::String("write".to_owned().into());
    normalized["visibleName"] = Json::String("write".to_owned().into());

    for event in &mut trace.engine_events {
        if !runtime_json_pointer_eq(event, "/item/id", "p1") {
            continue;
        }
        event["item"]["toolName"] = Json::String("write".to_owned().into());
        if runtime_json_field_eq(event, "type", "item.completed") {
            event["type"] = Json::String("item.updated".to_owned().into());
            event["item"]["status"] = Json::String("failed".to_owned().into());
            event["item"]["error"] = exact_json!({
                "message": "tool reported an error",
            });
        }
        *event = canonical_engine_event(event);
    }
    trace.engine_events.retain(|event| {
        !(runtime_json_field_eq(event, "type", "tool.event")
            && runtime_json_field_eq(event, "toolUseId", "p1")
            && runtime_json_pointer_eq(event, "/event/type", "file_change"))
    });

    let effect = trace
        .side_effects
        .iter_mut()
        .find(|effect| runtime_json_field_eq(effect, "id", "p1"))
        .unwrap();
    effect["name"] = Json::String("write".to_owned().into());
    effect["status"] = Json::String("failed".to_owned().into());
    effect["error"] = exact_json!({
        "message": "tool reported an error",
    });
    trace
        .side_effects
        .retain(|effect| !runtime_json_field_eq(effect, "type", "file_change"));
    trace.after_files = trace.before_files.clone();
    trace.result.success = false;
    trace.result.score_success = false;
    trace.result.behavior_success = false;
    trace.result.behavior_failures = vec![
        "tool apply_patch called 0 < 1".to_owned().into(),
        "tool failures 1 > 0".to_owned().into(),
    ];
    trace
}

fn net_zero_failed_patch_then_write_trace() -> ProviderBehaviorTrace {
    let mut trace = contract_trace("edit-multi", EvalProfile::Anthropic);
    let patch = "*** Begin Patch\n*** Update File: config.js\n@@\n-const port = 3000\n+const port = 4000\n*** Update File: dir/../config.js\n@@\n-const port = 4000\n+const port = 3000\n*** Update File: missing.js\n@@\n-old\n+new\n*** End Patch";
    let patch_input = exact_json!({ "patch": patch });
    let failure = "apply_patch failed: apply_patch: cannot update missing file: missing.js";
    rewrite_trace_tool_call(
        &mut trace,
        "e1",
        &patch_input,
        &patch_input,
        &exact_json!([{
            "type": "text",
            "text": (format!(
                "<tool_use_error tool=\"apply_patch\">\n{failure}\n</tool_use_error>"
            )),
        }]),
        &exact_json!({ "files": [] }),
    );
    rename_trace_tool_call(&mut trace, "e1", "apply_patch", "apply_patch");
    let set_error_result = |messages: &mut Vec<Json>| {
        for message in messages {
            if runtime_json_field_eq(message, "type", "tool_result")
                && runtime_json_field_eq(message, "toolUseId", "e1")
            {
                message["isError"] = Json::Bool(true);
            }
        }
    };
    set_error_result(&mut trace.history);
    for request in &mut trace.model_requests {
        set_error_result(request["messages"].as_array_mut().unwrap());
    }
    let patch_terminal = trace
        .engine_events
        .iter_mut()
        .find(|event| {
            runtime_json_field_eq(event, "type", "item.completed")
                && runtime_json_pointer_eq(event, "/item/id", "e1")
        })
        .unwrap();
    patch_terminal["type"] = Json::String("item.updated".to_owned().into());
    patch_terminal["item"]["status"] = Json::String("failed".to_owned().into());
    patch_terminal["item"]["error"] = exact_json!({
        "message": "tool reported an error",
    });
    *patch_terminal = canonical_engine_event(patch_terminal);
    trace.engine_events.retain(|event| {
        !(runtime_json_field_eq(event, "type", "tool.event")
            && runtime_json_field_eq(event, "toolUseId", "e1")
            && runtime_json_pointer_eq(event, "/event/type", "file_change"))
    });
    let patch_effect = trace
        .side_effects
        .iter_mut()
        .find(|effect| runtime_json_field_eq(effect, "id", "e1"))
        .unwrap();
    patch_effect["status"] = Json::String("failed".to_owned().into());
    patch_effect["error"] = exact_json!({
        "message": "tool reported an error",
    });

    let content = runtime_file(&trace.after_files, "config.js").clone();
    let write_input = exact_json!({
        "path": "config.js",
        "content": content,
    });
    rewrite_trace_tool_call(
        &mut trace,
        "e2",
        &write_input,
        &write_input,
        &exact_json!([{
            "type": "text",
            "text": (format!("Updated config.js ({} bytes)", content.len_utf16())),
        }]),
        &exact_json!({
            "path": "$WORKSPACE/config.js",
            "bytes": (content.len_utf16()),
            "kind": "modify",
        }),
    );
    rename_trace_tool_call(&mut trace, "e2", "write", "write");
    trace
}

#[tokio::test]
async fn trace_contract_validator_rejects_forged_result_booleans() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    trace.result.score_success = false;

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("scoreSuccess"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rebuilds_side_effects_from_events_and_files() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("add-function", EvalProfile::Anthropic);
    trace
        .side_effects
        .retain(|effect| !runtime_json_field_eq(effect, "type", "file_change"));

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("sideEffects"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_a_missing_write_file_event() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("add-function", EvalProfile::Anthropic);
    let before = trace.engine_events.len();
    trace.engine_events.retain(|event| {
        !(runtime_json_field_eq(event, "type", "tool.event")
            && runtime_json_field_eq(event, "toolUseId", "w1")
            && runtime_json_pointer_eq(event, "/event/type", "file_change"))
    });
    assert_eq!(trace.engine_events.len() + 1, before);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("file event"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_an_unconsumed_read_file_event() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    let terminal = trace
        .engine_events
        .iter()
        .position(|event| {
            runtime_json_field_eq(event, "type", "item.completed")
                && runtime_json_pointer_eq(event, "/item/id", "r1")
        })
        .unwrap();
    trace.engine_events.insert(
        terminal,
        exact_json!({
            "type": "tool.event",
            "toolUseId": "r1",
            "event": {
                "type": "file_change",
                "change": {
                    "path": "$WORKSPACE/README.md",
                    "kind": "modify",
                },
            },
        }),
    );

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(
        error.to_string().contains("unconsumed file event"),
        "{error}"
    );
}

#[tokio::test]
async fn trace_contract_validator_rejects_a_nested_unconsumed_file_event() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    let terminal = trace
        .engine_events
        .iter()
        .position(|event| {
            runtime_json_field_eq(event, "type", "item.completed")
                && runtime_json_pointer_eq(event, "/item/id", "r1")
        })
        .unwrap();
    trace.engine_events.insert(
        terminal,
        exact_json!({
            "type": "tool.event",
            "toolUseId": "r1",
            "event": {
                "type": "agent_progress",
                "agentId": "forged-child",
                "event": {
                    "type": "tool.event",
                    "turnId": "",
                    "toolUseId": "r1",
                    "event": {
                        "type": "file_change",
                        "change": {
                            "path": "$WORKSPACE/README.md",
                            "kind": "modify",
                        },
                    },
                },
            },
        }),
    );

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(
        error.to_string().contains("unconsumed file event"),
        "{error}"
    );
}

#[tokio::test]
async fn trace_contract_validator_accepts_the_hash_bound_code_mode_hidden_file_event() {
    let manifest = load_checked_in_manifest().unwrap();
    let trace = contract_trace("code-mode", EvalProfile::Anthropic);
    assert!(trace.engine_events.iter().any(|event| {
        runtime_json_field_eq(event, "type", "tool.event")
            && runtime_json_field_eq(event, "toolUseId", "fixture-000010")
            && runtime_json_pointer_eq(event, "/event/type", "file_change")
    }));

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
        .await
        .unwrap();
}

#[tokio::test]
async fn trace_contract_validator_accepts_flexible_write_input_with_canonical_persistence() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("add-function", EvalProfile::Anthropic);
    let display_path = "$WORKSPACE/./dir/../utils.js";
    let content = runtime_file(&trace.after_files, "utils.js").clone();
    let input = exact_json!({
        "path": display_path,
        "content": content,
    });
    rewrite_trace_tool_call(
        &mut trace,
        "w1",
        &input,
        &input,
        &exact_json!([{
            "type": "text",
            "text": (format!("Created {display_path} (63 bytes)")),
        }]),
        &exact_json!({
            "bytes": 63,
            "kind": "create",
            "path": "$WORKSPACE/utils.js",
        }),
    );

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
        .await
        .unwrap();
}

#[tokio::test]
async fn trace_contract_validator_rejects_a_write_file_event_path_alias() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("add-function", EvalProfile::Anthropic);
    let display_path = "./utils.js";
    let content = runtime_file(&trace.after_files, "utils.js").clone();
    let input = exact_json!({
        "path": display_path,
        "content": content,
    });
    rewrite_trace_tool_call(
        &mut trace,
        "w1",
        &input,
        &input,
        &exact_json!([{
            "type": "text",
            "text": "Created ./utils.js (63 bytes)",
        }]),
        &exact_json!({
            "bytes": 63,
            "kind": "create",
            "path": "$WORKSPACE/utils.js",
        }),
    );
    let event = trace
        .engine_events
        .iter_mut()
        .find(|event| {
            runtime_json_field_eq(event, "type", "tool.event")
                && runtime_json_field_eq(event, "toolUseId", "w1")
                && runtime_json_pointer_eq(event, "/event/type", "file_change")
        })
        .unwrap();
    event["event"]["change"]["path"] = Json::String("$WORKSPACE/./utils.js".to_owned().into());

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("file event"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_a_missing_edit_file_event() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-edit-search-shell-facades", EvalProfile::Anthropic);
    let before = trace.engine_events.len();
    trace.engine_events.retain(|event| {
        !(runtime_json_field_eq(event, "type", "tool.event")
            && runtime_json_field_eq(event, "toolUseId", "edit-calc")
            && runtime_json_pointer_eq(event, "/event/type", "file_change"))
    });
    assert_eq!(trace.engine_events.len() + 1, before);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("file event"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_a_missing_apply_patch_file_event() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-apply-patch", EvalProfile::Anthropic);
    let before = trace.engine_events.len();
    trace.engine_events.retain(|event| {
        !(runtime_json_field_eq(event, "type", "tool.event")
            && runtime_json_field_eq(event, "toolUseId", "p1")
            && runtime_json_pointer_eq(event, "/event/type", "file_change"))
    });
    assert_eq!(trace.engine_events.len() + 1, before);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("file event"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_accepts_raw_patch_alias_with_canonical_persistence() {
    let manifest = load_checked_in_manifest().unwrap();
    let trace = aliased_apply_patch_trace();

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
        .await
        .unwrap();
}

#[tokio::test]
async fn trace_contract_validator_accepts_a_deterministic_apply_patch_failure() {
    let manifest = load_checked_in_manifest().unwrap();
    let patch = "*** Begin Patch\n*** Update File: missing.js\n@@\n-old\n+new\n*** End Patch";
    let trace =
        failed_apply_patch_trace(patch, "apply_patch: cannot update missing file: missing.js");

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
        .await
        .unwrap();
}

#[tokio::test]
async fn trace_contract_validator_replays_partial_state_before_an_apply_patch_failure() {
    let manifest = load_checked_in_manifest().unwrap();
    let patch = "*** Begin Patch\n*** Update File: index.js\n@@\n-export const message = 'old'\n+export const message = 'partial'\n*** Update File: missing.js\n@@\n-old\n+new\n*** End Patch";
    let mut trace =
        failed_apply_patch_trace(patch, "apply_patch: cannot update missing file: missing.js");
    let before = runtime_file(&trace.before_files, "index.js").clone();
    let after = "export const message = 'partial'\n".to_owned();
    trace
        .after_files
        .insert(RuntimeString::from_utf8("index.js"), after.clone().into());
    trace.side_effects.push(exact_json!({
        "type": "file_change",
        "path": "index.js",
        "before": before,
        "after": after,
    }));

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
        .await
        .unwrap();
}

#[tokio::test]
async fn trace_contract_validator_accepts_a_deterministic_apply_patch_parse_failure() {
    let manifest = load_checked_in_manifest().unwrap();
    let trace = failed_apply_patch_trace(
        "not a patch",
        "The first line of the patch must be '*** Begin Patch'",
    );

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
        .await
        .unwrap();
}

#[tokio::test]
async fn trace_contract_validator_accepts_an_unread_existing_write_failure() {
    let manifest = load_checked_in_manifest().unwrap();
    let trace = unread_existing_write_trace();

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
        .await
        .unwrap();
}

#[tokio::test]
async fn trace_contract_validator_fails_closed_after_a_net_zero_partial_patch_failure() {
    let manifest = load_checked_in_manifest().unwrap();
    let trace = net_zero_failed_patch_then_write_trace();

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(
        error.to_string().contains("freshness became uncertain"),
        "{error}"
    );
}

#[tokio::test]
async fn trace_contract_validator_rejects_a_patch_file_event_path_alias() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = aliased_apply_patch_trace();
    let event = trace
        .engine_events
        .iter_mut()
        .find(|event| {
            runtime_json_field_eq(event, "type", "tool.event")
                && runtime_json_field_eq(event, "toolUseId", "p1")
                && runtime_json_pointer_eq(event, "/event/type", "file_change")
        })
        .unwrap();
    event["event"]["change"]["path"] = Json::String("$WORKSPACE/./index.js".to_owned().into());

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("file event"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_binds_write_and_edit_terminal_paths_to_replay_paths() {
    let manifest = load_checked_in_manifest().unwrap();
    for (task, id, forged_path) in [
        ("add-function", "w1", "$WORKSPACE/./utils.js"),
        (
            "provider-edit-search-shell-facades",
            "edit-calc",
            "$WORKSPACE/dir/../calc.js",
        ),
    ] {
        let mut trace = contract_trace(task, EvalProfile::Anthropic);
        trace
            .engine_events
            .iter_mut()
            .find(|event| {
                runtime_json_field_eq(event, "type", "item.completed")
                    && runtime_json_pointer_eq(event, "/item/id", id)
            })
            .unwrap()["item"]["result"]["path"] = Json::String(forged_path.to_owned().into());
        trace
            .side_effects
            .iter_mut()
            .find(|effect| runtime_json_field_eq(effect, "id", id))
            .unwrap()["result"]["path"] = Json::String(forged_path.to_owned().into());

        let error = validate_provider_behavior_trace_contract(
            &manifest,
            &trace,
            TraceContractMode::Scripted,
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("tool terminal result"),
            "{task}: {error}"
        );
    }
}

#[tokio::test]
async fn trace_contract_validator_rejects_a_file_event_from_a_failed_edit() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("tool-failure-recovery", EvalProfile::Anthropic);
    let terminal = trace
        .engine_events
        .iter()
        .position(|event| {
            runtime_json_field_eq(event, "type", "item.updated")
                && runtime_json_pointer_eq(event, "/item/id", "e1")
                && runtime_json_pointer_eq(event, "/item/status", "failed")
        })
        .unwrap();
    trace.engine_events.insert(
        terminal,
        exact_json!({
            "type": "tool.event",
            "toolUseId": "e1",
            "event": {
                "type": "file_change",
                "change": {
                    "path": "$WORKSPACE/calc.js",
                    "kind": "modify",
                },
            },
        }),
    );

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("file event"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_a_file_event_from_a_hook_blocked_edit() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("tool-result-envelope", EvalProfile::Anthropic);
    let terminal = trace
        .engine_events
        .iter()
        .position(|event| {
            runtime_json_field_eq(event, "type", "item.updated")
                && runtime_json_pointer_eq(event, "/item/id", "e1")
                && runtime_json_pointer_eq(event, "/item/status", "failed")
        })
        .unwrap();
    trace.engine_events.insert(
        terminal,
        exact_json!({
            "type": "tool.event",
            "toolUseId": "e1",
            "event": {
                "type": "file_change",
                "change": {
                    "path": "$WORKSPACE/env.txt",
                    "kind": "modify",
                },
            },
        }),
    );

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("file event"), "{error}");
}

#[tokio::test]
async fn trace_contract_rejects_second_edit_claiming_block_first_hook_failure() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("tool-result-envelope", EvalProfile::Anthropic);

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
        .await
        .unwrap();

    let message = "Eval fixture forced the first edit to fail. Retry with the current file text.";
    let blocked_content = trace
        .history
        .iter()
        .find(|message| runtime_json_field_eq(message, "toolUseId", "e1"))
        .unwrap()["content"]
        .clone();
    let rewrite_result = |messages: &mut Vec<Json>| {
        for result in messages.iter_mut().filter(|message| {
            runtime_json_field_eq(message, "type", "tool_result")
                && runtime_json_field_eq(message, "toolUseId", "e2")
        }) {
            result["content"] = blocked_content.clone();
            result["isError"] = Json::Bool(true);
        }
    };
    rewrite_result(&mut trace.history);
    for request in &mut trace.model_requests {
        rewrite_result(request["messages"].as_array_mut().unwrap());
    }

    let terminal = trace
        .engine_events
        .iter_mut()
        .find(|event| {
            runtime_json_field_eq(event, "type", "item.completed")
                && runtime_json_pointer_eq(event, "/item/id", "e2")
        })
        .unwrap();
    terminal["type"] = Json::String("item.updated".to_owned().into());
    terminal["item"]["status"] = Json::String("failed".to_owned().into());
    terminal["item"].as_object_mut().unwrap().remove("result");
    terminal["item"]["error"] = exact_json!({ "message": message });
    *terminal = canonical_engine_event(terminal);
    trace.engine_events.retain(|event| {
        !(runtime_json_field_eq(event, "type", "tool.event")
            && runtime_json_field_eq(event, "toolUseId", "e2")
            && runtime_json_pointer_eq(event, "/event/type", "file_change"))
    });

    let effect = trace
        .side_effects
        .iter_mut()
        .find(|effect| runtime_json_field_eq(effect, "id", "e2"))
        .unwrap();
    effect["status"] = Json::String("failed".to_owned().into());
    effect.as_object_mut().unwrap().remove("result");
    effect["error"] = exact_json!({ "message": message });
    trace
        .side_effects
        .retain(|effect| !runtime_json_field_eq(effect, "type", "file_change"));
    trace.after_files = trace.before_files.clone();
    trace.result.score_success = false;
    trace.result.success = false;

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("block-first hook"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_synchronized_before_file_and_side_effect_forgery() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("add-function", EvalProfile::Anthropic);
    let after = runtime_file(&trace.after_files, "utils.js").clone();
    trace
        .before_files
        .insert(RuntimeString::from_utf8("utils.js"), after);
    trace.side_effects.retain(|effect| {
        !runtime_json_field_eq(effect, "type", "file_change")
            || !runtime_json_field_eq(effect, "path", "utils.js")
    });

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("beforeFiles"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_noncanonical_persisted_file_aliases() {
    let manifest = load_checked_in_manifest().unwrap();
    for alias in [
        "./utils.js",
        "dir//utils.js",
        "dir\\utils.js",
        "dir/../utils.js",
        "/utils.js",
        "utils.js/",
    ] {
        let mut trace = contract_trace("add-function", EvalProfile::Anthropic);
        let content = trace
            .after_files
            .remove(&RuntimeString::from_utf8("utils.js"))
            .unwrap();
        trace
            .after_files
            .insert(RuntimeString::from_utf8(alias), content);
        trace
            .side_effects
            .iter_mut()
            .find(|effect| runtime_json_field_eq(effect, "type", "file_change"))
            .unwrap()["path"] = Json::String(alias.to_owned().into());

        let error = validate_provider_behavior_trace_contract(
            &manifest,
            &trace,
            TraceContractMode::Scripted,
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("canonical relative POSIX"),
            "{alias:?}: {error}"
        );
    }
}

#[tokio::test]
async fn trace_contract_validator_rejects_noncanonical_before_file_keys() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-apply-patch", EvalProfile::Anthropic);
    let content = trace
        .before_files
        .remove(&RuntimeString::from_utf8("index.js"))
        .unwrap();
    trace
        .before_files
        .insert(RuntimeString::from_utf8("./index.js"), content);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(
        error.to_string().contains("canonical relative POSIX"),
        "{error}"
    );
}

#[tokio::test]
async fn trace_contract_validator_rebuilds_visible_calls_from_history() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("add-function", EvalProfile::Anthropic);
    trace.model_visible_tool_uses[0]["name"] = Json::String("forged_write".to_owned().into());

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(
        error.to_string().contains("modelVisibleToolUses"),
        "{error}"
    );
}

#[tokio::test]
async fn trace_contract_validator_rebuilds_normalized_internal_calls() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("add-function", EvalProfile::Anthropic);
    trace.normalized_internal_calls[0]["internalName"] =
        Json::String("forged_write".to_owned().into());

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(
        error.to_string().contains("normalizedInternalCalls"),
        "{error}"
    );
}

#[tokio::test]
async fn trace_contract_validator_rejects_a_forged_main_thread_id() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    trace.engine_events[0]["threadId"] = Json::String("forged-session".to_owned().into());

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("threadId"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_an_empty_turn_id() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    trace.engine_events[1]["turnId"] = Json::String(RuntimeString::default());

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("turnId"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_a_duplicate_turn_start() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    trace
        .engine_events
        .insert(2, trace.engine_events[1].clone());

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("exactly one top-level turn.started"),
        "{error}"
    );
}

#[tokio::test]
async fn trace_contract_validator_rejects_a_completed_event_with_a_different_turn_id() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    trace
        .engine_events
        .last_mut()
        .unwrap()
        .as_object_mut()
        .unwrap()
        .insert(
            "turnId".to_owned(),
            Json::String("different-turn".to_owned().into()),
        );

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("turnId"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_binds_direct_agent_message_ids_to_history_uuids() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    let completed = trace
        .engine_events
        .iter_mut()
        .find(|event| {
            runtime_json_field_eq(event, "type", "item.completed")
                && runtime_json_pointer_eq(event, "/item/type", "agent_message")
        })
        .unwrap();
    completed["item"]["id"] = Json::String("forged-agent-message".to_owned().into());

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(
        error.to_string().contains("AgentMessage item id"),
        "{error}"
    );
}

#[tokio::test]
async fn trace_contract_validator_requires_nonempty_unique_history_assistant_uuids() {
    let manifest = load_checked_in_manifest().unwrap();
    let trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    let assistant_indices = trace
        .history
        .iter()
        .enumerate()
        .filter_map(|(index, message)| {
            (runtime_json_field_eq(message, "type", "assistant")).then_some(index)
        })
        .collect::<Vec<_>>();
    assert_eq!(assistant_indices.len(), 2);

    let mut empty = trace.clone();
    let old_uuid = runtime_scalar(
        empty.history[assistant_indices[0]]["uuid"]
            .as_str()
            .unwrap(),
        "assistant uuid",
    )
    .unwrap();
    empty.history[assistant_indices[0]]["uuid"] = Json::String(RuntimeString::default());
    for request in &mut empty.model_requests {
        for message in request["messages"].as_array_mut().unwrap() {
            if runtime_json_field_eq(message, "uuid", old_uuid.as_str()) {
                message["uuid"] = Json::String(RuntimeString::default());
            }
        }
    }
    let empty_error =
        validate_provider_behavior_trace_contract(&manifest, &empty, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(
        empty_error.to_string().contains("history Assistant uuid"),
        "{empty_error}"
    );

    let mut duplicate = trace;
    let duplicate_uuid = duplicate.history[assistant_indices[0]]["uuid"].clone();
    duplicate.history[assistant_indices[1]]["uuid"] = duplicate_uuid;
    let duplicate_error = validate_provider_behavior_trace_contract(
        &manifest,
        &duplicate,
        TraceContractMode::Scripted,
    )
    .await
    .unwrap_err();
    assert!(
        duplicate_error
            .to_string()
            .contains("history Assistant uuid"),
        "{duplicate_error}"
    );
}

#[tokio::test]
async fn trace_contract_validator_requires_nonempty_unique_direct_agent_message_ids() {
    let manifest = load_checked_in_manifest().unwrap();
    let trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    let completed_indices = trace
        .engine_events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| {
            (runtime_json_field_eq(event, "type", "item.completed")
                && runtime_json_pointer_eq(event, "/item/type", "agent_message"))
            .then_some(index)
        })
        .collect::<Vec<_>>();
    assert_eq!(completed_indices.len(), 2);

    let mut empty = trace.clone();
    empty.engine_events[completed_indices[0]]["item"]["id"] =
        Json::String(RuntimeString::default());
    let empty_error =
        validate_provider_behavior_trace_contract(&manifest, &empty, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(
        empty_error.to_string().contains("AgentMessage item id"),
        "{empty_error}"
    );

    let mut duplicate = trace;
    let duplicate_id = duplicate.engine_events[completed_indices[0]]["item"]["id"].clone();
    duplicate.engine_events[completed_indices[1]]["item"]["id"] = duplicate_id;
    let duplicate_error = validate_provider_behavior_trace_contract(
        &manifest,
        &duplicate,
        TraceContractMode::Scripted,
    )
    .await
    .unwrap_err();
    assert!(
        duplicate_error.to_string().contains("AgentMessage item id"),
        "{duplicate_error}"
    );
}

#[tokio::test]
async fn live_trace_contract_requires_every_tool_event_to_own_the_current_turn() {
    let manifest = load_checked_in_manifest().unwrap();
    let trace = contract_trace("command-output-envelope", EvalProfile::Anthropic);
    let turn_id = trace.engine_events[1]["turnId"]
        .as_str()
        .unwrap()
        .to_owned();
    let tool_event_index = trace
        .engine_events
        .iter()
        .position(|event| runtime_json_field_eq(event, "type", "tool.event"))
        .unwrap();

    let missing_error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
            .await
            .unwrap_err();
    assert!(
        missing_error.to_string().contains("tool.event turnId"),
        "{missing_error}"
    );

    let mut wrong = trace;
    wrong.engine_events[tool_event_index]
        .as_object_mut()
        .unwrap()
        .insert(
            "turnId".to_owned(),
            Json::String(turn_id.concat(&RuntimeString::from_utf8("-forged"))),
        );
    let wrong_error =
        validate_provider_behavior_trace_contract(&manifest, &wrong, TraceContractMode::Live)
            .await
            .unwrap_err();
    assert!(
        wrong_error.to_string().contains("tool.event turnId"),
        "{wrong_error}"
    );
}

#[tokio::test]
async fn scripted_trace_contract_requires_persisted_tool_events_to_omit_turn_id() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("add-function", EvalProfile::Anthropic);
    let turn_id = trace.engine_events[1]["turnId"].clone();
    let event = trace
        .engine_events
        .iter_mut()
        .find(|event| runtime_json_field_eq(event, "type", "tool.event"))
        .unwrap();
    event
        .as_object_mut()
        .unwrap()
        .insert("turnId".to_owned(), turn_id);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("scripted tool.event must omit turnId"),
        "{error}"
    );
}

#[tokio::test]
async fn trace_contract_validator_requires_completed_stop_for_a_successful_result() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    trace.engine_events.last_mut().unwrap()["stopReason"] =
        Json::String("aborted".to_owned().into());

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("stopReason"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_recomputes_aborted_with_passing_rules_as_unsuccessful() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    trace.engine_events.last_mut().unwrap()["stopReason"] =
        Json::String("aborted".to_owned().into());
    trace.result.success = false;
    assert!(trace.result.score_success && trace.result.behavior_success);

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
        .await
        .unwrap();
}

#[tokio::test]
async fn trace_contract_validator_accepts_a_legal_noncompleted_stop_for_a_failed_score() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    rewrite_final_agent_text(&mut trace, "NO_SCORE_MATCH");
    trace.result.score_success = false;
    trace.result.success = false;
    trace.engine_events.last_mut().unwrap()["stopReason"] =
        Json::String("aborted".to_owned().into());

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
        .await
        .unwrap();
}

#[tokio::test]
async fn trace_contract_validator_rejects_an_unknown_terminal_reason() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    trace.engine_events.last_mut().unwrap()["stopReason"] =
        Json::String("not_a_terminal_reason".to_owned().into());

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("engineEvents"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_non_manifest_task_body() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    trace.task.description = trace
        .task
        .description
        .concat(&RuntimeString::from_utf8(" forged"));

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("task body"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_recomputes_score_instead_of_trusting_true_booleans() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    rewrite_final_agent_text(&mut trace, "forged success");
    assert!(trace.result.success && trace.result.score_success && trace.result.behavior_success);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("scoreSuccess"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rebuilds_final_text_from_history_and_events() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("add-function", EvalProfile::Anthropic);
    trace.final_text = "forged final text".into();
    assert!(trace.result.success && trace.result.score_success && trace.result.behavior_success);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("finalText"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_a_schema_valid_untrusted_request_tool() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    trace.model_requests[0]["tools"]
        .as_array_mut()
        .unwrap()
        .push(exact_json!({
            "name": "forged_tool",
            "description": "A schema-valid tool that is absent from the trusted baseline trace.",
            "inputSchema": {
                "type": "object",
                "properties": {}
            }
        }));

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("request tool"), "{error}");
}

fn rename_hidden_code_mode_call(
    trace: &ProviderBehaviorTrace,
    id: &RuntimeString,
    replacement: &str,
) -> ProviderBehaviorTrace {
    let mut renamed = trace.clone();
    for event in &mut renamed.engine_events {
        for pointer in ["/item/id", "/toolUseId"] {
            if event.pointer(pointer).and_then(Json::as_str) == Some(id) {
                *event.pointer_mut(pointer).unwrap() = Json::from(replacement);
            }
        }
    }
    for effect in &mut renamed.side_effects {
        if effect.get("id").and_then(Json::as_str) == Some(id) {
            effect["id"] = Json::from(replacement);
        }
    }
    renamed
}

#[tokio::test]
async fn trace_contract_validator_rebuilds_all_adapted_code_mode_request_catalogs() {
    let manifest = load_checked_in_manifest().unwrap();
    let profiles = [
        EvalProfile::Anthropic,
        EvalProfile::Openai,
        EvalProfile::Generic,
    ];
    let traces = run_scripted_task_tolerant(
        validation_process_context(),
        &manifest,
        "code-mode",
        &profiles,
    )
    .await
    .unwrap();
    assert_eq!(traces.len(), profiles.len());
    for trace in &traces {
        let validated = validate_provider_behavior_trace_contract(
            &manifest,
            trace,
            TraceContractMode::Scripted,
        )
        .await
        .unwrap();
        assert!(matches!(
            validated.variant,
            ValidatedTraceContractVariant::RustScriptedAdaptation { .. }
        ));
    }

    let hidden_ids = traces[0]
        .engine_events
        .iter()
        .filter(|event| {
            event.get("type").and_then(Json::as_str) == Some(&RuntimeString::from("item.started"))
                && event.pointer("/item/type").and_then(Json::as_str)
                    == Some(&RuntimeString::from("tool_call"))
        })
        .filter_map(|event| event.pointer("/item/id").and_then(Json::as_str))
        .filter(|id| *id != "c1")
        .cloned()
        .collect::<Vec<_>>();
    let write_id = hidden_ids.last().unwrap();
    let renamed = rename_hidden_code_mode_call(&traces[0], write_id, "opaque-generated-write");
    let validated =
        validate_provider_behavior_trace_contract(&manifest, &renamed, TraceContractMode::Scripted)
            .await
            .unwrap();
    assert!(matches!(
        validated.variant,
        ValidatedTraceContractVariant::RustScriptedAdaptation { .. }
    ));

    let aliased =
        rename_hidden_code_mode_call(&traces[0], write_id, &hidden_ids[0].try_to_utf8().unwrap());
    let error =
        validate_provider_behavior_trace_contract(&manifest, &aliased, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    let duplicate_terminal = format!(
        "direct tool {} must terminate exactly once before the next request start, got 2",
        hidden_ids[0].try_to_utf8().unwrap()
    );
    assert!(error.to_string().contains(&duplicate_terminal), "{error}");

    let mut crossed = traces[0].clone();
    let terminal = crossed
        .engine_events
        .iter_mut()
        .find(|event| {
            event.get("type").and_then(Json::as_str) == Some(&RuntimeString::from("item.completed"))
                && event.pointer("/item/id").and_then(Json::as_str) == Some(write_id)
        })
        .unwrap();
    *terminal.pointer_mut("/item/id").unwrap() = Json::String(hidden_ids[0].clone());
    let error =
        validate_provider_behavior_trace_contract(&manifest, &crossed, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains(&duplicate_terminal), "{error}");

    let mut wrong_owner = traces[0].clone();
    let file_event = wrong_owner
        .engine_events
        .iter_mut()
        .find(|event| event.pointer("/event/change/diff").is_some())
        .unwrap();
    file_event["toolUseId"] = Json::String(hidden_ids[0].clone());
    let error = validate_provider_behavior_trace_contract(
        &manifest,
        &wrong_owner,
        TraceContractMode::Scripted,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("is not active"), "{error}");

    let mut wrong_effect = traces[0].clone();
    let effect = wrong_effect
        .side_effects
        .iter_mut()
        .find(|effect| effect.get("id").and_then(Json::as_str) == Some(write_id))
        .unwrap();
    effect["id"] = Json::String(hidden_ids[0].clone());
    let error = validate_provider_behavior_trace_contract(
        &manifest,
        &wrong_effect,
        TraceContractMode::Scripted,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("sideEffects differ"), "{error}");

    let mut forged_diff = traces[0].clone();
    let changed_event = forged_diff
        .engine_events
        .iter_mut()
        .find(|event| event.pointer("/event/change/diff").is_some())
        .expect("the real hidden write must emit its complete file contents");
    *changed_event.pointer_mut("/event/change/diff").unwrap() = exact_json!("forged file contents");
    let error = validate_provider_behavior_trace_contract(
        &manifest,
        &forged_diff,
        TraceContractMode::Scripted,
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("hidden lifecycle or sideEffects"),
        "{error}"
    );

    let mut wrong_inference = traces[0].clone();
    let hidden_read = wrong_inference
        .engine_events
        .iter_mut()
        .find(|event| {
            event.pointer("/item/toolName").and_then(Json::as_str)
                == Some(&RuntimeString::from("read"))
                && event.pointer("/inference/revision").is_some()
        })
        .expect("the real hidden read retains its captured inference");
    *hidden_read.pointer_mut("/inference/revision").unwrap() = exact_json!(1);
    let error = validate_provider_behavior_trace_contract(
        &manifest,
        &wrong_inference,
        TraceContractMode::Scripted,
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("hidden lifecycle or sideEffects"),
        "{error}"
    );

    let mut forged = traces[0].clone();
    forged.model_requests[0]["tools"]
        .as_array_mut()
        .unwrap()
        .push(exact_json!({
            "name": "forged_tool",
            "description": "A schema-valid tool absent from the adapted runtime catalog.",
            "inputSchema": {
                "type": "object",
                "properties": {}
            }
        }));
    let error =
        validate_provider_behavior_trace_contract(&manifest, &forged, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("request tool"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_accepts_all_three_hash_bound_source_code_mode_rows() {
    let manifest = load_checked_in_manifest().unwrap();
    for profile in [
        EvalProfile::Anthropic,
        EvalProfile::Openai,
        EvalProfile::Generic,
    ] {
        let trace = contract_trace("code-mode", profile);
        let validated = validate_provider_behavior_trace_contract(
            &manifest,
            &trace,
            TraceContractMode::Scripted,
        )
        .await
        .unwrap();
        assert_eq!(validated.variant, ValidatedTraceContractVariant::Source);
        let write_id = trace
            .engine_events
            .iter()
            .find(|event| {
                event.get("type").and_then(Json::as_str)
                    == Some(&RuntimeString::from("item.started"))
                    && event.pointer("/item/toolName").and_then(Json::as_str)
                        == Some(&RuntimeString::from("write"))
            })
            .unwrap()
            .pointer("/item/id")
            .and_then(Json::as_str)
            .unwrap();
        let renamed = rename_hidden_code_mode_call(&trace, write_id, "opaque-generated-write");
        let error = validate_provider_behavior_trace_contract(
            &manifest,
            &renamed,
            TraceContractMode::Scripted,
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("hidden lifecycle or sideEffects"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn trace_contract_validator_rejects_a_forged_hidden_code_mode_lifecycle() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("code-mode", EvalProfile::Anthropic);
    let start = trace
        .engine_events
        .iter()
        .find(|event| {
            runtime_json_field_eq(event, "type", "item.started")
                && runtime_json_pointer_eq(event, "/item/toolName", "read")
        })
        .unwrap()
        .clone();
    let completed = trace
        .engine_events
        .iter()
        .find(|event| {
            runtime_json_field_eq(event, "type", "item.completed")
                && runtime_json_pointer_eq(event, "/item/toolName", "read")
        })
        .unwrap()
        .clone();
    let mut forged_start = start;
    forged_start["item"]["id"] = Json::String("forged-hidden".to_owned().into());
    let mut forged_completed = completed;
    forged_completed["item"]["id"] = Json::String("forged-hidden".to_owned().into());
    let outer_terminal = trace
        .engine_events
        .iter()
        .position(|event| {
            runtime_json_field_eq(event, "type", "item.completed")
                && runtime_json_pointer_eq(event, "/item/id", "c1")
        })
        .unwrap();
    trace.engine_events.splice(
        outer_terminal..outer_terminal,
        [forged_start, forged_completed],
    );
    let mut forged_effect = trace
        .side_effects
        .iter()
        .find(|effect| runtime_json_field_eq(effect, "name", "read"))
        .unwrap()
        .clone();
    forged_effect["id"] = Json::String("forged-hidden".to_owned().into());
    let file_change = trace
        .side_effects
        .iter()
        .position(|effect| runtime_json_field_eq(effect, "type", "file_change"))
        .unwrap();
    trace.side_effects.insert(file_change, forged_effect);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("hidden lifecycle"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_hidden_code_mode_terminal_after_outer_terminal() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("code-mode", EvalProfile::Anthropic);
    let hidden_index = trace
        .engine_events
        .iter()
        .position(|event| {
            runtime_json_field_eq(event, "type", "item.completed")
                && runtime_json_pointer_eq(event, "/item/id", "fixture-000010")
        })
        .unwrap();
    let hidden_terminal = trace.engine_events.remove(hidden_index);
    let outer_index = trace
        .engine_events
        .iter()
        .position(|event| {
            runtime_json_field_eq(event, "type", "item.completed")
                && runtime_json_pointer_eq(event, "/item/id", "c1")
        })
        .unwrap();
    trace.engine_events.insert(outer_index + 1, hidden_terminal);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("outer tool window"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_deleting_a_hidden_code_mode_tool_event() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("code-mode", EvalProfile::Anthropic);
    let before = trace.engine_events.len();
    trace.engine_events.retain(|event| {
        !(runtime_json_field_eq(event, "type", "tool.event")
            && runtime_json_field_eq(event, "toolUseId", "fixture-000010"))
    });
    assert_eq!(trace.engine_events.len() + 1, before);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("hidden lifecycle"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_a_forged_outer_code_mode_tool_event() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("code-mode", EvalProfile::Anthropic);
    let outer_start = trace
        .engine_events
        .iter()
        .position(|event| {
            runtime_json_field_eq(event, "type", "item.started")
                && runtime_json_pointer_eq(event, "/item/id", "c1")
        })
        .unwrap();
    trace.engine_events.insert(
        outer_start + 1,
        exact_json!({
            "type": "tool.event",
            "toolUseId": "c1",
            "event": { "type": "stdout", "chunk": "forged" }
        }),
    );

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("outer c1 tool.event"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_a_duplicate_schema_valid_turn_completion() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    let completed = trace.engine_events.last().unwrap().clone();
    assert!(runtime_json_field_eq(&completed, "type", "turn.completed"));
    trace.engine_events.push(completed);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("turn.completed"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_runs_command_score_against_materialized_after_files() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-edit-search-shell-facades", EvalProfile::Anthropic);
    let broken =
        runtime_file(&trace.after_files, "calc.js").replace("return a * b", "return a - b");
    let visible_input = exact_json!({
        "file_path": "calc.js",
        "old_string": "  return a + b",
        "new_string": "  return a - b",
    });
    let internal_input = exact_json!({
        "path": "calc.js",
        "oldString": "  return a + b",
        "newString": "  return a - b",
    });
    rewrite_trace_tool_call(
        &mut trace,
        "edit-calc",
        &visible_input,
        &internal_input,
        &exact_json!([{
            "type": "text",
            "text": "Edited calc.js (1 replacement)",
        }]),
        &exact_json!({
            "path": "$WORKSPACE/calc.js",
            "replacements": 1,
        }),
    );
    trace
        .after_files
        .insert(RuntimeString::from_utf8("calc.js"), broken.clone());
    trace
        .side_effects
        .iter_mut()
        .find(|effect| {
            runtime_json_field_eq(effect, "type", "file_change")
                && runtime_json_field_eq(effect, "path", "calc.js")
        })
        .unwrap()["after"] = Json::String(broken);
    add_live_tool_event_turn_ids(&mut trace);
    assert!(trace.result.success && trace.result.score_success && trace.result.behavior_success);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("scoreSuccess"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_recomputes_behavior_instead_of_trusting_true_booleans() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-parallel-subagents", EvalProfile::Anthropic);
    rewrite_task_call_as_failure(&mut trace, "t1", "child failed");
    assert!(trace.result.success && trace.result.score_success && trace.result.behavior_success);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("behaviorFailures"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_non_roundtrippable_model_request_shape() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    trace.model_requests[0]
        .as_object_mut()
        .unwrap()
        .insert("unknownContractField".to_owned(), Json::Bool(true));

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("modelRequests"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_schema_valid_request_model_and_budget_drift() {
    let manifest = load_checked_in_manifest().unwrap();
    let trace = contract_trace("read-summarize", EvalProfile::Anthropic);

    let mut forged_model = trace.clone();
    forged_model.model_requests[0]["model"] = Json::String("forged-model".to_owned().into());
    let error = validate_provider_behavior_trace_contract(
        &manifest,
        &forged_model,
        TraceContractMode::Scripted,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("model"), "{error}");

    let mut forged_budget = trace;
    let current = forged_budget.model_requests[0]["maxOutputTokens"]
        .as_u64()
        .unwrap();
    forged_budget.model_requests[0]["maxOutputTokens"] = Json::from(current + 1);
    let error = validate_provider_behavior_trace_contract(
        &manifest,
        &forged_budget,
        TraceContractMode::Scripted,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("request metadata"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_unknown_and_explicit_null_history_fields() {
    let manifest = load_checked_in_manifest().unwrap();
    let trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    for (field, value) in [
        ("unknownContractField", Json::Bool(true)),
        ("isMeta", Json::Null),
    ] {
        let mut forged = trace.clone();
        forged.history[0]
            .as_object_mut()
            .unwrap()
            .insert(field.to_owned(), value);
        let error = validate_provider_behavior_trace_contract(
            &manifest,
            &forged,
            TraceContractMode::Scripted,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("history"), "{error}");
    }
}

#[tokio::test]
async fn trace_contract_validator_rejects_an_empty_main_request_history_prefix() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("prompt-tag-following", EvalProfile::Anthropic);
    trace.model_requests[0]["messages"] = Json::Array(Vec::new());

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("history-prefix"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_accepts_a_tool_only_assistant_without_agent_message() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    make_first_assistant_tool_only(&mut trace);

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
        .await
        .unwrap();
}

#[tokio::test]
async fn trace_contract_validator_concatenates_multiple_assistant_text_blocks_into_one_item() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    let first_assistant = trace
        .history
        .iter()
        .find(|message| runtime_json_field_eq(message, "type", "assistant"))
        .unwrap();
    let uuid = first_assistant["uuid"].as_str().unwrap().to_owned();
    let tool_use = first_assistant["content"]
        .as_array()
        .unwrap()
        .iter()
        .find(|block| runtime_json_field_eq(block, "type", "tool_use"))
        .unwrap()
        .clone();
    replace_assistant_content(
        &mut trace,
        &uuid,
        exact_json!([
            {"type":"text","text":"Let me "},
            tool_use,
            {"type":"text","text":"read it."}
        ]),
    );

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
        .await
        .unwrap();
}

#[tokio::test]
async fn trace_contract_validator_rejects_text_before_the_first_request_segment() {
    let manifest = load_checked_in_manifest().unwrap();
    let base = contract_trace("read-summarize", EvalProfile::Anthropic);
    let first_start = base
        .engine_events
        .iter()
        .position(|event| runtime_json_field_eq(event, "type", "stream.request_start"))
        .unwrap();

    let mut prelude_delta = base.clone();
    prelude_delta.engine_events.insert(
        first_start,
        exact_json!({"type":"text.delta","text":"forged prelude"}),
    );
    let delta_error = validate_provider_behavior_trace_contract(
        &manifest,
        &prelude_delta,
        TraceContractMode::Scripted,
    )
    .await
    .unwrap_err();
    assert!(delta_error
        .to_string()
        .contains("first stream.request_start"));
}

#[tokio::test]
async fn trace_contract_validator_rejects_agent_items_before_the_first_request_segment() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut prelude_item = contract_trace("read-summarize", EvalProfile::Anthropic);
    let uuid = make_first_assistant_tool_only(&mut prelude_item);
    let first_start = prelude_item
        .engine_events
        .iter()
        .position(|event| runtime_json_field_eq(event, "type", "stream.request_start"))
        .unwrap();
    prelude_item.engine_events.insert(
        first_start,
        exact_json!({
            "type":"item.completed",
            "item":{"type":"agent_message","id":uuid,"text":""}
        }),
    );
    let item_error = validate_provider_behavior_trace_contract(
        &manifest,
        &prelude_item,
        TraceContractMode::Scripted,
    )
    .await
    .unwrap_err();
    assert!(item_error
        .to_string()
        .contains("first stream.request_start"));
}

#[tokio::test]
async fn trace_contract_validator_rejects_missing_or_empty_extra_segment_agent_messages() {
    let manifest = load_checked_in_manifest().unwrap();
    let base = contract_trace("read-summarize", EvalProfile::Anthropic);
    let first_uuid = runtime_scalar(
        base.history
            .iter()
            .find(|message| runtime_json_field_eq(message, "type", "assistant"))
            .unwrap()["uuid"]
            .as_str()
            .unwrap(),
        "assistant uuid",
    )
    .unwrap();

    let mut missing = base.clone();
    missing.engine_events.retain(|event| {
        !(runtime_json_field_eq(event, "type", "item.completed")
            && runtime_json_pointer_eq(event, "/item/type", "agent_message")
            && runtime_json_pointer_eq(event, "/item/id", first_uuid.as_str()))
    });
    let missing_error =
        validate_provider_behavior_trace_contract(&manifest, &missing, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(missing_error.to_string().contains("AgentMessage"));

    let mut empty_extra = base;
    let uuid = make_first_assistant_tool_only(&mut empty_extra);
    let first_start = empty_extra
        .engine_events
        .iter()
        .position(|event| runtime_json_field_eq(event, "type", "stream.request_start"))
        .unwrap();
    empty_extra.engine_events.insert(
        first_start + 1,
        exact_json!({
            "type":"item.completed",
            "item":{"type":"agent_message","id":uuid,"text":""}
        }),
    );
    let extra_error = validate_provider_behavior_trace_contract(
        &manifest,
        &empty_extra,
        TraceContractMode::Scripted,
    )
    .await
    .unwrap_err();
    assert!(extra_error.to_string().contains("AgentMessage"));
}

#[tokio::test]
async fn trace_contract_validator_rejects_an_extra_request_start_without_an_assistant() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    let terminal = trace.engine_events.len() - 1;
    trace
        .engine_events
        .insert(terminal, exact_json!({"type":"stream.request_start"}));

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(
        error.to_string().contains("stream.request_start"),
        "{error}"
    );
}

#[tokio::test]
async fn trace_contract_validator_rebuilds_stream_text_from_history_and_completed_items() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("add-function", EvalProfile::Anthropic);
    let delta = trace
        .engine_events
        .iter_mut()
        .find(|event| runtime_json_field_eq(event, "type", "text.delta"))
        .unwrap();
    delta["text"] = Json::String("forged delta".to_owned().into());

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("stream text"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_delta_moved_past_its_completed_agent_message() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("add-function", EvalProfile::Anthropic);
    let delta_index = trace
        .engine_events
        .iter()
        .position(|event| runtime_json_field_eq(event, "type", "text.delta"))
        .unwrap();
    let completed_index = trace
        .engine_events
        .iter()
        .position(|event| {
            runtime_json_field_eq(event, "type", "item.completed")
                && runtime_json_pointer_eq(event, "/item/type", "agent_message")
        })
        .unwrap();
    assert!(delta_index < completed_index);
    let delta = trace.engine_events.remove(delta_index);
    trace.engine_events.insert(completed_index, delta);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("stream text"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_visible_tool_lifecycle_moved_across_request_segments() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("prompt-tag-following", EvalProfile::Anthropic);
    let mut moved = Vec::new();
    trace.engine_events.retain(|event| {
        if runtime_json_pointer_eq(event, "/item/id", "r1") {
            moved.push(event.clone());
            false
        } else {
            true
        }
    });
    assert_eq!(moved.len(), 2);
    let second_start = trace
        .engine_events
        .iter()
        .enumerate()
        .filter(|(_, event)| runtime_json_field_eq(event, "type", "stream.request_start"))
        .nth(1)
        .unwrap()
        .0;
    trace
        .engine_events
        .splice(second_start + 1..second_start + 1, moved);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("request segment"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_binds_tool_result_error_to_its_unique_terminal() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("add-function", EvalProfile::Anthropic);
    let result = trace
        .history
        .iter_mut()
        .find(|message| {
            runtime_json_field_eq(message, "type", "tool_result")
                && runtime_json_field_eq(message, "toolUseId", "w1")
        })
        .unwrap();
    result["isError"] = Json::Bool(true);
    for request in &mut trace.model_requests {
        let Some(messages) = request.get_mut("messages").and_then(Json::as_array_mut) else {
            continue;
        };
        for message in messages {
            if runtime_json_field_eq(message, "type", "tool_result")
                && runtime_json_field_eq(message, "toolUseId", "w1")
            {
                message["isError"] = Json::Bool(true);
            }
        }
    }

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("isError"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_synchronized_tool_result_content_forgery() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("add-function", EvalProfile::Anthropic);
    let forged = "forged write result";
    let result = trace
        .history
        .iter_mut()
        .find(|message| {
            runtime_json_field_eq(message, "type", "tool_result")
                && runtime_json_field_eq(message, "toolUseId", "w1")
        })
        .unwrap();
    result["content"][0]["text"] = Json::String(forged.to_owned().into());
    for request in &mut trace.model_requests {
        let Some(messages) = request.get_mut("messages").and_then(Json::as_array_mut) else {
            continue;
        };
        for message in messages {
            if runtime_json_field_eq(message, "type", "tool_result")
                && runtime_json_field_eq(message, "toolUseId", "w1")
            {
                message["content"][0]["text"] = Json::String(forged.to_owned().into());
            }
        }
    }

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("tool_result content"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_synchronized_terminal_and_side_effect_result_forgery() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("add-function", EvalProfile::Anthropic);
    let terminal = trace
        .engine_events
        .iter_mut()
        .find(|event| {
            runtime_json_field_eq(event, "type", "item.completed")
                && runtime_json_pointer_eq(event, "/item/id", "w1")
        })
        .unwrap();
    terminal["item"]["result"]["bytes"] = Json::from(999);
    let effect = trace
        .side_effects
        .iter_mut()
        .find(|effect| {
            runtime_json_field_eq(effect, "type", "tool_call")
                && runtime_json_field_eq(effect, "id", "w1")
        })
        .unwrap();
    effect["result"]["bytes"] = Json::from(999);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(
        error.to_string().contains("tool terminal result"),
        "{error}"
    );
}

#[tokio::test]
async fn live_trace_contract_accepts_same_turn_full_read_then_existing_write() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-plan-patch", EvalProfile::Anthropic);
    let read_visible_input = exact_json!({ "file_path": "status.txt" });
    let read_internal_input = exact_json!({ "path": "status.txt" });
    rewrite_trace_tool_call(
        &mut trace,
        "plan1",
        &read_visible_input,
        &read_internal_input,
        &exact_json!([{
            "type": "text",
            "text": "     1\tstate=old\n     2\t",
        }]),
        &exact_json!({
            "found": true,
            "kind": "text",
            "lines": 2,
            "path": "$WORKSPACE/status.txt",
        }),
    );
    rename_trace_tool_call(&mut trace, "plan1", "Read", "read");
    rewrite_trace_call_as_successful_existing_write(
        &mut trace,
        "patch1",
        "status.txt",
        "state=new\n",
    );
    trace.result.behavior_failures = vec![
        "tool TaskCreate called 0 < 1".to_owned().into(),
        "tool apply_patch called 0 < 1".to_owned().into(),
    ];
    trace.result.behavior_success = false;
    trace.result.success = false;
    add_live_tool_event_turn_ids(&mut trace);
    add_missing_plan_reminder_after_task_create_removal(&mut trace);

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();

    for mutation in 0..7 {
        let mut forged = trace.clone();
        let system = forged.model_requests[1]["system"].as_array_mut().unwrap();
        let index = system
            .iter()
            .position(|part| runtime_json_field_eq(part, "name", "task-plan-reminder"))
            .unwrap();
        match mutation {
            0 => {
                system.remove(index);
            }
            1 => system[index]["text"] = Json::from("forged reminder"),
            2 => system.push(system[index].clone()),
            3 => {
                let mut unknown = system[index].clone();
                unknown["name"] = Json::from("unknown-live-fragment");
                system.push(unknown);
            }
            4 => {
                let base = system
                    .iter_mut()
                    .find(|part| runtime_json_field_eq(part, "name", "base"))
                    .unwrap();
                base["text"] = Json::from("forged live guidance");
            }
            5 => {
                system[index]
                    .as_object_mut()
                    .unwrap()
                    .insert("cacheable", Json::Bool(true));
            }
            6 => {
                system[index]
                    .as_object_mut()
                    .unwrap()
                    .insert("cacheScope", Json::from("global"));
            }
            _ => unreachable!(),
        }
        let error =
            validate_provider_behavior_trace_contract(&manifest, &forged, TraceContractMode::Live)
                .await
                .unwrap_err();
        assert!(
            error.to_string().contains("request metadata"),
            "{mutation}: {error}"
        );
    }
}

#[tokio::test]
async fn live_trace_contract_accepts_same_turn_full_read_then_existing_edit() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-plan-patch", EvalProfile::Anthropic);
    rewrite_trace_tool_call(
        &mut trace,
        "plan1",
        &exact_json!({ "file_path": "status.txt" }),
        &exact_json!({ "path": "status.txt" }),
        &exact_json!([{
            "type": "text",
            "text": "     1\tstate=old\n     2\t",
        }]),
        &exact_json!({
            "found": true,
            "kind": "text",
            "lines": 2,
            "path": "$WORKSPACE/status.txt",
        }),
    );
    rename_trace_tool_call(&mut trace, "plan1", "Read", "read");
    rewrite_trace_tool_call(
        &mut trace,
        "patch1",
        &exact_json!({
            "file_path": "status.txt",
            "old_string": "state=old",
            "new_string": "state=new",
        }),
        &exact_json!({
            "path": "status.txt",
            "oldString": "state=old",
            "newString": "state=new",
        }),
        &exact_json!([{
            "type": "text",
            "text": "Edited status.txt (1 replacement)",
        }]),
        &exact_json!({
            "path": "$WORKSPACE/status.txt",
            "replacements": 1,
        }),
    );
    rename_trace_tool_call(&mut trace, "patch1", "Edit", "edit");
    trace.result.behavior_failures = vec![
        "tool TaskCreate called 0 < 1".to_owned().into(),
        "tool apply_patch called 0 < 1".to_owned().into(),
    ];
    trace.result.behavior_success = false;
    trace.result.success = false;
    add_live_tool_event_turn_ids(&mut trace);
    add_missing_plan_reminder_after_task_create_removal(&mut trace);

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();
}

#[tokio::test]
async fn live_trace_contract_accepts_later_prestarted_read_before_earlier_write() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-plan-patch", EvalProfile::Anthropic);
    rewrite_trace_call_as_successful_existing_write(
        &mut trace,
        "plan1",
        "status.txt",
        "state=new\n",
    );
    rewrite_trace_tool_call(
        &mut trace,
        "patch1",
        &exact_json!({ "file_path": "status.txt" }),
        &exact_json!({ "path": "status.txt" }),
        &exact_json!([{
            "type": "text",
            "text": "     1\tstate=old\n     2\t",
        }]),
        &exact_json!({
            "found": true,
            "kind": "text",
            "lines": 2,
            "path": "$WORKSPACE/status.txt",
        }),
    );
    rename_trace_tool_call(&mut trace, "patch1", "Read", "read");

    for event in &mut trace.engine_events {
        if runtime_json_field_eq(event, "type", "tool.event")
            && runtime_json_field_eq(event, "toolUseId", "patch1")
        {
            event["toolUseId"] = Json::String("plan1".to_owned().into());
        }
    }
    let read_start = trace.engine_events[8].clone();
    let read_terminal = trace.engine_events[10].clone();
    let write_start = trace.engine_events[6].clone();
    let write_file_event = trace.engine_events[9].clone();
    let write_terminal = trace.engine_events[7].clone();
    trace.engine_events.splice(
        6..11,
        [
            read_start,
            read_terminal,
            write_start,
            write_file_event,
            write_terminal,
        ],
    );
    let read_effect = trace
        .side_effects
        .iter()
        .find(|effect| runtime_json_field_eq(effect, "id", "patch1"))
        .unwrap()
        .clone();
    let write_effect = trace
        .side_effects
        .iter()
        .find(|effect| runtime_json_field_eq(effect, "id", "plan1"))
        .unwrap()
        .clone();
    let file_effect = trace
        .side_effects
        .iter()
        .find(|effect| runtime_json_field_eq(effect, "type", "file_change"))
        .unwrap()
        .clone();
    trace.side_effects = vec![read_effect, write_effect, file_effect];
    trace.result.behavior_failures = vec![
        "tool TaskCreate called 0 < 1".to_owned().into(),
        "tool apply_patch called 0 < 1".to_owned().into(),
    ];
    trace.result.behavior_success = false;
    trace.result.success = false;
    add_live_tool_event_turn_ids(&mut trace);
    add_missing_plan_reminder_after_task_create_removal(&mut trace);

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();

    trace.engine_events.swap(7, 8);
    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
            .await
            .unwrap_err();
    assert!(
        error.to_string().contains("freshness became uncertain"),
        "{error}"
    );
}

#[tokio::test]
async fn live_trace_contract_accepts_cross_turn_fresh_read_then_existing_write() {
    if !skip_without_node() {
        return;
    }
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-edit-search-shell-facades", EvalProfile::Anthropic);
    let content = runtime_file(&trace.after_files, "calc.js").clone();
    let content = runtime_scalar(&content, "calc.js content").unwrap();
    rewrite_trace_call_as_successful_existing_write(&mut trace, "edit-calc", "calc.js", &content);
    trace.result.behavior_failures = vec!["model-visible tool Edit called 0 < 1".to_owned().into()];
    trace.result.behavior_success = false;
    trace.result.success = false;
    add_live_tool_event_turn_ids(&mut trace);

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();
}

#[tokio::test]
async fn live_trace_contract_rejects_synchronized_after_file_and_file_change_forgery() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-tool-search", EvalProfile::Anthropic);
    trace.after_files.insert(
        RuntimeString::from_utf8("found.txt"),
        RuntimeString::from_utf8("  found  \n"),
    );
    let change = trace
        .side_effects
        .iter_mut()
        .find(|effect| {
            runtime_json_field_eq(effect, "type", "file_change")
                && runtime_json_field_eq(effect, "path", "found.txt")
        })
        .unwrap();
    change["after"] = Json::String("  found  \n".to_owned().into());
    add_live_tool_event_turn_ids(&mut trace);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("virtual file"), "{error}");
}

#[tokio::test]
async fn live_trace_contract_rejects_synchronized_nonzero_bash_exit_forgery() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("command-output-envelope", EvalProfile::Anthropic);
    let terminal = trace
        .engine_events
        .iter_mut()
        .find(|event| {
            runtime_json_field_eq(event, "type", "item.completed")
                && runtime_json_pointer_eq(event, "/item/id", "cmd-1")
        })
        .unwrap();
    terminal["item"]["result"]["exitCode"] = Json::from(1);
    let effect = trace
        .side_effects
        .iter_mut()
        .find(|effect| runtime_json_field_eq(effect, "id", "cmd-1"))
        .unwrap();
    effect["result"]["exitCode"] = Json::from(1);
    add_live_tool_event_turn_ids(&mut trace);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("bash"), "{error}");
}

#[tokio::test]
async fn live_trace_contract_reconstructs_the_hash_bound_edited_files_bash_effect() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("edited-files-diff-envelope", EvalProfile::Anthropic);
    add_live_tool_event_turn_ids(&mut trace);

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();
}

#[tokio::test]
async fn live_opaque_file_effect_requires_the_exact_hash_bound_bash_call() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("edited-files-diff-envelope", EvalProfile::Anthropic);
    add_live_tool_event_turn_ids(&mut trace);
    let id = "edited-bash";
    let mut input = trace
        .normalized_internal_calls
        .iter()
        .find(|call| runtime_json_field_eq(call, "id", id))
        .unwrap()["internalInput"]
        .clone();
    input["command"] = Json::String(
        input["command"]
            .as_str()
            .unwrap()
            .concat(&RuntimeString::from_utf8(" ")),
    );
    let content = trace
        .history
        .iter()
        .find(|message| {
            runtime_json_field_eq(message, "type", "tool_result")
                && runtime_json_field_eq(message, "toolUseId", id)
        })
        .unwrap()["content"]
        .clone();
    let terminal = trace
        .engine_events
        .iter()
        .find(|event| {
            runtime_json_field_eq(event, "type", "item.completed")
                && runtime_json_pointer_eq(event, "/item/id", id)
        })
        .unwrap()["item"]["result"]
        .clone();
    rewrite_trace_tool_call(&mut trace, id, &input, &input, &content, &terminal);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
            .await
            .unwrap_err();
    assert!(
        error.to_string().contains("hash-bound opaque-file"),
        "{error}"
    );
}

#[tokio::test]
async fn scripted_query_records_each_bash_chunk_once_and_rebuilds_its_result() {
    let manifest = load_checked_in_manifest().unwrap();
    let traces = run_scripted_task_tolerant(
        validation_process_context(),
        &manifest,
        "command-output-envelope",
        &[EvalProfile::Anthropic],
    )
    .await
    .unwrap();
    assert_eq!(traces.len(), 1);
    let trace = &traces[0];
    let chunks = trace
        .engine_events
        .iter()
        .filter(|event| {
            runtime_json_field_eq(event, "type", "tool.event")
                && runtime_json_field_eq(event, "toolUseId", "cmd-1")
                && runtime_json_pointer_eq(event, "/event/type", "stdout")
        })
        .filter_map(|event| event.pointer("/event/chunk").and_then(Json::as_str))
        .collect::<Vec<_>>();
    assert_eq!(chunks, ["COMMAND_OK"]);
    let result_text = trace
        .history
        .iter()
        .find(|message| runtime_json_field_eq(message, "toolUseId", "cmd-1"))
        .and_then(|message| message.pointer("/content/0/text"))
        .and_then(Json::as_str)
        .unwrap();
    assert_eq!(
        result_text,
        "<command_output tool=\"Bash\">\nCOMMAND_OK\n</command_output>"
    );
    validate_provider_behavior_trace_contract(&manifest, trace, TraceContractMode::Scripted)
        .await
        .unwrap();
}

#[tokio::test]
async fn live_trace_contract_rebuilds_tool_search_missing_from_its_catalog() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-tool-search", EvalProfile::Anthropic);
    let original_result = trace
        .side_effects
        .iter()
        .find(|effect| runtime_json_field_eq(effect, "id", "s1"))
        .and_then(|effect| effect.get("result"))
        .cloned()
        .unwrap();
    let mut result = original_result;
    result["missing"] = exact_json!(["missing"]);
    let input = exact_json!({ "query": "select:write,missing" });
    let text = trace
        .history
        .iter()
        .find(|message| runtime_json_field_eq(message, "toolUseId", "s1"))
        .and_then(|message| message.pointer("/content/0/text"))
        .and_then(Json::as_str)
        .unwrap()
        .concat(&RuntimeString::from_utf8(
            "\nNo matching tools found for:\n- missing",
        ));
    rewrite_trace_tool_call(
        &mut trace,
        "s1",
        &input,
        &input,
        &exact_json!([{ "type": "text", "text": text }]),
        &result,
    );
    for message in &mut trace.history {
        if runtime_json_field_eq(message, "toolUseId", "s1") {
            message["nativeToolOutput"] = result.clone();
        }
    }
    for request in &mut trace.model_requests {
        for message in request["messages"].as_array_mut().unwrap() {
            if runtime_json_field_eq(message, "toolUseId", "s1") {
                message["nativeToolOutput"] = result.clone();
            }
        }
    }
    add_live_tool_event_turn_ids(&mut trace);

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();
}

#[tokio::test]
async fn scripted_provider_tool_search_advances_request_tools_after_its_real_result() {
    let manifest = load_checked_in_manifest().unwrap();
    let traces = run_scripted_task_tolerant(
        validation_process_context(),
        &manifest,
        "provider-tool-search",
        &[EvalProfile::Anthropic],
    )
    .await
    .unwrap();
    assert_eq!(traces.len(), 1);
    validate_provider_behavior_trace_contract(&manifest, &traces[0], TraceContractMode::Scripted)
        .await
        .unwrap();
}

#[tokio::test]
async fn live_trace_contract_rebuilds_a_repeated_tool_search() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-tool-search", EvalProfile::Anthropic);
    let first_use = trace.history[1]["content"]
        .as_array()
        .unwrap()
        .iter()
        .find(|block| runtime_json_field_eq(block, "id", "s1"))
        .unwrap()
        .clone();
    let mut repeated_use = first_use;
    repeated_use["id"] = Json::String("s2".to_owned().into());
    trace.history[3]["content"]
        .as_array_mut()
        .unwrap()
        .insert(1, repeated_use);
    let mut repeated_result = trace.history[2].clone();
    repeated_result["toolUseId"] = Json::String("s2".to_owned().into());
    repeated_result["uuid"] = Json::String("fixture-repeat-result".to_owned().into());
    trace.history.insert(4, repeated_result);
    trace.model_requests[2]["messages"] = Json::Array(trace.history[..6].to_vec());

    let mut visible = trace
        .model_visible_tool_uses
        .iter()
        .find(|call| runtime_json_field_eq(call, "id", "s1"))
        .unwrap()
        .clone();
    visible["id"] = Json::String("s2".to_owned().into());
    trace.model_visible_tool_uses.insert(1, visible);
    let mut normalized = trace
        .normalized_internal_calls
        .iter()
        .find(|call| runtime_json_field_eq(call, "id", "s1"))
        .unwrap()
        .clone();
    normalized["id"] = Json::String("s2".to_owned().into());
    trace.normalized_internal_calls.insert(1, normalized);

    let start = trace
        .engine_events
        .iter()
        .find(|event| {
            runtime_json_field_eq(event, "type", "item.started")
                && runtime_json_pointer_eq(event, "/item/id", "s1")
        })
        .unwrap()
        .clone();
    let completed = trace
        .engine_events
        .iter()
        .find(|event| {
            runtime_json_field_eq(event, "type", "item.completed")
                && runtime_json_pointer_eq(event, "/item/id", "s1")
        })
        .unwrap()
        .clone();
    let mut repeated_start = start;
    repeated_start["item"]["id"] = Json::String("s2".to_owned().into());
    let mut repeated_completed = completed;
    repeated_completed["item"]["id"] = Json::String("s2".to_owned().into());
    let write_start = trace
        .engine_events
        .iter()
        .position(|event| runtime_json_pointer_eq(event, "/item/id", "w1"))
        .unwrap();
    trace.engine_events.splice(
        write_start..write_start,
        [repeated_start, repeated_completed],
    );

    let mut effect = trace
        .side_effects
        .iter()
        .find(|effect| runtime_json_field_eq(effect, "id", "s1"))
        .unwrap()
        .clone();
    effect["id"] = Json::String("s2".to_owned().into());
    trace.side_effects.insert(1, effect);
    add_live_tool_event_turn_ids(&mut trace);

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();
}

#[tokio::test]
async fn request_tools_cannot_surface_before_their_tool_search_result() {
    let manifest = load_checked_in_manifest().unwrap();
    for mode in [TraceContractMode::Scripted, TraceContractMode::Live] {
        let mut trace = contract_trace("provider-tool-search", EvalProfile::Anthropic);
        trace.model_requests[0]["tools"] = trace.model_requests[1]["tools"].clone();
        if mode == TraceContractMode::Live {
            add_live_tool_event_turn_ids(&mut trace);
        }

        let error = validate_provider_behavior_trace_contract(&manifest, &trace, mode)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("activation")
                || error.to_string().contains("causal")
                || error.to_string().contains("request tool"),
            "{mode:?}: {error}"
        );
    }
}

#[tokio::test]
async fn live_request_tool_order_is_noncausal_but_scripted_order_is_exact() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-tool-search", EvalProfile::Anthropic);
    trace.model_requests[1]["tools"]
        .as_array_mut()
        .unwrap()
        .reverse();

    let mut live_trace = trace.clone();
    add_live_tool_event_turn_ids(&mut live_trace);
    validate_provider_behavior_trace_contract(&manifest, &live_trace, TraceContractMode::Live)
        .await
        .unwrap();
    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(
        error.to_string().contains("scripted main ordinal"),
        "{error}"
    );
}

#[tokio::test]
async fn live_child_request_tools_cannot_leak_from_another_task_catalog() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-parallel-subagents", EvalProfile::Anthropic);
    let donor = contract_trace("code-mode", EvalProfile::Anthropic);
    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
        .await
        .unwrap();
    validate_provider_behavior_trace_contract(&manifest, &donor, TraceContractMode::Scripted)
        .await
        .unwrap();
    add_live_tool_event_turn_ids(&mut trace);
    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();
    let recipient_index = trace
        .model_requests
        .iter()
        .position(|request| !raw_request_is_history_prefix(request, &trace.history))
        .unwrap();
    let foreign = donor.model_requests[0]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|tool| runtime_json_field_eq(tool, "name", "code_exec"))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        foreign.len(),
        1,
        "the source donor has one complete foreign descriptor"
    );
    assert!(trace.model_requests.iter().all(|request| request["tools"]
        .as_array()
        .unwrap()
        .iter()
        .all(|tool| !runtime_json_field_eq(tool, "name", "code_exec"))));
    let before = trace.model_requests[recipient_index].clone();
    trace.model_requests[recipient_index]["tools"]
        .as_array_mut()
        .unwrap()
        .push(foreign[0].clone());
    let mut restored = trace.model_requests[recipient_index].clone();
    assert_eq!(
        restored["tools"].as_array_mut().unwrap().pop(),
        Some(foreign[0].clone())
    );
    assert_eq!(
        restored, before,
        "only the complete foreign descriptor may change"
    );

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
            .await
            .unwrap_err();
    assert!(
        error.to_string().contains("child request tool state")
            || error.to_string().contains("trusted baseline spec"),
        "{error}"
    );
}

#[tokio::test]
async fn live_trace_contract_accepts_nonzero_declared_glob_duration() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-file-facades", EvalProfile::Anthropic);
    let terminal = trace
        .engine_events
        .iter_mut()
        .find(|event| {
            runtime_json_field_eq(event, "type", "item.completed")
                && runtime_json_pointer_eq(event, "/item/id", "glob-target")
        })
        .unwrap();
    terminal["item"]["result"]["durationMs"] = Json::from(17);
    let effect = trace
        .side_effects
        .iter_mut()
        .find(|effect| runtime_json_field_eq(effect, "id", "glob-target"))
        .unwrap();
    effect["result"]["durationMs"] = Json::from(17);

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();
}

#[tokio::test]
async fn live_trace_contract_accepts_glob_progress_before_its_terminal_duration() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-file-facades", EvalProfile::Anthropic);
    let terminal_index = trace
        .engine_events
        .iter()
        .position(|event| {
            runtime_json_field_eq(event, "type", "item.completed")
                && runtime_json_pointer_eq(event, "/item/id", "glob-target")
        })
        .unwrap();
    let mut progress = trace.engine_events[terminal_index].clone();
    progress["type"] = Json::String("item.updated".to_owned().into());
    progress["item"]["status"] = Json::String("in_progress".to_owned().into());
    progress["item"].as_object_mut().unwrap().remove("result");
    trace.engine_events.insert(terminal_index, progress);
    for pointer in &mut trace.normalization.non_contract_time_pointers {
        if let Some(suffix) =
            runtime_strip_prefix(pointer, &format!("/engineEvents/{terminal_index}/"))
        {
            *pointer = RuntimeString::from_utf8(&format!("/engineEvents/{}/", terminal_index + 1))
                .concat(&suffix);
        }
    }

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();
}

#[tokio::test]
async fn live_trace_contract_reconstructs_read_offset_and_limit() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-file-facades", EvalProfile::Anthropic);
    let ranged_input = exact_json!({
        "offset": 2.0,
        "limit": 1e0,
        "path": "$WORKSPACE/nested/facade-target-alpha.txt",
    });
    let visible_ranged_input = exact_json!({
        "file_path": "$WORKSPACE/nested/facade-target-alpha.txt",
        "offset": 2.0,
        "limit": 1e0,
    });
    rewrite_trace_tool_call(
        &mut trace,
        "read-target",
        &visible_ranged_input,
        &ranged_input,
        &exact_json!([{
            "type": "text",
            "text": "     2\t",
        }]),
        &exact_json!({
            "path": "$WORKSPACE/nested/facade-target-alpha.txt",
            "found": true,
            "kind": "text",
            "lines": 1,
        }),
    );
    add_live_tool_event_turn_ids(&mut trace);

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();
}

#[tokio::test]
async fn trace_contract_matches_production_write_trailing_whitespace_semantics() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("add-function", EvalProfile::Anthropic);
    let effective_content = runtime_file(&trace.after_files, "utils.js").clone();
    let input_content = effective_content.replace("  return a + b\n", "  return a + b   \n");
    assert_ne!(input_content, effective_content);
    let input = exact_json!({
        "path": "utils.js",
        "content": input_content,
    });
    rewrite_trace_tool_call(
        &mut trace,
        "w1",
        &input,
        &input,
        &exact_json!([{
            "type": "text",
            "text": (format!("Created utils.js ({} bytes)", effective_content.len_utf16())),
        }]),
        &exact_json!({
            "bytes": (effective_content.len_utf16()),
            "kind": "create",
            "path": "$WORKSPACE/utils.js",
        }),
    );
    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
        .await
        .unwrap();
}

#[tokio::test]
async fn trace_contract_matches_production_write_crlf_disk_canonicalization() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("add-function", EvalProfile::Anthropic);
    let written_content = runtime_file(&trace.after_files, "utils.js").clone();
    let reported_content = written_content.replace("\n", "\r\n");
    let input_content = reported_content.replace("  return a + b\r\n", "  return a + b   \r\n");
    let input = exact_json!({
        "path": "utils.js",
        "content": input_content,
    });
    rewrite_trace_tool_call(
        &mut trace,
        "w1",
        &input,
        &input,
        &exact_json!([{
            "type": "text",
            "text": (format!("Created utils.js ({} bytes)", reported_content.len_utf16())),
        }]),
        &exact_json!({
            "bytes": (reported_content.len_utf16()),
            "kind": "create",
            "path": "$WORKSPACE/utils.js",
        }),
    );

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
        .await
        .unwrap();
}

#[tokio::test]
async fn trace_contract_rejects_synchronized_write_trailing_whitespace_forgery() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("add-function", EvalProfile::Anthropic);
    let forged_content = runtime_file(&trace.after_files, "utils.js")
        .replace("  return a + b\n", "  return a + b   \n");
    let input = exact_json!({
        "path": "utils.js",
        "content": forged_content,
    });
    rewrite_trace_tool_call(
        &mut trace,
        "w1",
        &input,
        &input,
        &exact_json!([{
            "type": "text",
            "text": (format!("Created utils.js ({} bytes)", forged_content.len_utf16())),
        }]),
        &exact_json!({
            "bytes": (forged_content.len_utf16()),
            "kind": "create",
            "path": "$WORKSPACE/utils.js",
        }),
    );
    trace
        .after_files
        .insert(RuntimeString::from_utf8("utils.js"), forged_content.clone());
    trace
        .side_effects
        .iter_mut()
        .find(|effect| {
            runtime_json_field_eq(effect, "type", "file_change")
                && runtime_json_field_eq(effect, "path", "utils.js")
        })
        .unwrap()["after"] = Json::String(forged_content);
    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("terminal result w1"), "{error}");
}

#[tokio::test]
async fn live_trace_contract_matches_production_edit_crlf_disk_canonicalization() {
    if !skip_without_node() {
        return;
    }
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-edit-search-shell-facades", EvalProfile::Anthropic);
    let visible_input = exact_json!({
        "file_path": "calc.js",
        "old_string": "  return a + b",
        "new_string": "  return a * b   \r\n",
    });
    let internal_input = exact_json!({
        "path": "calc.js",
        "oldString": "  return a + b",
        "newString": "  return a * b   \r\n",
    });
    rewrite_trace_tool_call(
        &mut trace,
        "edit-calc",
        &visible_input,
        &internal_input,
        &exact_json!([{
            "type": "text",
            "text": "Edited calc.js (1 replacement)",
        }]),
        &exact_json!({
            "path": "$WORKSPACE/calc.js",
            "replacements": 1,
        }),
    );
    let written_after = runtime_file(&trace.after_files, "calc.js")
        .replace("  return a * b\n", "  return a * b\n\n");
    trace
        .after_files
        .insert(RuntimeString::from_utf8("calc.js"), written_after.clone());
    trace
        .side_effects
        .iter_mut()
        .find(|effect| {
            runtime_json_field_eq(effect, "type", "file_change")
                && runtime_json_field_eq(effect, "path", "calc.js")
        })
        .unwrap()["after"] = Json::String(written_after);
    add_live_tool_event_turn_ids(&mut trace);

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();
}

#[tokio::test]
async fn live_trace_contract_matches_production_edit_trailing_whitespace_semantics() {
    if !skip_without_node() {
        return;
    }
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-edit-search-shell-facades", EvalProfile::Anthropic);
    let visible_input = exact_json!({
        "file_path": "calc.js",
        "old_string": "  return a + b",
        "new_string": "  return a * b   ",
    });
    let internal_input = exact_json!({
        "path": "calc.js",
        "oldString": "  return a + b",
        "newString": "  return a * b   ",
    });
    rewrite_trace_tool_call(
        &mut trace,
        "edit-calc",
        &visible_input,
        &internal_input,
        &exact_json!([{
            "type": "text",
            "text": "Edited calc.js (1 replacement)",
        }]),
        &exact_json!({
            "path": "$WORKSPACE/calc.js",
            "replacements": 1,
        }),
    );
    add_live_tool_event_turn_ids(&mut trace);

    assert!(runtime_file(&trace.after_files, "calc.js").contains("  return a * b\n"));
    assert!(!runtime_file(&trace.after_files, "calc.js").contains("  return a * b   \n"));
    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();
}

#[tokio::test]
async fn live_trace_contract_rejects_synchronized_edit_trailing_whitespace_forgery() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-edit-search-shell-facades", EvalProfile::Anthropic);
    let visible_input = exact_json!({
        "file_path": "calc.js",
        "old_string": "  return a + b",
        "new_string": "  return a * b   ",
    });
    let internal_input = exact_json!({
        "path": "calc.js",
        "oldString": "  return a + b",
        "newString": "  return a * b   ",
    });
    rewrite_trace_tool_call(
        &mut trace,
        "edit-calc",
        &visible_input,
        &internal_input,
        &exact_json!([{
            "type": "text",
            "text": "Edited calc.js (1 replacement)",
        }]),
        &exact_json!({
            "path": "$WORKSPACE/calc.js",
            "replacements": 1,
        }),
    );
    let forged_after = runtime_file(&trace.after_files, "calc.js")
        .replace("  return a * b\n", "  return a * b   \n");
    trace
        .after_files
        .insert(RuntimeString::from_utf8("calc.js"), forged_after.clone());
    trace
        .side_effects
        .iter_mut()
        .find(|effect| {
            runtime_json_field_eq(effect, "type", "file_change")
                && runtime_json_field_eq(effect, "path", "calc.js")
        })
        .unwrap()["after"] = Json::String(forged_after);
    add_live_tool_event_turn_ids(&mut trace);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
            .await
            .unwrap_err();
    assert!(
        error.to_string().contains("virtual file terminal"),
        "{error}"
    );
}

/// A ranged read leaves no full-content baseline, and the edit tool does not ask
/// for one: it resolves oldString against the text it reads itself. So a trace
/// where the model read one line and then edited that line is legitimate, and the
/// validator must reconstruct the edit as the success it reports.
#[tokio::test]
async fn live_trace_contract_accepts_a_ranged_read_then_successful_edit() {
    if !skip_without_node() {
        return;
    }
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-edit-search-shell-facades", EvalProfile::Anthropic);
    let visible_input = exact_json!({
        "file_path": "calc.js",
        "offset": 2,
        "limit": 1,
    });
    let internal_input = exact_json!({
        "offset": 2,
        "limit": 1,
        "path": "calc.js",
    });
    rewrite_trace_tool_call(
        &mut trace,
        "read-calc",
        &visible_input,
        &internal_input,
        &exact_json!([{
            "type": "text",
            "text": "     2\t  // BROKEN_MULTIPLY",
        }]),
        &exact_json!({
            "path": "$WORKSPACE/calc.js",
            "found": true,
            "kind": "text",
            "lines": 1,
        }),
    );
    add_live_tool_event_turn_ids(&mut trace);

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();
}

#[tokio::test]
async fn live_trace_contract_matches_production_edit_flexible_line_semantics() {
    if !skip_without_node() {
        return;
    }
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-edit-search-shell-facades", EvalProfile::Anthropic);
    let visible_input = exact_json!({
        "file_path": "calc.js",
        "old_string": "    return a + b",
        "new_string": "    return a * b",
    });
    let internal_input = exact_json!({
        "path": "calc.js",
        "oldString": "    return a + b",
        "newString": "    return a * b",
    });
    rewrite_trace_tool_call(
        &mut trace,
        "edit-calc",
        &visible_input,
        &internal_input,
        &exact_json!([{
            "type": "text",
            "text": "Edited calc.js (1 replacement)",
        }]),
        &exact_json!({
            "path": "$WORKSPACE/calc.js",
            "replacements": 1,
        }),
    );
    add_live_tool_event_turn_ids(&mut trace);

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();
}

#[tokio::test]
async fn live_trace_contract_matches_production_edit_representation_semantics() {
    if !skip_without_node() {
        return;
    }
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-edit-search-shell-facades", EvalProfile::Anthropic);
    let visible_input = exact_json!({
        "file_path": "calc.js",
        "old_string": "  return \\u0061 + b",
        "new_string": "  return \\u0061 * b",
    });
    let internal_input = exact_json!({
        "path": "calc.js",
        "oldString": "  return \\u0061 + b",
        "newString": "  return \\u0061 * b",
    });
    rewrite_trace_tool_call(
        &mut trace,
        "edit-calc",
        &visible_input,
        &internal_input,
        &exact_json!([{
            "type": "text",
            "text": "Edited calc.js (1 replacement)",
        }]),
        &exact_json!({
            "path": "$WORKSPACE/calc.js",
            "replacements": 1,
        }),
    );
    add_live_tool_event_turn_ids(&mut trace);

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();
}

#[tokio::test]
async fn trace_contract_reconstructs_production_edit_no_op_failure() {
    if !skip_without_node() {
        return;
    }
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("tool-failure-recovery", EvalProfile::Anthropic);
    let visible_input = exact_json!({
        "file_path": "greet.js",
        "old_string": "\"Hi \"",
        "new_string": "\"Hi \"",
    });
    let internal_input = exact_json!({
        "path": "greet.js",
        "oldString": "\"Hi \"",
        "newString": "\"Hi \"",
    });
    rewrite_trace_tool_call(
        &mut trace,
        "e1",
        &visible_input,
        &internal_input,
        &exact_json!([{
            "type": "text",
            "text": "<tool_use_error tool=\"Edit\">\noldString and newString are identical\n</tool_use_error>",
        }]),
        &exact_json!({
            "path": "$WORKSPACE/greet.js",
            "replacements": 0,
        }),
    );
    for event in &mut trace.engine_events {
        if runtime_json_pointer_eq(event, "/item/id", "e1")
            && runtime_option_in_utf8(
                event.get("type").and_then(Json::as_str),
                &["item.completed", "item.updated"],
            )
        {
            event["item"]["error"] =
                exact_json!({ "message": "oldString and newString are identical" });
            *event = canonical_engine_event(event);
        }
    }
    let effect = trace
        .side_effects
        .iter_mut()
        .find(|effect| runtime_json_field_eq(effect, "id", "e1"))
        .unwrap();
    effect["error"] = exact_json!({ "message": "oldString and newString are identical" });
    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
        .await
        .unwrap();
}

#[tokio::test]
async fn live_trace_contract_reconstructs_read_offset_beyond_eof() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-file-facades", EvalProfile::Anthropic);
    let ranged_input = exact_json!({ "offset": 99, "path": "nested/facade-target-alpha.txt" });
    let visible_ranged_input =
        exact_json!({ "file_path": "nested/facade-target-alpha.txt", "offset": 99 });
    rewrite_trace_tool_call(
        &mut trace,
        "read-target",
        &visible_ranged_input,
        &ranged_input,
        &exact_json!([{
            "type": "text",
            "text": "<system-reminder>\nStart line 99 is past the end of nested/facade-target-alpha.txt, which has 2 line(s).\n</system-reminder>",
        }]),
        &exact_json!({
            "path": "$WORKSPACE/nested/facade-target-alpha.txt",
            "found": true,
            "kind": "text",
            "lines": 0,
            "beyondEof": true,
        }),
    );
    add_live_tool_event_turn_ids(&mut trace);

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();
}

#[tokio::test]
async fn live_trace_contract_rejects_zero_read_limit() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-edit-search-shell-facades", EvalProfile::Anthropic);
    let ranged_input = exact_json!({ "offset": 2, "limit": 0, "path": "calc.js" });
    let visible_ranged_input = exact_json!({ "file_path": "calc.js", "offset": 2, "limit": 0 });
    rewrite_trace_tool_call(
        &mut trace,
        "read-calc",
        &visible_ranged_input,
        &ranged_input,
        &exact_json!([{ "type": "text", "text": "" }]),
        &exact_json!({
            "path": "$WORKSPACE/calc.js",
            "found": true,
            "kind": "text",
            "lines": 0,
        }),
    );
    add_live_tool_event_turn_ids(&mut trace);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("positive integer"), "{error}");
}

#[tokio::test]
async fn live_trace_contract_rebuilds_grep_with_omitted_default_mode() {
    if !skip_without_node() {
        return;
    }
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-edit-search-shell-facades", EvalProfile::Anthropic);
    let input = exact_json!({
        "glob": "*.js",
        "path": ".",
        "pattern": "BROKEN_MULTIPLY",
    });
    let content = trace
        .history
        .iter()
        .find(|message| runtime_json_field_eq(message, "toolUseId", "grep-broken"))
        .and_then(|message| message.get("content"))
        .cloned()
        .unwrap();
    let result = trace
        .side_effects
        .iter()
        .find(|effect| runtime_json_field_eq(effect, "id", "grep-broken"))
        .and_then(|effect| effect.get("result"))
        .cloned()
        .unwrap();
    rewrite_trace_tool_call(&mut trace, "grep-broken", &input, &input, &content, &result);
    add_live_tool_event_turn_ids(&mut trace);

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();
}

#[tokio::test]
async fn live_trace_contract_rejects_an_anchored_files_grep_no_match_forgery() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-edit-search-shell-facades", EvalProfile::Anthropic);
    let input = exact_json!({
        "glob": "*.js",
        "path": ".",
        "pattern": "^  // BROKEN_MULTIPLY$",
    });
    rewrite_trace_tool_call(
        &mut trace,
        "grep-broken",
        &input,
        &input,
        &exact_json!([{
            "type": "text",
            "text": "No files matched.",
        }]),
        &exact_json!({
            "mode": "files_with_matches",
            "numFiles": 0,
            "filenames": [],
        }),
    );
    add_live_tool_event_turn_ids(&mut trace);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("grep"), "{error}");
}

#[tokio::test]
async fn live_trace_contract_rebuilds_grep_content_mode() {
    if !skip_without_node() {
        return;
    }
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-edit-search-shell-facades", EvalProfile::Anthropic);
    let input = exact_json!({
        "glob": "*.js",
        "output_mode": "content",
        "path": ".",
        "pattern": "BROKEN_MULTIPLY",
    });
    let content = "calc.js:2:  // BROKEN_MULTIPLY";
    rewrite_trace_tool_call(
        &mut trace,
        "grep-broken",
        &input,
        &input,
        &exact_json!([{ "type": "text", "text": content }]),
        &exact_json!({
            "mode": "content",
            "numFiles": 0,
            "filenames": [],
            "content": content,
            "numLines": 1,
        }),
    );
    add_live_tool_event_turn_ids(&mut trace);

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();
}

#[tokio::test]
async fn live_trace_contract_fails_closed_for_multi_file_grep_content_order() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-edit-search-shell-facades", EvalProfile::Anthropic);
    let input = exact_json!({
        "glob": "*.js",
        "output_mode": "content",
        "path": ".",
        "pattern": "multiply",
    });
    let content = [
        "calc.js:1:function multiply(a, b) {",
        "calc.js:5:module.exports = multiply",
        "test.js:1:const multiply = require('./calc')",
        "test.js:2:if (multiply(3, 4) !== 12) { throw new Error('FAIL') }",
    ]
    .join("\n");
    rewrite_trace_tool_call(
        &mut trace,
        "grep-broken",
        &input,
        &input,
        &exact_json!([{ "type": "text", "text": content }]),
        &exact_json!({
            "mode": "content",
            "numFiles": 0,
            "filenames": [],
            "content": content,
            "numLines": 4,
        }),
    );
    add_live_tool_event_turn_ids(&mut trace);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("multi-file grep"), "{error}");
}

#[tokio::test]
async fn live_trace_contract_rebuilds_grep_count_mode() {
    if !skip_without_node() {
        return;
    }
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-edit-search-shell-facades", EvalProfile::Anthropic);
    let input = exact_json!({
        "glob": "*.js",
        "output_mode": "count",
        "path": ".",
        "pattern": "BROKEN_MULTIPLY",
    });
    let content = "calc.js:1";
    rewrite_trace_tool_call(
        &mut trace,
        "grep-broken",
        &input,
        &input,
        &exact_json!([{
            "type": "text",
            "text": "calc.js:1\n\n1 match in 1 file",
        }]),
        &exact_json!({
            "mode": "count",
            "numFiles": 1,
            "filenames": [],
            "content": content,
            "numMatches": 1,
        }),
    );
    add_live_tool_event_turn_ids(&mut trace);

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();
}

#[tokio::test]
async fn live_trace_contract_rebuilds_brace_glob_by_terminal_order() {
    let manifest = load_checked_in_manifest().unwrap();
    let trace = two_file_glob_trace(&["nested/other.txt", "nested/facade-target-alpha.txt"]);

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();
}

#[tokio::test]
async fn live_trace_contract_matches_a_slashless_glob_against_nested_basenames() {
    let manifest = load_checked_in_manifest().unwrap();
    let trace = glob_trace(
        "*.txt",
        &["nested/other.txt", "nested/facade-target-alpha.txt"],
    );

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();
}

#[tokio::test]
async fn live_trace_contract_rejects_duplicate_extra_and_omitted_glob_members() {
    let manifest = load_checked_in_manifest().unwrap();
    for (label, filenames) in [
        ("duplicate", vec!["nested/other.txt", "nested/other.txt"]),
        (
            "extra",
            vec![
                "nested/other.txt",
                "nested/facade-target-alpha.txt",
                "nested/missing.txt",
            ],
        ),
        ("omitted", vec!["nested/other.txt"]),
    ] {
        let trace = two_file_glob_trace(&filenames);
        let error =
            validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
                .await
                .unwrap_err();
        assert!(error.to_string().contains("glob"), "{label}: {error}");
    }
}

#[tokio::test]
async fn trace_contract_validator_rejects_unexpected_tool_result_attachments() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("add-function", EvalProfile::Anthropic);
    for message in &mut trace.history {
        if runtime_json_field_eq(message, "type", "tool_result")
            && runtime_json_field_eq(message, "toolUseId", "w1")
        {
            message["extraUserBlocks"] = exact_json!([{
                "type": "text",
                "text": "forged attachment",
            }]);
        }
    }
    for request in &mut trace.model_requests {
        for message in request["messages"].as_array_mut().unwrap() {
            if runtime_json_field_eq(message, "type", "tool_result")
                && runtime_json_field_eq(message, "toolUseId", "w1")
            {
                message["extraUserBlocks"] = exact_json!([{
                    "type": "text",
                    "text": "forged attachment",
                }]);
            }
        }
    }

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("extraUserBlocks"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_synchronized_partial_line_spill_previews() {
    if !skip_without_node() {
        return;
    }
    let manifest = load_checked_in_manifest().unwrap();
    for profile in [
        EvalProfile::Anthropic,
        EvalProfile::Openai,
        EvalProfile::Generic,
    ] {
        let mut trace = contract_trace("tool-result-spill-envelope", profile);
        let full = &trace.after_files[&RuntimeString::from(".klaus/tool-results/spill-1.txt")];
        let current = trace.history[2]
            .pointer("/content/0/text")
            .unwrap()
            .as_str()
            .unwrap();
        let predecessor = full
            .slice_utf16(..1_000)
            .concat(&current.slice_utf16(951..));
        trace.history[2]["content"][0]["text"] = Json::String(predecessor.clone());
        trace.model_requests[1]["messages"][2]["content"][0]["text"] = Json::String(predecessor);
        let error = validate_provider_behavior_trace_contract(
            &manifest,
            &trace,
            TraceContractMode::Scripted,
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("differs from independent reconstruction"),
            "{profile:?}: {error}"
        );
    }
}

#[tokio::test]
async fn trace_contract_validator_reconstructs_all_23_fixed_eval_internal_tools() {
    if !skip_without_node() {
        return;
    }
    let manifest = load_checked_in_manifest().unwrap();
    let cases = [
        ("fix-bug", EvalProfile::Anthropic),
        ("tool-failure-recovery", EvalProfile::Openai),
        ("provider-apply-patch", EvalProfile::Generic),
        ("command-output-envelope", EvalProfile::Anthropic),
        ("background-completion-envelope", EvalProfile::Openai),
        ("tool-result-spill-envelope", EvalProfile::Generic),
        ("write-report-nudge-envelope", EvalProfile::Anthropic),
        ("provider-tool-search", EvalProfile::Openai),
        ("provider-parallel-subagents", EvalProfile::Openai),
        ("provider-agent-control-facades", EvalProfile::Openai),
        ("provider-swarm-peer-facades", EvalProfile::Generic),
        ("code-mode", EvalProfile::Generic),
        ("provider-plan-patch", EvalProfile::Generic),
        ("provider-file-facades", EvalProfile::Generic),
        ("provider-edit-search-shell-facades", EvalProfile::Generic),
        ("provider-exec-stdin-facades", EvalProfile::Generic),
        ("provider-direct-model-only-exposure", EvalProfile::Generic),
        ("permission-denial-envelope", EvalProfile::Generic),
    ];
    let mut covered = BTreeSet::new();
    for (task, profile) in cases {
        let trace = contract_trace(task, profile);
        covered.extend(
            trace
                .normalized_internal_calls
                .iter()
                .filter_map(|call| call.get("internalName").and_then(Json::as_str))
                .cloned(),
        );
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_or_else(|error| panic!("{task}/{profile:?}: {error}"));
    }
    let expected = [
        "ListPeers",
        "SendMessage",
        "apply_patch",
        "background_notice_probe",
        "bash",
        "code_exec",
        "deferred_probe",
        "direct_model_probe",
        "edit",
        "exec_command",
        "glob",
        "grep",
        "read",
        "selfdeny",
        "spill_probe",
        "subagent_report_write",
        "task",
        "task_output",
        "task_stop",
        "TaskCreate",
        "tool_search",
        "write",
        "write_stdin",
    ]
    .into_iter()
    .map(RuntimeString::from_utf8)
    .collect::<BTreeSet<_>>();
    assert_eq!(covered, expected);
}

#[tokio::test]
async fn trace_contract_validator_requires_one_later_result_for_each_visible_tool_use() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("add-function", EvalProfile::Anthropic);
    trace.history.retain(|message| {
        !runtime_json_field_eq(message, "type", "tool_result")
            || !runtime_json_field_eq(message, "toolUseId", "w1")
    });
    for request in &mut trace.model_requests {
        if let Some(messages) = request.get_mut("messages").and_then(Json::as_array_mut) {
            messages.retain(|message| {
                !runtime_json_field_eq(message, "type", "tool_result")
                    || !runtime_json_field_eq(message, "toolUseId", "w1")
            });
        }
    }

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("later tool_result"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_unknown_nested_engine_item_fields() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("add-function", EvalProfile::Anthropic);
    trace
        .engine_events
        .iter_mut()
        .find(|event| runtime_json_pointer_eq(event, "/item/type", "tool_call"))
        .unwrap()["item"]
        .as_object_mut()
        .unwrap()
        .insert("unknownContractField".to_owned(), Json::Bool(true));

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("engineEvents"), "{error}");
}

#[tokio::test]
async fn scripted_trace_contract_validator_rejects_synchronized_run_profile_drift() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("hook-additional-context-envelope", EvalProfile::Anthropic);
    trace.run.provider_info.default_model = RuntimeString::from_utf8("claude-forged");

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("scripted run"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_forged_fixed_normalization_tokens() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    trace.normalization.workspace_root = RuntimeString::from_utf8("/tmp/self-declared-ignore");

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("workspaceRoot"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_a_residual_raw_project_slug() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("read-summarize", EvalProfile::Anthropic);
    assert!(trace.model_requests.iter_mut().any(|request| {
        replace_first_runtime_string(request, "$PROJECT_SLUG", "raw-project-slug")
    }));

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("project slug"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_a_synchronized_raw_eval_workspace_path() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("add-function", EvalProfile::Anthropic);
    let raw_path = "/tmp/klaus-provider-behavior-forged/utils.js";
    let event = trace
        .engine_events
        .iter_mut()
        .find(|event| runtime_json_pointer_eq(event, "/item/result/path", "$WORKSPACE/utils.js"))
        .unwrap();
    event["item"]["result"]["path"] = Json::String(raw_path.to_owned().into());
    let effect = trace
        .side_effects
        .iter_mut()
        .find(|effect| runtime_json_pointer_eq(effect, "/result/path", "$WORKSPACE/utils.js"))
        .unwrap();
    effect["result"]["path"] = Json::String(raw_path.to_owned().into());

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("workspace path"), "{error}");
}

/// Independently validate every checked-in row of one profile, no model involved.
///
/// Split per profile because validating all 100 rows in one test took 90 seconds
/// and no run finishes faster than its longest test. `expected` is pinned per
/// profile so a row that disappears from the snapshot still fails here, and the
/// four counts add up to the full 100.
async fn assert_fixed_source_rows_validate(profile: EvalProfile, expected: usize) {
    if !skip_without_node() {
        return;
    }
    let manifest = load_checked_in_manifest().unwrap();
    let mut validated = 0;
    for row in fixture_values() {
        let row_profile: EvalProfile = serde_json::from_value(row["profile"].clone()).unwrap();
        if row_profile != profile {
            continue;
        }
        let task = row["task"].as_str().unwrap();
        let trace = load_baseline_reference_trace(task, row_profile).unwrap();
        let contract = validate_provider_behavior_trace_contract(
            &manifest,
            &trace,
            TraceContractMode::Scripted,
        )
        .await
        .unwrap_or_else(|error| {
            panic!(
                "{}/{} failed independent validation: {error}",
                row["task"], row["profile"]
            )
        });
        assert_eq!(contract.variant, ValidatedTraceContractVariant::Source);
        validated += 1;
    }
    assert_eq!(validated, expected);
}

#[tokio::test]
async fn trace_contract_validator_accepts_the_fixed_anthropic_source_rows() {
    assert_fixed_source_rows_validate(EvalProfile::Anthropic, 33).await;
}

#[tokio::test]
async fn trace_contract_validator_accepts_the_fixed_openai_source_rows() {
    assert_fixed_source_rows_validate(EvalProfile::Openai, 33).await;
}

#[tokio::test]
async fn trace_contract_validator_accepts_the_fixed_generic_source_rows() {
    assert_fixed_source_rows_validate(EvalProfile::Generic, 33).await;
}

#[tokio::test]
async fn trace_contract_validator_accepts_the_fixed_qwen_source_row() {
    assert_fixed_source_rows_validate(EvalProfile::Qwen, 1).await;
}

/// The four per-profile tests above must together cover the whole snapshot.
#[test]
fn the_fixed_source_rows_are_exactly_100() {
    assert_eq!(fixture_values().len(), 100);
}

#[tokio::test]
async fn trace_contract_validator_rejects_parallel_result_uuid_declaration_drift() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-parallel-subagents", EvalProfile::Anthropic);
    let pointer = trace.normalization.non_contract_parallel_uuid_pointers[0].clone();
    let pointer = runtime_scalar(&pointer, "parallel UUID normalization pointer").unwrap();
    if let Some(index) = pointer
        .strip_prefix("/history/")
        .and_then(|rest| rest.strip_suffix("/uuid"))
        .and_then(|value| value.parse::<usize>().ok())
    {
        trace.history[index]["uuid"] = Json::String("forged-result-uuid".to_owned().into());
    } else {
        let rest = pointer.strip_prefix("/modelRequests/").unwrap();
        let mut parts = rest.split('/');
        let request = parts.next().unwrap().parse::<usize>().unwrap();
        assert_eq!(parts.next(), Some("messages"));
        let message = parts.next().unwrap().parse::<usize>().unwrap();
        assert_eq!(parts.next(), Some("uuid"));
        trace.model_requests[request]["messages"][message]["uuid"] =
            Json::String("forged-result-uuid".to_owned().into());
    }

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("parallel UUID"), "{error}");
}

#[tokio::test]
async fn parallel_uuid_multi_copy_reaches_scripted_exact_main_request_cardinality() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-parallel-subagents", EvalProfile::Anthropic);
    let source_index =
        trace
            .model_requests
            .iter()
            .rposition(|request| {
                runtime_json_field_eq(request, "cacheKey", "provider-behavior-session")
                    && request
                        .get("messages")
                        .and_then(Json::as_array)
                        .is_some_and(|messages| {
                            messages.iter().any(|message| {
                                runtime_json_field_eq(message, "type", "tool_result")
                                    && message.get("uuid").and_then(Json::as_str).is_some_and(
                                        |uuid| uuid.starts_with("$PARALLEL_TOOL_RESULT_UUID:"),
                                    )
                            })
                        })
            })
            .unwrap();
    let request_index = trace.model_requests.len();
    let retry = trace.model_requests[source_index].clone();
    for (message_index, message) in retry["messages"].as_array().unwrap().iter().enumerate() {
        if message
            .get("uuid")
            .and_then(Json::as_str)
            .is_some_and(|uuid| uuid.starts_with("$PARALLEL_TOOL_RESULT_UUID:"))
        {
            trace
                .normalization
                .non_contract_parallel_uuid_pointers
                .push(RuntimeString::from_utf8(&format!(
                    "/modelRequests/{request_index}/messages/{message_index}/uuid"
                )));
        }
    }
    trace
        .normalization
        .non_contract_parallel_uuid_pointers
        .sort();
    let source_prefix = format!("/modelRequests/{source_index}/");
    let retry_prefix = RuntimeString::from_utf8(&format!("/modelRequests/{request_index}/"));
    let host_pointers = trace
        .normalization
        .non_contract_host_pointers
        .iter()
        .filter_map(|pointer| runtime_strip_prefix(pointer, &source_prefix))
        .map(|suffix| retry_prefix.concat(&suffix))
        .collect::<Vec<_>>();
    trace
        .normalization
        .non_contract_host_pointers
        .extend(host_pointers);
    trace.normalization.non_contract_host_pointers.sort();
    trace.model_requests.push(retry);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(
        error.to_string().contains("extra scripted main request"),
        "UUID reconstruction must accept the complete second copy before exact scripted cardinality rejects it: {error}"
    );
}

#[tokio::test]
async fn trace_contract_validator_keeps_only_the_legacy_omitted_tool_event_turn_id() {
    let manifest = load_checked_in_manifest().unwrap();
    let trace = contract_trace("add-function", EvalProfile::Anthropic);
    assert!(trace.engine_events.iter().any(|event| {
        runtime_json_field_eq(event, "type", "tool.event") && event.get("turnId").is_none()
    }));
    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
        .await
        .unwrap();

    for value in [Json::Bool(true), Json::Null] {
        let mut forged = trace.clone();
        let event = forged
            .engine_events
            .iter_mut()
            .find(|event| runtime_json_field_eq(event, "type", "tool.event"))
            .unwrap();
        event
            .as_object_mut()
            .unwrap()
            .insert("turnId".to_owned(), value);
        let error = validate_provider_behavior_trace_contract(
            &manifest,
            &forged,
            TraceContractMode::Scripted,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("engineEvents"), "{error}");
    }
}

#[tokio::test]
async fn scripted_runner_strips_verified_runtime_tool_event_turn_ids_before_persistence() {
    let manifest = load_checked_in_manifest().unwrap();
    let traces = run_scripted_task_tolerant(
        validation_process_context(),
        &manifest,
        "command-output-envelope",
        &[EvalProfile::Anthropic],
    )
    .await
    .unwrap();
    assert_eq!(traces.len(), 1);
    assert!(runtime_json_field_eq(
        traces[0].engine_events.last().unwrap(),
        "stopReason",
        "completed"
    ));
    assert!(traces[0].result.success);
    let tool_events = traces[0]
        .engine_events
        .iter()
        .filter(|event| runtime_json_field_eq(event, "type", "tool.event"))
        .collect::<Vec<_>>();
    assert!(!tool_events.is_empty());
    assert!(tool_events
        .iter()
        .all(|event| event.get("turnId").is_none()));
    validate_provider_behavior_trace_contract(&manifest, &traces[0], TraceContractMode::Scripted)
        .await
        .unwrap();
}

#[tokio::test]
async fn trace_contract_validator_rejects_parallel_lifecycle_order_drift() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-parallel-subagents", EvalProfile::Anthropic);
    let terminal_indices = trace
        .normalization
        .non_contract_parallel_event_pointers
        .iter()
        .map(|pointer| {
            runtime_scalar(
                &runtime_strip_prefix(pointer, "/engineEvents/").unwrap(),
                "parallel event normalization pointer",
            )
            .unwrap()
            .parse::<usize>()
            .unwrap()
        })
        .collect::<Vec<_>>();
    assert!(terminal_indices.len() >= 2);
    trace
        .engine_events
        .swap(terminal_indices[0], terminal_indices[1]);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("parallel terminal"), "{error}");
}

#[tokio::test]
async fn live_trace_contract_allows_a_real_default_model_distinct_from_requested_model() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("prompt-tag-following", EvalProfile::Anthropic);
    trace.run.provider_info.default_model = RuntimeString::from_utf8("claude-sonnet-4-6");

    let validated =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
            .await
            .unwrap();
    assert_eq!(validated.variant, ValidatedTraceContractVariant::Source);
    assert!(validate_provider_behavior_trace_contract(
        &manifest,
        &trace,
        TraceContractMode::Scripted,
    )
    .await
    .is_err());
}

#[tokio::test]
async fn live_trace_contract_normalizes_a_non_fixture_request_model_in_exact_metadata_positions() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("prompt-tag-following", EvalProfile::Anthropic);
    let fixture_model = trace.run.model.clone();
    let live_model = "claude-sonnet-4-6";
    trace.run.model = RuntimeString::from_utf8(live_model);
    trace.run.provider_info.default_model = RuntimeString::from_utf8("claude-opus-4-6");
    for request in &mut trace.model_requests {
        request["model"] = Json::String(live_model.to_owned().into());
        let system = request["system"].as_array_mut().unwrap();
        let env = system
            .iter_mut()
            .find(|part| runtime_json_field_eq(part, "name", "env"))
            .unwrap();
        let old_suffix = RuntimeString::from_utf8(" - You are powered by the model ")
            .concat(&fixture_model)
            .concat(&RuntimeString::from_utf8("."));
        let new_suffix =
            RuntimeString::from_utf8(&format!(" - You are powered by the model {live_model}."));
        let text = env["text"].as_str().unwrap();
        assert!(text.as_utf16().ends_with(old_suffix.as_utf16()));
        env["text"] = Json::String(
            text.slice_utf16(..text.len_utf16() - old_suffix.len_utf16())
                .concat(&new_suffix),
        );
    }

    let validated =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
            .await
            .unwrap();
    assert_eq!(validated.variant, ValidatedTraceContractVariant::Source);
}

#[tokio::test]
async fn trace_contract_validator_bounds_distinct_child_request_keys_by_task_calls() {
    let manifest = load_checked_in_manifest().unwrap();
    let trace = contract_trace("provider-parallel-subagents", EvalProfile::Anthropic);
    let mut extra_child = trace.clone();
    let source_index = extra_child
        .model_requests
        .iter()
        .position(|request| !raw_request_is_history_prefix(request, &extra_child.history))
        .unwrap();
    let mut forged_child = extra_child.model_requests[source_index].clone();
    for (index, message) in forged_child["messages"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .enumerate()
        .skip(2)
    {
        message["uuid"] = Json::from(format!("forged-third-child-{index}"));
    }
    let new_index = extra_child.model_requests.len();
    extra_child.model_requests.push(forged_child);
    let host_additions = extra_child
        .normalization
        .non_contract_host_pointers
        .iter()
        .filter_map(|pointer| {
            runtime_strip_prefix(pointer, &format!("/modelRequests/{source_index}/"))
        })
        .map(|suffix| {
            RuntimeString::from_utf8(&format!("/modelRequests/{new_index}/")).concat(&suffix)
        })
        .collect::<Vec<_>>();
    extra_child
        .normalization
        .non_contract_host_pointers
        .extend(host_additions);
    extra_child.normalization.non_contract_host_pointers.sort();

    let error = validate_provider_behavior_trace_contract(
        &manifest,
        &extra_child,
        TraceContractMode::Scripted,
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("child request cacheKeys"),
        "{error}"
    );

    let mut missing_children = trace;
    let kept_indices = missing_children
        .model_requests
        .iter()
        .enumerate()
        .filter_map(|(index, request)| {
            raw_request_is_history_prefix(request, &missing_children.history).then_some(index)
        })
        .collect::<Vec<_>>();
    missing_children.model_requests = kept_indices
        .iter()
        .map(|index| missing_children.model_requests[*index].clone())
        .collect();
    let index_map = kept_indices
        .into_iter()
        .enumerate()
        .map(|(new, old)| (old, new))
        .collect::<BTreeMap<_, _>>();
    remap_model_request_pointers(
        &mut missing_children.normalization.non_contract_host_pointers,
        &index_map,
    );
    remap_model_request_pointers(
        &mut missing_children
            .normalization
            .non_contract_parallel_uuid_pointers,
        &index_map,
    );
    let error = validate_provider_behavior_trace_contract(
        &manifest,
        &missing_children,
        TraceContractMode::Scripted,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("task prompts"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_rejects_deleting_a_child_that_inherits_the_main_cache_key() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-spawn-partial-fork-turns", EvalProfile::Anthropic);
    assert!(trace.model_requests.iter().all(|request| {
        runtime_json_field_eq(request, "cacheKey", "provider-behavior-session")
    }));
    let kept_indices = trace
        .model_requests
        .iter()
        .enumerate()
        .filter_map(|(index, request)| {
            raw_request_is_history_prefix(request, &trace.history).then_some(index)
        })
        .collect::<Vec<_>>();
    assert_eq!(kept_indices.len(), 2);
    trace.model_requests = kept_indices
        .iter()
        .map(|index| trace.model_requests[*index].clone())
        .collect();
    let index_map = kept_indices
        .into_iter()
        .enumerate()
        .map(|(new, old)| (old, new))
        .collect::<BTreeMap<_, _>>();
    remap_model_request_pointers(
        &mut trace.normalization.non_contract_host_pointers,
        &index_map,
    );
    remap_model_request_pointers(
        &mut trace.normalization.non_contract_parallel_uuid_pointers,
        &index_map,
    );

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("task prompts"), "{error}");
}

#[tokio::test]
async fn live_trace_contract_accepts_parallel_fork_children_with_generated_placeholder_uuids() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-spawn-partial-fork-turns", EvalProfile::Anthropic);
    add_live_tool_event_turn_ids(&mut trace);

    let second_id = "second-child";
    let second_input = exact_json!({"model": "low",
        "description": "Second child",
        "prompt": "Answer CHILD_READY_2.",
    });
    let second_result = exact_json!({"ok":true,"text":"CHILD_READY_2"});

    let mut second_use = trace.history[1]["content"]
        .as_array()
        .unwrap()
        .iter()
        .find(|block| runtime_json_field_eq(block, "id", "recent-child"))
        .unwrap()
        .clone();
    second_use["id"] = Json::String(second_id.to_owned().into());
    second_use["input"] = second_input.clone();
    trace.history[1]["content"]
        .as_array_mut()
        .unwrap()
        .push(second_use.clone());
    trace.history[2]["uuid"] =
        Json::String("$PARALLEL_TOOL_RESULT_UUID:recent-child".to_owned().into());
    let second_history_result = exact_json!({
        "type": "tool_result",
        "uuid": "$PARALLEL_TOOL_RESULT_UUID:second-child",
        "toolUseId": second_id,
        "content": [{
            "type": "text",
            "text": "<subagent_report tool=\"Task\">\nCHILD_READY_2\n</subagent_report>",
        }],
        "isError": false,
    });
    trace.history.insert(3, second_history_result.clone());

    let final_main_request = trace.model_requests.get_mut(2).unwrap();
    final_main_request["messages"][1]["content"]
        .as_array_mut()
        .unwrap()
        .push(second_use);
    final_main_request["messages"][2]["uuid"] =
        Json::String("$PARALLEL_TOOL_RESULT_UUID:recent-child".to_owned().into());
    final_main_request["messages"]
        .as_array_mut()
        .unwrap()
        .push(second_history_result);

    let inherited = trace.history[..2]
        .iter()
        .map(runtime_codec::decode_value::<klaus_agent_core::schemas::message::Message>)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let build_child_messages = |child_number: usize, directive: &str| {
        let next_uuid = std::cell::Cell::new(0usize);
        klaus_agent_core::engine::fork::build_fork_transcript(&inherited, directive, || {
            let ordinal = next_uuid.get();
            next_uuid.set(ordinal + 1);
            format!("live-fork-{child_number}-{ordinal}")
        })
    };
    trace.model_requests[1]["messages"] =
        runtime_codec::encode_value(&build_child_messages(0, "Answer CHILD_RECENT.")).unwrap();
    let mut second_child_request = trace.model_requests[1].clone();
    second_child_request["messages"] =
        runtime_codec::encode_value(&build_child_messages(1, "Answer CHILD_READY_2.")).unwrap();
    trace.model_requests.insert(2, second_child_request);
    let index_map = BTreeMap::from([(0usize, 0usize), (1, 1), (2, 3)]);
    remap_model_request_pointers(
        &mut trace.normalization.non_contract_host_pointers,
        &index_map,
    );
    duplicate_model_request_normalization_pointers(&mut trace, 1, 2);

    let mut second_visible = trace.model_visible_tool_uses[0].clone();
    second_visible["id"] = Json::String(second_id.to_owned().into());
    second_visible["input"] = second_input.clone();
    trace.model_visible_tool_uses.push(second_visible);
    let mut second_normalized = trace.normalized_internal_calls[0].clone();
    second_normalized["id"] = Json::String(second_id.to_owned().into());
    second_normalized["visibleInput"] = second_input.clone();
    second_normalized["internalInput"] = second_input.clone();
    trace.normalized_internal_calls.push(second_normalized);

    let mut second_started = trace.engine_events[6].clone();
    second_started["item"]["id"] = Json::String(second_id.to_owned().into());
    second_started["item"]["arguments"] = second_input.clone();
    let mut second_completed = trace.engine_events[7].clone();
    second_completed["item"]["id"] = Json::String(second_id.to_owned().into());
    second_completed["item"]["arguments"] = second_input.clone();
    second_completed["item"]["result"] = second_result.clone();
    trace.engine_events.insert(7, second_started);
    trace.engine_events.insert(9, second_completed);
    trace.normalization.non_contract_parallel_event_pointers =
        vec!["/engineEvents/8".into(), "/engineEvents/9".into()];

    let mut second_effect = trace.side_effects[0].clone();
    second_effect["id"] = Json::String(second_id.to_owned().into());
    second_effect["input"] = second_input;
    second_effect["result"] = second_result;
    trace.side_effects.push(second_effect);
    trace.normalization.non_contract_parallel_uuid_pointers = vec![
        "/history/2/uuid".into(),
        "/history/3/uuid".into(),
        "/modelRequests/3/messages/2/uuid".into(),
        "/modelRequests/3/messages/3/uuid".into(),
    ];

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();

    let parent_uuid = trace.history[0]["uuid"].clone();
    let forged_placeholder = trace.model_requests[1]["messages"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|message| {
            runtime_json_field_eq(message, "type", "tool_result")
                && runtime_json_field_eq(message, "toolUseId", "recent-child")
        })
        .unwrap();
    forged_placeholder["uuid"] = parent_uuid;
    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
            .await
            .unwrap_err();
    assert!(
        error.to_string().contains("child session contract"),
        "{error}"
    );
}

#[tokio::test]
async fn trace_contract_validator_pairs_each_child_request_to_its_task_prompt() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-parallel-subagents", EvalProfile::Anthropic);
    let child_indices = trace
        .model_requests
        .iter()
        .enumerate()
        .filter_map(|(index, request)| {
            (!raw_request_is_history_prefix(request, &trace.history)).then_some(index)
        })
        .collect::<Vec<_>>();
    assert_eq!(child_indices.len(), 2);
    let first = trace.model_requests[child_indices[0]].clone();
    let second_key = trace.model_requests[child_indices[1]]["cacheKey"].clone();
    trace.model_requests[child_indices[1]] = first;
    trace.model_requests[child_indices[1]]["cacheKey"] = second_key;

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("task prompt"), "{error}");
}

#[tokio::test]
async fn trace_contract_validator_allows_child_requests_backed_by_a_failed_task_terminal() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-parallel-subagents", EvalProfile::Anthropic);
    rewrite_task_call_as_failure(&mut trace, "t1", "child failed");

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("behaviorFailures"), "{error}");
}

#[tokio::test]
async fn scripted_child_sessions_do_not_collapse_identical_task_prompts() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-parallel-subagents", EvalProfile::Anthropic);
    let first_prompt = "Read alpha.txt and report its keyword.";

    let visible_input = trace
        .model_visible_tool_uses
        .iter()
        .find(|call| runtime_json_field_eq(call, "id", "t1"))
        .unwrap()["input"]
        .clone();
    let internal_input = trace
        .normalized_internal_calls
        .iter()
        .find(|call| runtime_json_field_eq(call, "id", "t1"))
        .unwrap()["internalInput"]
        .clone();
    let result_content = trace
        .history
        .iter()
        .find(|message| runtime_json_field_eq(message, "toolUseId", "t2"))
        .unwrap()["content"]
        .clone();
    let terminal_result = trace
        .side_effects
        .iter()
        .find(|effect| runtime_json_field_eq(effect, "id", "t2"))
        .unwrap()["result"]
        .clone();
    rewrite_trace_tool_call(
        &mut trace,
        "t2",
        &visible_input,
        &internal_input,
        &result_content,
        &terminal_result,
    );

    let duplicate_child = child_request_indices(&trace)
        .into_iter()
        .find(|index| {
            trace.model_requests[*index]["messages"]
                .as_array()
                .unwrap()
                .last()
                .and_then(|message| message.pointer("/content/0/text"))
                .and_then(Json::as_str)
                .is_some_and(|text| {
                    text.ends_with("Directive: Read beta.txt and report its keyword.")
                })
        })
        .unwrap();
    let directive = trace.model_requests[duplicate_child]["messages"]
        .as_array_mut()
        .unwrap()
        .last_mut()
        .unwrap();
    directive["content"][0]["text"] = Json::from(
        klaus_agent_core::engine::fork::build_fork_boilerplate(first_prompt),
    );
    remove_model_request(&mut trace, duplicate_child);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("child session"), "{error}");
}

#[tokio::test]
async fn scripted_child_session_rejects_arbitrary_history_after_the_fork_seed() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-parallel-subagents", EvalProfile::Anthropic);
    let child = child_request_indices(&trace)[0];
    trace.model_requests[child]["messages"]
        .as_array_mut()
        .unwrap()
        .push(exact_json!({
            "type": "user",
            "uuid": "forged-child-history",
            "content": [{"type":"text","text":"unrelated appended history"}],
        }));

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("child session"), "{error}");
}

#[tokio::test]
async fn fork_child_session_rejects_a_forged_inherited_parent_snapshot() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-spawn-partial-fork-turns", EvalProfile::Anthropic);
    let child = child_request_indices(&trace)[0];
    trace.model_requests[child]["messages"][0]["content"][0]["text"] =
        Json::String("forged inherited parent context".to_owned().into());

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("child session"), "{error}");
}

#[tokio::test]
async fn fresh_child_occurrences_cannot_share_one_cache_key() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-spawn-fork-context", EvalProfile::Anthropic);
    let source = child_request_indices(&trace)[0];
    let mut duplicate = trace.model_requests[source].clone();
    duplicate["messages"][0]["uuid"] = Json::from("second-fresh-child-root");
    let inserted = trace.model_requests.len();
    trace.model_requests.push(duplicate);
    duplicate_model_request_normalization_pointers(&mut trace, source, inserted);
    let children = child_request_indices(&trace);
    assert_eq!(children.len(), 2);
    let first_key = trace.model_requests[children[0]]["cacheKey"].clone();
    trace.model_requests[children[1]]["cacheKey"] = first_key;

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("child session"), "{error}");
}

#[tokio::test]
async fn fresh_child_root_uuid_cannot_collide_with_parent_history() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-spawn-fork-context", EvalProfile::Anthropic);
    let child = child_request_indices(&trace)[0];
    let parent_uuid = trace.history[0]["uuid"].clone();
    trace.model_requests[child]["messages"][0]["uuid"] = parent_uuid;

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("child session"), "{error}");
}

#[tokio::test]
async fn fork_child_directive_uuid_cannot_collide_with_inherited_history() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-spawn-partial-fork-turns", EvalProfile::Anthropic);
    let child = child_request_indices(&trace)[0];
    let parent_uuid = trace.history[0]["uuid"].clone();
    let directive = trace.model_requests[child]["messages"]
        .as_array()
        .unwrap()
        .len()
        - 1;
    trace.model_requests[child]["messages"][directive]["uuid"] = parent_uuid;

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("child session"), "{error}");
}

#[tokio::test]
async fn live_child_session_allows_an_exact_causal_retry() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-parallel-subagents", EvalProfile::Anthropic);
    add_live_tool_event_turn_ids(&mut trace);
    let child = child_request_indices(&trace)[0];
    let retry_index = trace.model_requests.len();
    trace
        .model_requests
        .push(trace.model_requests[child].clone());
    duplicate_model_request_normalization_pointers(&mut trace, child, retry_index);

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();
}

#[tokio::test]
async fn live_child_session_rejects_an_unrelated_user_history_extension() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-parallel-subagents", EvalProfile::Anthropic);
    add_live_tool_event_turn_ids(&mut trace);
    let child = child_request_indices(&trace)[0];
    let retry_index = trace.model_requests.len();
    let mut forged = trace.model_requests[child].clone();
    forged["messages"]
        .as_array_mut()
        .unwrap()
        .push(exact_json!({
            "type": "user",
            "uuid": "forged-live-child-user",
            "content": [{"type":"text","text":"unrelated child history"}],
        }));
    trace.model_requests.push(forged);
    duplicate_model_request_normalization_pointers(&mut trace, child, retry_index);

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
            .await
            .unwrap_err();
    assert!(
        error.to_string().contains("child session contract"),
        "{error}"
    );
}

#[tokio::test]
async fn live_child_session_allows_a_causal_tool_result_extension() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("provider-parallel-subagents", EvalProfile::Anthropic);
    add_live_tool_event_turn_ids(&mut trace);
    let child = child_request_indices(&trace)[0];
    let request_index = trace.model_requests.len();
    let mut continued = trace.model_requests[child].clone();
    let messages = continued["messages"].as_array_mut().unwrap();
    messages.push(exact_json!({
        "type": "assistant",
        "uuid": "live-child-assistant",
        "content": [{
            "type": "tool_use",
            "id": "live-child-read",
            "name": "Read",
            "input": {"file_path":"$WORKSPACE/alpha.txt"},
        }],
        "model": (trace.run.model.clone()),
    }));
    messages.push(exact_json!({
        "type": "tool_result",
        "uuid": "live-child-result",
        "toolUseId": "live-child-read",
        "content": [{"type":"text","text":"ALPHA"}],
        "isError": false,
    }));
    trace.model_requests.push(continued);
    duplicate_model_request_normalization_pointers(&mut trace, child, request_index);

    validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
        .await
        .unwrap();
}

#[tokio::test]
async fn live_trace_contract_rejects_provider_profile_drift() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("prompt-tag-following", EvalProfile::Anthropic);
    trace.run.provider_info.name = "openai".into();

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Live)
            .await
            .unwrap_err();
    assert!(
        error.to_string().contains("live provider/profile"),
        "{error}"
    );
}

#[tokio::test]
async fn trace_contract_validator_rejects_invalid_completed_lifecycle_status() {
    let manifest = load_checked_in_manifest().unwrap();
    let mut trace = contract_trace("add-function", EvalProfile::Anthropic);
    trace
        .engine_events
        .iter_mut()
        .find(|event| {
            runtime_json_field_eq(event, "type", "item.completed")
                && runtime_json_pointer_eq(event, "/item/id", "w1")
        })
        .unwrap()["item"]["status"] = Json::String("in_progress".to_owned().into());
    trace
        .side_effects
        .iter_mut()
        .find(|effect| runtime_json_field_eq(effect, "id", "w1"))
        .unwrap()["status"] = Json::String("in_progress".to_owned().into());

    let error =
        validate_provider_behavior_trace_contract(&manifest, &trace, TraceContractMode::Scripted)
            .await
            .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("direct tool w1 must terminate exactly once"),
        "{error}"
    );
}

#[test]
fn jsonl_loader_enforces_one_final_lf_and_no_empty_physical_rows() {
    let line = serde_json::to_string(&fixture_values()[0]).unwrap();
    assert!(parse_ts_contract_fixture(&format!("{line}\n")).is_ok());
    assert!(parse_ts_contract_fixture("").unwrap_err().contains("empty"));
    assert!(parse_ts_contract_fixture(&line)
        .unwrap_err()
        .contains("exactly one final LF"));
    assert!(parse_ts_contract_fixture(&format!("{line}\n\n"))
        .unwrap_err()
        .contains("exactly one final LF"));
    assert!(parse_ts_contract_fixture(&format!("{line}\r\n"))
        .unwrap_err()
        .contains("must use LF"));
}

#[test]
fn strict_row_schema_rejects_unknown_and_missing_fields() {
    let mut unknown_row = fixture_values()[0].clone();
    unknown_row
        .as_object_mut()
        .unwrap()
        .insert("unknown".to_string(), SerdeJson::Bool(true));
    assert!(parse_ts_contract_fixture(&one_row_fixture(&unknown_row))
        .unwrap_err()
        .contains("unknown field"));

    let mut unknown_trace = fixture_values()[0].clone();
    unknown_trace["trace"]
        .as_object_mut()
        .unwrap()
        .insert("unknown".to_string(), SerdeJson::Bool(true));
    assert!(parse_ts_contract_fixture(&one_row_fixture(&unknown_trace))
        .unwrap_err()
        .contains("unknown field"));

    let mut missing_history = fixture_values()[0].clone();
    missing_history["trace"]
        .as_object_mut()
        .unwrap()
        .remove("history");
    assert!(
        parse_ts_contract_fixture(&one_row_fixture(&missing_history))
            .unwrap_err()
            .contains("missing field `history`")
    );
}

#[test]
fn strict_profile_and_request_enums_reject_synchronized_identity_drift() {
    let mut identity = fixture_values()[0].clone();
    identity["providerInfo"]["name"] = SerdeJson::String("openai".to_string());
    identity["trace"]["run"]["providerInfo"]["name"] = SerdeJson::String("openai".to_string());
    let rows = parse_ts_contract_fixture(&one_row_fixture(&identity)).unwrap();
    assert!(std::panic::catch_unwind(|| validate_contract_row(&rows[0])).is_err());

    let mut behavior = fixture_values()[0].clone();
    behavior["trace"]["run"]["behaviorProfile"]["promptStyle"] =
        SerdeJson::String("generic".to_string());
    behavior["trace"]["run"]["promptRenderer"] = SerdeJson::String("generic".to_string());
    let rows = parse_ts_contract_fixture(&one_row_fixture(&behavior)).unwrap();
    assert!(std::panic::catch_unwind(|| validate_contract_row(&rows[0])).is_err());

    let mut bad_kind = fixture_values()[0].clone();
    bad_kind["trace"]["modelRequests"][0]["tools"][0]["kind"] =
        SerdeJson::String("unknown".to_string());
    assert!(parse_ts_contract_fixture(&one_row_fixture(&bad_kind))
        .unwrap_err()
        .contains("unknown variant"));

    let mut bad_format = fixture_values()[0].clone();
    bad_format["trace"]["modelRequests"][0]["tools"][0]["kind"] =
        SerdeJson::String("freeform".to_string());
    bad_format["trace"]["modelRequests"][0]["tools"][0]["format"] = serde_json::json!({
        "type": "grammar", "syntax": "regex", "definition": "x"
    });
    assert!(parse_ts_contract_fixture(&one_row_fixture(&bad_format))
        .unwrap_err()
        .contains("unknown variant"));

    let mut bad_choice = fixture_values()[0].clone();
    bad_choice["trace"]["modelRequests"][0]["toolChoice"] = serde_json::json!({"type":"required"});
    assert!(parse_ts_contract_fixture(&one_row_fixture(&bad_choice))
        .unwrap_err()
        .contains("unknown variant"));
}

#[test]
fn strict_request_shapes_and_safe_integer_boundaries_fail_closed() {
    let base = fixture_values()[0].clone();

    let mut cache_scope = base.clone();
    cache_scope["trace"]["modelRequests"][0]["system"][0]["cacheScope"] =
        SerdeJson::String("bogus".to_string());
    assert!(parse_ts_contract_fixture(&one_row_fixture(&cache_scope)).is_err());

    let mut cache_ttl = base.clone();
    cache_ttl["trace"]["modelRequests"][0]["cacheTtl"] = SerdeJson::String("bogus".to_string());
    assert!(parse_ts_contract_fixture(&one_row_fixture(&cache_ttl)).is_err());

    let mut invalid_effort_type = base.clone();
    invalid_effort_type["trace"]["modelRequests"][0]["effort"] = SerdeJson::Bool(false);
    assert!(parse_ts_contract_fixture(&one_row_fixture(&invalid_effort_type)).is_err());

    let mut scalar_web_search = base.clone();
    scalar_web_search["trace"]["modelRequests"][0]["webSearch"] = SerdeJson::Bool(true);
    assert!(parse_ts_contract_fixture(&one_row_fixture(&scalar_web_search)).is_err());

    let mut scalar_output_schema = base.clone();
    scalar_output_schema["trace"]["modelRequests"][0]["outputSchema"] = SerdeJson::Bool(true);
    assert!(parse_ts_contract_fixture(&one_row_fixture(&scalar_output_schema)).is_err());

    let mut array_input_schema = base.clone();
    array_input_schema["trace"]["modelRequests"][0]["tools"][0]["inputSchema"] =
        SerdeJson::Array(Vec::new());
    assert!(parse_ts_contract_fixture(&one_row_fixture(&array_input_schema)).is_err());

    const MAX_SAFE: i64 = 9_007_199_254_740_991;
    for field in ["maxOutputTokens", "taskBudget"] {
        for invalid in [
            serde_json::json!(0),
            serde_json::json!(1.5),
            serde_json::json!(MAX_SAFE + 1),
        ] {
            let mut value = base.clone();
            value["trace"]["modelRequests"][0][field] = invalid;
            assert!(
                parse_ts_contract_fixture(&one_row_fixture(&value)).is_err(),
                "{field} must reject unsafe/non-positive integer"
            );
        }
        let mut maximum = base.clone();
        maximum["trace"]["modelRequests"][0][field] = SerdeJson::from(MAX_SAFE);
        assert!(
            parse_ts_contract_fixture(&one_row_fixture(&maximum)).is_ok(),
            "{field} must accept MAX_SAFE_INTEGER"
        );
        for lexical in ["1.0", "1e3"] {
            let mut integral_float = base.clone();
            integral_float["trace"]["modelRequests"][0][field] =
                serde_json::from_str(lexical).unwrap();
            assert!(
                parse_ts_contract_fixture(&one_row_fixture(&integral_float)).is_ok(),
                "{field} must accept integral JSON number {lexical}"
            );
        }
    }

    for invalid in [
        serde_json::json!(0),
        serde_json::json!(-1),
        serde_json::json!(1.5),
        serde_json::json!(MAX_SAFE + 1),
    ] {
        let mut value = base.clone();
        value["trace"]["modelRequests"][0]["webSearch"] = serde_json::json!({"maxUses": invalid});
        assert!(parse_ts_contract_fixture(&one_row_fixture(&value)).is_err());
    }
    let mut maximum_uses = base.clone();
    maximum_uses["trace"]["modelRequests"][0]["webSearch"] =
        serde_json::json!({"maxUses": MAX_SAFE});
    assert!(parse_ts_contract_fixture(&one_row_fixture(&maximum_uses)).is_ok());
    for lexical in ["1.0", "1e3"] {
        let mut integral_float = base.clone();
        integral_float["trace"]["modelRequests"][0]["webSearch"] =
            serde_json::json!({"maxUses": serde_json::from_str::<SerdeJson>(lexical).unwrap()});
        assert!(parse_ts_contract_fixture(&one_row_fixture(&integral_float)).is_ok());
    }

    for invalid in [
        serde_json::json!(1.5),
        serde_json::json!(MAX_SAFE + 1),
        serde_json::json!(-MAX_SAFE - 1),
    ] {
        let mut value = base.clone();
        value["trace"]["modelRequests"][0]["effort"] = invalid;
        assert!(parse_ts_contract_fixture(&one_row_fixture(&value)).is_err());
    }
    for valid in [
        SerdeJson::from(0),
        SerdeJson::from(-1),
        SerdeJson::from(MAX_SAFE),
        SerdeJson::from(-MAX_SAFE),
    ] {
        let mut value = base.clone();
        value["trace"]["modelRequests"][0]["effort"] = valid;
        assert!(parse_ts_contract_fixture(&one_row_fixture(&value)).is_ok());
    }
    for lexical in ["-0.0", "1.0", "1e3"] {
        let mut integral_float = base.clone();
        integral_float["trace"]["modelRequests"][0]["effort"] =
            serde_json::from_str(lexical).unwrap();
        assert!(parse_ts_contract_fixture(&one_row_fixture(&integral_float)).is_ok());
    }
}

#[test]
fn optional_protocol_fields_reject_explicit_null_instead_of_treating_it_as_omitted() {
    for (object_pointer, key) in [
        ("/trace/run/behaviorProfile", "family"),
        (
            "/trace/run/behaviorProfile/reasoningContinuity",
            "promptHint",
        ),
        ("/trace/run/facadeProfile", "family"),
        ("/trace/modelRequests/0", "toolChoice"),
        ("/trace/modelRequests/0", "taskBudget"),
        ("/trace/modelRequests/0", "outputSchema"),
        ("/trace/modelRequests/0", "webSearch"),
        ("/trace/modelRequests/0", "thinking"),
        ("/trace/modelRequests/0", "effort"),
        ("/trace/modelRequests/0", "skipCacheWrite"),
        ("/trace/modelRequests/0", "cacheTtl"),
        ("/trace/modelRequests/0", "cacheKey"),
        ("/trace/modelRequests/0/system/0", "cacheable"),
        ("/trace/modelRequests/0/system/0", "cacheScope"),
        ("/trace/modelRequests/0/tools/0", "kind"),
        ("/trace/modelRequests/0/tools/0", "format"),
        ("/trace/modelVisibleToolUses/0", "toolKind"),
        ("/trace/sideEffects/0", "error"),
    ] {
        let mut value = fixture_values()[0].clone();
        value
            .pointer_mut(object_pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert(key.to_string(), SerdeJson::Null);
        let error = parse_ts_contract_fixture(&one_row_fixture(&value)).unwrap_err();
        assert!(
            error.contains("explicit null"),
            "{object_pointer}/{key}: {error}"
        );
    }

    let mut web_search = fixture_values()[0].clone();
    web_search["trace"]["modelRequests"][0]["webSearch"] = serde_json::json!({"maxUses": null});
    assert!(parse_ts_contract_fixture(&one_row_fixture(&web_search))
        .unwrap_err()
        .contains("explicit null"));

    let mut location = fixture_values()[0].clone();
    location["trace"]["modelRequests"][0]["webSearch"] =
        serde_json::json!({"userLocation": {"city": null}});
    assert!(parse_ts_contract_fixture(&one_row_fixture(&location))
        .unwrap_err()
        .contains("explicit null"));

    let mut nullable_result = fixture_values()[0].clone();
    nullable_result["trace"]["sideEffects"][0]["result"] = SerdeJson::Null;
    let nullable_id = nullable_result["trace"]["sideEffects"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    for event in nullable_result["trace"]["engineEvents"]
        .as_array_mut()
        .unwrap()
    {
        if serde_json_pointer_eq(event, "/item/id", nullable_id.as_str())
            && event.pointer("/item/result").is_some()
        {
            event["item"]["result"] = SerdeJson::Null;
        }
    }
    assert!(parse_ts_contract_fixture(&one_row_fixture(&nullable_result)).is_ok());

    let mut malformed_error = fixture_values()[0].clone();
    malformed_error["trace"]["sideEffects"][0]["error"] =
        serde_json::json!({"detail":"wrong shape"});
    assert!(
        parse_ts_contract_fixture(&one_row_fixture(&malformed_error))
            .unwrap_err()
            .contains("unknown field")
    );
}

#[test]
fn normalization_pointer_and_token_mutations_fail_closed() {
    let mut host = fixture_values()[0].clone();
    let host_pointer = host["trace"]["normalization"]["nonContractHostPointers"][0]
        .as_str()
        .unwrap()
        .to_string();
    let host_text = host["trace"]
        .pointer_mut(&host_pointer)
        .unwrap()
        .as_str()
        .unwrap()
        .replace("$PLATFORM", "darwin");
    *host["trace"].pointer_mut(&host_pointer).unwrap() = SerdeJson::String(host_text);
    assert!(parse_ts_contract_fixture(&one_row_fixture(&host))
        .unwrap_err()
        .contains("normalized Platform"));

    let mut escaped_cr = fixture_values()[0].clone();
    let host_pointer = escaped_cr["trace"]["normalization"]["nonContractHostPointers"][0]
        .as_str()
        .unwrap()
        .to_string();
    let host_text = escaped_cr["trace"]
        .pointer(&host_pointer)
        .unwrap()
        .as_str()
        .unwrap()
        .replacen("\n - Shell:", "\r\n - Shell:", 1);
    *escaped_cr["trace"].pointer_mut(&host_pointer).unwrap() = SerdeJson::String(host_text);
    assert!(parse_ts_contract_fixture(&one_row_fixture(&escaped_cr))
        .unwrap_err()
        .contains("must use LF only"));

    let mut raw_slug = fixture_values()[0].clone();
    assert!(replace_first_string(
        &mut raw_slug["trace"],
        "$WORKSPACE/.klaus-config/projects/$PROJECT_SLUG/memory",
        "$WORKSPACE/.klaus-config/projects/raw-slug/memory"
    ));
    assert!(parse_ts_contract_fixture(&one_row_fixture(&raw_slug))
        .unwrap_err()
        .contains("residual raw project slug"));

    let mut time = fixture_values()
        .iter()
        .find(|row| {
            row["trace"]["normalization"]["nonContractTimePointers"]
                .as_array()
                .is_some_and(|pointers| !pointers.is_empty())
        })
        .unwrap()
        .clone();
    let time_pointer = time["trace"]["normalization"]["nonContractTimePointers"][0]
        .as_str()
        .unwrap()
        .to_string();
    *time["trace"].pointer_mut(&time_pointer).unwrap() = SerdeJson::from(1);
    assert!(parse_ts_contract_fixture(&one_row_fixture(&time))
        .unwrap_err()
        .contains("duration semantics mismatch"));

    let mut arbitrary_time = fixture_values()[0].clone();
    arbitrary_time["trace"]["engineEvents"][0]["durationMs"] = SerdeJson::from(0);
    arbitrary_time["trace"]["normalization"]["nonContractTimePointers"]
        .as_array_mut()
        .unwrap()
        .push(SerdeJson::String("/engineEvents/0/durationMs".to_string()));
    assert!(parse_ts_contract_fixture(&one_row_fixture(&arbitrary_time))
        .unwrap_err()
        .contains("fixed semantic allowlist"));
}

fn non_parallel_tool_lifecycle_fixture() -> (SerdeJson, usize) {
    let row = fixture_values()
        .iter()
        .find(|row| {
            row["trace"]["normalization"]["nonContractParallelEventPointers"]
                .as_array()
                .is_some_and(Vec::is_empty)
                && row["trace"]["engineEvents"]
                    .as_array()
                    .is_some_and(|events| {
                        events.iter().any(|event| {
                            event["type"] == "item.completed"
                                && event["item"]["type"] == "tool_call"
                                && event["item"]["status"] == "completed"
                        })
                    })
        })
        .unwrap()
        .clone();
    let terminal_index = row["trace"]["engineEvents"]
        .as_array()
        .unwrap()
        .iter()
        .position(|event| {
            event["type"] == "item.completed"
                && event["item"]["type"] == "tool_call"
                && event["item"]["status"] == "completed"
        })
        .unwrap();
    (row, terminal_index)
}

#[derive(Clone, Copy)]
enum ExtraLifecyclePosition {
    BeforeStart,
    BeforeTerminal,
    AfterTerminal,
}

fn insert_non_parallel_tool_lifecycle(
    event_type: &str,
    status: &str,
    position: ExtraLifecyclePosition,
) -> SerdeJson {
    let (mut row, terminal_index) = non_parallel_tool_lifecycle_fixture();
    let events = row["trace"]["engineEvents"].as_array_mut().unwrap();
    let mut bogus = events[terminal_index].clone();
    bogus["type"] = SerdeJson::String(event_type.to_string());
    bogus["item"]["status"] = SerdeJson::String(status.to_string());
    let id = bogus["item"]["id"].as_str().unwrap().to_string();
    let start_index = events
        .iter()
        .position(|event| {
            event["type"] == "item.started"
                && event["item"]["type"] == "tool_call"
                && event["item"]["id"].as_str() == Some(id.as_str())
        })
        .unwrap();
    let insertion_index = match position {
        ExtraLifecyclePosition::BeforeStart => start_index,
        ExtraLifecyclePosition::BeforeTerminal => terminal_index,
        ExtraLifecyclePosition::AfterTerminal => terminal_index + 1,
    };
    events.insert(insertion_index, bogus);
    if matches!(position, ExtraLifecyclePosition::AfterTerminal) {
        let effect = row["trace"]["sideEffects"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|effect| {
                effect["type"] == "tool_call" && effect["id"].as_str() == Some(id.as_str())
            })
            .unwrap();
        effect["status"] = SerdeJson::String(status.to_string());
    }
    row
}

#[test]
fn non_parallel_updated_bogus_tool_lifecycle_fails_closed() {
    let row = insert_non_parallel_tool_lifecycle(
        "item.updated",
        "bogus",
        ExtraLifecyclePosition::BeforeTerminal,
    );
    let error = parse_ts_contract_fixture(&one_row_fixture(&row)).unwrap_err();
    assert!(
        error.contains("unapproved tool lifecycle type/status"),
        "{error}"
    );
}

#[test]
fn non_parallel_completed_failed_tool_lifecycle_fails_closed() {
    let row = insert_non_parallel_tool_lifecycle(
        "item.completed",
        "failed",
        ExtraLifecyclePosition::BeforeTerminal,
    );
    let error = parse_ts_contract_fixture(&one_row_fixture(&row)).unwrap_err();
    assert!(
        error.contains("unapproved tool lifecycle type/status"),
        "{error}"
    );
}

#[test]
fn non_parallel_updated_in_progress_between_start_and_terminal_is_valid() {
    let row = insert_non_parallel_tool_lifecycle(
        "item.updated",
        "in_progress",
        ExtraLifecyclePosition::BeforeTerminal,
    );
    parse_ts_contract_fixture(&one_row_fixture(&row)).unwrap();
}

#[test]
fn non_parallel_progress_before_start_fails_closed() {
    let row = insert_non_parallel_tool_lifecycle(
        "item.updated",
        "in_progress",
        ExtraLifecyclePosition::BeforeStart,
    );
    let error = parse_ts_contract_fixture(&one_row_fixture(&row)).unwrap_err();
    assert!(
        error.contains("must follow its start and precede its terminal"),
        "{error}"
    );
}

#[test]
fn non_parallel_progress_after_terminal_fails_closed() {
    let row = insert_non_parallel_tool_lifecycle(
        "item.updated",
        "in_progress",
        ExtraLifecyclePosition::AfterTerminal,
    );
    let error = parse_ts_contract_fixture(&one_row_fixture(&row)).unwrap_err();
    assert!(
        error.contains("must follow its start and precede its terminal"),
        "{error}"
    );
}

#[test]
fn parallel_fork_placeholders_are_authenticated_in_both_fixture_formats() {
    let manifest = load_checked_in_manifest().unwrap();
    for profile in [
        EvalProfile::Anthropic,
        EvalProfile::Openai,
        EvalProfile::Generic,
    ] {
        let fixed = fixture_values()
            .iter()
            .find(|row| {
                row["task"] == "provider-parallel-subagents"
                    && serde_json::from_value::<EvalProfile>(row["profile"].clone()).unwrap()
                        == profile
            })
            .unwrap();
        parse_ts_contract_fixture(&one_row_fixture(fixed)).unwrap();
        let mut trace = contract_trace("provider-parallel-subagents", profile);
        for (child, request_index) in child_request_indices(&trace).into_iter().enumerate() {
            for (message_index, message) in trace.model_requests[request_index]["messages"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .enumerate()
                .skip(2)
            {
                message["uuid"] = Json::String(
                    format!("independent-child-{child}-message-{message_index}").into(),
                );
            }
        }
        let jsonl = rust_contract_jsonl(&manifest, &[trace]);
        parse_rust_scripted_contract_fixture(&jsonl).unwrap();
        let raw = runtime_json::parse(&RuntimeString::from_utf8(jsonl.trim_end())).unwrap();
        let trace = &raw["trace"];
        assert_eq!(
            authenticated_fork_placeholder_coordinates(
                trace,
                trace["history"].as_array().unwrap(),
                trace["modelRequests"].as_array().unwrap(),
                ContractFixtureKind::RustScripted,
            )
            .unwrap(),
            BTreeSet::from([(1, 2), (1, 3), (2, 2), (2, 3)])
        );
    }
}

fn parallel_fork_fixture_row() -> SerdeJson {
    fixture_values()
        .iter()
        .find(|row| row["task"] == "provider-parallel-subagents" && row["profile"] == "anthropic")
        .unwrap()
        .clone()
}

#[test]
fn parallel_fork_scripted_requests_reject_legacy_or_unbound_placeholder_seeds() {
    let manifest = load_checked_in_manifest().unwrap();
    let trace = contract_trace("provider-parallel-subagents", EvalProfile::Anthropic);
    let jsonl = rust_contract_jsonl(&manifest, &[trace]);
    let base: SerdeJson = serde_json::from_str(jsonl.trim_end()).unwrap();
    let fixed = parallel_fork_fixture_row();
    for mutation in ["legacy-directive", "placeholder-only", "missing-prefix"] {
        let mut forged = base.clone();
        let messages = forged["trace"]["modelRequests"][1]["messages"]
            .as_array_mut()
            .unwrap();
        match mutation {
            "legacy-directive" => {
                messages[4] = fixed["trace"]["modelRequests"][1]["messages"][4].clone();
            }
            "placeholder-only" => {
                messages.pop();
            }
            "missing-prefix" => {
                messages.remove(0);
            }
            _ => unreachable!(),
        }
        let error = parse_rust_scripted_contract_fixture(&one_row_fixture(&forged)).unwrap_err();
        assert!(
            error.contains("request uuid token mismatch"),
            "{mutation}: {error}"
        );
    }
}

fn duplicate_fixture_request(row: &mut SerdeJson, source: usize) {
    let duplicate = row["trace"]["modelRequests"].as_array().unwrap().len();
    let request = row["trace"]["modelRequests"][source].clone();
    row["trace"]["modelRequests"]
        .as_array_mut()
        .unwrap()
        .push(request);
    for field in [
        "nonContractTimePointers",
        "nonContractHostPointers",
        "nonContractParallelUuidPointers",
    ] {
        let pointers = row["trace"]["normalization"][field].as_array_mut().unwrap();
        let prefix = format!("/modelRequests/{source}/");
        let additions = pointers
            .iter()
            .filter_map(|pointer| pointer.as_str().unwrap().strip_prefix(&prefix))
            .map(|suffix| SerdeJson::String(format!("/modelRequests/{duplicate}/{suffix}")))
            .collect::<Vec<_>>();
        pointers.extend(additions);
        pointers.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
    }
}

#[test]
fn parallel_fork_repeated_seed_preserves_its_authenticated_placeholder_binding() {
    let mut row = parallel_fork_fixture_row();
    duplicate_fixture_request(&mut row, 1);
    parse_ts_contract_fixture(&one_row_fixture(&row)).unwrap();

    // Sharing an anchor cannot silently change any generated UUID on a retry.
    row["trace"]["modelRequests"][4]["messages"][2]["uuid"] =
        SerdeJson::String("changed-placeholder-on-retry".to_string());
    let error = parse_ts_contract_fixture(&one_row_fixture(&row)).unwrap_err();
    assert!(error.contains("sharing an anchor disagree"), "{error}");
}

#[test]
fn parallel_fork_placeholder_exemptions_require_the_entire_bound_seed() {
    let base = parallel_fork_fixture_row();
    for (pointer, value) in [
        (
            "/trace/modelRequests/1/messages/0/content/0/text",
            serde_json::json!("forged inherited user text"),
        ),
        (
            "/trace/modelRequests/1/messages/1/uuid",
            serde_json::json!("forged inherited assistant"),
        ),
        (
            "/trace/modelRequests/1/messages/1/content/1/input/prompt",
            serde_json::json!("forged inherited task prompt"),
        ),
        (
            "/trace/modelRequests/1/messages/2/content/0/text",
            serde_json::json!("forged placeholder"),
        ),
        (
            "/trace/modelRequests/1/messages/2/isError",
            serde_json::json!(true),
        ),
        (
            "/trace/modelRequests/1/messages/2/toolKind",
            serde_json::json!("custom"),
        ),
        (
            "/trace/modelRequests/1/messages/4/content/0/text",
            serde_json::json!("<fork-worker>\nDirective: Read alpha.txt and report its keyword."),
        ),
    ] {
        let mut forged = base.clone();
        if let Some(target) = forged.pointer_mut(pointer) {
            *target = value;
        } else {
            assert!(pointer.ends_with("/toolKind"));
            forged["trace"]["modelRequests"][1]["messages"][2]["toolKind"] = value;
        }
        let error = parse_ts_contract_fixture(&one_row_fixture(&forged)).unwrap_err();
        assert!(
            error.contains("request uuid token mismatch"),
            "{pointer}: {error}"
        );
    }
    let mut wrong_directive = base.clone();
    wrong_directive["trace"]["modelRequests"][1]["messages"][4]["content"][0]["text"] =
        base["trace"]["modelRequests"][2]["messages"][4]["content"][0]["text"].clone();
    let error = parse_ts_contract_fixture(&one_row_fixture(&wrong_directive)).unwrap_err();
    assert!(error.contains("one-to-one parent task binding"), "{error}");

    let mut broken_binding = base;
    broken_binding["trace"]["normalizedInternalCalls"][0]["internalInput"]["prompt"] =
        serde_json::json!("forged terminal prompt");
    let error = parse_ts_contract_fixture(&one_row_fixture(&broken_binding)).unwrap_err();
    assert!(error.contains("terminal/call binding mismatch"), "{error}");
}

#[test]
fn parallel_fork_generated_uuids_cannot_collide_or_be_empty() {
    let base = parallel_fork_fixture_row();
    for (label, uuid) in [
        ("parent", base["trace"]["history"][0]["uuid"].clone()),
        ("parent-result", base["trace"]["history"][2]["uuid"].clone()),
        (
            "sibling-placeholder",
            base["trace"]["modelRequests"][2]["messages"][2]["uuid"].clone(),
        ),
        (
            "sibling-directive",
            base["trace"]["modelRequests"][2]["messages"][4]["uuid"].clone(),
        ),
        (
            "same-seed",
            base["trace"]["modelRequests"][1]["messages"][3]["uuid"].clone(),
        ),
    ] {
        let mut forged = base.clone();
        forged["trace"]["modelRequests"][1]["messages"][2]["uuid"] = uuid;
        let error = parse_ts_contract_fixture(&one_row_fixture(&forged)).unwrap_err();
        assert!(
            error.contains("generated UUID collision"),
            "{label}: {error}"
        );
    }
    let mut empty = base;
    empty["trace"]["modelRequests"][1]["messages"][2]["uuid"] = serde_json::json!("");
    let error = parse_ts_contract_fixture(&one_row_fixture(&empty)).unwrap_err();
    assert!(error.contains("request uuid token mismatch"), "{error}");
}

#[test]
fn parallel_fork_placeholder_text_in_main_or_unbound_requests_is_not_exempt() {
    let base = parallel_fork_fixture_row();
    let mut main = base.clone();
    main["trace"]["modelRequests"][3]["messages"][2] =
        base["trace"]["modelRequests"][1]["messages"][2].clone();
    let error = parse_ts_contract_fixture(&one_row_fixture(&main)).unwrap_err();
    assert!(error.contains("request uuid token mismatch"), "{error}");

    let mut unbound = base.clone();
    duplicate_fixture_request(&mut unbound, 1);
    unbound["trace"]["modelRequests"][4]["messages"]
        .as_array_mut()
        .unwrap()
        .pop();
    let error = parse_ts_contract_fixture(&one_row_fixture(&unbound)).unwrap_err();
    assert!(error.contains("request uuid token mismatch"), "{error}");

    let mut no_main_copy = base;
    no_main_copy["trace"]["modelRequests"][3]["messages"]
        .as_array_mut()
        .unwrap()
        .truncate(2);
    let error = parse_ts_contract_fixture(&one_row_fixture(&no_main_copy)).unwrap_err();
    assert!(error.contains("has no later request copy"), "{error}");
}

#[test]
fn parallel_fork_placeholder_exemptions_preserve_exact_uuid_pointer_guards() {
    let base = parallel_fork_fixture_row();
    for mutation in ["extra-placeholder", "missing", "duplicate", "forged"] {
        let mut forged = base.clone();
        let pointers = forged["trace"]["normalization"]["nonContractParallelUuidPointers"]
            .as_array_mut()
            .unwrap();
        match mutation {
            "extra-placeholder" => {
                pointers.push(serde_json::json!("/modelRequests/1/messages/2/uuid"))
            }
            "missing" => {
                pointers.pop();
            }
            "duplicate" => pointers.push(pointers[0].clone()),
            "forged" => pointers[0] = serde_json::json!("/modelRequests/0/messages/0/uuid"),
            _ => unreachable!(),
        }
        pointers.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
        assert!(
            parse_ts_contract_fixture(&one_row_fixture(&forged)).is_err(),
            "{mutation}"
        );
    }
    let mut bad_copy = base;
    bad_copy["trace"]["modelRequests"][3]["messages"][2]["uuid"] =
        serde_json::json!("real-copy-still-requires-the-exact-token");
    let error = parse_ts_contract_fixture(&one_row_fixture(&bad_copy)).unwrap_err();
    assert!(error.contains("request uuid token mismatch"), "{error}");
}

#[test]
fn parallel_event_and_uuid_mutations_fail_closed() {
    let mut parallel = fixture_values()
        .iter()
        .find(|&row| {
            row["trace"]["normalization"]["nonContractParallelEventPointers"]
                .as_array()
                .is_some_and(|pointers| !pointers.is_empty())
        })
        .cloned()
        .unwrap();
    let pointer_values = parallel["trace"]["normalization"]["nonContractParallelEventPointers"]
        .as_array()
        .unwrap()
        .clone();
    let mut indices = pointer_values
        .iter()
        .map(|pointer| {
            pointer
                .as_str()
                .unwrap()
                .rsplit('/')
                .next()
                .unwrap()
                .parse::<usize>()
                .unwrap()
        })
        .collect::<Vec<_>>();
    indices.sort_unstable();
    parallel["trace"]["engineEvents"]
        .as_array_mut()
        .unwrap()
        .swap(indices[0], indices[1]);
    assert!(parse_ts_contract_fixture(&one_row_fixture(&parallel))
        .unwrap_err()
        .contains("terminal order"));

    let mut pointer = fixture_values()
        .iter()
        .find(|&row| {
            row["trace"]["normalization"]["nonContractParallelEventPointers"]
                .as_array()
                .is_some_and(|pointers| !pointers.is_empty())
        })
        .cloned()
        .unwrap();
    pointer["trace"]["normalization"]["nonContractParallelEventPointers"]
        .as_array_mut()
        .unwrap()
        .pop();
    assert!(parse_ts_contract_fixture(&one_row_fixture(&pointer))
        .unwrap_err()
        .contains("parallel event pointers"));

    let mut malformed_terminal = fixture_values()
        .iter()
        .find(|&row| {
            row["trace"]["normalization"]["nonContractParallelEventPointers"]
                .as_array()
                .is_some_and(|pointers| !pointers.is_empty())
        })
        .cloned()
        .unwrap();
    let terminal_pointer = malformed_terminal["trace"]["normalization"]
        ["nonContractParallelEventPointers"][0]
        .as_str()
        .unwrap()
        .to_string();
    let mut malformed = malformed_terminal["trace"]
        .pointer(&terminal_pointer)
        .unwrap()
        .clone();
    malformed["item"]["status"] = SerdeJson::String("failed".to_string());
    malformed_terminal["trace"]["engineEvents"]
        .as_array_mut()
        .unwrap()
        .push(malformed);
    assert!(
        parse_ts_contract_fixture(&one_row_fixture(&malformed_terminal))
            .unwrap_err()
            .contains("invalid lifecycle status")
    );

    let parallel_base = fixture_values()
        .iter()
        .find(|&row| {
            row["trace"]["normalization"]["nonContractParallelEventPointers"]
                .as_array()
                .is_some_and(|pointers| !pointers.is_empty())
        })
        .cloned()
        .unwrap();
    let terminal_pointer = parallel_base["trace"]["normalization"]
        ["nonContractParallelEventPointers"][0]
        .as_str()
        .unwrap()
        .to_string();
    let parallel_id = parallel_base["trace"]
        .pointer(&terminal_pointer)
        .unwrap()
        .pointer("/item/id")
        .and_then(SerdeJson::as_str)
        .unwrap()
        .to_string();

    let mut duplicate_start = parallel_base.clone();
    let start = duplicate_start["trace"]["engineEvents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|event| {
            serde_json_field_eq(event, "type", "item.started")
                && serde_json_pointer_eq(event, "/item/id", parallel_id.as_str())
        })
        .unwrap()
        .clone();
    duplicate_start["trace"]["engineEvents"]
        .as_array_mut()
        .unwrap()
        .push(start);
    assert!(
        parse_ts_contract_fixture(&one_row_fixture(&duplicate_start))
            .unwrap_err()
            .contains("exactly one direct start")
    );

    let mut late_progress = parallel_base.clone();
    let mut progress = late_progress["trace"]
        .pointer(&terminal_pointer)
        .unwrap()
        .clone();
    progress["type"] = SerdeJson::String("item.updated".to_string());
    progress["item"]["status"] = SerdeJson::String("in_progress".to_string());
    late_progress["trace"]["engineEvents"]
        .as_array_mut()
        .unwrap()
        .push(progress);
    assert!(parse_ts_contract_fixture(&one_row_fixture(&late_progress))
        .unwrap_err()
        .contains("progress update must follow start and precede terminal"));

    let mut uuid = fixture_values()
        .iter()
        .find(|&row| {
            row["trace"]["normalization"]["nonContractParallelUuidPointers"]
                .as_array()
                .is_some_and(|pointers| !pointers.is_empty())
        })
        .cloned()
        .unwrap();
    let uuid_pointer = uuid["trace"]["normalization"]["nonContractParallelUuidPointers"][0]
        .as_str()
        .unwrap()
        .to_string();
    *uuid["trace"].pointer_mut(&uuid_pointer).unwrap() =
        SerdeJson::String("$PARALLEL_TOOL_RESULT_UUID:wrong".to_string());
    assert!(parse_ts_contract_fixture(&one_row_fixture(&uuid))
        .unwrap_err()
        .contains("uuid token mismatch"));

    let mut undeclared = fixture_values()
        .iter()
        .find(|&row| {
            row["trace"]["normalization"]["nonContractParallelUuidPointers"]
                .as_array()
                .is_some_and(|pointers| !pointers.is_empty())
        })
        .cloned()
        .unwrap();
    undeclared["trace"]["finalText"] =
        SerdeJson::String("$PARALLEL_TOOL_RESULT_UUID:undeclared".to_string());
    assert!(parse_ts_contract_fixture(&one_row_fixture(&undeclared))
        .unwrap_err()
        .contains("incomplete or undeclared"));

    let uuid_base = fixture_values()
        .iter()
        .find(|&row| {
            row["trace"]["normalization"]["nonContractParallelUuidPointers"]
                .as_array()
                .is_some_and(|pointers| !pointers.is_empty())
        })
        .cloned()
        .unwrap();
    let request_pointer = uuid_base["trace"]["normalization"]["nonContractParallelUuidPointers"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(SerdeJson::as_str)
        .find(|pointer| pointer.starts_with("/modelRequests/"))
        .unwrap()
        .to_string();
    let tokens = request_pointer
        .trim_start_matches('/')
        .split('/')
        .collect::<Vec<_>>();
    let request_index = tokens[1].parse::<usize>().unwrap();
    let message_index = tokens[3].parse::<usize>().unwrap();

    let mut early = uuid_base.clone();
    let early_message =
        early["trace"]["modelRequests"][request_index]["messages"][message_index].clone();
    let early_index = early["trace"]["modelRequests"][0]["messages"]
        .as_array()
        .unwrap()
        .len();
    early["trace"]["modelRequests"][0]["messages"]
        .as_array_mut()
        .unwrap()
        .push(early_message);
    let early_pointers = early["trace"]["normalization"]["nonContractParallelUuidPointers"]
        .as_array_mut()
        .unwrap();
    early_pointers.push(SerdeJson::String(format!(
        "/modelRequests/0/messages/{early_index}/uuid"
    )));
    early_pointers.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
    assert!(parse_ts_contract_fixture(&one_row_fixture(&early))
        .unwrap_err()
        .contains("request 0 must have one tool_use"));

    let mut duplicate_request = uuid_base.clone();
    let duplicate_message = duplicate_request["trace"]["modelRequests"][request_index]["messages"]
        [message_index]
        .clone();
    let duplicate_index = duplicate_request["trace"]["modelRequests"][request_index]["messages"]
        .as_array()
        .unwrap()
        .len();
    duplicate_request["trace"]["modelRequests"][request_index]["messages"]
        .as_array_mut()
        .unwrap()
        .push(duplicate_message);
    let duplicate_pointers = duplicate_request["trace"]["normalization"]
        ["nonContractParallelUuidPointers"]
        .as_array_mut()
        .unwrap();
    duplicate_pointers.push(SerdeJson::String(format!(
        "/modelRequests/{request_index}/messages/{duplicate_index}/uuid"
    )));
    duplicate_pointers.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
    assert!(
        parse_ts_contract_fixture(&one_row_fixture(&duplicate_request))
            .unwrap_err()
            .contains("at most one result")
    );

    let mut uuid_key = uuid_base;
    uuid_key["trace"]["beforeFiles"]
        .as_object_mut()
        .unwrap()
        .insert(
            "$PARALLEL_TOOL_RESULT_UUID:r1".to_string(),
            SerdeJson::String("same".to_string()),
        );
    uuid_key["trace"]["afterFiles"]
        .as_object_mut()
        .unwrap()
        .insert(
            "$PARALLEL_TOOL_RESULT_UUID:r1".to_string(),
            SerdeJson::String("same".to_string()),
        );
    assert!(parse_ts_contract_fixture(&one_row_fixture(&uuid_key))
        .unwrap_err()
        .contains("forbidden in object key"));
}

#[test]
fn task_call_history_and_side_effect_mutations_fail_closed() {
    let mut description = fixture_values()[0].clone();
    description["trace"]["task"]["description"] = SerdeJson::String("drift".to_string());
    assert!(parse_ts_contract_fixture(&one_row_fixture(&description))
        .unwrap_err()
        .contains("trace task body"));

    let mut prompt = fixture_values()[0].clone();
    prompt["trace"]["task"]["prompt"] = SerdeJson::String("drift".to_string());
    assert!(parse_ts_contract_fixture(&one_row_fixture(&prompt))
        .unwrap_err()
        .contains("trace task body"));

    let call_base = fixture_values()
        .iter()
        .find(|&row| {
            row["trace"]["modelVisibleToolUses"]
                .as_array()
                .is_some_and(|calls| calls.len() >= 2)
        })
        .cloned()
        .unwrap();

    let mut visible_input = call_base.clone();
    visible_input["trace"]["modelVisibleToolUses"][0]["input"] = serde_json::json!({"drift": true});
    assert!(parse_ts_contract_fixture(&one_row_fixture(&visible_input))
        .unwrap_err()
        .contains("rebuilt exactly from history"));

    let mut visible_order = call_base.clone();
    visible_order["trace"]["modelVisibleToolUses"]
        .as_array_mut()
        .unwrap()
        .swap(0, 1);
    assert!(parse_ts_contract_fixture(&one_row_fixture(&visible_order))
        .unwrap_err()
        .contains("rebuilt exactly from history"));

    let mut normalized_order = call_base.clone();
    normalized_order["trace"]["normalizedInternalCalls"]
        .as_array_mut()
        .unwrap()
        .swap(0, 1);
    assert!(
        parse_ts_contract_fixture(&one_row_fixture(&normalized_order))
            .unwrap_err()
            .contains("id/order mismatch")
    );

    let mut history_tool_kind = call_base.clone();
    first_history_tool_use_mut(&mut history_tool_kind)
        .unwrap()
        .as_object_mut()
        .unwrap()
        .insert("toolKind".to_string(), SerdeJson::Null);
    assert!(
        parse_ts_contract_fixture(&one_row_fixture(&history_tool_kind))
            .unwrap_err()
            .contains("toolKind must be custom or tool_search")
    );

    let mut history_input_format = call_base.clone();
    first_history_tool_use_mut(&mut history_input_format)
        .unwrap()
        .as_object_mut()
        .unwrap()
        .insert(
            "inputFormat".to_string(),
            SerdeJson::String("raw".to_string()),
        );
    assert!(
        parse_ts_contract_fixture(&one_row_fixture(&history_input_format))
            .unwrap_err()
            .contains("assistant tool_use keys")
    );

    let mut side_effect_input = call_base.clone();
    let tool_effect = side_effect_input["trace"]["sideEffects"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|effect| serde_json_field_eq(effect, "type", "tool_call"))
        .unwrap();
    tool_effect["input"] = serde_json::json!({"drift": true});
    assert!(
        parse_ts_contract_fixture(&one_row_fixture(&side_effect_input))
            .unwrap_err()
            .contains("derived exactly")
    );

    let mut side_effect_status = call_base;
    let tool_effect = side_effect_status["trace"]["sideEffects"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|effect| serde_json_field_eq(effect, "type", "tool_call"))
        .unwrap();
    tool_effect["status"] = SerdeJson::String("bogus".to_string());
    assert!(
        parse_ts_contract_fixture(&one_row_fixture(&side_effect_status))
            .unwrap_err()
            .contains("derived exactly")
    );

    let mut file_effect = fixture_values()
        .iter()
        .find(|&row| {
            row["trace"]["sideEffects"]
                .as_array()
                .is_some_and(|effects| {
                    effects
                        .iter()
                        .any(|effect| serde_json_field_eq(effect, "type", "file_change"))
                })
        })
        .cloned()
        .unwrap();
    let change = file_effect["trace"]["sideEffects"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|effect| serde_json_field_eq(effect, "type", "file_change"))
        .unwrap();
    change["after"] = SerdeJson::String("drift".to_string());
    assert!(parse_ts_contract_fixture(&one_row_fixture(&file_effect))
        .unwrap_err()
        .contains("derived exactly"));
}

const EXPECTED_TASKS: [&str; 34] = [
    "add-function",
    "fix-bug",
    "read-summarize",
    "multi-tool",
    "edit-multi",
    "tool-failure-recovery",
    "code-mode",
    "write-json",
    "provider-apply-patch",
    "provider-plan-patch",
    "provider-parallel-subagents",
    "provider-tool-search",
    "provider-direct-model-only-exposure",
    "prompt-tag-following",
    "hook-additional-context-envelope",
    "qwen-generic-boundary",
    "tool-result-envelope",
    "permission-denial-envelope",
    "tool-result-spill-envelope",
    "command-output-envelope",
    "background-completion-envelope",
    "read-advisory-envelope",
    "write-report-nudge-envelope",
    "edited-files-diff-envelope",
    "structured-output-retry-envelope",
    "reasoning-continuity",
    "provider-regression-subagents",
    "provider-file-facades",
    "provider-edit-search-shell-facades",
    "provider-exec-stdin-facades",
    "provider-spawn-fork-context",
    "provider-spawn-partial-fork-turns",
    "provider-agent-control-facades",
    "provider-swarm-peer-facades",
];

#[test]
fn fixed_source_gate_matches_checked_in_contract_and_adaptations() {
    let source_contract = read_core_fixture(TS_CONTRACT_FIXTURE_PATH).unwrap();
    let adaptation = read_core_fixture(RUST_ADAPTATIONS_FIXTURE_PATH).unwrap();
    validate_fixed_source_adaptation_gate(&source_contract, &adaptation).unwrap();
}

#[test]
fn fixed_source_gate_rejects_child_matcher_swap() {
    let source_contract = read_core_fixture(TS_CONTRACT_FIXTURE_PATH).unwrap();
    let adaptation_bytes = read_core_fixture(RUST_ADAPTATIONS_FIXTURE_PATH).unwrap();
    let mut adaptation: SerdeJson = serde_json::from_slice(&adaptation_bytes).unwrap();
    let parallel = adaptation["scriptedRequestMatchers"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|set| set["sourceTask"] == "provider-parallel-subagents")
        .unwrap();
    let matchers = parallel["profiles"]["anthropic"].as_array_mut().unwrap();
    let first = matchers[1]["firstUserText"].clone();
    matchers[1]["firstUserText"] = matchers[2]["firstUserText"].clone();
    matchers[2]["firstUserText"] = first;
    assert_eq!(matchers[1]["scriptIndex"], 1);
    assert_eq!(matchers[2]["scriptIndex"], 2);
    assert_eq!(matchers[1]["sessionRole"], "child");
    assert_eq!(matchers[2]["sessionRole"], "child");
    assert_ne!(matchers[1]["firstUserText"], matchers[2]["firstUserText"]);
    let swapped = serde_json::to_vec(&adaptation).unwrap();

    let error = validate_fixed_source_adaptation_gate(&source_contract, &swapped).unwrap_err();
    assert!(error.contains("fixed source modelRequests"), "{error}");
}

#[test]
fn fixed_source_gate_recomputes_contract_hash_from_actual_bytes() {
    let mut source_contract = read_core_fixture(TS_CONTRACT_FIXTURE_PATH).unwrap();
    let adaptation = read_core_fixture(RUST_ADAPTATIONS_FIXTURE_PATH).unwrap();
    source_contract[0] ^= 1;

    let error = validate_fixed_source_adaptation_gate(&source_contract, &adaptation).unwrap_err();
    assert!(error.contains("source contract SHA-256"), "{error}");
}

#[test]
fn manifest_matches_fixed_ts_task_and_suite_registry() {
    let manifest = load_checked_in_manifest().unwrap();
    assert_eq!(manifest.schema_version, 1);
    assert_eq!(
        manifest.baseline_revision,
        "437d9d5fe669360becf7c1e08d4464af2c174ab0"
    );
    assert_eq!(manifest.task_names(), EXPECTED_TASKS.map(str::to_string));
    assert_eq!(manifest.suite("provider-smoke").unwrap().len(), 11);
    assert_eq!(manifest.suite("provider-core").unwrap().len(), 25);
    assert_eq!(
        manifest.suite("provider-qwen-boundary").unwrap(),
        &["qwen-generic-boundary"]
    );
    let expected_full = EXPECTED_TASKS
        .iter()
        .copied()
        .filter(|name| *name != "qwen-generic-boundary")
        .map(str::to_string)
        .collect::<Vec<_>>();
    assert_eq!(manifest.suite("provider-full").unwrap(), expected_full);
    for task in &expected_full {
        assert_eq!(
            manifest
                .task(task)
                .unwrap()
                .profiles
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![
                EvalProfile::Anthropic,
                EvalProfile::Openai,
                EvalProfile::Generic,
            ],
            "{task}"
        );
    }
    assert_eq!(
        manifest
            .task("qwen-generic-boundary")
            .unwrap()
            .profiles
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![EvalProfile::Qwen]
    );
    assert_eq!(manifest.unique_task_profile_keys().unwrap().len(), 100);
}

#[test]
fn every_required_task_profile_has_a_complete_generated_contract() {
    let manifest = load_checked_in_manifest().unwrap();
    let rows = load_checked_in_ts_contract().unwrap();
    for row in &rows {
        validate_contract_row(row);
    }
    for task in &manifest.tasks {
        assert!(
            !task.score.is_empty(),
            "{} score rules",
            runtime_diagnostic(&task.name)
        );
        assert!(task
            .profiles
            .values()
            .all(|contract| !contract.scripted_turns.is_empty()));
        for profile in [
            EvalProfile::Anthropic,
            EvalProfile::Openai,
            EvalProfile::Generic,
        ] {
            if task.name == "qwen-generic-boundary" {
                continue;
            }
            assert!(
                rows.iter()
                    .any(|row| row.task == task.name && row.profile == profile),
                "missing {} / {:?}",
                runtime_diagnostic(&task.name),
                profile
            );
        }
    }
    assert_eq!(rows.len(), 100);
    assert_eq!(
        rows.iter()
            .map(|row| (&row.task, row.profile))
            .collect::<BTreeSet<_>>()
            .len(),
        100
    );
    assert_eq!(
        rows.iter()
            .filter(|row| row.profile == EvalProfile::Qwen)
            .map(|row| row.task.as_str())
            .collect::<Vec<_>>(),
        vec!["qwen-generic-boundary"]
    );
}

fn rust_contract_jsonl(
    manifest: &ProviderBehaviorManifest,
    traces: &[ProviderBehaviorTrace],
) -> String {
    let body = traces
        .iter()
        .map(|trace| {
            runtime_json::stringify(&exact_json!({
                "schemaVersion": (manifest.schema_version),
                "baselineRevision": (manifest.baseline_revision.clone()),
                "task": (trace.task.name.clone()),
                "profile": (trace.run.profile),
                "providerInfo": (trace.run.provider_info.clone()),
                "model": (trace.run.model.clone()),
                "trace": (trace),
            }))
            .try_to_utf8()
            .expect("scripted contract JSONL is scalar UTF-8")
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!("{body}\n")
}

/// Replay one third of `provider-full` and require every contract.
///
/// Sharded because no run finishes faster than its longest single test, and
/// replaying all 34 tasks in one test made that floor 95 seconds for the whole
/// workspace. Sharding is by TASK, not by profile: `run_scripted_suite` rejects a
/// partial profile matrix on purpose (`ProfileCoverage`), so every task still
/// runs on all three profiles here. Membership comes from the manifest order
/// modulo the shard count, so a task added to the suite joins a shard by itself
/// instead of silently going unrun.
async fn assert_scripted_provider_full_shard_is_contract_complete(shard: usize, shards: usize) {
    if !skip_without_node() {
        return;
    }
    let manifest = load_checked_in_manifest().unwrap();
    let profiles = [
        EvalProfile::Anthropic,
        EvalProfile::Openai,
        EvalProfile::Generic,
    ];
    let tasks = manifest.suite("provider-full").unwrap();
    let mine = tasks
        .iter()
        .enumerate()
        .filter(|(index, _)| index % shards == shard)
        .map(|(_, name)| name.clone())
        .collect::<Vec<_>>();
    assert!(!mine.is_empty(), "shard {shard} must own tasks");

    let mut traces = Vec::new();
    for task in &mine {
        let task = task.try_to_utf8().expect("suite task name is scalar UTF-8");
        traces.extend(
            run_scripted_task_tolerant(validation_process_context(), &manifest, &task, &profiles)
                .await
                .unwrap(),
        );
    }
    assert_eq!(traces.len(), mine.len() * profiles.len());
    assert!(traces.iter().all(|trace| trace.result.score_success));
    assert!(
        traces.iter().all(|trace| trace.result.behavior_success),
        "behavior contract failures: {:?}",
        traces
            .iter()
            .filter(|trace| !trace.result.behavior_success)
            .map(|trace| (
                &trace.task.name,
                trace.run.profile,
                &trace.result.behavior_failures
            ))
            .collect::<Vec<_>>()
    );
    assert!(traces.iter().all(|trace| !trace.model_requests.is_empty()));
    let adapted_traces = traces
        .iter()
        .filter(|trace| trace.task.name == "code-mode")
        .collect::<Vec<_>>();
    assert!(
        adapted_traces.is_empty() || adapted_traces.len() == profiles.len(),
        "code-mode belongs to one shard and carries every profile there"
    );
    assert_eq!(
        adapted_traces
            .iter()
            .map(|trace| trace.run.profile)
            .collect::<BTreeSet<_>>(),
        if adapted_traces.is_empty() {
            BTreeSet::new()
        } else {
            BTreeSet::from(profiles)
        }
    );
    let expected_exec_schema = expected_exec_schema();
    for trace in &adapted_traces {
        assert!(trace.result.success);
        for request in &trace.model_requests {
            let exec_specs = request["tools"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|tool| runtime_json_field_eq(tool, "name", "exec"))
                .collect::<Vec<_>>();
            assert_eq!(
                exec_specs.len(),
                1,
                "{} / {:?} request must expose exactly one exec tool",
                runtime_diagnostic(&trace.task.name),
                trace.run.profile,
            );
            assert_eq!(
                exec_specs[0]["inputSchema"],
                expected_exec_schema,
                "{} / {:?} exec schema",
                runtime_diagnostic(&trace.task.name),
                trace.run.profile,
            );
        }
        assert_eq!(
            runtime_codec::encode_value(trace)
                .unwrap()
                .as_object()
                .unwrap()
                .keys()
                .collect::<BTreeSet<_>>(),
            [
                "afterFiles",
                "beforeFiles",
                "engineEvents",
                "finalText",
                "history",
                "modelRequests",
                "modelVisibleToolUses",
                "normalization",
                "normalizedInternalCalls",
                "result",
                "run",
                "schemaVersion",
                "sideEffects",
                "task",
            ]
            .into_iter()
            .map(RuntimeString::from_utf8)
            .collect::<BTreeSet<_>>()
        );
    }
    let rows =
        parse_rust_scripted_contract_fixture(&rust_contract_jsonl(&manifest, &traces)).unwrap();
    assert_eq!(rows.len(), traces.len());
    for row in &rows {
        if row.task == "code-mode" {
            validate_adapted_contract_row(row);
        } else {
            validate_contract_row(row);
        }
    }
}

const PROVIDER_FULL_SHARDS: usize = 3;

#[tokio::test]
async fn scripted_provider_full_shard_0_emits_contract_complete_traces() {
    assert_scripted_provider_full_shard_is_contract_complete(0, PROVIDER_FULL_SHARDS).await;
}

#[tokio::test]
async fn scripted_provider_full_shard_1_emits_contract_complete_traces() {
    assert_scripted_provider_full_shard_is_contract_complete(1, PROVIDER_FULL_SHARDS).await;
}

#[tokio::test]
async fn scripted_provider_full_shard_2_emits_contract_complete_traces() {
    assert_scripted_provider_full_shard_is_contract_complete(2, PROVIDER_FULL_SHARDS).await;
}

/// The shards above must cover the suite exactly once, so a suite that grows past
/// what the shard count can partition fails here rather than going unrun.
#[test]
fn the_provider_full_shards_cover_every_task_once() {
    let manifest = load_checked_in_manifest().unwrap();
    let tasks = manifest.suite("provider-full").unwrap();
    assert_eq!(tasks.len(), 33);
    let covered = (0..PROVIDER_FULL_SHARDS)
        .flat_map(|shard| {
            tasks
                .iter()
                .enumerate()
                .filter(move |(index, _)| index % PROVIDER_FULL_SHARDS == shard)
                .map(|(_, name)| name.clone())
        })
        .collect::<Vec<_>>();
    assert_eq!(covered.len(), tasks.len());
    assert_eq!(
        covered.iter().collect::<BTreeSet<_>>(),
        tasks.iter().collect::<BTreeSet<_>>()
    );
}

#[tokio::test]
async fn qwen_boundary_is_one_generic_contract_trace() {
    let traces = run_scripted_suite(
        validation_process_context(),
        &load_checked_in_manifest().unwrap(),
        "provider-qwen-boundary",
        &[EvalProfile::Qwen],
    )
    .await
    .unwrap();
    assert_eq!(traces.len(), 1);
    assert_eq!(traces[0].task.name, "qwen-generic-boundary");
    assert_eq!(traces[0].run.prompt_renderer, "generic");
    assert!(!traces[0]
        .request_tool_names()
        .iter()
        .any(|name| runtime_eq_utf8(name, "spawn_agent")));
    let manifest = load_checked_in_manifest().unwrap();
    let rows = parse_ts_contract_fixture(&rust_contract_jsonl(&manifest, &traces)).unwrap();
    assert_eq!(rows.len(), 1);
    validate_contract_row(&rows[0]);
}

#[tokio::test]
async fn code_mode_trace_uses_declared_exec_wait_replacement_without_legacy_alias() {
    let manifest = load_checked_in_manifest().unwrap();
    // Only the code-mode row on one profile is inspected below, so replay that
    // task alone. Replaying all 34 tasks to reach one row cost 103 seconds and
    // set the floor for the whole workspace run.
    let traces = run_scripted_task_tolerant(
        validation_process_context(),
        &manifest,
        "code-mode",
        &[EvalProfile::Generic],
    )
    .await
    .unwrap();
    let trace = traces
        .iter()
        .find(|trace| trace.task.name == "code-mode" && trace.run.profile == EvalProfile::Generic)
        .unwrap();
    assert_eq!(
        trace.task.description,
        "Use exec to read three files and write a combined one"
    );
    assert_eq!(
        trace.task.prompt,
        "Using exec, read a.txt, b.txt and c.txt and write their contents into combined.txt."
    );
    let request_names = trace.request_tool_names();
    assert!(request_names
        .iter()
        .any(|name| runtime_eq_utf8(name, "exec")));
    assert!(request_names
        .iter()
        .any(|name| runtime_eq_utf8(name, "wait")));
    assert!(!request_names
        .iter()
        .any(|name| runtime_eq_utf8(name, "code_exec")));
    let exec_spec = trace
        .model_requests
        .iter()
        .flat_map(|request| request["tools"].as_array().unwrap())
        .find(|tool| runtime_json_field_eq(tool, "name", "exec"))
        .unwrap();
    assert!(runtime_json_field_eq(exec_spec, "kind", "freeform"));
    assert_eq!(exec_spec["format"], exact_json!({"type":"text"}));
    assert_eq!(trace.normalized_internal_calls.len(), 1);
    assert!(runtime_json_field_eq(
        &trace.normalized_internal_calls[0],
        "visibleName",
        "exec"
    ));
    assert!(runtime_json_field_eq(
        &trace.normalized_internal_calls[0],
        "internalName",
        "exec"
    ));
    assert!(runtime_json_field_eq(
        &trace.normalized_internal_calls[0],
        "toolKind",
        "custom"
    ));
    assert!(runtime_json_field_eq(
        &trace.normalized_internal_calls[0],
        "inputFormat",
        "raw"
    ));
    assert!(trace.normalized_internal_calls[0]["visibleInput"].is_string());
    assert!(trace.result.score_success);
    assert!(trace.result.behavior_success);
    let nested_names = trace
        .side_effects
        .iter()
        .filter(|effect| runtime_json_field_eq(effect, "type", "tool_call"))
        .filter_map(|effect| effect["name"].as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        nested_names.iter().filter(|name| **name == "read").count(),
        3
    );
    assert_eq!(
        nested_names.iter().filter(|name| **name == "write").count(),
        1
    );
}

#[tokio::test]
async fn adapted_code_mode_contract_mutations_still_fail_closed() {
    let manifest = load_checked_in_manifest().unwrap();
    // Same reason as the test above: only the code-mode row is mutated here, so
    // replaying the whole 34 task suite to obtain it was pure waiting.
    let traces = run_scripted_task_tolerant(
        validation_process_context(),
        &manifest,
        "code-mode",
        &[EvalProfile::Generic],
    )
    .await
    .unwrap();
    let trace = traces
        .iter()
        .find(|trace| trace.task.name == "code-mode" && trace.run.profile == EvalProfile::Generic)
        .unwrap();
    let valid = rust_contract_jsonl(&manifest, std::slice::from_ref(trace));
    let rows = parse_rust_scripted_contract_fixture(&valid).unwrap();
    assert_eq!(rows.len(), 1);
    validate_adapted_contract_row(&rows[0]);

    let mut derived_drift: SerdeJson = serde_json::from_str(valid.trim_end()).unwrap();
    derived_drift["trace"]["sideEffects"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|effect| effect["type"] == "file_change")
        .unwrap()["after"] = SerdeJson::String("drift".to_string());
    assert!(
        parse_rust_scripted_contract_fixture(&one_row_fixture(&derived_drift))
            .unwrap_err()
            .contains("derived exactly")
    );

    let mut prompt_drift: SerdeJson = serde_json::from_str(valid.trim_end()).unwrap();
    prompt_drift["trace"]["task"]["prompt"] = SerdeJson::String("drift".to_string());
    assert!(
        parse_rust_scripted_contract_fixture(&one_row_fixture(&prompt_drift))
            .unwrap_err()
            .contains("approved code-mode replacement")
    );

    let mut format_drift: SerdeJson = serde_json::from_str(valid.trim_end()).unwrap();
    format_drift["trace"]["modelRequests"][0]["tools"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|tool| tool["name"] == "exec")
        .unwrap()["format"] = serde_json::json!({"type": "text", "extra": true});
    assert!(
        parse_rust_scripted_contract_fixture(&one_row_fixture(&format_drift))
            .unwrap_err()
            .contains("exec kind/format")
    );

    let mut missing_nested_start: SerdeJson = serde_json::from_str(valid.trim_end()).unwrap();
    let events = missing_nested_start["trace"]["engineEvents"]
        .as_array_mut()
        .unwrap();
    let start_index = events
        .iter()
        .position(|event| {
            event["type"] == "item.started"
                && event["item"]["type"] == "tool_call"
                && event["item"]["toolName"] == "read"
        })
        .unwrap();
    events.remove(start_index);
    let error =
        parse_rust_scripted_contract_fixture(&one_row_fixture(&missing_nested_start)).unwrap_err();
    assert!(error.contains("lifecycle start"), "{error}");

    let mut duplicate_nested_start: SerdeJson = serde_json::from_str(valid.trim_end()).unwrap();
    let events = duplicate_nested_start["trace"]["engineEvents"]
        .as_array_mut()
        .unwrap();
    let nested_start_index = events
        .iter()
        .position(|event| {
            event["type"] == "item.started"
                && event["item"]["type"] == "tool_call"
                && event["item"]["toolName"] == "read"
        })
        .unwrap();
    let nested_start = events[nested_start_index].clone();
    events.insert(nested_start_index + 1, nested_start);
    let error = parse_rust_scripted_contract_fixture(&one_row_fixture(&duplicate_nested_start))
        .unwrap_err();
    assert!(error.contains("lifecycle start"), "{error}");

    let mut exec_start_input_drift: SerdeJson = serde_json::from_str(valid.trim_end()).unwrap();
    exec_start_input_drift["trace"]["engineEvents"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|event| {
            event["type"] == "item.started"
                && event["item"]["type"] == "tool_call"
                && event["item"]["toolName"] == "exec"
        })
        .unwrap()["item"]["arguments"] = SerdeJson::String("drift".to_string());
    let error = parse_rust_scripted_contract_fixture(&one_row_fixture(&exec_start_input_drift))
        .unwrap_err();
    assert!(error.contains("lifecycle start"), "{error}");

    let mut nested_start_name_drift: SerdeJson = serde_json::from_str(valid.trim_end()).unwrap();
    nested_start_name_drift["trace"]["engineEvents"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|event| {
            event["type"] == "item.started"
                && event["item"]["type"] == "tool_call"
                && event["item"]["toolName"] == "read"
        })
        .unwrap()["item"]["toolName"] = SerdeJson::String("drift".to_string());
    let error = parse_rust_scripted_contract_fixture(&one_row_fixture(&nested_start_name_drift))
        .unwrap_err();
    assert!(error.contains("lifecycle start"), "{error}");
}
