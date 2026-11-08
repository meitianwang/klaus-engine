use crate::mcp::mcp_hub::{
    McpCatalog, McpCatalogEntry, McpCatalogSearchOptions, McpInstallFromCatalogOptions,
};
use crate::mcp::tool_wrapper::{wrap_mcp_tool, McpToolCall, WrapMcpToolOptions};
use crate::mcp::types::{
    McpCallResult, McpContentBlock, McpServerConfig, McpToolAnnotations, McpToolDescriptor,
};
use crate::runtime_process::{RuntimeEnv, RuntimeProcessContext};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeJson as Value, RuntimeObject, RuntimePath,
    RuntimeString,
};
use crate::tools::builtins::mcp_prompts::{
    create_get_mcp_prompt_tool, create_list_mcp_prompts_tool,
};
use crate::tools::builtins::mcp_resources::{
    create_list_mcp_resources_tool, create_read_mcp_resource_tool,
};
use crate::tools::builtins::mcp_search::{create_mcp_search_tool, McpSearchToolOptions};
use crate::types::abort::AbortSignal;
use crate::types::file_state::{FileStateCache, FileStateEntry, RecordOptions};
use crate::types::json::Json;
use crate::types::permission::PermissionContext;
use crate::types::sandbox::{ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec};
use crate::types::tool::{
    BoxFuture, McpPromptArgument, McpPromptDescriptor, McpResourceClient, McpResourceContent,
    McpResourceDescriptor, Tool, ToolContext, ToolProgress, ToolResult,
};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

const SCENARIO: &str = "mcp-resource-prompt-search-wrapper";

macro_rules! json {
    (@array [$($elements:expr,)*]) => {
        vec![$($elements,)*]
    };
    (@array [$($elements:expr),*]) => {
        vec![$($elements),*]
    };
    (@array [$($elements:expr,)*] null $($rest:tt)*) => {
        json!(@array [$($elements,)* Value::Null] $($rest)*)
    };
    (@array [$($elements:expr,)*] true $($rest:tt)*) => {
        json!(@array [$($elements,)* Value::Bool(true)] $($rest)*)
    };
    (@array [$($elements:expr,)*] false $($rest:tt)*) => {
        json!(@array [$($elements,)* Value::Bool(false)] $($rest)*)
    };
    (@array [$($elements:expr,)*] [$($array:tt)*] $($rest:tt)*) => {
        json!(@array [$($elements,)* json!([$($array)*])] $($rest)*)
    };
    (@array [$($elements:expr,)*] {$($object:tt)*} $($rest:tt)*) => {
        json!(@array [$($elements,)* json!({$($object)*})] $($rest)*)
    };
    (@array [$($elements:expr,)*] $next:expr, $($rest:tt)*) => {
        json!(@array [$($elements,)* json!($next),] $($rest)*)
    };
    (@array [$($elements:expr,)*] $last:expr) => {
        json!(@array [$($elements,)* json!($last)])
    };
    (@array [$($elements:expr),*] , $($rest:tt)*) => {
        json!(@array [$($elements,)*] $($rest)*)
    };

    (@object $object:ident) => {};
    (@object $object:ident ,) => {};
    (@object $object:ident $key:literal : null, $($rest:tt)*) => {{
        $object.insert($key, Value::Null);
        json!(@object $object $($rest)*);
    }};
    (@object $object:ident $key:literal : null) => {{
        $object.insert($key, Value::Null);
    }};
    (@object $object:ident $key:literal : true, $($rest:tt)*) => {{
        $object.insert($key, Value::Bool(true));
        json!(@object $object $($rest)*);
    }};
    (@object $object:ident $key:literal : true) => {{
        $object.insert($key, Value::Bool(true));
    }};
    (@object $object:ident $key:literal : false, $($rest:tt)*) => {{
        $object.insert($key, Value::Bool(false));
        json!(@object $object $($rest)*);
    }};
    (@object $object:ident $key:literal : false) => {{
        $object.insert($key, Value::Bool(false));
    }};
    (@object $object:ident $key:literal : [$($array:tt)*], $($rest:tt)*) => {{
        $object.insert($key, json!([$($array)*]));
        json!(@object $object $($rest)*);
    }};
    (@object $object:ident $key:literal : [$($array:tt)*]) => {{
        $object.insert($key, json!([$($array)*]));
    }};
    (@object $object:ident $key:literal : {$($nested:tt)*}, $($rest:tt)*) => {{
        $object.insert($key, json!({$($nested)*}));
        json!(@object $object $($rest)*);
    }};
    (@object $object:ident $key:literal : {$($nested:tt)*}) => {{
        $object.insert($key, json!({$($nested)*}));
    }};
    (@object $object:ident $key:literal : $value:expr, $($rest:tt)*) => {{
        $object.insert($key, json!($value));
        json!(@object $object $($rest)*);
    }};
    (@object $object:ident $key:literal : $value:expr) => {{
        $object.insert($key, json!($value));
    }};

    (null) => { Value::Null };
    (true) => { Value::Bool(true) };
    (false) => { Value::Bool(false) };
    ([]) => { Value::Array(Vec::new()) };
    ([$($tokens:tt)+]) => {
        Value::Array(json!(@array [] $($tokens)+))
    };
    ({}) => { Value::Object(RuntimeObject::new()) };
    ({$($tokens:tt)+}) => {{
        let mut object = RuntimeObject::new();
        json!(@object object $($tokens)+);
        Value::Object(object)
    }};
    ($value:expr) => {
        runtime_codec::encode_value(&$value).expect("MCP trace exact JSON expression must encode")
    };
}

pub async fn mcp_trace(
    process_context: &RuntimeProcessContext,
) -> Result<Value, RuntimeCodecError> {
    let clients = make_clients();
    let ctx = ctx_with_clients(process_context, clients);

    let list_resources = create_list_mcp_resources_tool();
    let read_resource = create_read_mcp_resource_tool();
    let list_prompts = create_list_mcp_prompts_tool();
    let get_prompt = create_get_mcp_prompt_tool();

    let search = create_mcp_search_tool(McpSearchToolOptions {
        backend: Some(Box::new(FakeCatalog::new())),
    });
    let search_no_backend = create_mcp_search_tool(McpSearchToolOptions { backend: None });

    let wrapped_events: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let wrapped = wrap_mcp_tool(
        "alpha",
        McpToolDescriptor {
            name: "lookup".into(),
            title: None,
            description: Some("Lookup facts.".into()),
            input_schema: Some(json!({
                "type": "object",
                "properties": { "q": { "type": "string" } },
                "required": ["q"],
                "additionalProperties": false
            })),
            annotations: Some(McpToolAnnotations {
                read_only_hint: Some(true),
                idempotent_hint: None,
                destructive_hint: Some(true),
                open_world_hint: None,
            }),
            meta: Some(json_object(json!({
                "anthropic/searchHint": "  lookup   facts\nquickly  "
            }))),
        },
        wrapped_call(),
        Some(WrapMcpToolOptions {
            max_result_chars: Some(44),
            ..Default::default()
        }),
    );

    let wrapped_events_for_cb = wrapped_events.clone();
    let on_progress: ToolProgress = Box::new(move |event| {
        wrapped_events_for_cb
            .lock()
            .unwrap()
            .push(runtime_codec::encode_value(&event).expect("tool event has an exact codec"));
        Box::pin(async {})
    });
    let wrapped_result = wrapped
        .call(
            json!({ "q": "emoji" }),
            &make_ctx(process_context, "/tmp/klaus-mcp-trace"),
            Some(on_progress),
        )
        .await
        .unwrap();

    let trace = json!({
        "scenario": SCENARIO,
        "metadata": [
            tool_meta(&list_resources, &json!({})),
            tool_meta(&read_resource, &json!({ "server": "alpha", "uri": "res://note" })),
            tool_meta(&list_prompts, &json!({})),
            tool_meta(&get_prompt, &json!({ "server": "alpha", "name": "brief" })),
            tool_meta(&search, &json!({ "query": "github integration" })),
            tool_meta(&wrapped, &json!({ "q": "emoji" })),
        ],
        "resources": {
            "listAll": serializable(list_resources.call(json!({}), &ctx, None).await.unwrap()),
            "listFiltered": serializable(list_resources.call(json!({ "server": "alpha" }), &ctx, None).await.unwrap()),
            "listUnknown": serializable(list_resources.call(json!({ "server": "missing" }), &ctx, None).await.unwrap()),
            "readText": serializable(read_resource.call(json!({ "server": "alpha", "uri": "res://note" }), &ctx, None).await.unwrap()),
            "readBlob": serializable(read_resource.call(json!({ "server": "alpha", "uri": "res://bin" }), &ctx, None).await.unwrap()),
            "readUnknown": serializable(read_resource.call(json!({ "server": "missing", "uri": "res://note" }), &ctx, None).await.unwrap()),
        },
        "prompts": {
            "listAll": serializable(list_prompts.call(json!({}), &ctx, None).await.unwrap()),
            "listNoPromptServer": serializable(list_prompts.call(json!({ "server": "beta" }), &ctx, None).await.unwrap()),
            "listUnknown": serializable(list_prompts.call(json!({ "server": "missing" }), &ctx, None).await.unwrap()),
            "getRendered": serializable(get_prompt.call(json!({ "server": "alpha", "name": "brief", "arguments": { "topic": "rust" } }), &ctx, None).await.unwrap()),
            "getEmpty": serializable(get_prompt.call(json!({ "server": "alpha", "name": "empty" }), &ctx, None).await.unwrap()),
            "getNoPromptServer": serializable(get_prompt.call(json!({ "server": "beta", "name": "brief" }), &ctx, None).await.unwrap()),
            "getUnknown": serializable(get_prompt.call(json!({ "server": "missing", "name": "brief" }), &ctx, None).await.unwrap()),
        },
        "search": {
            "summarizeUtf16": search.summarize(&json!({ "query": "A😀".repeat(40) })),
            "found": serializable(search.call(json!({ "query": "github integration" }), &ctx, None).await.unwrap()),
            "filtered": serializable(search.call(json!({ "query": "github integration", "source": "OFFICIAL" }), &ctx, None).await.unwrap()),
            "none": serializable(search.call(json!({ "query": "none" }), &ctx, None).await.unwrap()),
            "error": serializable(search.call(json!({ "query": "boom" }), &ctx, None).await.unwrap()),
            "noBackend": serializable(search_no_backend.call(json!({ "query": "github" }), &ctx, None).await.unwrap()),
        },
        "wrappedTool": {
            "progressEvents": wrapped_events.lock().unwrap().clone(),
            "result": serializable(wrapped_result),
        },
    });

    Ok(normalize_mcp_blob_paths(trace))
}

fn wrapped_call() -> McpToolCall {
    Arc::new(
        |name: RuntimeString,
         args: RuntimeObject,
         on_progress: Option<ToolProgress>,
         _signal: AbortSignal|
         -> BoxFuture<'static, Result<McpCallResult, crate::types::errors::ToolError>> {
            Box::pin(async move {
                if let Some(cb) = on_progress {
                    let q = args
                        .get("q")
                        .and_then(Value::as_str)
                        .cloned()
                        .unwrap_or_default();
                    cb(crate::types::events::ToolDomainEvent::Status {
                        message: RuntimeString::from("calling ")
                            .concat(&name)
                            .concat(&RuntimeString::from(":"))
                            .concat(&q),
                    })
                    .await;
                }
                let mut image = McpContentBlock {
                    block_type: "image".into(),
                    text: None,
                    data: Some("SU1H".into()),
                    mime_type: Some("image/png".into()),
                    resource: None,
                    uri: None,
                    name: None,
                    description: None,
                };
                image.block_type = "image".into();
                Ok(McpCallResult {
                    content: vec![
                        McpContentBlock {
                            block_type: "text".into(),
                            text: Some("A😀BC".into()),
                            data: None,
                            mime_type: None,
                            resource: None,
                            uri: None,
                            name: None,
                            description: None,
                        },
                        image,
                    ],
                    is_error: Some(true),
                    meta: Some(json!({ "trace": "meta" })),
                    structured_content: Some(json!({ "score": 7, "ok": true })),
                })
            })
        },
    )
}

fn tool_meta(tool: &Arc<dyn Tool>, input: &Json) -> Value {
    json!({
        "name": tool.name(),
        "description": tool.description(),
        "searchHint": tool.search_hint(),
        "exposure": tool.exposure(),
        "maxResultSizeChars": tool.max_result_size_chars(),
        "mcpServer": tool.mcp_server(),
        "isReadOnly": tool.is_read_only(input),
        "isConcurrencySafe": tool.is_concurrency_safe(input),
        "isDestructive": tool.is_destructive(input),
        "summarize": tool.summarize(input),
    })
}

fn serializable(result: ToolResult) -> Value {
    let mut model_content = result.serializable.model_content;
    // The wall-time header carries a measured duration; pin the golden to a
    // fixed rendering so the baseline stays byte-exact.
    if let Some(crate::schemas::content::OutputBlock::Text(first)) = model_content.first_mut() {
        if let Ok(utf8) = first.text.try_to_utf8() {
            if let Some(rest) = utf8.strip_prefix("Wall time: ") {
                if let Some(tail_index) = rest.find(" seconds\nOutput:") {
                    let tail = &rest[tail_index..];
                    first.text = RuntimeString::from_utf8(&format!("Wall time: 0.0000{tail}"));
                }
            }
        }
    }
    json!({
        "output": result.serializable.output,
        "modelContent": model_content,
        "isError": result.serializable.is_error.unwrap_or(false),
        "mcpMeta": result.serializable.mcp_meta,
    })
}

fn make_clients() -> Vec<Arc<dyn McpResourceClient>> {
    vec![
        Arc::new(FakeMcpClient {
            name: "alpha".into(),
            instructions: Some("alpha instructions".into()),
            prompt_support: true,
        }),
        Arc::new(FakeMcpClient {
            name: "beta".into(),
            instructions: None,
            prompt_support: false,
        }),
    ]
}

fn ctx_with_clients(
    process_context: &RuntimeProcessContext,
    clients: Vec<Arc<dyn McpResourceClient>>,
) -> ToolContext {
    let mut ctx = make_ctx(process_context, "/tmp/klaus-mcp-trace");
    ctx.mcp_clients = Some(Box::new(move || clients.clone()));
    ctx
}

struct FakeMcpClient {
    name: RuntimeString,
    instructions: Option<RuntimeString>,
    prompt_support: bool,
}

#[async_trait]
impl McpResourceClient for FakeMcpClient {
    fn name(&self) -> &RuntimeString {
        &self.name
    }

    fn instructions(&self) -> Option<&RuntimeString> {
        self.instructions.as_ref()
    }

    async fn list_resources(&self) -> Vec<McpResourceDescriptor> {
        if self.name != "alpha" {
            return Vec::new();
        }
        vec![
            McpResourceDescriptor {
                uri: "res://note".into(),
                name: Some("Note".into()),
                mime_type: Some("text/plain".into()),
                description: Some("A text note".into()),
            },
            McpResourceDescriptor {
                uri: "res://bin".into(),
                name: Some("Binary".into()),
                mime_type: Some("application/octet-stream".into()),
                description: None,
            },
        ]
    }

    async fn read_resource(&self, uri: &RuntimeString) -> Vec<McpResourceContent> {
        if self.name != "alpha" {
            return Vec::new();
        }
        if uri == "res://note" {
            vec![McpResourceContent {
                uri: Some(uri.clone()),
                text: Some("hello from alpha".into()),
                mime_type: Some("text/plain".into()),
                blob: None,
            }]
        } else if uri == "res://bin" {
            vec![McpResourceContent {
                uri: Some(uri.clone()),
                text: None,
                mime_type: Some("application/octet-stream".into()),
                blob: Some("QUJDRA==".into()),
            }]
        } else {
            Vec::new()
        }
    }

    async fn list_prompts(&self) -> Option<Vec<McpPromptDescriptor>> {
        if !self.prompt_support {
            return None;
        }
        Some(vec![McpPromptDescriptor {
            name: "brief".into(),
            description: Some("Write a brief".into()),
            arguments: Some(vec![McpPromptArgument {
                name: "topic".into(),
                description: Some("Topic".into()),
                required: Some(true),
            }]),
        }])
    }

    async fn get_prompt(
        &self,
        name: &RuntimeString,
        args: Option<RuntimeObject>,
    ) -> Option<RuntimeString> {
        if !self.prompt_support {
            return None;
        }
        if name == "empty" {
            return Some(RuntimeString::default());
        }
        let topic = args
            .as_ref()
            .and_then(|object| object.get("topic"))
            .and_then(Value::as_str)
            .cloned()
            .unwrap_or_else(|| RuntimeString::from("(none)"));
        Some(
            RuntimeString::from("Prompt ")
                .concat(name)
                .concat(&RuntimeString::from(": "))
                .concat(&topic),
        )
    }
}

struct FakeCatalog {
    entries: Vec<McpCatalogEntry>,
}

impl FakeCatalog {
    fn new() -> Self {
        Self {
            entries: vec![
                McpCatalogEntry {
                    slug: RuntimeString::from("github"),
                    name: RuntimeString::from("GitHub"),
                    description: RuntimeString::from("Work with repositories and pull requests"),
                    source: Some(RuntimeString::from("OFFICIAL")),
                    server: crate::runtime_values::RuntimeJson::Object(RuntimeObject::new()),
                    keywords: None,
                },
                McpCatalogEntry {
                    slug: RuntimeString::from("linear"),
                    name: RuntimeString::from("Linear"),
                    description: RuntimeString::from("Track issues from Linear"),
                    source: Some(RuntimeString::from("COMMUNITY")),
                    server: crate::runtime_values::RuntimeJson::Object(RuntimeObject::new()),
                    keywords: None,
                },
            ],
        }
    }
}

#[async_trait]
impl McpCatalog for FakeCatalog {
    async fn search(
        &self,
        query: &RuntimeString,
        opts: McpCatalogSearchOptions,
    ) -> Result<Vec<McpCatalogEntry>, Box<dyn std::error::Error + Send + Sync>> {
        if query == "boom" {
            return Err("catalog unavailable".into());
        }
        if query == "none" {
            return Ok(Vec::new());
        }
        let mut entries = self.entries.clone();
        if let Some(sources) = opts.sources {
            entries.retain(|entry| {
                entry
                    .source
                    .as_ref()
                    .map(|source| sources.contains(source))
                    .unwrap_or(false)
            });
        }
        Ok(entries)
    }

    async fn get(
        &self,
        _slug: &RuntimeString,
        _signal: Option<AbortSignal>,
    ) -> Result<Option<McpCatalogEntry>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(None)
    }

    async fn install(
        &self,
        _slug: &RuntimeString,
        _opts: McpInstallFromCatalogOptions,
    ) -> Result<Option<McpServerConfig>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(None)
    }
}

fn json_object(value: Value) -> RuntimeObject {
    value.as_object().cloned().unwrap_or_default()
}

fn normalize_mcp_blob_paths(value: Value) -> Value {
    let mut value = value;
    normalize_value(&mut value);
    value
}

fn normalize_value(value: &mut Value) {
    match value {
        Value::String(text) => *text = normalize_blob_path(text),
        Value::Array(items) => {
            for item in items {
                normalize_value(item);
            }
        }
        Value::Object(object) => {
            for item in object.values_mut() {
                normalize_value(item);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn normalize_blob_path(value: &RuntimeString) -> RuntimeString {
    let units = value.as_utf16();
    let needle = "klaus-agent-mcp-".encode_utf16().collect::<Vec<_>>();
    let replacement = "<MCP_BLOB>".encode_utf16().collect::<Vec<_>>();
    let mut output = Vec::with_capacity(units.len());
    let mut copied_until = 0;
    let mut search_from = 0;

    while search_from + needle.len() <= units.len() {
        let Some(relative) = units[search_from..]
            .windows(needle.len())
            .position(|window| window == needle.as_slice())
        else {
            break;
        };
        let match_at = search_from + relative;
        let mut start = match_at;
        while start > copied_until && !is_blob_path_delimiter(units[start - 1]) {
            start -= 1;
        }
        let mut end = match_at + needle.len();
        while end < units.len() && !is_blob_path_delimiter(units[end]) {
            end += 1;
        }
        output.extend_from_slice(&units[copied_until..start]);
        output.extend_from_slice(&replacement);
        copied_until = end;
        search_from = end;
    }

    output.extend_from_slice(&units[copied_until..]);
    RuntimeString::from_utf16(output)
}

fn is_blob_path_delimiter(unit: u16) -> bool {
    unit == u16::from(b'"')
        || unit == u16::from(b'\'')
        || unit == u16::from(b']')
        || char::from_u32(u32::from(unit)).is_some_and(char::is_whitespace)
}

struct NoopFileState;

impl FileStateCache for NoopFileState {
    fn get(&self, _path: &RuntimePath) -> Option<FileStateEntry> {
        None
    }
    fn record(&self, _path: &RuntimePath, _mtime_ms: f64, _opts: RecordOptions) {}
    fn has(&self, _path: &RuntimePath) -> bool {
        false
    }
    fn entries(&self) -> Vec<(RuntimePath, FileStateEntry)> {
        Vec::new()
    }
    fn delete(&self, _path: &RuntimePath) {}
    fn clear(&self) {}
    fn clone_box(&self) -> Box<dyn FileStateCache> {
        Box::new(NoopFileState)
    }
}

struct NoopSandbox;

impl SandboxBackend for NoopSandbox {
    fn kind(&self) -> SandboxKind {
        SandboxKind::Process
    }
    fn is_available(&self) -> bool {
        false
    }
    fn transform(&self, req: &ExecRequest, _policy: &SandboxPolicy) -> SpawnSpec {
        SpawnSpec {
            file: req.argv.first().cloned().unwrap_or_default(),
            args: req.argv.iter().skip(1).cloned().collect(),
            cwd: req.cwd.clone(),
            env: RuntimeEnv::new(),
        }
    }
}

fn make_ctx(process_context: &RuntimeProcessContext, cwd: &str) -> ToolContext {
    ToolContext {
        async_question: None,
        background_processes: Arc::new(
            crate::command_runtime::background::BackgroundCommandRegistry::new(),
        ),
        process_context: process_context.clone(),
        cwd: RuntimePath::from(cwd),
        signal: AbortSignal::new(),
        file_state: Arc::new(NoopFileState),
        permission: PermissionContext {
            allow_rules: Vec::new(),
            deny_rules: Vec::new(),
            ask_rules: Vec::new(),
            additional_writable_roots: Vec::new(),
            internal_writable_roots: Vec::new(),
            pinned_memory_root: None,
            should_avoid_prompts: false,
        },
        sandbox: Arc::new(NoopSandbox),
        planning_read_only: false,
        approval_policy: None,
        sandbox_mode: None,
        approvals_reviewer: None,
        web_search_access: None,
        network_allowed: None,
        emit: std::sync::Arc::new(|_| Box::pin(async {})),
        native_pdf: None,
        vision: None,
        shell_env: None,
        depth: None,
        forward_subagent_text: None,
        spawn_sub_agent: None,
        spawn_background_agent: None,
        spawn_workflow_agent: None,
        surface_tools: None,
        activate_skill_paths: None,
        tool_catalog: None,
        elicit: None,
        plan_file: None,
        apply_skill_context: None,
        run_bash: None,
        mcp_clients: None,
        background_tasks: None,
        task_list_id: None,
        task_plan: None,
        worktree: None,
        workflow_journal: None,
        swarm: None,
        cron: None,
        goal: None,
        goal_generation: None,
        goal_owned_resources: None,
        lsp: None,
        browser: None,
        browser_tool_mapping: None,
        run_hook: None,
        invoke_tool: None,
        advisor: None,
        sandbox_retry_on_denial: None,
        request_approval: None,
        request_network_access: None,
        enqueue_background_notice: None,
        record_rollout_event: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_path_normalization_preserves_unrelated_surrogate_units() {
        let mut input = vec![0xd800, u16::from(b' ')];
        input.extend("/tmp/klaus-agent-mcp-123/blob.bin".encode_utf16());
        input.extend([u16::from(b' '), 0xdc00]);

        let mut expected = vec![0xd800, u16::from(b' ')];
        expected.extend("<MCP_BLOB>".encode_utf16());
        expected.extend([u16::from(b' '), 0xdc00]);

        assert_eq!(
            normalize_blob_path(&RuntimeString::from_utf16(input)),
            RuntimeString::from_utf16(expected)
        );
    }
}
