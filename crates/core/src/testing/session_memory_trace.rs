use std::sync::{Arc, Mutex};

use futures::stream;
use serde_json::Value as LegacyConfigValue;

use crate::engine::agent::{Agent, AgentOptions, StartSessionOptions};
use crate::engine::deps::EngineDepsOverrides;
use crate::engine::session::{Input, TurnOptions};
use crate::fs::node_path::{project_runtime_path, runtime_path_from_native};
use crate::memory::agent_memory::{
    AgentMemoryHost, AgentMemoryLayout, AgentMemoryScope, MAIN_MEMORY_AGENT_TYPE,
};
use crate::runtime_process::RuntimeProcessContext;
use crate::runtime_values::{
    runtime_codec, RuntimeJson, RuntimeObject, RuntimePath, RuntimeString,
};
use crate::testing::{ev, fake_provider, make_usage};
use crate::types::abort::AbortSignal;
use crate::types::provider::{CallModel, ModelRequest, ModelStopReason, ModelStream};

const SCENARIO: &str = "zero-config-session-memory-behavior";
const INDEX_LINE: &str = "- [Build cmd](build.md) — use pnpm not npm\n";

macro_rules! exact_json {
    (null) => {
        RuntimeJson::Null
    };
    ([ $($value:tt),* $(,)? ]) => {
        RuntimeJson::Array(vec![$(exact_json!($value)),*])
    };
    ({ $($key:literal : $value:tt),* $(,)? }) => {{
        let mut object = RuntimeObject::new();
        $(object.insert($key, exact_json!($value));)*
        RuntimeJson::Object(object)
    }};
    ($value:expr) => {
        runtime_codec::encode_value(&$value)
            .expect("exact session memory trace JSON expression must encode")
    };
}

fn legacy_agent_memory_off_config() -> LegacyConfigValue {
    LegacyConfigValue::Object(serde_json::Map::from_iter([(
        "agentMemory".to_owned(),
        LegacyConfigValue::Bool(false),
    )]))
}

pub async fn session_memory_trace(
    process_context: &RuntimeProcessContext,
) -> Result<RuntimeJson, Box<dyn std::error::Error + Send + Sync>> {
    let base = std::env::temp_dir().join("klaus-session-memory-trace");
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let base_runtime = runtime_path_from_native(&base);

    Ok(exact_json!({
        "scenario": SCENARIO,
        "default": (run_scenario(process_context, &base, "default", None, true).await?),
        "agentMemoryOff": (run_scenario(process_context, &base, "agent-memory-off", Some(legacy_agent_memory_off_config()), false).await?),
    })
    .pipe(|value| normalize_paths(value, base_runtime.as_runtime_string())))
}

async fn run_scenario(
    process_context: &RuntimeProcessContext,
    base: &std::path::Path,
    label: &str,
    config: Option<LegacyConfigValue>,
    write_index: bool,
) -> Result<RuntimeJson, Box<dyn std::error::Error + Send + Sync>> {
    let root = base.join(label);
    let cwd = root.join("workspace");
    let memory_base = root.join("memory");
    let main_memory_dir = memory_base
        .join(MAIN_MEMORY_AGENT_TYPE)
        .join(AgentMemoryScope::Project.as_str());
    std::fs::create_dir_all(&cwd).unwrap();
    if write_index {
        std::fs::create_dir_all(&main_memory_dir).unwrap();
        std::fs::write(main_memory_dir.join("MEMORY.md"), INDEX_LINE).unwrap();
    }
    let cwd_runtime = runtime_path_from_native(&cwd);
    let memory_base_runtime = runtime_path_from_native(&memory_base);
    let main_memory_dir_runtime = runtime_path_from_native(&main_memory_dir);

    let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let call_model = recording_call_model(Arc::clone(&requests));
    let agent = Agent::new(AgentOptions {
        process_context: process_context.clone(),
        provider: Arc::new(fake_provider()),
        config,
        tools: None,
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: Some(host_path_string(
            &cwd_runtime,
            "session memory trace agent cwd",
        )?),
        deps: EngineDepsOverrides {
            call_model: Some(call_model),
            uuid: Some(Arc::new(|| "session-memory-id".to_string())),
            now: Some(Arc::new(|| 0)),
            ..Default::default()
        },
        compactor: None,
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        credential_store: None,
        hooks: Vec::new(),
        observer: Some(None),
        logger: None,
        pricing: None,
        agents: None,
        include_default_agents: None,
        skills: None,
        output_styles: None,
        workflows: None,
        mcp_catalog: None,
        plugin_catalog: None,
        lsp_servers: None,
        agent_memory: Some(Arc::new(FixedMemoryHost {
            layout: FixedMemoryLayout {
                base: memory_base_runtime,
            },
        })),
        mcp_clients: None,
        mcp_server_configs: Vec::new(),
        mcp_tools: None,
        refresh_tools: None,
        system_prompt_provider: None,
        plugins: Vec::new(),
        plugin_load_report: None,
    });

    // Isolate the user tier so the trace does not inherit the developer's real
    // ~/.klaus/AGENTS.md, which would make the agentMemoryOff assertion depend
    // on the machine the test runs on.
    let sopts = StartSessionOptions {
        user_config_dir: Some(runtime_path_from_native(&base.join("userhome"))),
        ..Default::default()
    };
    agent
        .start_session(sopts)
        .run(Input::Text("go".into()), TurnOptions::default())
        .await;

    let requests = requests.lock().unwrap();
    let req = requests.first().expect("one model request captured");
    let memory_part = req.system.iter().find(|part| part.name == "memory");
    Ok(exact_json!({
        "requestCount": (requests.len()),
        // The same POSIX rendering the memory prompt itself shows, so the trace does not
        // carry two spellings of one directory.
        "memoryDir": (crate::fs::node_path::posix_rendered_runtime(&main_memory_dir_runtime)),
        "memoryDirExistsAfterTurn": (main_memory_dir.is_dir()),
        "systemMemoryPartName": (memory_part.map(|part| part.name.clone())),
        "systemMemoryPartText": (memory_part.map(|part| part.text.clone())),
        "systemMemoryPartContainsIndex": (memory_part
            .map(|part| part.text.contains("use pnpm not npm"))
            .unwrap_or(false)),
    }))
}

fn recording_call_model(sink: Arc<Mutex<Vec<ModelRequest>>>) -> CallModel {
    Box::new(
        move |req: ModelRequest, _signal: AbortSignal| -> ModelStream {
            sink.lock().unwrap().push(req);
            Box::pin(stream::iter(vec![
                ev::message_start(None),
                ev::text("ok"),
                ev::stop(Some(ModelStopReason::EndTurn), Some(make_usage(None, None))),
            ]))
        },
    )
}

struct FixedMemoryHost {
    layout: FixedMemoryLayout,
}

impl AgentMemoryHost for FixedMemoryHost {
    fn layout(&self) -> &dyn AgentMemoryLayout {
        &self.layout
    }
}

struct FixedMemoryLayout {
    base: RuntimePath,
}

impl AgentMemoryLayout for FixedMemoryLayout {
    fn dir_for(&self, agent_type: &str, scope: AgentMemoryScope) -> String {
        let base = project_runtime_path(&self.base, "session memory trace memory root")
            .expect("fixture memory root must project to a native path");
        native_path_string(
            base.join(agent_type).join(scope.as_str()),
            "session memory trace memory directory",
        )
    }

    fn snapshot_dir_for(&self, agent_type: &str) -> String {
        let base = project_runtime_path(&self.base, "session memory trace memory root")
            .expect("fixture memory root must project to a native path");
        native_path_string(
            base.join(agent_type).join(".snapshots"),
            "session memory trace snapshot directory",
        )
    }
}

fn host_path_string(
    path: &RuntimePath,
    argument_label: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let native = project_runtime_path(path, argument_label).map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("runtime path projection failed: {:?}", error.message()),
        )
    })?;
    native.into_os_string().into_string().map_err(|path| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{argument_label} cannot cross the legacy UTF-8 path seam: {path:?}"),
        )
        .into()
    })
}

fn native_path_string(path: std::path::PathBuf, argument_label: &str) -> String {
    path.into_os_string().into_string().unwrap_or_else(|path| {
        panic!("{argument_label} cannot cross the legacy UTF-8 path seam: {path:?}")
    })
}

/// Erase the temp root so one committed golden covers every host.
///
/// Both spellings are erased: `base` arrives in the host dialect, while the paths the trace
/// reports are POSIX-rendered, which is the shape the engine hands the model.
fn normalize_paths(mut value: RuntimeJson, base: &RuntimeString) -> RuntimeJson {
    match &mut value {
        RuntimeJson::String(text) => {
            let token = RuntimeString::from("<TMP>");
            let host_erased = runtime_replace_all(text, base, &token);
            *text = runtime_replace_all(&host_erased, &posix_spelling(base), &token);
        }
        RuntimeJson::Array(items) => {
            for item in items {
                *item = normalize_paths(std::mem::take(item), base);
            }
        }
        RuntimeJson::Object(map) => {
            for item in map.values_mut() {
                *item = normalize_paths(std::mem::take(item), base);
            }
        }
        RuntimeJson::Null | RuntimeJson::Bool(_) | RuntimeJson::Number(_) => {}
    }
    value
}

/// The same path with `/` separators, for erasing the spelling the engine reports.
fn posix_spelling(path: &RuntimeString) -> RuntimeString {
    path.replace("\\", "/")
}

fn runtime_replace_all(
    value: &RuntimeString,
    from: &RuntimeString,
    to: &RuntimeString,
) -> RuntimeString {
    let value = value.as_utf16();
    let from = from.as_utf16();
    if from.is_empty() {
        return RuntimeString::from_utf16(value.to_vec());
    }
    let mut output = Vec::with_capacity(value.len());
    let mut index = 0;
    while index < value.len() {
        if value[index..].starts_with(from) {
            output.extend_from_slice(to.as_utf16());
            index += from.len();
        } else {
            output.push(value[index]);
            index += 1;
        }
    }
    RuntimeString::from_utf16(output)
}

trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}

impl<T> Pipe for T {}
