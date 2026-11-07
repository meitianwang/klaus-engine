//! Deterministic workflow scenario drivers producing golden regression traces.

use std::sync::{Arc, Mutex};

use futures::stream;

use crate::engine::js_runtime::NodeJsWorkflowScriptRunner;
use crate::engine::workflow::builtin_workflows::builtin_workflows;
use crate::engine::workflow::discover::{
    discover_workflows, load_workflows_from_dir, save_workflow, workflow_dirs, workflow_file_slug,
    SaveWorkflowOptions, WorkflowScope,
};
use crate::engine::workflow::runtime::{
    run_workflow, ResolveWorkflowFn, WorkflowRunInput, WorkflowRuntimeDeps,
};
use crate::engine::workflow::WorkflowError;
use crate::runtime_process::RuntimeProcessContext;
use crate::runtime_values::{runtime_codec, RuntimeJson, RuntimeObject, RuntimeString};
use crate::testing::make_usage;
use crate::types::abort::AbortSignal;
use crate::types::errors::TerminalReason;
use crate::types::events::{EngineEvent, ThreadItem, ToolDomainEvent};
use crate::types::json::Json;
use crate::types::tool::{
    EngineEventStream, Isolation, SpawnSubAgent, SpawnSubAgentOptions, WorkflowJournal,
    WorkflowJournalEntry,
};

const RUN_ID: &str = "wf_deep_research_strict_trace";
const QUESTION: &str = "the question";
const INLINE_RUN_ID: &str = "wf_inline_basic_trace";
const REGISTERED_RUN_ID: &str = "wf_registered_nested_trace";

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
        runtime_codec::encode_value(&$value).expect("exact workflow trace JSON expression must encode")
    };
}

#[derive(Default)]
struct MemJournal {
    entries: Mutex<Vec<WorkflowJournalEntry>>,
}

#[async_trait::async_trait]
impl WorkflowJournal for MemJournal {
    async fn append(&self, entry: WorkflowJournalEntry) -> std::io::Result<()> {
        self.entries.lock().unwrap().push(entry);
        Ok(())
    }

    fn prior(&self, run_id: &RuntimeString) -> Vec<WorkflowJournalEntry> {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .filter(|entry| &entry.run_id == run_id)
            .cloned()
            .collect()
    }
}

#[derive(Default)]
struct TraceState {
    calls: Mutex<Vec<Json>>,
    statuses: Mutex<Vec<RuntimeString>>,
}

/// Generate the stable `deep-research-strict` workflow regression trace.
pub async fn deep_research_strict_trace(
    process_context: &RuntimeProcessContext,
) -> Result<Json, WorkflowError> {
    let journal = Arc::new(MemJournal::default());
    let fresh = run_trace(process_context, "fresh", Arc::clone(&journal)).await?;
    let resume = run_trace(process_context, "resume", Arc::clone(&journal)).await?;
    Ok(exact_json!({
        "scenario": "deep-research-strict",
        "meta": {
            "name": "deep-research-strict",
            "phases": ["Decompose", "Research", "Verify", "Synthesize"]
        },
        "runs": [fresh, resume]
    }))
}

/// Generate the stable inline workflow regression trace.
pub async fn inline_workflow_trace(
    process_context: &RuntimeProcessContext,
) -> Result<Json, WorkflowError> {
    let run = run_scenario_trace(
        process_context,
        ScenarioTraceSpec {
            name: "fresh",
            script: INLINE_WORKFLOW_SCRIPT,
            args: exact_json!({ "seed": "topic" }),
            run_id: INLINE_RUN_ID,
            budget_tokens: Some(100),
            response_for_kind: inline_response_for,
            kind_for_prompt: inline_prompt_kind,
            resolve_workflow: None,
        },
    )
    .await?;
    Ok(exact_json!({
        "scenario": "inline-basic",
        "meta": {
            "name": "inline-basic",
            "phases": ["Plan", "Execute"]
        },
        "runs": [run]
    }))
}

/// Generate the stable registered/nested workflow regression trace.
pub async fn registered_workflow_trace(
    process_context: &RuntimeProcessContext,
) -> Result<Json, WorkflowError> {
    let resolve: ResolveWorkflowFn = Arc::new(|name| {
        if name == "registered-child" {
            Some(RuntimeString::from(REGISTERED_CHILD_WORKFLOW_SCRIPT))
        } else {
            None
        }
    });
    let run = run_scenario_trace(
        process_context,
        ScenarioTraceSpec {
            name: "fresh",
            script: REGISTERED_PARENT_WORKFLOW_SCRIPT,
            args: exact_json!({ "topic": "root topic" }),
            run_id: REGISTERED_RUN_ID,
            budget_tokens: Some(100),
            response_for_kind: registered_response_for,
            kind_for_prompt: registered_prompt_kind,
            resolve_workflow: Some(resolve),
        },
    )
    .await?;
    Ok(exact_json!({
        "scenario": "registered-nested",
        "meta": {
            "name": "registered-parent",
            "phases": ["Parent", "Child", "Done"]
        },
        "runs": [run]
    }))
}

/// Generate the stable workflow disk discovery/save regression trace.
pub async fn workflow_disk_trace() -> Result<Json, WorkflowError> {
    let root = make_temp_root("disk");
    let _guard = ConfigEnvGuard::set(host_path(&root.join("home")));
    let result = async {
        let outer = root.join("proj");
        let inner = outer.join("pkg");
        let outer_dir = outer.join(".klaus").join("workflows");
        let inner_dir = inner.join(".klaus").join("workflows");
        let user_dir = root.join("home").join("workflows");
        for dir in [&outer_dir, &inner_dir, &user_dir] {
            std::fs::create_dir_all(dir).map_err(io_err)?;
        }
        crate::git::git::seed_git_dir(&outer.join(".git")).map_err(io_err)?;

        seed_workflow_dir(&inner_dir, "inner")?;
        seed_workflow_dir(&outer_dir, "outer")?;
        seed_workflow_dir(&user_dir, "user")?;

        let user_save = save_workflow(SaveWorkflowOptions {
            name: "My Cool Flow!".into(),
            scope: WorkflowScope::User,
            script: workflow_script("my-cool-flow", "saved user"),
            overwrite: false,
            cwd: None,
        })
        .await?;
        let duplicate_error = match save_workflow(SaveWorkflowOptions {
            name: "My Cool Flow!".into(),
            scope: WorkflowScope::User,
            script: workflow_script("my-cool-flow", "saved user duplicate"),
            overwrite: false,
            cwd: None,
        })
        .await
        {
            Ok(_) => RuntimeString::default(),
            Err(e) => normalize_message(&root, &e.message),
        };
        save_workflow(SaveWorkflowOptions {
            name: "My Cool Flow!".into(),
            scope: WorkflowScope::User,
            script: workflow_script("my-cool-flow", "saved user v2"),
            overwrite: true,
            cwd: None,
        })
        .await?;

        let project_existing = save_workflow(SaveWorkflowOptions {
            name: "Project Flow!".into(),
            scope: WorkflowScope::Project,
            script: workflow_script("project-flow", "saved project"),
            overwrite: false,
            cwd: Some(host_path(&inner).to_owned()),
        })
        .await?;

        let fallback_root = root.join("repo-no-workflows");
        crate::git::git::seed_git_dir(&fallback_root.join(".git")).map_err(io_err)?;
        let project_fallback = save_workflow(SaveWorkflowOptions {
            name: "Fallback Flow!".into(),
            scope: WorkflowScope::Project,
            script: workflow_script("fallback-flow", "saved fallback"),
            overwrite: false,
            cwd: Some(host_path(&fallback_root).to_owned()),
        })
        .await?;

        let invalid_save_error = match save_workflow(SaveWorkflowOptions {
            name: "Bad".into(),
            scope: WorkflowScope::User,
            script: "no meta here".into(),
            overwrite: false,
            cwd: None,
        })
        .await
        {
            Ok(_) => RuntimeString::default(),
            Err(e) => normalize_message(&root, &e.message),
        };

        Ok(exact_json!({
            "scenario": "disk-discovery-save",
            "dirs": (workflow_dirs(host_path(&inner))
                .into_iter()
                .map(|path| rel(&root, &path))
                .collect::<Vec<_>>()),
            "loadedInner": (summarize_workflow_refs(load_workflows_from_dir(host_path(&inner_dir)))),
            "discovered": (summarize_workflow_refs(discover_workflows(host_path(&inner)))),
            "slugCases": [
                ["Deep Research!", (workflow_file_slug(&RuntimeString::from("Deep Research!")))],
                ["  ", (workflow_file_slug(&RuntimeString::from("  ")))],
                ["a__b--c", (workflow_file_slug(&RuntimeString::from("a__b--c")))],
                ["@@@", (workflow_file_slug(&RuntimeString::from("@@@")))]
            ],
            "save": {
                "userPath": (rel(&root, &user_save.path)),
                "userContent": (std::fs::read_to_string(&user_save.path).map_err(io_err)?),
                "duplicateError": (duplicate_error),
                "projectExistingPath": (rel(&root, &project_existing.path)),
                "projectFallbackPath": (rel(&root, &project_fallback.path)),
                "invalidSaveError": (invalid_save_error)
            }
        }))
    }
    .await;
    let _ = std::fs::remove_dir_all(&root);
    result
}

async fn run_trace(
    process_context: &RuntimeProcessContext,
    name: &str,
    journal: Arc<MemJournal>,
) -> Result<Json, WorkflowError> {
    let workflow = builtin_workflows()
        .into_iter()
        .find(|w| w.name == "deep-research-strict")
        .expect("deep-research-strict workflow is missing");
    let state = Arc::new(TraceState::default());
    let result = run_workflow(
        WorkflowRunInput {
            script: workflow.script,
            args: Some(exact_json!(QUESTION)),
            resume_from_run_id: Some(RUN_ID.into()),
            budget_tokens: Some(100),
        },
        WorkflowRuntimeDeps {
            process_context: process_context.clone(),
            spawn_workflow_agent: trace_spawn(Arc::clone(&state), response_for, prompt_kind),
            journal: Some(journal.clone() as Arc<dyn WorkflowJournal>),
            emit: Some(trace_emit(Arc::clone(&state))),
            resolve_workflow: None,
            signal: AbortSignal::new(),
            max_concurrency: Some(8),
        },
        Arc::new(NodeJsWorkflowScriptRunner),
    )
    .await?;

    let calls = state.calls.lock().unwrap().clone();
    let statuses = state.statuses.lock().unwrap().clone();
    let mut run = exact_json!({
        "name": name,
        "statuses": (statuses),
        "agentCalls": (calls),
        "result": (result.result),
        "agentsSpawned": (result.agents_spawned),
        "outputTokensSpent": (result.output_tokens_spent)
    });
    if name == "fresh" {
        run["journal"] = Json::Array(journal_snapshot(&journal));
    }
    Ok(run)
}

struct ScenarioTraceSpec<'a> {
    name: &'a str,
    script: &'a str,
    args: Json,
    run_id: &'a str,
    budget_tokens: Option<i64>,
    response_for_kind: fn(&str) -> Response,
    kind_for_prompt: fn(&RuntimeString) -> &'static str,
    resolve_workflow: Option<ResolveWorkflowFn>,
}

async fn run_scenario_trace(
    process_context: &RuntimeProcessContext,
    spec: ScenarioTraceSpec<'_>,
) -> Result<Json, WorkflowError> {
    let ScenarioTraceSpec {
        name,
        script,
        args,
        run_id,
        budget_tokens,
        response_for_kind,
        kind_for_prompt,
        resolve_workflow,
    } = spec;
    let journal = Arc::new(MemJournal::default());
    let state = Arc::new(TraceState::default());
    let result = run_workflow(
        WorkflowRunInput {
            script: RuntimeString::from(script),
            args: Some(args),
            resume_from_run_id: Some(run_id.into()),
            budget_tokens,
        },
        WorkflowRuntimeDeps {
            process_context: process_context.clone(),
            spawn_workflow_agent: trace_spawn(
                Arc::clone(&state),
                response_for_kind,
                kind_for_prompt,
            ),
            journal: Some(journal.clone() as Arc<dyn WorkflowJournal>),
            emit: Some(trace_emit(Arc::clone(&state))),
            resolve_workflow,
            signal: AbortSignal::new(),
            max_concurrency: Some(8),
        },
        Arc::new(NodeJsWorkflowScriptRunner),
    )
    .await?;

    let calls = state.calls.lock().unwrap().clone();
    let statuses = state.statuses.lock().unwrap().clone();
    let mut run = exact_json!({
        "name": name,
        "statuses": (statuses),
        "agentCalls": (calls),
        "result": (result.result),
        "agentsSpawned": (result.agents_spawned),
        "outputTokensSpent": (result.output_tokens_spent)
    });
    if name == "fresh" {
        run["journal"] = Json::Array(journal_snapshot_for(&journal, &RuntimeString::from(run_id)));
    }
    Ok(run)
}

fn trace_emit(state: Arc<TraceState>) -> Arc<dyn Fn(ToolDomainEvent) + Send + Sync> {
    Arc::new(move |event| {
        if let ToolDomainEvent::Status { message } = event {
            state.statuses.lock().unwrap().push(message);
        }
    })
}

fn trace_spawn(
    state: Arc<TraceState>,
    response_for_kind: fn(&str) -> Response,
    kind_for_prompt: fn(&RuntimeString) -> &'static str,
) -> SpawnSubAgent {
    Arc::new(move |opts: SpawnSubAgentOptions| {
        let state = Arc::clone(&state);
        let kind = kind_for_prompt(&opts.prompt);
        let response = response_for_kind(kind);
        let call = {
            let mut calls = state.calls.lock().unwrap();
            let idx = calls.len();
            let call = exact_json!({
                "idx": idx,
                "kind": kind,
                "prompt": (opts.prompt.clone()),
                "options": (normalize_options(&opts))
            });
            calls.push(call);
            calls.len()
        };
        let events = vec![
            EngineEvent::ItemCompleted {
                inference: None,
                item: ThreadItem::AgentMessage {
                    phase: None,
                    id: format!("agent-{call}").into(),
                    text: response.text,
                },
            },
            EngineEvent::TurnCompleted {
                turn_id: "trace".into(),
                usage: Some(make_usage(Some(0), Some(response.output_tokens))),
                stop_reason: TerminalReason::Completed,
            },
        ];
        Box::pin(stream::iter(events)) as EngineEventStream
    })
}

struct Response {
    text: RuntimeString,
    output_tokens: u64,
}

fn response_for(kind: &str) -> Response {
    match kind {
        "decompose" => Response {
            text: json_response(exact_json!({ "subquestions": ["only sub-question"] })),
            output_tokens: 3,
        },
        "research" => Response {
            text: json_response(exact_json!({
                "claims": [
                    {
                        "text": "Fact A",
                        "sourceUrl": "https://a.example/x",
                        "sourceTitle": "Site A",
                        "quote": "qa",
                    },
                    {
                        "text": "Fact B",
                        "sourceUrl": "https://b.example/y",
                        "sourceTitle": "Site B",
                        "quote": "qb",
                    },
                ],
            })),
            output_tokens: 5,
        },
        "verify:Fact A" => Response {
            text: json_response(exact_json!({
                "supported": true,
                "reason": "source supports it",
            })),
            output_tokens: 7,
        },
        "verify:Fact B" => Response {
            text: json_response(exact_json!({
                "supported": false,
                "reason": "source does not support it",
            })),
            output_tokens: 11,
        },
        "synthesize" => Response {
            text: "Fact A is established [1]. An unfounded aside [9].".into(),
            output_tokens: 13,
        },
        _ => panic!("unexpected deep-research-strict prompt kind: {kind}"),
    }
}

fn prompt_kind(prompt: &RuntimeString) -> &'static str {
    if prompt.contains("Break this research question into") {
        return "decompose";
    }
    if prompt.contains("Research this question with web_search and web_fetch") {
        return "research";
    }
    if prompt.contains("Decide whether the SOURCE actually supports the CLAIM") {
        if prompt.contains("CLAIM: Fact A") {
            return "verify:Fact A";
        }
        if prompt.contains("CLAIM: Fact B") {
            return "verify:Fact B";
        }
    }
    if prompt.contains("Write a clear, conclusion-first report") {
        return "synthesize";
    }
    panic!("unexpected deep-research-strict prompt: {prompt:?}");
}

const INLINE_WORKFLOW_SCRIPT: &str = r#"export const meta = {
  name: 'inline-basic',
  description: 'Inline workflow trace fixture',
  phases: [{ title: 'Plan' }, { title: 'Execute' }],
}
phase('Plan')
log('planning ' + args.seed)
const first = await agent('inline:first:' + args.seed, {
  label: 'first',
  phase: 'Plan',
  model: 'inline-model',
  schema: {
    type: 'object',
    properties: { value: { type: 'string' } },
    required: ['value'],
  },
})
phase('Execute')
const branches = await parallel([
  () => agent('inline:branch:' + first.value, { label: 'branch-a', agentType: 'researcher' }),
  () => { throw new Error('drop') },
  'not-a-function',
])
const piped = await pipeline(
  ['x', 'y'],
  (item) => agent('inline:pipe:' + item, { label: 'pipe', isolation: 'worktree' }),
  (prev, item) => prev + ':' + item,
)
return {
  first,
  branches,
  piped,
  spent: budget.spent(),
  remaining: budget.remaining(),
}"#;

fn inline_prompt_kind(prompt: &RuntimeString) -> &'static str {
    if prompt == "inline:first:topic" {
        "inline:first"
    } else if prompt == "inline:branch:alpha" {
        "inline:branch"
    } else if prompt == "inline:pipe:x" {
        "inline:pipe:x"
    } else if prompt == "inline:pipe:y" {
        "inline:pipe:y"
    } else {
        panic!("unexpected inline workflow prompt: {prompt:?}")
    }
}

fn inline_response_for(kind: &str) -> Response {
    match kind {
        "inline:first" => Response {
            text: json_response(exact_json!({ "value": "alpha" })),
            output_tokens: 4,
        },
        "inline:branch" => Response {
            text: "branch-alpha".into(),
            output_tokens: 6,
        },
        "inline:pipe:x" => Response {
            text: "pipe-x".into(),
            output_tokens: 8,
        },
        "inline:pipe:y" => Response {
            text: "pipe-y".into(),
            output_tokens: 10,
        },
        _ => panic!("unexpected inline workflow kind: {kind}"),
    }
}

const REGISTERED_PARENT_WORKFLOW_SCRIPT: &str = r#"export const meta = {
  name: 'registered-parent',
  description: 'Registered parent workflow trace fixture',
  phases: [{ title: 'Parent' }, { title: 'Child' }, { title: 'Done' }],
}
phase('Parent')
const parent = await agent('registered:parent:' + args.topic, { label: 'parent' })
log('before child')
const child = await workflow('registered-child', { topic: parent })
phase('Done')
return { parent, child }"#;

const REGISTERED_CHILD_WORKFLOW_SCRIPT: &str = r#"export const meta = {
  name: 'registered-child',
  description: 'Registered child workflow trace fixture',
  phases: [{ title: 'Child' }],
}
phase('Child')
const child = await agent('registered:child:' + args.topic, {
  label: 'child',
  phase: 'Child',
  model: 'child-model',
})
return { topic: args.topic, child }"#;

fn registered_prompt_kind(prompt: &RuntimeString) -> &'static str {
    if prompt == "registered:parent:root topic" {
        "registered:parent"
    } else if prompt == "registered:child:parent answer" {
        "registered:child"
    } else {
        panic!("unexpected registered workflow prompt: {prompt:?}")
    }
}

fn registered_response_for(kind: &str) -> Response {
    match kind {
        "registered:parent" => Response {
            text: "parent answer".into(),
            output_tokens: 12,
        },
        "registered:child" => Response {
            text: "child answer".into(),
            output_tokens: 14,
        },
        _ => panic!("unexpected registered workflow kind: {kind}"),
    }
}

fn json_response(value: Json) -> RuntimeString {
    crate::runtime_values::runtime_json::stringify(&value)
}

fn normalize_options(opts: &SpawnSubAgentOptions) -> Json {
    exact_json!({
        "agentType": (opts.agent_type.clone()),
        "model": (opts.model.clone()),
        "isolation": (opts.isolation.map(|Isolation::Worktree| "worktree")),
        "outputSchema": (opts.output_schema.clone().map(Json::Object).unwrap_or(Json::Null)),
    })
}

fn journal_snapshot(journal: &MemJournal) -> Vec<Json> {
    journal_snapshot_for(journal, &RuntimeString::from(RUN_ID))
}

fn journal_snapshot_for(journal: &MemJournal, run_id: &RuntimeString) -> Vec<Json> {
    let mut entries = journal
        .entries
        .lock()
        .unwrap()
        .iter()
        .filter(|entry| &entry.run_id == run_id)
        .cloned()
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.idx);
    entries
        .into_iter()
        .map(|entry| {
            exact_json!({
                "runId": (entry.run_id),
                "idx": (entry.idx),
                "hash": (entry.hash),
                "result": (entry.result),
                "outputTokens": (entry.output_tokens),
            })
        })
        .collect()
}

fn make_temp_root(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "ae-wf-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct ConfigEnvGuard {
    #[cfg(test)]
    _config_env_guard: std::sync::MutexGuard<'static, ()>,
    prev: Option<String>,
}

impl ConfigEnvGuard {
    fn set(value: &str) -> Self {
        #[cfg(test)]
        let config_env_guard = crate::test_support::CONFIG_ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prev = std::env::var("KLAUS_CONFIG_DIR").ok();
        crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", value);
        Self {
            #[cfg(test)]
            _config_env_guard: config_env_guard,
            prev,
        }
    }
}

impl Drop for ConfigEnvGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(value) => {
                crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", value)
            }
            None => crate::runtime_process::remove_process_environment_var("KLAUS_CONFIG_DIR"),
        }
    }
}

fn seed_workflow_dir(dir: &std::path::Path, tier: &str) -> Result<(), WorkflowError> {
    std::fs::write(
        dir.join("shared.js"),
        workflow_script_text("shared", &format!("from {tier}")),
    )
    .map_err(io_err)?;
    std::fs::write(
        dir.join(format!("only-{tier}.js")),
        workflow_script_text(&format!("only-{tier}"), &format!("from {tier}")),
    )
    .map_err(io_err)?;
    std::fs::write(dir.join("invalid.js"), "const x = 1 // no workflow meta").map_err(io_err)?;
    std::fs::write(
        dir.join("ignored.txt"),
        workflow_script_text(&format!("ignored-{tier}"), &format!("ignored {tier}")),
    )
    .map_err(io_err)?;
    std::fs::write(
        dir.join("ignored.flow.json"),
        r#"{"meta":{"name":"flow","description":"flow"},"body":[]}"#,
    )
    .map_err(io_err)?;
    Ok(())
}

fn workflow_script(name: &str, description: &str) -> RuntimeString {
    workflow_script_text(name, description).into()
}

fn workflow_script_text(name: &str, description: &str) -> String {
    format!("export const meta = {{ name: '{name}', description: '{description}' }}\nreturn null")
}

fn summarize_workflow_refs(refs: Vec<crate::tools::builtins::workflow::WorkflowRef>) -> Json {
    let mut refs = refs;
    refs.sort_by(|a, b| a.name.cmp(&b.name));
    Json::Array(
        refs.into_iter()
            .map(|wf| {
                exact_json!({
                    "name": (wf.name),
                    "description": (wf.description),
                })
            })
            .collect(),
    )
}

fn rel(root: &std::path::Path, path: &str) -> String {
    let path = std::path::Path::new(path);
    // The engine reports POSIX-rendered paths, so strip the root in that spelling too: on
    // Windows `strip_prefix` compares components and a `C:/...` path does not start with the
    // `C:\...` root as `Path` sees it.
    let posix_root = host_path(root).replace('\\', "/");
    let suffix = path
        .strip_prefix(root)
        .or_else(|_| path.strip_prefix(&posix_root))
        .unwrap_or(path)
        .to_str()
        .expect("workflow trace temp paths must be UTF-8")
        .replace('\\', "/");
    if suffix.is_empty() {
        "$ROOT".to_string()
    } else {
        format!("$ROOT/{suffix}")
    }
}

/// Erase the temp root from a message so one committed golden covers every host.
///
/// Both spellings are erased: the root arrives in the host dialect, while the paths the
/// engine names in its messages are POSIX-rendered.
fn normalize_message(root: &std::path::Path, message: &RuntimeString) -> RuntimeString {
    let host = host_path(root);
    message
        .replace(host, "$ROOT")
        .replace(&host.replace('\\', "/"), "$ROOT")
}

fn host_path(path: &std::path::Path) -> &str {
    path.to_str()
        .expect("workflow trace temp paths must be UTF-8")
}

fn io_err(err: std::io::Error) -> WorkflowError {
    WorkflowError::new(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Whether the shared config-env lock becomes free within a bounded wait.
    ///
    /// Two things a bare `try_lock().is_ok()` gets wrong. The test binary runs cases in
    /// parallel, so another case's guard may hold the same lock at that instant and the
    /// assertion fails for a reason that has nothing to do with this guard; other holders
    /// keep it for milliseconds, so a bounded wait tells that apart from a guard that
    /// leaked it. And a `Mutex<()>` that any other case panicked while holding stays
    /// POISONED for the rest of the run, so `is_ok()` is false forever after an unrelated
    /// failure — poisoning carries no meaning for a unit-typed lock, which is why every
    /// other holder in this crate takes it with `unwrap_or_else(|e| e.into_inner())`.
    fn shared_config_lock_becomes_free() -> bool {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match crate::test_support::CONFIG_ENV_LOCK.try_lock() {
                Ok(_) | Err(std::sync::TryLockError::Poisoned(_)) => return true,
                Err(std::sync::TryLockError::WouldBlock) => {}
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    #[test]
    fn config_env_guard_holds_the_shared_config_env_lock() {
        let root = make_temp_root("shared-config-lock");
        let guard = ConfigEnvGuard::set(host_path(&root.join("home")));

        assert!(crate::test_support::CONFIG_ENV_LOCK.try_lock().is_err());
        drop(guard);
        assert!(shared_config_lock_becomes_free());

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn exact_workflow_journal_wire_preserves_lone_surrogates() {
        let strings = [
            RuntimeString::from_utf16([0xd800]),
            RuntimeString::from_utf16([0xdc00]),
            RuntimeString::from_utf16([0xfffd]),
        ];
        let mut wires = Vec::new();
        for text in &strings {
            let journal = MemJournal::default();
            journal
                .append(WorkflowJournalEntry {
                    run_id: text.clone(),
                    idx: 0,
                    hash: text.clone(),
                    result: exact_json!({ "value": (text.clone()) }),
                    output_tokens: 0,
                })
                .await
                .unwrap();
            let value = Json::Array(journal_snapshot_for(&journal, text));
            assert_eq!(value.pointer("/0/runId").and_then(Json::as_str), Some(text));
            assert_eq!(value.pointer("/0/hash").and_then(Json::as_str), Some(text));
            assert_eq!(
                value.pointer("/0/result/value").and_then(Json::as_str),
                Some(text)
            );
            wires.push(crate::runtime_values::runtime_json::stringify(&value));
        }

        assert_ne!(wires[0], wires[1]);
        assert_ne!(wires[0], wires[2]);
        assert_ne!(wires[1], wires[2]);
    }
}
