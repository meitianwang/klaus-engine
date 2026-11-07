//! The `save_memory` [`Tool`]: persists a single durable fact to the USER-tier
//! memory file (`<config home>/AGENTS.md`) so it loads into every future
//! session's context via the hierarchical memory loader.
//!
//! The write goes straight to the engine's managed config home (not the project
//! tree, and not through the file path-permission gate — the config home is a
//! protected path that would otherwise prompt on every save). Because it bypasses
//! the sensitive-path prompt, the fact is HARD-NEUTRALIZED so the tool can only
//! ever add one inert bullet: [`one_line_fact`] collapses it to a single
//! length-capped line (a multi-line value can't escape the `- ` bullet to plant a
//! heading or a line-start `@import` directive the loader would follow to an
//! external file), and secrets are redacted (a saved fact re-enters context
//! forever). It IS a mutation (`is_read_only` is false), so plan mode blocks it
//! like any other write.

use std::future::Future;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use regex::Regex;
use serde_json::{json, Map, Value};

use crate::fs::paths::config_home;
use crate::permission::bash::node_path::resolve;
use crate::runtime_values::{RuntimeJson, RuntimeNumber, RuntimeObject, RuntimeString};
use crate::secrets::sanitizer::sanitize_secrets;
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};

/// The heading `save_memory` appends under, in the user-tier memory file.
const SECTION: &str = "## Added Memories";
/// A lock held longer than this is treated as abandoned (holder crashed) and reclaimed.
const LOCK_STALE_MS: u64 = 30_000;

/// Collapse a run of whitespace to a single space (`/\s+/g`).
static WS_RUN: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\s+").unwrap());
/// Any leading bullet dashes the model added (`^(?:-+\s*)+`).
static LEADING_BULLETS: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(?:-+\s*)+").unwrap());

/// String length measured in UTF-16 code units — the unit `slice(0, n)` counts.
fn slice_utf16(s: &str, n: usize) -> String {
    let units: Vec<u16> = s.encode_utf16().take(n).collect();
    String::from_utf16_lossy(&units)
}

/// Build the tool's input JSON Schema: a single required, non-empty `memory` string.
fn build_input_schema() -> JsonSchema {
    let mut memory = Map::new();
    memory.insert("type".to_string(), json!("string"));
    memory.insert("minLength".to_string(), json!(1));
    memory.insert(
        "description".to_string(),
        json!("The single fact to remember, phrased as a standalone statement (e.g. a stable user preference, project fact, or decision). Not transient task state."),
    );
    let mut properties = Map::new();
    properties.insert("memory".to_string(), Value::Object(memory));
    let mut schema = Map::new();
    schema.insert("type".to_string(), json!("object"));
    schema.insert("properties".to_string(), Value::Object(properties));
    schema.insert("required".to_string(), json!(["memory"]));
    schema.insert("additionalProperties".to_string(), json!(false));
    schema
}

/// Persist a durable fact to long-term memory.
pub struct SaveMemoryTool {
    input_schema: JsonSchema,
}

impl SaveMemoryTool {
    fn new() -> Self {
        SaveMemoryTool {
            input_schema: build_input_schema(),
        }
    }
}

/// Construct the `save_memory` tool as an erased registry-side [`Tool`].
pub fn create_save_memory_tool() -> Arc<dyn Tool> {
    Arc::new(SaveMemoryTool::new())
}

#[async_trait]
impl Tool for SaveMemoryTool {
    fn static_name(&self) -> &str {
        "save_memory"
    }

    fn static_description(&self) -> &str {
        "Save a durable fact to long-term memory so it is available in future sessions. Use for stable preferences, project facts, or decisions worth remembering across sessions — not for transient task state (use the todo list for that)."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    fn summarize(&self, _input: &Json) -> Option<RuntimeString> {
        Some("Saving to long-term memory".into())
    }

    async fn call(
        &self,
        input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let dir = config_home();
        let path = resolve(&[dir.as_str(), "AGENTS.md"]);
        let memory = input
            .get("memory")
            .and_then(Json::as_str)
            .cloned()
            .unwrap_or_default();
        let memory = String::from_utf8(memory.to_node_utf8())
            .expect("Node UTF-8 save-memory projection is valid UTF-8");
        // Collapse to one inert line, then redact secrets (see the module doc for why both).
        let fact = sanitize_secrets(&one_line_fact(&memory));
        if fact.is_empty() {
            let mut output = RuntimeObject::new();
            output.insert("path", RuntimeJson::String(path.clone().into()));
            output.insert("bytes", RuntimeJson::Number(RuntimeNumber::from(0_u64)));
            return Ok(text_result(
                RuntimeJson::Object(output),
                "Nothing to save (empty memory).".to_string(),
                false,
            ));
        }
        let path_for_write = path.clone();
        let fact_for_write = fact.clone();
        let bytes = with_memory_lock(&dir, move || async move {
            let existing = tokio::fs::read_to_string(&path_for_write)
                .await
                .unwrap_or_default();
            let next = append_memory(&existing, &fact_for_write);
            tokio::fs::write(&path_for_write, next.as_bytes()).await?;
            Ok::<u64, std::io::Error>(next.len() as u64)
        })
        .await
        .map_err(|e| ToolError::new(e.to_string()))?;
        let mut output = RuntimeObject::new();
        output.insert("path", RuntimeJson::String(path.into()));
        output.insert("bytes", RuntimeJson::Number(RuntimeNumber::from(bytes)));
        Ok(text_result(
            RuntimeJson::Object(output),
            "Saved to long-term memory.".to_string(),
            false,
        ))
    }
}

/// Best-effort cross-process advisory lock so two sessions sharing the same
/// config home don't clobber each other's append (the read-modify-write below is
/// not atomic). Exclusive-create a lockfile, retry briefly, reclaim a stale one;
/// always released. `is_concurrency_safe` false only serializes calls within one
/// process — this covers the multi-session case.
async fn with_memory_lock<F, Fut, T>(dir: &str, f: F) -> std::io::Result<T>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = std::io::Result<T>>,
{
    tokio::fs::create_dir_all(dir).await?;
    let lock = resolve(&[dir, ".save-memory-lock"]);
    let deadline = Instant::now() + Duration::from_millis(3_000);
    loop {
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock)
            .await
        {
            // Atomic exclusive create; the handle is closed on drop.
            Ok(_file) => break,
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                if let Ok(meta) = tokio::fs::metadata(&lock).await {
                    if let Ok(mtime) = meta.modified() {
                        let age = SystemTime::now().duration_since(mtime).unwrap_or_default();
                        if age > Duration::from_millis(LOCK_STALE_MS) {
                            let _ = tokio::fs::remove_file(&lock).await;
                            continue; // reclaim an abandoned lock
                        }
                    }
                }
                if Instant::now() >= deadline {
                    break; // waited long enough — proceed best-effort
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(err) => return Err(err),
        }
    }
    let result = f().await;
    let _ = tokio::fs::remove_file(&lock).await;
    result
}

/// Collapse a fact to a single trimmed, length-capped line — all whitespace runs
/// (incl. newlines) become one space — so it can only ever be rendered as one
/// inert `- ` bullet. This is the injection guard: with no newline, the value
/// cannot start a new line with `#` (a heading) or `@import` (a memory directive
/// the loader would follow to an external file). Exported for testing.
pub fn one_line_fact(memory: &str) -> String {
    // Collapse whitespace → single line, strip any leading bullet dashes the model
    // added (so `- fact` doesn't become `- - fact`), then cap length.
    let collapsed = WS_RUN.replace_all(memory, " ");
    let trimmed = collapsed.trim();
    let debulleted = LEADING_BULLETS.replace(trimmed, "");
    let cleaned = debulleted.trim();
    slice_utf16(cleaned, 4000)
}

/// Append `fact` as a bullet under the `## Added Memories` section of `existing`,
/// creating the section (at EOF) if it's absent. Inserts at the end of an
/// existing section, before the next `## ` heading. Exported for testing.
pub fn append_memory(existing: &str, fact: &str) -> String {
    let bullet = format!("- {fact}");
    let mut lines: Vec<String> = existing.split('\n').map(str::to_string).collect();
    let start = lines.iter().position(|l| l.trim() == SECTION);
    let Some(start) = start else {
        // No section yet — append it at EOF with a clean separator.
        let sep = if existing.is_empty() {
            ""
        } else if existing.ends_with('\n') {
            "\n"
        } else {
            "\n\n"
        };
        return format!("{existing}{sep}{SECTION}\n\n{bullet}\n");
    };
    // Find the end of the section (next '## ' heading or EOF), then back up over
    // trailing blank lines.
    let mut end = lines.len();
    let mut i = start + 1;
    while i < lines.len() {
        if lines[i].starts_with("## ") {
            end = i;
            break;
        }
        i += 1;
    }
    let mut insert_at = end;
    while insert_at > start + 1 && lines[insert_at - 1].trim().is_empty() {
        insert_at -= 1;
    }
    lines.insert(insert_at, bullet);
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_process::RuntimeEnv;
    use crate::runtime_values::{runtime_json, RuntimePath};
    use crate::schemas::content::OutputBlock;
    use crate::test_support::CONFIG_ENV_LOCK as ENV_LOCK;
    use crate::types::abort::AbortSignal;
    use crate::types::events::ToolDomainEvent;
    use crate::types::file_state::{FileStateCache, FileStateEntry, RecordOptions};
    use crate::types::permission::PermissionContext;
    use crate::types::sandbox::{
        ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
    };
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    /// A unique temp dir (atomic counter + pid + nanos) so parallel tests never collide.
    fn temp_dir() -> PathBuf {
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-savememory-tool-{}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        dir.push(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Serializes tests that read/write `KLAUS_CONFIG_DIR` and restores the prior value.
    struct EnvGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
        prev: Option<String>,
    }

    impl EnvGuard {
        fn set(value: Option<&str>) -> Self {
            let lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let prev = std::env::var("KLAUS_CONFIG_DIR").ok();
            match value {
                Some(v) => {
                    crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", v)
                }
                None => crate::runtime_process::remove_process_environment_var("KLAUS_CONFIG_DIR"),
            }
            EnvGuard { _lock: lock, prev }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.prev {
                Some(v) => {
                    crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", v)
                }
                None => crate::runtime_process::remove_process_environment_var("KLAUS_CONFIG_DIR"),
            }
        }
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

    fn make_ctx(cwd: &str) -> ToolContext {
        ToolContext {
            async_question: None,
            background_processes: Arc::new(
                crate::command_runtime::background::BackgroundCommandRegistry::new(),
            ),
            process_context: crate::runtime_process::test_runtime_process_context(),
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
            sandbox_retry_on_denial: None,
            request_approval: None,
            request_network_access: None,
            enqueue_background_notice: None,
            record_rollout_event: None,
            emit: std::sync::Arc::new(|_ev: ToolDomainEvent| Box::pin(async {})),
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
        }
    }

    fn text_of(res: &ToolResult) -> RuntimeString {
        match &res.serializable.model_content[0] {
            OutputBlock::Text(t) => t.text.clone(),
            _ => panic!("expected a text block"),
        }
    }

    fn exact_json(source: &str) -> RuntimeJson {
        runtime_json::parse(&RuntimeString::from(source)).unwrap()
    }

    // ---- one_line_fact (injection hardening) ---------------------------------

    #[test]
    fn collapses_newlines_so_a_multi_line_fact_cannot_escape_the_bullet() {
        let out = one_line_fact("line1\nline2\n## Heading\n@import /etc/passwd");
        assert!(!out.contains('\n'));
        assert!(!out.starts_with('#'));
        assert!(!out.starts_with('@'));
    }

    #[test]
    fn strips_a_leading_bullet_the_model_may_add() {
        assert_eq!(one_line_fact("- already bulleted"), "already bulleted");
        assert_eq!(one_line_fact("--  double dash"), "double dash");
    }

    #[test]
    fn caps_length() {
        assert_eq!(one_line_fact(&"x".repeat(5000)).chars().count(), 4000);
    }

    // ---- append_memory -------------------------------------------------------

    #[test]
    fn creates_the_section_in_an_empty_file() {
        assert_eq!(
            append_memory("", "fact one"),
            "## Added Memories\n\n- fact one\n"
        );
    }

    #[test]
    fn appends_under_an_existing_section() {
        assert!(append_memory("## Added Memories\n\n- old\n", "new").contains("- old\n- new"));
    }

    #[test]
    fn inserts_before_the_next_heading_not_after_it() {
        let out = append_memory("## Added Memories\n\n- old\n\n## Other\n\nbody\n", "new");
        let new_at = out.find("- new").expect("has - new");
        let other_at = out.find("## Other").expect("has ## Other");
        assert!(new_at < other_at);
    }

    #[test]
    fn appends_the_section_after_existing_non_section_content() {
        let out = append_memory("# Title\n\nintro\n", "fact");
        assert!(out.contains("# Title"));
        assert!(out.contains("## Added Memories\n\n- fact"));
    }

    // ---- tool ----------------------------------------------------------------

    #[test]
    fn exposes_expected_metadata() {
        let tool = create_save_memory_tool();
        assert_eq!(tool.name(), RuntimeString::from("save_memory"));
        assert!(tool
            .description()
            .starts_with("Save a durable fact to long-term memory"));
        let input = exact_json(r#"{"memory":"x"}"#);
        assert!(!tool.is_read_only(&input));
        assert!(!tool.is_concurrency_safe(&input));
        assert_eq!(tool.is_destructive(&input), Some(false));
        assert!(tool.is_enabled());
        assert_eq!(
            tool.summarize(&input),
            Some(RuntimeString::from("Saving to long-term memory"))
        );
    }

    #[test]
    fn input_schema_describes_the_memory_field() {
        let tool = create_save_memory_tool();
        let schema = tool.input_schema();
        assert_eq!(
            schema.get("type").and_then(RuntimeJson::as_string),
            Some(&RuntimeString::from("object"))
        );
        let memory = schema
            .get("properties")
            .and_then(|p| p.get("memory"))
            .expect("memory property");
        assert_eq!(
            memory.get("type").and_then(RuntimeJson::as_string),
            Some(&RuntimeString::from("string"))
        );
        assert_eq!(
            memory.get("minLength").and_then(RuntimeJson::as_i64),
            Some(1)
        );
        assert_eq!(
            memory.get("description").and_then(RuntimeJson::as_string),
            Some(&RuntimeString::from("The single fact to remember, phrased as a standalone statement (e.g. a stable user preference, project fact, or decision). Not transient task state."))
        );
        assert_eq!(
            schema.get("required").unwrap(),
            &RuntimeJson::Array(vec![RuntimeJson::from("memory")])
        );
        assert_eq!(
            schema.get("additionalProperties").unwrap(),
            &RuntimeJson::Bool(false)
        );
    }

    #[tokio::test]
    async fn persists_a_fact_and_sanitizes_secrets() {
        let dir = temp_dir();
        let dir_str = dir.to_string_lossy().into_owned();
        let _g = EnvGuard::set(Some(&dir_str));
        let ctx = make_ctx(&dir_str);
        let tool = create_save_memory_tool();

        let res = tool
            .call(
                exact_json(r#"{"memory":"user prefers dark mode; token=abcdef123456"}"#),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            text_of(&res),
            RuntimeString::from("Saved to long-term memory.")
        );
        let agents = resolve(&[dir_str.as_str(), "AGENTS.md"]);
        let raw = std::fs::read_to_string(&agents).unwrap();
        assert!(raw.contains("## Added Memories"));
        assert!(raw.contains("user prefers dark mode"));
        assert!(raw.contains("[REDACTED]"));
        assert!(!raw.contains("abcdef123456"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn saves_nothing_for_an_empty_memory() {
        let dir = temp_dir();
        let dir_str = dir.to_string_lossy().into_owned();
        let _g = EnvGuard::set(Some(&dir_str));
        let ctx = make_ctx(&dir_str);
        let tool = create_save_memory_tool();

        let res = tool
            .call(exact_json(r#"{"memory":"   "}"#), &ctx, None)
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            text_of(&res),
            RuntimeString::from("Nothing to save (empty memory).")
        );
        let agents = resolve(&[dir_str.as_str(), "AGENTS.md"]);
        assert!(std::fs::read_to_string(&agents).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }
}
