//! `CronCreate` / `CronList` / `CronDelete` — scheduled prompts. A prompt is
//! enqueued when its 5-field cron expression (local time) matches: recurring
//! jobs fire until deleted or auto-expired, one-shots fire once then delete
//! themselves. Jobs are session-only by default; `durable: true` persists them
//! to `.klaus/scheduled_tasks.json`. Present only when the host enables the
//! cron subsystem (`config.cron`).

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimeString};
use crate::tasks::cron::{cron_to_human, next_cron_run_ms, parse_cron_expression};
use crate::tasks::cron_store::DEFAULT_MAX_AGE_DAYS;
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};

/// The maximum number of live jobs (file-backed + session-only combined).
const MAX_JOBS: usize = 50;

/// The default "cron unavailable" message.
const CRON_DISABLED: &str = "Scheduled tasks are not enabled for this session.";

fn runtime_object(fields: impl IntoIterator<Item = (&'static str, RuntimeJson)>) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    for (key, value) in fields {
        object.insert(key, value);
    }
    RuntimeJson::Object(object)
}

/// A `{ ok: false, message }` result flagged as an error.
fn cron_error(message: impl Into<RuntimeString>) -> ToolResult {
    let message = message.into();
    let output = runtime_object([
        ("ok", RuntimeJson::Bool(false)),
        ("message", RuntimeJson::String(message.clone())),
    ]);
    text_result(output, message, true)
}

/// Single-line prompt preview: cut at the first newline, then bound the
/// remaining length (UTF-16 units), appending `…` whenever anything was cut.
fn truncate_single_line(text: &RuntimeString, max_units: usize) -> RuntimeString {
    let units = text.as_utf16();
    let line_end = units
        .iter()
        .position(|unit| *unit == u16::from(b'\n'))
        .unwrap_or(units.len());
    let cut_newline = line_end < units.len();
    if line_end <= max_units.saturating_sub(1) {
        let head = RuntimeString::from_utf16(units[..line_end].to_vec());
        return if cut_newline {
            head.concat(&RuntimeString::from("…"))
        } else {
            head
        };
    }
    if !cut_newline && units.len() <= max_units {
        return text.clone();
    }
    RuntimeString::from_utf16(units[..max_units.saturating_sub(1)].to_vec())
        .concat(&RuntimeString::from("…"))
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ---- CronCreate --------------------------------------------------------------

fn cron_create_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "cron": {
                "type": "string",
                "description": "Standard 5-field cron expression in local time: \"M H DoM Mon DoW\" (e.g. \"*/5 * * * *\" = every 5 minutes, \"30 14 28 2 *\" = Feb 28 at 2:30pm local once)."
            },
            "prompt": {
                "type": "string",
                "description": "Clear, cohesive instructions replayed at each scheduled run. Preserve the authorized task scope, relevant context, completion conditions, and the user's notification intent."
            },
            "recurring": {
                "type": "boolean",
                "description": format!("true (default) = fire on every cron match until deleted or auto-expired after {DEFAULT_MAX_AGE_DAYS} days. false = fire once at the next match, then auto-delete. Use false for \"remind me at X\" one-shot requests with pinned minute/hour/dom/month.")
            },
            "durable": {
                "type": "boolean",
                "description": "true = persist to .klaus/scheduled_tasks.json and survive restarts. false (default) = in-memory only, dies when this session ends. Use true only when the user asks the task to survive across sessions."
            }
        },
        "required": ["cron", "prompt"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

/// Schedule a prompt to run at a future time.
pub struct CronCreateTool {
    input_schema: JsonSchema,
}

/// Construct the `CronCreate` tool.
pub fn create_cron_create_tool() -> Arc<dyn Tool> {
    Arc::new(CronCreateTool {
        input_schema: cron_create_input_schema(),
    })
}

#[async_trait]
impl Tool for CronCreateTool {
    fn static_name(&self) -> &str {
        "CronCreate"
    }

    fn static_description(&self) -> &str {
        "Schedule a prompt to run at a future time, recurring on a local-time cron schedule or once at the next matching time. Use when the user requests scheduled work, a reminder, monitoring, or a later follow-up; do not substitute shell sleep or timers. Write clear, cohesive, human-readable instructions. Inspect CronList before replacing an existing schedule or creating a possible duplicate. Creation always adds a new job; use an existing ID with CronDelete only when the user asked to cancel or replace that job. Preserve notification intent: unless periodic updates were explicitly requested, monitoring prompts should stay quiet while the state is unchanged or non-actionable and notify only for meaningful change, completion, failure, or required user action. Confirm the schedule in plain language rather than raw cron syntax unless requested. Use recurring: false for a one-shot task. Pass durable: true only when the user asks for persistence in .klaus/scheduled_tasks.json; otherwise the job is session-only. Recurring jobs remain subject to the scheduler's automatic expiry."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn input_schema_property_order(
        &self,
        object_path: &[String],
    ) -> Option<&'static [&'static str]> {
        object_path
            .is_empty()
            .then_some(&["cron", "prompt", "recurring", "durable"])
    }

    fn static_search_hint(&self) -> Option<&str> {
        Some("schedule a recurring or one-shot prompt")
    }

    fn max_result_size_chars(&self) -> Option<u64> {
        Some(100_000)
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

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let cron = input
            .get("cron")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        Some(RuntimeString::from("Scheduling ").concat(&cron))
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let Some(controller) = &ctx.cron else {
            return Ok(cron_error(CRON_DISABLED));
        };
        let cron = input
            .get("cron")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        let cron_text = cron.try_to_utf8().unwrap_or_default();
        if parse_cron_expression(&cron_text).is_none() {
            return Ok(cron_error(format!(
                "Invalid cron expression '{cron_text}'. Expected 5 fields: M H DoM Mon DoW."
            )));
        }
        if next_cron_run_ms(&cron_text, now_ms()).is_none() {
            return Ok(cron_error(format!(
                "Cron expression '{cron_text}' does not match any calendar date in the next year."
            )));
        }
        let Some(prompt) = input.get("prompt").and_then(RuntimeJson::as_str).cloned() else {
            return Ok(cron_error("prompt is required for CronCreate"));
        };
        if controller.list().await.len() >= MAX_JOBS {
            return Ok(cron_error(format!(
                "Too many scheduled jobs (max {MAX_JOBS}). Cancel one first."
            )));
        }
        let recurring = input
            .get("recurring")
            .and_then(RuntimeJson::as_bool)
            .unwrap_or(true);
        let durable = input
            .get("durable")
            .and_then(RuntimeJson::as_bool)
            .unwrap_or(false);
        let id = match controller.create(cron, prompt, recurring, durable).await {
            Ok(id) => id,
            Err(message) => return Ok(cron_error(message)),
        };
        let human = cron_to_human(&cron_text);
        let output = runtime_object([
            ("id", RuntimeJson::String(id.clone())),
            (
                "humanSchedule",
                RuntimeJson::String(RuntimeString::from(human.as_str())),
            ),
            ("recurring", RuntimeJson::Bool(recurring)),
            ("durable", RuntimeJson::Bool(durable)),
        ]);
        let location = if durable {
            "Persisted to .klaus/scheduled_tasks.json"
        } else {
            "Session-only (not written to disk, dies when the session ends)"
        };
        let id_text = String::from_utf8(id.to_node_utf8()).unwrap_or_default();
        let text = if recurring {
            format!(
                "Scheduled recurring job {id_text} ({human}). {location}. Auto-expires after {DEFAULT_MAX_AGE_DAYS} days. Use CronDelete to cancel sooner."
            )
        } else {
            format!(
                "Scheduled one-shot task {id_text} ({human}). {location}. It will fire once then auto-delete."
            )
        };
        Ok(text_result(output, text, false))
    }
}

// ---- CronList ------------------------------------------------------------------

fn empty_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {},
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

/// List scheduled cron jobs.
pub struct CronListTool {
    input_schema: JsonSchema,
}

/// Construct the `CronList` tool.
pub fn create_cron_list_tool() -> Arc<dyn Tool> {
    Arc::new(CronListTool {
        input_schema: empty_input_schema(),
    })
}

#[async_trait]
impl Tool for CronListTool {
    fn static_name(&self) -> &str {
        "CronList"
    }

    fn static_description(&self) -> &str {
        "List current scheduled jobs with their real IDs, schedules, and saved prompts. Use when the user asks about an existing job, or before replacing a schedule or creating a possible duplicate."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn static_search_hint(&self) -> Option<&str> {
        Some("list active cron jobs")
    }

    fn max_result_size_chars(&self) -> Option<u64> {
        Some(100_000)
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    async fn call(
        &self,
        _input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let Some(controller) = &ctx.cron else {
            return Ok(cron_error(CRON_DISABLED));
        };
        let jobs = controller.list().await;
        let mut rows: Vec<RuntimeJson> = Vec::with_capacity(jobs.len());
        let mut lines: Vec<RuntimeString> = Vec::with_capacity(jobs.len());
        for job in &jobs {
            let cron_text = job.cron.try_to_utf8().unwrap_or_default();
            let human = cron_to_human(&cron_text);
            let mut row = RuntimeObject::new();
            row.insert("id", RuntimeJson::String(job.id.clone()));
            row.insert("cron", RuntimeJson::String(job.cron.clone()));
            row.insert(
                "humanSchedule",
                RuntimeJson::String(RuntimeString::from(human.as_str())),
            );
            row.insert("prompt", RuntimeJson::String(job.prompt.clone()));
            if job.recurring {
                row.insert("recurring", RuntimeJson::Bool(true));
            }
            if !job.durable {
                row.insert("durable", RuntimeJson::Bool(false));
            }
            rows.push(RuntimeJson::Object(row));
            let kind = if job.recurring {
                " (recurring)"
            } else {
                " (one-shot)"
            };
            let scope = if job.durable { "" } else { " [session-only]" };
            lines.push(
                job.id
                    .concat(&RuntimeString::from(format!(" — {human}{kind}{scope}: ")))
                    .concat(&truncate_single_line(&job.prompt, 80)),
            );
        }
        let output = runtime_object([("jobs", RuntimeJson::Array(rows))]);
        let text = if lines.is_empty() {
            RuntimeString::from("No scheduled jobs.")
        } else {
            let mut text = lines[0].clone();
            for line in &lines[1..] {
                text = text.concat(&RuntimeString::from("\n")).concat(line);
            }
            text
        };
        Ok(text_result(output, text, false))
    }
}

// ---- CronDelete ------------------------------------------------------------------

fn cron_delete_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "id": {
                "type": "string",
                "description": "Job ID returned by CronCreate."
            }
        },
        "required": ["id"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

/// Cancel a scheduled cron job by ID.
pub struct CronDeleteTool {
    input_schema: JsonSchema,
}

/// Construct the `CronDelete` tool.
pub fn create_cron_delete_tool() -> Arc<dyn Tool> {
    Arc::new(CronDeleteTool {
        input_schema: cron_delete_input_schema(),
    })
}

#[async_trait]
impl Tool for CronDeleteTool {
    fn static_name(&self) -> &str {
        "CronDelete"
    }

    fn static_description(&self) -> &str {
        "Cancel the scheduled job identified by an ID returned by CronCreate or CronList. Use only for the job the user asked to cancel or replace; inspect CronList when its identity is uncertain."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn static_search_hint(&self) -> Option<&str> {
        Some("cancel a scheduled cron job")
    }

    fn max_result_size_chars(&self) -> Option<u64> {
        Some(100_000)
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

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let id = input
            .get("id")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        Some(RuntimeString::from("Cancelling job ").concat(&id))
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let Some(controller) = &ctx.cron else {
            return Ok(cron_error(CRON_DISABLED));
        };
        let id = input
            .get("id")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        let known = controller.list().await.iter().any(|job| job.id == id);
        if !known {
            let id_text = String::from_utf8(id.to_node_utf8()).unwrap_or_default();
            return Ok(cron_error(format!("No scheduled job with id '{id_text}'")));
        }
        if let Err(message) = controller.delete(&id).await {
            return Ok(cron_error(message));
        }
        let output = runtime_object([("id", RuntimeJson::String(id.clone()))]);
        let text = RuntimeString::from("Cancelled job ")
            .concat(&id)
            .concat(&RuntimeString::from("."));
        Ok(text_result(output, text, false))
    }
}

/// All cron tools, included when the host enables the cron subsystem.
pub fn create_cron_tools() -> Vec<Arc<dyn Tool>> {
    vec![
        create_cron_create_tool(),
        create_cron_list_tool(),
        create_cron_delete_tool(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::runtime_json;
    use crate::schemas::content::OutputBlock;
    use crate::tools::builtins::test_ctx::make_ctx;
    use crate::types::tool::{CronController, CronJobInfo};
    use std::sync::Mutex;

    macro_rules! rjson {
        ($($json:tt)+) => {{
            runtime_json::parse(&RuntimeString::from(stringify!($($json)+)))
                .expect("test JSON literal must parse as exact runtime JSON")
        }};
    }

    fn rs(value: &str) -> RuntimeString {
        RuntimeString::from(value)
    }

    /// A mock cron controller returning canned jobs and recording calls.
    #[derive(Default)]
    struct MockCron {
        jobs: Vec<CronJobInfo>,
        created: Mutex<Vec<(RuntimeString, RuntimeString, bool, bool)>>,
        deleted: Mutex<Vec<RuntimeString>>,
    }

    #[async_trait]
    impl CronController for MockCron {
        async fn create(
            &self,
            cron: RuntimeString,
            prompt: RuntimeString,
            recurring: bool,
            durable: bool,
        ) -> Result<RuntimeString, RuntimeString> {
            self.created
                .lock()
                .unwrap()
                .push((cron, prompt, recurring, durable));
            Ok(rs("1a2b3c4d"))
        }
        async fn list(&self) -> Vec<CronJobInfo> {
            self.jobs.clone()
        }
        async fn delete(&self, id: &RuntimeString) -> Result<(), RuntimeString> {
            self.deleted.lock().unwrap().push(id.clone());
            Ok(())
        }
    }

    fn job(id: &str, cron: &str, prompt: &str, recurring: bool, durable: bool) -> CronJobInfo {
        CronJobInfo {
            id: rs(id),
            cron: rs(cron),
            prompt: rs(prompt),
            recurring,
            durable,
        }
    }

    fn text_of(res: &ToolResult) -> RuntimeString {
        match &res.serializable.model_content[0] {
            OutputBlock::Text(t) => t.text.clone(),
            _ => panic!("expected a text block"),
        }
    }

    // ---- metadata + schemas --------------------------------------------------

    #[test]
    fn tools_expose_expected_metadata_and_flags() {
        let create = create_cron_create_tool();
        assert_eq!(create.name(), "CronCreate");
        assert_eq!(
            create.search_hint(),
            Some(rs("schedule a recurring or one-shot prompt"))
        );
        assert_eq!(create.max_result_size_chars(), Some(100_000));
        assert!(!create.is_read_only(&rjson!({})));
        assert!(!create.is_concurrency_safe(&rjson!({})));
        assert_eq!(create.is_destructive(&rjson!({})), Some(false));
        assert_eq!(
            create.summarize(&rjson!({ "cron": "*/5 * * * *" })),
            Some(rs("Scheduling */5 * * * *"))
        );

        let list = create_cron_list_tool();
        assert_eq!(list.name(), "CronList");
        assert!(list.is_read_only(&rjson!({})));
        assert!(list.is_concurrency_safe(&rjson!({})));

        let delete = create_cron_delete_tool();
        assert_eq!(delete.name(), "CronDelete");
        assert!(!delete.is_read_only(&rjson!({})));
        assert_eq!(
            delete.summarize(&rjson!({ "id": "1a2b3c4d" })),
            Some(rs("Cancelling job 1a2b3c4d"))
        );
    }

    #[test]
    fn schemas_match_exactly() {
        let create_schema = create_cron_create_tool().input_schema();
        assert_eq!(
            create_schema.get("required").unwrap(),
            &RuntimeJson::Array(vec![RuntimeJson::from("cron"), RuntimeJson::from("prompt")])
        );
        let properties = create_schema.get("properties").unwrap();
        for field in ["cron", "prompt", "recurring", "durable"] {
            assert!(properties.get(field).is_some(), "missing {field}");
        }
        assert_eq!(
            create_cron_list_tool().input_schema(),
            rjson!({ "type": "object", "properties": {}, "additionalProperties": false })
        );
        assert_eq!(
            create_cron_delete_tool().input_schema(),
            rjson!({
                "type": "object",
                "properties": {
                    "id": { "type": "string", "description": "Job ID returned by CronCreate." }
                },
                "required": ["id"],
                "additionalProperties": false
            })
        );
    }

    // ---- CronCreate ------------------------------------------------------------

    #[tokio::test]
    async fn create_reports_disabled_cron() {
        let tool = create_cron_create_tool();
        let ctx = make_ctx(".");
        let res = tool
            .call(rjson!({ "cron": "*/5 * * * *", "prompt": "p" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(text_of(&res), CRON_DISABLED);
    }

    #[tokio::test]
    async fn create_rejects_invalid_and_unmatchable_crons() {
        let tool = create_cron_create_tool();
        let mut ctx = make_ctx(".");
        ctx.cron = Some(Arc::new(MockCron::default()));

        let res = tool
            .call(rjson!({ "cron": "@daily", "prompt": "p" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "Invalid cron expression '@daily'. Expected 5 fields: M H DoM Mon DoW."
        );

        // Valid syntax but no calendar match in the next year (Feb 30).
        let res = tool
            .call(rjson!({ "cron": "0 0 30 2 *", "prompt": "p" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "Cron expression '0 0 30 2 *' does not match any calendar date in the next year."
        );

        // Missing prompt.
        let res = tool
            .call(rjson!({ "cron": "*/5 * * * *" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(text_of(&res), "prompt is required for CronCreate");
    }

    #[tokio::test]
    async fn create_enforces_the_job_cap() {
        let tool = create_cron_create_tool();
        let mut ctx = make_ctx(".");
        let jobs = (0..MAX_JOBS)
            .map(|i| job(&format!("{i:08x}"), "*/5 * * * *", "p", true, false))
            .collect();
        ctx.cron = Some(Arc::new(MockCron {
            jobs,
            ..MockCron::default()
        }));
        let res = tool
            .call(rjson!({ "cron": "*/5 * * * *", "prompt": "p" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "Too many scheduled jobs (max 50). Cancel one first."
        );
    }

    #[tokio::test]
    async fn create_defaults_recurring_true_and_durable_false() {
        let tool = create_cron_create_tool();
        let mock = Arc::new(MockCron::default());
        let mut ctx = make_ctx(".");
        ctx.cron = Some(mock.clone());
        let res = tool
            .call(
                rjson!({ "cron": "*/5 * * * *", "prompt": "check the build" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output,
            rjson!({ "id": "1a2b3c4d", "humanSchedule": "Every 5 minutes", "recurring": true, "durable": false })
        );
        assert_eq!(
            text_of(&res),
            "Scheduled recurring job 1a2b3c4d (Every 5 minutes). Session-only (not written to disk, dies when the session ends). Auto-expires after 7 days. Use CronDelete to cancel sooner."
        );
        let created = mock.created.lock().unwrap();
        assert_eq!(
            created.as_slice(),
            &[(rs("*/5 * * * *"), rs("check the build"), true, false)]
        );
    }

    #[tokio::test]
    async fn create_one_shot_durable_reports_persistence() {
        let tool = create_cron_create_tool();
        let mock = Arc::new(MockCron::default());
        let mut ctx = make_ctx(".");
        ctx.cron = Some(mock.clone());
        let res = tool
            .call(
                rjson!({ "cron": "30 14 28 2 *", "prompt": "p", "recurring": false, "durable": true }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            text_of(&res),
            "Scheduled one-shot task 1a2b3c4d (30 14 28 2 *). Persisted to .klaus/scheduled_tasks.json. It will fire once then auto-delete."
        );
        let created = mock.created.lock().unwrap();
        assert_eq!(
            created.as_slice(),
            &[(rs("30 14 28 2 *"), rs("p"), false, true)]
        );
    }

    // ---- CronList ------------------------------------------------------------

    #[tokio::test]
    async fn list_reports_disabled_then_empty_then_lines() {
        let tool = create_cron_list_tool();
        let ctx = make_ctx(".");
        let res = tool.call(rjson!({}), &ctx, None).await.unwrap();
        assert_eq!(text_of(&res), CRON_DISABLED);

        let mut ctx2 = make_ctx(".");
        ctx2.cron = Some(Arc::new(MockCron::default()));
        let res2 = tool.call(rjson!({}), &ctx2, None).await.unwrap();
        assert_eq!(res2.serializable.is_error, Some(false));
        assert_eq!(text_of(&res2), "No scheduled jobs.");
        assert_eq!(res2.serializable.output, rjson!({ "jobs": [] }));

        let mut ctx3 = make_ctx(".");
        ctx3.cron = Some(Arc::new(MockCron {
            jobs: vec![
                job("aaaaaaaa", "*/5 * * * *", "watch the build", true, true),
                job(
                    "bbbbbbbb",
                    "30 14 28 2 *",
                    "remind me\nwith details",
                    false,
                    false,
                ),
            ],
            ..MockCron::default()
        }));
        let res3 = tool.call(rjson!({}), &ctx3, None).await.unwrap();
        assert_eq!(
            text_of(&res3),
            "aaaaaaaa — Every 5 minutes (recurring): watch the build\nbbbbbbbb — 30 14 28 2 * (one-shot) [session-only]: remind me…"
        );
        assert_eq!(
            res3.serializable.output,
            rjson!({
                "jobs": [
                    { "id": "aaaaaaaa", "cron": "*/5 * * * *", "humanSchedule": "Every 5 minutes", "prompt": "watch the build", "recurring": true },
                    { "id": "bbbbbbbb", "cron": "30 14 28 2 *", "humanSchedule": "30 14 28 2 *", "prompt": "remind me\nwith details", "durable": false }
                ]
            })
        );
    }

    // ---- CronDelete ------------------------------------------------------------

    #[tokio::test]
    async fn delete_requires_cron_then_a_known_id() {
        let tool = create_cron_delete_tool();
        let ctx = make_ctx(".");
        let res = tool
            .call(rjson!({ "id": "1a2b3c4d" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(text_of(&res), CRON_DISABLED);

        let mut ctx2 = make_ctx(".");
        ctx2.cron = Some(Arc::new(MockCron::default()));
        let res2 = tool
            .call(rjson!({ "id": "deadbeef" }), &ctx2, None)
            .await
            .unwrap();
        assert_eq!(res2.serializable.is_error, Some(true));
        assert_eq!(text_of(&res2), "No scheduled job with id 'deadbeef'");
    }

    #[tokio::test]
    async fn delete_cancels_a_known_job() {
        let tool = create_cron_delete_tool();
        let mock = Arc::new(MockCron {
            jobs: vec![job("1a2b3c4d", "*/5 * * * *", "p", true, false)],
            ..MockCron::default()
        });
        let mut ctx = make_ctx(".");
        ctx.cron = Some(mock.clone());
        let res = tool
            .call(rjson!({ "id": "1a2b3c4d" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(text_of(&res), "Cancelled job 1a2b3c4d.");
        assert_eq!(res.serializable.output, rjson!({ "id": "1a2b3c4d" }));
        assert_eq!(mock.deleted.lock().unwrap().as_slice(), &[rs("1a2b3c4d")]);
    }

    // ---- prompt preview -------------------------------------------------------

    #[test]
    fn truncate_single_line_bounds_length_and_newlines() {
        assert_eq!(truncate_single_line(&rs("short"), 80), rs("short"));
        assert_eq!(truncate_single_line(&rs("one\ntwo"), 80), rs("one…"));
        let long = "x".repeat(100);
        let cut = truncate_single_line(&rs(&long), 80);
        assert_eq!(cut.as_utf16().len(), 80);
        let text = String::from_utf8(cut.to_node_utf8()).unwrap();
        assert!(text.ends_with('…'));
    }
}
