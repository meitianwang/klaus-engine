//! Oversized-result handling: spill a single huge tool result to disk (level 1)
//! and persist the largest fresh results when a batch exceeds its aggregate
//! budget (level 2). Both keep the complete text available on disk.

use std::ffi::OsString;
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;

use crate::fs::node_path::posix_rendered_runtime;
use crate::runtime_process::fs_path_projection::Node20FsPathProjection;
use crate::runtime_process::{path_join, NodePathDialect};
use crate::runtime_values::{RuntimePath, RuntimeString};
use crate::schemas::content::{OutputBlock, TextBlock};
use crate::schemas::message::ToolResultMessage;

/// Shared per-tool spill ceiling; tools may declare a lower budget.
pub const DEFAULT_MAX_RESULT_CHARS: f64 = 50_000.0;

/// String length in UTF-16 code units — the unit result budgets are counted in.
fn len_utf16(s: &RuntimeString) -> usize {
    s.len_utf16()
}

/// Slice the first `n` UTF-16 code units of `s`, preserving a dangling half as
/// the runtime's reversible lone-surrogate marker just like JavaScript.
fn slice_utf16(s: &RuntimeString, n: usize) -> RuntimeString {
    s.slice_utf16(..n)
}

/// The concatenated text of a result's blocks (image blocks contribute nothing).
pub fn output_blocks_text(content: &[OutputBlock]) -> RuntimeString {
    let mut units = Vec::new();
    for b in content {
        if let OutputBlock::Text(t) = b {
            units.extend_from_slice(t.text.as_utf16());
        }
    }
    RuntimeString::from_utf16(units)
}

/// Make a successful empty result explicit without replacing native media.
pub fn normalize_successful_tool_output(
    content: Vec<OutputBlock>,
    tool_name: &RuntimeString,
) -> Vec<OutputBlock> {
    let empty = content.iter().all(|block| match block {
        OutputBlock::Text(text) => text.text.as_utf16().iter().all(|unit| {
            matches!(*unit,
                0x0009..=0x000d | 0x0020 | 0x00a0 | 0x1680 | 0x2000..=0x200a |
                0x2028 | 0x2029 | 0x202f | 0x205f | 0x3000 | 0xfeff)
        }),
        OutputBlock::Image(_) => false,
    });
    if !empty {
        return content;
    }
    vec![OutputBlock::Text(TextBlock {
        text: tool_name.concat(&" completed without output.".into()),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    })]
}

/// Where a spill writes and how big it may grow before it does.
#[derive(Debug, Clone)]
pub struct SpillOptions {
    pub max_chars: f64,
    pub dir: RuntimePath,
    pub id: RuntimeString,
}

/// The outcome of [`maybe_spill_result`]: the (possibly replaced) content plus
/// the on-disk path when a spill happened.
#[derive(Debug, Clone, PartialEq)]
pub struct SpillResult {
    pub content: Vec<OutputBlock>,
    pub spilled_path: Option<RuntimePath>,
}

/// Pure, deterministic portion of an oversized-result spill. Both the live
/// Query path and contract replay use this plan so UTF-16 accounting, preview
/// slicing, and path construction cannot drift.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillPlan {
    pub full_text: RuntimeString,
    pub full_chars: usize,
    pub preview: RuntimeString,
    pub path: RuntimePath,
}

/// Determine whether `content` must spill and, if so, return the exact payload,
/// preview, character count, and destination Query will use.
pub fn plan_spill_result(content: &[OutputBlock], opts: &SpillOptions) -> Option<SpillPlan> {
    if !opts.max_chars.is_finite()
        || content
            .iter()
            .any(|block| !matches!(block, OutputBlock::Text(_)))
    {
        return None;
    }
    let full_text = output_blocks_text(content);
    let full_chars = len_utf16(&full_text);
    if (full_chars as f64) <= opts.max_chars {
        return None;
    }
    let preview_units = opts.max_chars.min(2_000.0) as usize;
    let preview_end = full_text
        .as_utf16()
        .iter()
        .take(preview_units)
        .rposition(|unit| *unit == b'\n' as u16)
        .filter(|index| *index > preview_units / 2)
        .unwrap_or(preview_units);
    let preview = slice_utf16(&full_text, preview_end);
    let filename = opts.id.concat(&RuntimeString::from(".txt"));
    // Host join: the spill directory the caller passes is a real host path, so a POSIX
    // join produced `C:\dir/file.txt` — one path with two separators, which then differs
    // from every other spelling of the same file the engine records.
    let path = path_join(
        host_path_dialect(),
        &[opts.dir.clone(), RuntimePath::from_runtime_string(filename)],
    );
    Some(SpillPlan {
        full_text,
        full_chars,
        preview,
        path,
    })
}

/// Render a successfully persisted spill plan into the exact replacement block
/// Query gives the model.
///
/// The path handed to `format_spilled` is POSIX-rendered: it goes into the tool result the
/// model reads, and the engine spells model-facing paths with `/` on every host. The plan
/// keeps the host spelling, which is what actually opens the file.
pub fn render_spill_plan_content<F>(plan: &SpillPlan, format_spilled: F) -> Vec<OutputBlock>
where
    F: Fn(&RuntimeString, usize, &RuntimePath) -> RuntimeString,
{
    vec![OutputBlock::Text(TextBlock {
        text: format_spilled(
            &plan.preview,
            plan.full_chars,
            &posix_rendered_runtime(&plan.path),
        ),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    })]
}

/// Level 1 — per-tool spill: if a single tool result exceeds its budget, write
/// the full text to disk and hand the model a preview + path instead of bloating
/// the context. (§6.5)
pub async fn maybe_spill_result<F>(
    content: Vec<OutputBlock>,
    opts: SpillOptions,
    format_spilled: F,
) -> SpillResult
where
    F: Fn(&RuntimeString, usize, &RuntimePath) -> RuntimeString + Send,
{
    let Some(plan) = plan_spill_result(&content, &opts) else {
        return SpillResult {
            content,
            spilled_path: None,
        };
    };
    let spilled = async {
        let dir = project_spill_host_path(&opts.dir, "path")?;
        let path = project_spill_host_path(&plan.path, "path")?;
        tokio::fs::create_dir_all(dir).await?;
        let bytes = plan.full_text.to_node_utf8();
        let mut file = match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing = tokio::fs::read(&path).await?;
                return if existing == bytes {
                    Ok(())
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        "The tool-result artifact contains different output.",
                    ))
                };
            }
            Err(error) => return Err(error),
        };
        let written = async {
            file.write_all(&bytes).await?;
            file.flush().await
        }
        .await;
        drop(file);
        if let Err(error) = written {
            // This branch alone owns a newly created artifact. A failed open or
            // an existing artifact must never trigger cleanup of another result.
            let _ = tokio::fs::remove_file(&path).await;
            return Err(error);
        }
        Ok::<(), std::io::Error>(())
    }
    .await;
    if spilled.is_err() {
        // Spilling is an optimization, not a contract: if the disk write fails
        // (full disk, read-only mount, …) hand back the FULL in-memory output
        // rather than letting the I/O error escape — a successful tool must not
        // be reported as a failure, and (crucially) must not fall into the
        // caller's catch and trip the tool-failure path. A failed persist
        // simply returns the original block unchanged.
        return SpillResult {
            content,
            spilled_path: None,
        };
    }
    let replaced = render_spill_plan_content(&plan, format_spilled);
    SpillResult {
        content: replaced,
        // Reported, never opened: the same POSIX rendering the model sees, so a trace
        // carries one spelling of the spill.
        spilled_path: Some(posix_rendered_runtime(&plan.path)),
    }
}

/// Persist the largest fresh text results until this tool batch fits its budget.
/// Callers run this once before storing the batch in history, so later requests
/// and resumed sessions replay the exact same replacements. Read and native
/// media keep their own sizing contracts. Only model content is replaced; UI
/// output and provider-native metadata stay attached to the original result.
pub async fn persist_per_message_budget<F, S>(
    mut results: Vec<ToolResultMessage>,
    max_chars: usize,
    dir: RuntimePath,
    should_skip: S,
    format_spilled: F,
) -> Vec<ToolResultMessage>
where
    F: Fn(&RuntimeString, usize, &RuntimePath) -> RuntimeString + Send + Sync,
    S: Fn(&ToolResultMessage) -> bool,
{
    let mut order: Vec<(usize, usize)> = results
        .iter()
        .enumerate()
        .filter(|(_, result)| {
            !should_skip(result)
                && result
                    .content
                    .iter()
                    .all(|block| matches!(block, OutputBlock::Text(_)))
        })
        .map(|(index, result)| (index, len_utf16(&output_blocks_text(&result.content))))
        .collect();
    let mut total: usize = order.iter().map(|(_, size)| size).sum();
    order.sort_by(|left, right| right.1.cmp(&left.1));
    for (index, size) in order {
        if total <= max_chars {
            break;
        }
        // A separate artifact never overwrites a full per-item spill with its
        // preview. Small results cannot gain space from a 2K preview and path.
        let options = SpillOptions {
            max_chars: 2_000.0,
            dir: dir.clone(),
            id: results[index].tool_use_id.concat(&".aggregate".into()),
        };
        let Some(plan) = plan_spill_result(&results[index].content, &options) else {
            continue;
        };
        let replacement = render_spill_plan_content(&plan, &format_spilled);
        let replacement_size = len_utf16(&output_blocks_text(&replacement));
        if replacement_size >= size {
            continue;
        }
        let spill =
            maybe_spill_result(results[index].content.clone(), options, &format_spilled).await;
        if spill.spilled_path.is_some() {
            total = total.saturating_sub(size - replacement_size);
            results[index].content = spill.content;
        }
    }
    results
}

/// Legacy pure trim projection retained for existing replay consumers. Live
/// tool dispatch uses `persist_per_message_budget` to retain complete outputs.
/// Level 2 — per-message aggregate budget: when the combined tool results for one
/// assistant turn exceed the budget, trim the largest first (big > small), each
/// down to a floor, until under budget. (§6.5)
pub fn apply_per_message_budget<F>(
    results: Vec<ToolResultMessage>,
    max_chars: usize,
    format_trimmed: F,
) -> Vec<ToolResultMessage>
where
    F: Fn(&RuntimeString, usize) -> RuntimeString,
{
    let mut total: i64 = 0;
    for r in &results {
        total += len_utf16(&output_blocks_text(&r.content)) as i64;
    }
    if total <= max_chars as i64 {
        return results;
    }

    let mut out = results;
    // Indices ordered by descending result length (a stable sort keeps ties in
    // their original order).
    let mut order: Vec<(usize, usize)> = out
        .iter()
        .enumerate()
        .map(|(i, r)| (i, len_utf16(&output_blocks_text(&r.content))))
        .collect();
    order.sort_by(|a, b| b.1.cmp(&a.1));

    for (i, _len) in order {
        if total <= max_chars as i64 {
            break;
        }
        let text = output_blocks_text(&out[i].content);
        let text_len = len_utf16(&text);
        let keep = 200usize.max(((text_len as f64) * 0.3).floor() as usize);
        if text_len <= keep {
            continue;
        }
        let kept = slice_utf16(&text, keep);
        let trimmed = format_trimmed(&kept, text_len - keep);
        let trimmed_len = len_utf16(&trimmed);
        out[i].content = vec![OutputBlock::Text(TextBlock {
            text: trimmed,
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })];
        total -= (text_len as i64) - (trimmed_len as i64);
    }
    out
}

fn host_path_dialect() -> NodePathDialect {
    if cfg!(windows) {
        NodePathDialect::Win32
    } else {
        NodePathDialect::Posix
    }
}

#[cfg(unix)]
fn project_spill_host_path(path: &RuntimePath, label: &str) -> std::io::Result<PathBuf> {
    use std::os::unix::ffi::OsStringExt;

    let native = Node20FsPathProjection::project(path, label).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "spill path contains an invalid NUL code unit",
        )
    })?;
    Ok(PathBuf::from(OsString::from_vec(
        native.as_bytes().to_vec(),
    )))
}

#[cfg(windows)]
fn project_spill_host_path(path: &RuntimePath, label: &str) -> std::io::Result<PathBuf> {
    use std::os::windows::ffi::OsStringExt;

    let native = Node20FsPathProjection::project(path, label).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "spill path contains an invalid NUL code unit",
        )
    })?;
    Ok(PathBuf::from(OsString::from_wide(native.as_wide())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::behavior::{
        render_provider_text_envelope, render_tool_result_content,
        resolve_provider_behavior_profile, ProviderBehaviorProfile, ProviderPromptStyle,
        ProviderTextEnvelopeKind, ProviderTextEnvelopeOptions,
    };
    use crate::runtime_values::{runtime_json, RuntimeJson};
    use crate::schemas::content::ContentBlock;
    use crate::types::provider::ProviderToolCallKind;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-spill-{}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        dir.push(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn big(n: usize) -> Vec<OutputBlock> {
        vec![OutputBlock::Text(TextBlock {
            text: "x".repeat(n).into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })]
    }

    #[test]
    fn spill_plan_uses_utf16_preview_limit_and_joined_path() {
        let text = "A😀B".repeat(40);
        let plan = plan_spill_result(
            &[OutputBlock::Text(TextBlock {
                text: text.into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            &SpillOptions {
                max_chars: 7.0,
                dir: "$WORKSPACE/.klaus/tool-results".into(),
                id: "unicode-call".into(),
            },
        )
        .unwrap();

        assert_eq!(plan.full_chars, 160);
        assert_eq!(plan.preview, "A😀BA😀");
        // The directory and the file are joined in the host dialect, so the separator is
        // the host's; the layout is the same on every platform.
        let expected = if cfg!(windows) {
            r"$WORKSPACE\.klaus\tool-results\unicode-call.txt"
        } else {
            "$WORKSPACE/.klaus/tool-results/unicode-call.txt"
        };
        assert_eq!(plan.path, RuntimePath::from(expected));
    }

    fn msg(text: &str) -> ToolResultMessage {
        ToolResultMessage {
            uuid: "u".into(),
            tool_use_id: "t".into(),
            content: vec![OutputBlock::Text(TextBlock {
                text: text.into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_error: false,
            tool_kind: None,
            native_tool_output: None,
            output: None,
            extra_user_blocks: None,
        }
    }

    fn render_spilled_tool_result(
        preview: &RuntimeString,
        full_chars: usize,
        path: &RuntimePath,
        profile: &ProviderBehaviorProfile,
    ) -> RuntimeString {
        match profile.prompt_style {
            ProviderPromptStyle::OpenAiDeveloper | ProviderPromptStyle::AnthropicReminder => {
                let body = match profile.prompt_style {
                    ProviderPromptStyle::OpenAiDeveloper => concat(&[
                        format!("The full output is {full_chars} chars and was saved to ").into(),
                        path.as_runtime_string().clone(),
                        ". Read that file only if the omitted content is needed for the task."
                            .into(),
                    ]),
                    ProviderPromptStyle::AnthropicReminder => concat(&[
                        format!(
                            "Tool output was truncated. The full {full_chars} chars were saved to "
                        )
                        .into(),
                        path.as_runtime_string().clone(),
                        "; read it only if the omitted content is needed.".into(),
                    ]),
                    ProviderPromptStyle::Generic => unreachable!(),
                };
                concat(&[
                    preview.clone(),
                    "\n\n".into(),
                    render_provider_text_envelope(
                        ProviderTextEnvelopeKind::TruncatedToolOutput,
                        body,
                        profile,
                        ProviderTextEnvelopeOptions::default(),
                    ),
                ])
            }
            ProviderPromptStyle::Generic => concat(&[
                preview.clone(),
                format!("\n\n… [output truncated; full {full_chars} chars saved to ").into(),
                path.as_runtime_string().clone(),
                "] …".into(),
            ]),
        }
    }

    fn render_trimmed_tool_result(
        kept_text: &RuntimeString,
        omitted_chars: usize,
        profile: &ProviderBehaviorProfile,
    ) -> RuntimeString {
        match profile.prompt_style {
            ProviderPromptStyle::OpenAiDeveloper | ProviderPromptStyle::AnthropicReminder => {
                let body = format!(
                    "Omitted {omitted_chars} chars from this tool result to fit the context budget. Re-run or narrow the tool call if you need the missing details."
                );
                concat(&[
                    kept_text.clone(),
                    "\n\n".into(),
                    render_provider_text_envelope(
                        ProviderTextEnvelopeKind::ContextBudgetTrim,
                        body.into(),
                        profile,
                        ProviderTextEnvelopeOptions::default(),
                    ),
                ])
            }
            ProviderPromptStyle::Generic => kept_text.concat(
                &format!("\n… [trimmed {omitted_chars} chars to fit the context budget] …").into(),
            ),
        }
    }

    fn concat(values: &[RuntimeString]) -> RuntimeString {
        values
            .iter()
            .fold(RuntimeString::default(), |out, value| out.concat(value))
    }

    fn runtime_path(path: &std::path::Path) -> RuntimePath {
        RuntimePath::from(path.to_str().expect("test path is valid Unicode"))
    }

    #[tokio::test]
    async fn result_pipeline_spills_raw_then_envelopes_then_trims_rendered_text() {
        let raw_text = "😀".repeat(15_001);
        assert_eq!(raw_text.encode_utf16().count(), 30_002);
        let observed_spill = Arc::new(Mutex::new(None));
        let spill_sink = Arc::clone(&observed_spill);
        let dir = temp_dir();
        let profile = resolve_provider_behavior_profile(None, &"gpt-5".into());
        let spill_profile = profile.clone();
        let spill = maybe_spill_result(
            vec![OutputBlock::Text(TextBlock {
                text: raw_text.into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            SpillOptions {
                max_chars: 30_000.0,
                dir: runtime_path(&dir),
                id: "call-1".into(),
            },
            move |preview, full, path| {
                *spill_sink.lock().unwrap() = Some((preview.clone(), full, path.clone()));
                render_spilled_tool_result(preview, full, path, &spill_profile)
            },
        )
        .await;
        let (raw_preview, spill_input_chars, _) = observed_spill.lock().unwrap().clone().unwrap();
        assert_eq!(spill_input_chars, 30_002);
        let spill_text = output_blocks_text(&spill.content);
        assert_eq!(
            spill_text.slice_utf16(..raw_preview.len_utf16()),
            raw_preview
        );
        assert!(spill_text.contains("\n\n# Truncated Tool Output\n\n"));

        let rendered =
            render_tool_result_content(spill.content, &"exec_command".into(), false, &profile);
        let rendered_text = output_blocks_text(&rendered);
        assert!(rendered_text.starts_with("# Command Output\n\n"));
        let aggregate_input_chars = rendered_text.len_utf16();
        let result = ToolResultMessage {
            uuid: "r1".into(),
            tool_use_id: "call-1".into(),
            content: rendered,
            is_error: false,
            output: None,
            extra_user_blocks: None,
            tool_kind: None,
            native_tool_output: None,
        };
        let trimmed = apply_per_message_budget(
            vec![result, msg("small sibling")],
            1_000,
            |kept, omitted| render_trimmed_tool_result(kept, omitted, &profile),
        );
        assert!(aggregate_input_chars > 1_000);
        let persisted_text = output_blocks_text(&trimmed[0].content);
        assert!(persisted_text.starts_with("# Command Output"));
        assert!(persisted_text.contains("\n\n# Context-Budget Trim\n\n"));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn utf16_accounting_and_slice_match_javascript_strings() {
        let astral = RuntimeString::from("😀x");
        assert_eq!(
            runtime_json::stringify(&RuntimeJson::String(slice_utf16(&astral, 1))),
            r#""\ud83d""#
        );
        assert_eq!(slice_utf16(&astral, 2), "😀");

        let parsed = runtime_json::parse(&RuntimeString::from(r#""\ud800\uFDD0x""#)).unwrap();
        let text = parsed.as_str().unwrap();
        assert_eq!(len_utf16(text), 3);
        assert_eq!(
            runtime_json::stringify(&RuntimeJson::String(slice_utf16(text, 1))),
            r#""\ud800""#
        );
    }

    #[tokio::test]
    async fn spill_writes_node_utf8_without_internal_surrogate_or_sentinel_markers() {
        let dir = temp_dir();
        let parsed = runtime_json::parse(&RuntimeString::from(r#""\ud800\uFDD0😀xx""#)).unwrap();
        let full_text = parsed.as_str().unwrap().to_owned();
        let content = vec![OutputBlock::Text(TextBlock {
            text: full_text,
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })];
        let path = dir.join("node-utf8.txt");

        let result = maybe_spill_result(
            content,
            SpillOptions {
                max_chars: 1.0,
                dir: runtime_path(&dir),
                id: "node-utf8".into(),
            },
            |preview, _, _| preview.clone(),
        )
        .await;

        // The reported path is POSIX-rendered; the write still used the host spelling.
        assert_eq!(
            result.spilled_path,
            Some(posix_rendered_runtime(&runtime_path(&path)))
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "�\u{fdd0}😀xx");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn leaves_a_small_result_untouched() {
        let dir = temp_dir();
        let content = big(10);
        let opts = SpillOptions {
            max_chars: 100.0,
            dir: runtime_path(&dir.join("s")),
            id: "a".into(),
        };
        let r = maybe_spill_result(content.clone(), opts, |_, _, _| {
            panic!("small result must not invoke formatter")
        })
        .await;
        assert_eq!(r.content, content);
        assert!(r.spilled_path.is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn leaves_a_result_with_a_non_finite_limit_untouched() {
        let dir = temp_dir();
        let content = big(50_000);
        let opts = SpillOptions {
            max_chars: f64::INFINITY,
            dir: runtime_path(&dir.join("s")),
            id: "infinite".into(),
        };
        let r = maybe_spill_result(content.clone(), opts, |_, _, _| {
            panic!("non-finite threshold must not invoke formatter")
        })
        .await;
        assert_eq!(r.content, content);
        assert!(r.spilled_path.is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn spills_an_oversized_result_to_disk_and_hands_back_a_preview_and_path() {
        let dir = temp_dir();
        let s_dir = dir.join("s");
        let opts = SpillOptions {
            max_chars: 100.0,
            dir: runtime_path(&s_dir),
            id: "b".into(),
        };
        let r = maybe_spill_result(big(5_000), opts, |preview, full, path| {
            concat(&[
                preview.clone(),
                format!("\n\n… [output truncated; full {full} chars saved to ").into(),
                path.as_runtime_string().clone(),
                "] …".into(),
            ])
        })
        .await;
        let expected_path = s_dir.join("b.txt");
        assert_eq!(
            r.spilled_path,
            Some(posix_rendered_runtime(&runtime_path(&expected_path)))
        );
        let text = output_blocks_text(&r.content);
        assert!(text.contains("output truncated"));
        // The full text was written to disk.
        let on_disk = std::fs::read_to_string(expected_path).unwrap();
        assert_eq!(on_disk.chars().count(), 5_000);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn degrades_to_full_in_memory_output_when_the_spill_write_fails() {
        // A file occupies the parent path → creating a nested dir under it fails,
        // exercising the graceful-degradation branch instead of letting the I/O
        // error escape.
        let dir = temp_dir();
        std::fs::write(dir.join("blocker"), "x").unwrap();
        let content = big(5_000);
        let opts = SpillOptions {
            max_chars: 100.0,
            dir: runtime_path(&dir.join("blocker").join("nested")),
            id: "c".into(),
        };
        let r = maybe_spill_result(content.clone(), opts, |_, _, _| {
            panic!("failed spill must not invoke formatter")
        })
        .await;
        // Full content returned unchanged, no path, and no panic.
        assert_eq!(r.content, content);
        assert!(r.spilled_path.is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn persistence_budget_keeps_read_media_and_small_siblings() {
        let dir = temp_dir();
        let mut read = msg(&"r".repeat(250_000));
        read.tool_use_id = "read-result".into();
        let mut media = msg(&"m".repeat(60_000));
        media.tool_use_id = "media-result".into();
        media
            .content
            .push(OutputBlock::Image(crate::schemas::content::ImageBlock {
                mime_type: "image/png".into(),
                base64: "AAAA".into(),
                detail: None,
                source_path: None,
            }));
        assert!(
            plan_spill_result(
                &media.content,
                &SpillOptions {
                    max_chars: DEFAULT_MAX_RESULT_CHARS,
                    dir: runtime_path(&dir),
                    id: media.tool_use_id.clone(),
                }
            )
            .is_none(),
            "per-item spilling must preserve all media blocks"
        );
        let mut largest = msg(&"a".repeat(40_000));
        largest.tool_use_id = "largest".into();
        let tiny = msg("tiny");
        let out = persist_per_message_budget(
            vec![read.clone(), media.clone(), largest, tiny.clone()],
            10_000,
            runtime_path(&dir),
            |result| result.tool_use_id == "read-result",
            |preview, _, path| preview.concat(path.as_runtime_string()),
        )
        .await;
        assert_eq!(out[0], read);
        assert_eq!(out[1], media);
        assert_eq!(out[3], tiny);
        assert!(output_blocks_text(&out[2].content).contains("largest.aggregate.txt"));
        assert_eq!(
            std::fs::read_to_string(dir.join("largest.aggregate.txt")).unwrap(),
            "a".repeat(40_000)
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn persistence_budget_preserves_results_when_artifact_write_fails() {
        let dir = temp_dir();
        std::fs::create_dir_all(&dir).unwrap();
        let blocked = dir.join("blocked");
        std::fs::write(&blocked, "not a directory").unwrap();
        let results = vec![msg(&"x".repeat(40_000))];
        let out = persist_per_message_budget(
            results.clone(),
            10_000,
            runtime_path(&blocked),
            |_| false,
            |preview, _, path| preview.concat(path.as_runtime_string()),
        )
        .await;
        assert_eq!(
            out, results,
            "a failed persist must never discard tool output"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn per_message_budget_passes_through_when_under_budget() {
        let results = vec![msg("small"), msg("also small")];
        let out = apply_per_message_budget(results.clone(), 10_000, |_, _| {
            panic!("under-budget results must not invoke formatter")
        });
        assert_eq!(out, results);
    }

    #[test]
    fn per_message_budget_trims_the_largest_result_first() {
        let big_text = "x".repeat(5_000);
        let results = vec![msg(&big_text), msg("tiny")];
        let out = apply_per_message_budget(results, 1_000, |kept, omitted| {
            kept.concat(
                &format!("\n… [trimmed {omitted} chars to fit the context budget] …").into(),
            )
        });
        let trimmed = output_blocks_text(&out[0].content);
        assert!(trimmed.contains("trimmed 3500 chars to fit the context budget"));
        assert!(trimmed.starts_with(&"x".repeat(1_500)));
        // The small result is left untouched (below the per-result floor).
        assert_eq!(output_blocks_text(&out[1].content), "tiny");
    }

    #[test]
    fn per_message_budget_replaces_only_content() {
        let mut result = msg(&"x".repeat(5_000));
        result.uuid = "stable-uuid".into();
        result.tool_use_id = "search-1".into();
        result.is_error = true;
        result.tool_kind = Some(ProviderToolCallKind::ToolSearch);
        result.native_tool_output =
            Some(runtime_json::parse(&RuntimeString::from(r#"{"native":true}"#)).unwrap());
        result.output =
            Some(runtime_json::parse(&RuntimeString::from(r#"{"visible":"stable"}"#)).unwrap());
        result.extra_user_blocks = Some(vec![ContentBlock::Text(TextBlock {
            text: "attached".into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })]);

        let out = apply_per_message_budget(vec![result.clone()], 1_000, |kept, omitted| {
            kept.concat(
                &format!("\n… [trimmed {omitted} chars to fit the context budget] …").into(),
            )
        });

        assert_ne!(out[0].content, result.content);
        assert_eq!(out[0].uuid, result.uuid);
        assert_eq!(out[0].tool_use_id, result.tool_use_id);
        assert_eq!(out[0].is_error, result.is_error);
        assert_eq!(out[0].tool_kind, result.tool_kind);
        assert_eq!(out[0].native_tool_output, result.native_tool_output);
        assert_eq!(out[0].output, result.output);
        assert_eq!(out[0].extra_user_blocks, result.extra_user_blocks);
    }
}
